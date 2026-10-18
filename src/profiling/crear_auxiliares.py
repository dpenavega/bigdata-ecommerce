"""
crear_auxiliares.py
-------------------
Genera las dos tablas auxiliares (sinteticas) a partir del CSV real de
REES46 (2019-Nov.csv):

  aux_compras_resenas   -> 1 fila por (user_id, product_id) con >= 1 compra real
  aux_productos         -> 1 fila por product_id real (aparezca en cualquier evento)

Reproducible: toda la aleatoriedad sale de un hash de los ids (xxhash64), asi
que si lo ejecutais dos veces (o lo ejecuta otro companero) sale lo mismo,
da igual cuantas particiones tenga Spark.

Uso (dentro del contenedor de Spark):
  spark-submit crear_auxiliares.py \
      --nov hdfs://namenode:9000/data/2019-Nov.csv \
      --out hdfs://namenode:9000/data/auxiliares
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Ultimo dia del "historico" sintetico: el dia antes de que empiece noviembre.
# Todo lo historico de aux_productos es ANTERIOR a esta fecha -> sin fuga de datos.
FIN_HISTORICO = "2019-10-31"
ANIOS_HISTORICO = [2015, 2016, 2017, 2018, 2019]
FECHA_MIN_LANZAMIENTO = "2015-01-01"

ESQUEMA = T.StructType([
    T.StructField("event_time", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("product_id", T.LongType()),
    T.StructField("category_id", T.LongType()),
    T.StructField("category_code", T.StringType()),
    T.StructField("brand", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("user_session", T.StringType()),
])


# ---------------------------------------------------------------- utilidades
def u(sal, *cols):
    """Numero 'aleatorio' uniforme en [0,1) pero determinista para cada id."""
    return F.pmod(F.xxhash64(F.lit(sal), *cols), F.lit(1_000_000)) / F.lit(1_000_000.0)


def normal(sal, *cols):
    """Aprox. normal(0,1): suma de 3 uniformes (centrada y escalada)."""
    s = u(sal + "_a", *cols) + u(sal + "_b", *cols) + u(sal + "_c", *cols)
    return (s - F.lit(1.5)) / F.lit(0.5)


def clip(col, lo, hi):
    return F.least(F.greatest(col, F.lit(lo)), F.lit(hi))


def leer_eventos(spark, ruta_nov):
    df = (spark.read.csv(ruta_nov, header=True, schema=ESQUEMA)
          .withColumn("event_time",
                      F.to_timestamp(F.regexp_replace("event_time", " UTC", ""),
                                     "yyyy-MM-dd HH:mm:ss")))
    return df


# ------------------------------------------------- base comun por producto
def base_productos(eventos):
    """Datos REALES por producto + su 'calidad' latente (sintetica)."""
    reales = (eventos.groupBy("product_id").agg(
        F.min("event_time").alias("primera_aparicion"),
        F.first("category_id", ignorenulls=True).alias("category_id"),
        F.first("category_code", ignorenulls=True).alias("category_code"),
        F.first("brand", ignorenulls=True).alias("brand"),
        F.round(F.avg("price"), 2).alias("precio_medio"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("vistas_nov"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("ventas_nov"),
    ))

    p = F.col("product_id")
    return (reales
            # calidad media del producto: entre ~2.8 y ~4.7 estrellas,
            # sesgada hacia valores altos (como en las tiendas reales)
            .withColumn("rating_medio_latente",
                        F.lit(2.8) + F.lit(1.9) * F.pow(u("calidad", p), F.lit(0.6)))
            # % de compradores que dejan resena (8% - 25%)
            .withColumn("tasa_resena", F.lit(0.08) + F.lit(0.17) * u("tasa_res", p))
            # fecha de lanzamiento: entre 1 mes y ~5 anos antes de verse en noviembre
            .withColumn("dias_antes", (F.lit(30) + u("lanz", p) * F.lit(1800)).cast("int"))
            .withColumn("fecha_lanzamiento",
                        F.greatest(F.date_sub(F.to_date("primera_aparicion"), F.col("dias_antes")),
                                   F.to_date(F.lit(FECHA_MIN_LANZAMIENTO))))
            .drop("dias_antes"))


def prob_recomienda(rating):
    """Probabilidad de thumbs up segun las estrellas."""
    return (F.when(rating == 1, 0.05).when(rating == 2, 0.15).when(rating == 3, 0.50)
            .when(rating == 4, 0.85).otherwise(0.97))


# --------------------------------------------------------------- AUXILIAR 1
def crear_aux_compras_resenas(eventos, prods):
    compras = eventos.filter(F.col("event_type") == "purchase")

    pares = compras.groupBy("user_id", "product_id").agg(
        F.count("*").alias("n_compras"),
        F.min("event_time").alias("primera_compra"),
        F.max("event_time").alias("ultima_compra"),
        F.round(F.sum("price"), 2).alias("gasto_total"),
        F.countDistinct("user_session").alias("n_sesiones_compra"),
    )

    uid, pid = F.col("user_id"), F.col("product_id")
    df = pares.join(prods.select("product_id", "rating_medio_latente", "tasa_resena"),
                    "product_id", "left")

    # quien recompra tiene algo mas de probabilidad de dejar resena
    p_resena = clip(F.col("tasa_resena") + F.lit(0.05) * (F.least(F.col("n_compras"), F.lit(5)) - 1),
                    0.0, 0.6)
    df = df.withColumn("ha_dejado_resena", (u("resena", uid, pid) < p_resena).cast("int"))

    # estrellas = calidad del producto + sesgo del usuario (generoso/critico) + ruido
    sesgo_usuario = F.lit(0.5) * normal("sesgo_user", uid)
    ruido = F.lit(0.7) * normal("ruido", uid, pid)
    rating = F.round(clip(F.col("rating_medio_latente") + sesgo_usuario + ruido, 1.0, 5.0)).cast("int")
    df = df.withColumn("valoracion",
                       F.when(F.col("ha_dejado_resena") == 1, rating))

    df = df.withColumn(
        "recomienda",
        F.when(F.col("ha_dejado_resena") == 1,
               (u("thumb", uid, pid) < prob_recomienda(F.col("valoracion"))).cast("int")))

    # la resena se escribe entre 1 y 21 dias despues de la PRIMERA compra
    # (sirve para no usar resenas "del futuro" al entrenar)
    segundos = ((F.lit(1) + u("dias_res", uid, pid) * F.lit(20)) * F.lit(86400)).cast("long")
    df = df.withColumn(
        "fecha_resena",
        F.when(F.col("ha_dejado_resena") == 1,
               (F.col("primera_compra").cast("long") + segundos).cast("timestamp")))

    # devolucion (variable extra): mas probable si la valoracion es mala
    p_dev = (F.when(F.col("valoracion") <= 2, 0.30)
             .when(F.col("valoracion") == 3, 0.10)
             .otherwise(0.03))
    df = df.withColumn("devuelto", (u("dev", uid, pid) < p_dev).cast("int"))

    return df.select("user_id", "product_id", "n_compras", "primera_compra", "ultima_compra",
                     "gasto_total", "n_sesiones_compra", "ha_dejado_resena", "valoracion",
                     "recomienda", "fecha_resena", "devuelto")


# --------------------------------------------------------------- AUXILIAR 2
def crear_aux_productos(prods, aux1):
    pid = F.col("product_id")
    fin_hist = F.to_date(F.lit(FIN_HISTORICO))
    lanz = F.col("fecha_lanzamiento")

    # ventas al mes en el historico: proporcional a las ventas reales de nov
    # (los productos que se venden mucho en nov tambien vendian antes)
    df = prods.withColumn(
        "ventas_mes",
        (F.col("ventas_nov") + F.lit(0.3))
        * (F.lit(0.5) + u("ritmo", pid)))

    col_hist = []
    num_media, den_media = F.lit(0.0), F.lit(0.0)
    for anio in ANIOS_HISTORICO:
        ini = F.greatest(lanz, F.to_date(F.lit(f"{anio}-01-01")))
        fin = F.least(fin_hist, F.to_date(F.lit(f"{anio}-12-31")))
        meses = F.greatest(F.months_between(fin, ini), F.lit(0.0))

        ventas = F.round(F.col("ventas_mes") * meses).cast("int")
        resenas = F.round(ventas * F.col("tasa_resena")).cast("int")
        # cada ano la media se mueve un poco (versiones, competencia...)
        media = F.round(clip(F.col("rating_medio_latente")
                             + F.lit(0.25) * normal(f"deriva_{anio}", pid), 1.0, 5.0), 2)

        df = (df.withColumn(f"ventas_{anio}", ventas)
                .withColumn(f"resenas_{anio}", resenas)
                .withColumn(f"valoracion_media_{anio}",
                            F.when(F.col(f"resenas_{anio}") > 0, media)))
        col_hist.append(anio)
        num_media = num_media + F.coalesce(F.col(f"valoracion_media_{anio}") * F.col(f"resenas_{anio}"),
                                           F.lit(0.0))
        den_media = den_media + F.col(f"resenas_{anio}")

    df = (df.withColumn("ventas_hist", sum(F.col(f"ventas_{a}") for a in col_hist))
            .withColumn("resenas_hist", sum(F.col(f"resenas_{a}") for a in col_hist))
            .withColumn("valoracion_media_hist",
                        F.when(den_media > 0, F.round(num_media / den_media, 2))))

    # thumbs up historicos: % coherente con la valoracion media
    pct_up = clip((F.coalesce(F.col("valoracion_media_hist"), F.col("rating_medio_latente")) - 1) / 4
                  + F.lit(0.05) * normal("pct_up", pid), 0.0, 1.0)
    df = (df.withColumn("thumbs_up_hist", F.round(F.col("resenas_hist") * pct_up).cast("int"))
            .withColumn("thumbs_down_hist", F.col("resenas_hist") - F.col("thumbs_up_hist")))

    # ---- lo REAL de noviembre, sacado de la auxiliar 1 (coherente con ella)
    real = aux1.groupBy("product_id").agg(
        F.countDistinct("user_id").alias("compradores_nov"),
        F.sum("ha_dejado_resena").alias("resenas_nov"),
        F.sum(F.when(F.col("recomienda") == 1, 1).otherwise(0)).alias("thumbs_up_nov"),
        F.sum(F.when(F.col("recomienda") == 0, 1).otherwise(0)).alias("thumbs_down_nov"),
        F.sum("devuelto").alias("devoluciones_nov"),
    )
    df = df.join(real, "product_id", "left").fillna(
        0, ["compradores_nov", "resenas_nov", "thumbs_up_nov",
            "thumbs_down_nov", "devoluciones_nov"])

    df = (df.withColumn("ventas_total", F.col("ventas_hist") + F.col("ventas_nov"))
            .withColumn("resenas_total", F.col("resenas_hist") + F.col("resenas_nov"))
            .withColumn("thumbs_up_total", F.col("thumbs_up_hist") + F.col("thumbs_up_nov"))
            .withColumn("thumbs_down_total", F.col("thumbs_down_hist") + F.col("thumbs_down_nov"))
            .withColumn("pct_recomendacion_hist",
                        F.when(F.col("resenas_hist") > 0,
                               F.round(F.col("thumbs_up_hist") / F.col("resenas_hist"), 3)))
            .withColumn("antiguedad_dias", F.datediff(fin_hist, lanz)))

    columnas = (["product_id", "category_id", "category_code", "brand", "precio_medio",
                 "fecha_lanzamiento", "antiguedad_dias"]
                + [f"valoracion_media_{a}" for a in ANIOS_HISTORICO]
                + [f"resenas_{a}" for a in ANIOS_HISTORICO]
                + ["valoracion_media_hist", "resenas_hist", "ventas_hist",
                   "thumbs_up_hist", "thumbs_down_hist", "pct_recomendacion_hist",
                   "vistas_nov", "ventas_nov", "compradores_nov",
                   "resenas_nov", "thumbs_up_nov", "thumbs_down_nov",
                   "devoluciones_nov",
                   "ventas_total", "resenas_total", "thumbs_up_total", "thumbs_down_total"])
    return df.select(*columnas)


# --------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nov", required=True, help="ruta a 2019-Nov.csv")
    ap.add_argument("--out", required=True, help="carpeta de salida (HDFS o local)")
    ap.add_argument("--csv", action="store_true", help="ademas de parquet, guardar en CSV")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("crear_auxiliares").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    eventos = leer_eventos(spark, args.nov)
    prods = base_productos(eventos).cache()

    aux1 = crear_aux_compras_resenas(eventos, prods).cache()
    aux2 = crear_aux_productos(prods, aux1)

    aux1.write.mode("overwrite").parquet(f"{args.out}/aux_compras_resenas")
    aux2.write.mode("overwrite").parquet(f"{args.out}/aux_productos")
    if args.csv:
        aux1.write.mode("overwrite").option("header", True).csv(f"{args.out}/aux_compras_resenas_csv")
        aux2.write.mode("overwrite").option("header", True).csv(f"{args.out}/aux_productos_csv")

    a1 = spark.read.parquet(f"{args.out}/aux_compras_resenas")
    a2 = spark.read.parquet(f"{args.out}/aux_productos")
    print(f"aux_compras_resenas: {a1.count():,} filas")
    print(f"aux_productos:       {a2.count():,} filas")
    a1.show(5, truncate=False)
    a2.select("product_id", "fecha_lanzamiento", "valoracion_media_hist", "resenas_hist",
              "ventas_hist", "ventas_nov", "ventas_total").show(5)

    spark.stop()


if __name__ == "__main__":
    main()
