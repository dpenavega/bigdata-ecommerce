"""
perfilado_dataset.py
--------------------
Analiza el CSV de noviembre de REES46 para elegir las claves de union con las
auxiliares. Responde a: que columnas sirven de clave, cuantos nulos hay, si un
product_id tiene siempre la misma categoria/marca/precio, cuantos pares
(usuario, producto) con compra hay (= filas de aux_compras_resenas) y como se
reparte la actividad entre la 1a y la 2a quincena (para separar features y etiqueta).

Uso:
  spark-submit perfilado_dataset.py --nov hdfs://namenode:9000/data/2019-Nov.csv
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from crear_auxiliares import leer_eventos

# Dia que separa "pasado" (features) de "futuro" (etiqueta)
CORTE = "2019-11-16"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nov", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("perfilado").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ev = leer_eventos(spark, args.nov).withColumn(
        "quincena",
        F.when(F.col("event_time") < F.to_timestamp(F.lit(CORTE)), "1a (1-15)")
         .otherwise("2a (16-30)")).cache()

    print("\n=== 1. Filas por quincena y tipo de evento ===")
    ev.groupBy("quincena").pivot("event_type", ["view", "cart", "purchase"]).count() \
      .orderBy("quincena").show()

    print("=== 1b. Compras por dia (para ver picos tipo Black Friday) ===")
    ev.filter(F.col("event_type") == "purchase") \
      .groupBy(F.to_date("event_time").alias("dia")).count().orderBy("dia").show(31)

    print("=== 2. Nulos por columna (%) ===")
    total = ev.count()
    print(f"total de eventos: {total:,}")
    ev.select([F.round(F.sum(F.col(c).isNull().cast("int")) / total * 100, 2).alias(c)
               for c in ev.columns if c != "quincena"]).show()

    print("=== 3. Cardinalidad (valores distintos) ===")
    ev.select([F.countDistinct(c).alias(c) for c in
               ["product_id", "category_id", "category_code", "brand", "user_id", "user_session"]]).show()

    print("=== 4. Es product_id una buena clave? (1 producto -> 1 categoria / 1 marca?) ===")
    por_prod = ev.groupBy("product_id").agg(
        F.countDistinct("category_id").alias("n_cat"),
        F.countDistinct("brand").alias("n_marcas"),
        F.countDistinct("price").alias("n_precios"))
    por_prod.select(
        F.sum((F.col("n_cat") > 1).cast("int")).alias("prods_con_>1_categoria"),
        F.sum((F.col("n_marcas") > 1).cast("int")).alias("prods_con_>1_marca"),
        F.sum((F.col("n_precios") > 1).cast("int")).alias("prods_con_precio_variable"),
    ).show()

    print("=== 5. Productos: solo 1a quincena / solo 2a / ambas ===")
    pq = ev.groupBy("product_id").agg(F.collect_set("quincena").alias("q"))
    pq.groupBy(F.array_join(F.array_sort("q"), " + ").alias("quincenas")).count().show(truncate=False)

    print("=== 6. Pares (user_id, product_id) con compra -> filas de aux_compras_resenas ===")
    pares = (ev.filter(F.col("event_type") == "purchase")
             .groupBy("user_id", "product_id").agg(F.count("*").alias("n_compras")))
    print(f"pares con compra: {pares.count():,}")
    pares.groupBy(F.least("n_compras", F.lit(5)).alias("n_compras (5=5+)")).count() \
         .orderBy("n_compras (5=5+)").show()

    print("=== 7. Usuarios activos en la 1a quincena: cuantos compran en la 2a? ===")
    u1 = ev.filter(F.col("quincena") == "1a (1-15)").select("user_id").distinct()
    c2 = (ev.filter((F.col("quincena") == "2a (16-30)") & (F.col("event_type") == "purchase"))
          .select("user_id").distinct().withColumn("compra_2a", F.lit(1)))
    et = u1.join(c2, "user_id", "left").fillna(0, ["compra_2a"])
    et.groupBy("compra_2a").count().orderBy("compra_2a").show()
    print("(esto es el balance de clases de la etiqueta 'compra en la 2a quincena')")

    print("=== 8. Embudo view -> cart -> purchase (por par usuario-producto) ===")
    emb = ev.groupBy("user_id", "product_id").agg(
        F.max((F.col("event_type") == "view").cast("int")).alias("v"),
        F.max((F.col("event_type") == "cart").cast("int")).alias("c"),
        F.max((F.col("event_type") == "purchase").cast("int")).alias("p"))
    emb.select(F.sum("v").alias("pares_vistos"),
               F.sum(F.col("v") * F.col("c")).alias("vistos_y_carrito"),
               F.sum(F.col("v") * F.col("p")).alias("vistos_y_comprados")).show()

    spark.stop()


if __name__ == "__main__":
    main()
