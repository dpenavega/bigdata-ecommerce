# NodeManager from class + python3, needed by Hadoop Streaming (section C).
# The bde2020 images are Debian 9 (stretch), which is end-of-life: its packages
# moved to archive.debian.org, so apt has to point there.
FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list \
 && apt-get -o Acquire::Check-Valid-Until=false update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 \
 && rm -rf /var/lib/apt/lists/* \
 && python3 --version
