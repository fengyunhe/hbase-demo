FROM  azul/zulu-openjdk-debian:11-jre-latest
MAINTAINER yangyan <277160299@qq.com>

ENV HADOOP_COMMON_LIB_NATIVE_DIR=/opt/hbase/lib/
ENV HADOOP_OPTS="-Djava.library.path=/opt/hbase/lib/"

COPY asserts/hadoop-native.tar.gz /tmp
RUN mkdir -p /opt/hbase/lib/
RUN set -x \
    && tar -xvf /tmp/hadoop-native.tar.gz -C /opt/hbase/lib/

COPY target/hbase-demo-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/hadoop/applications/hbase-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
CMD "java -jar /opt/hadoop/applications/hbase-demo-1.0-SNAPSHOT-jar-with-dependencies.jar"