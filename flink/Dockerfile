# Apache Flink 2.3 with Iceberg, as a service: JobManager + TaskManager + SQL Gateway (REST).
#
# PyFlink cannot live in the lab's Python environment (it pins pyarrow<21, pandas<2.3,
# py4j==0.10.9.7 and pulls apache-beam), so the tests talk Flink SQL over the SQL Gateway's
# REST API instead - the way platforms usually run Flink anyway.
FROM flink:2.3.0-java17

ARG ICEBERG_VERSION=1.12.0
ARG HADOOP_VERSION=3.5.0

# Iceberg's Flink runtime + S3FileIO (AWS bundle, incl. the remote-signing client) and the
# Hadoop client classes the Iceberg Flink catalog needs (Configuration etc.).
RUN cd /opt/flink/lib \
    && for url in \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.3/${ICEBERG_VERSION}/iceberg-flink-runtime-2.3-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" ; \
       do wget -q --tries=5 "$url" || exit 1; done

# Lab sizing: the defaults (1.6 GB per process) are too much next to Spark/Trino on 15 GB.
RUN { echo "jobmanager.memory.process.size: 1024m"; \
      echo "taskmanager.memory.process.size: 2560m"; \
      echo "taskmanager.memory.task.off-heap.size: 512m"; \
      echo "taskmanager.numberOfTaskSlots: 4"; \
      echo "parallelism.default: 1"; \
      echo "rest.address: localhost"; \
      echo "rest.bind-address: 0.0.0.0"; } >> /opt/flink/conf/config.yaml

COPY start-lab.sh /opt/flink/bin/start-lab.sh
ENTRYPOINT ["/opt/flink/bin/start-lab.sh"]
