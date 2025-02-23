FROM bitnami/spark:3.2.0

WORKDIR /app

USER root
RUN apt-get update && apt-get install -y \
    wget \
    python3-pip \
    netcat \
    && apt-get clean

ENV SQOOP_VERSION=1.4.7
ENV HADOOP_VERSION=2.7.7
RUN wget -q "http://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz" -O /tmp/sqoop.tar.gz && \
    tar -xzf /tmp/sqoop.tar.gz -C /opt/ && \
    mv /opt/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0 /opt/sqoop && \
    rm /tmp/sqoop.tar.gz

ENV PATH="/opt/sqoop/bin:${PATH}"

RUN mkdir -p /opt/spark-jars && \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.0/spark-sql-kafka-0-10_2.12-3.2.0.jar" -O /opt/spark-jars/spark-sql-kafka-0-10_2.12-3.2.0.jar && \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.2.0/spark-streaming-kafka-0-10_2.12-3.2.0.jar" -O /opt/spark-jars/spark-streaming-kafka-0-10_2.12-3.2.0.jar && \
    wget -q "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar" -O /opt/spark-jars/kafka-clients-2.8.0.jar && \
    wget -q "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" -O /opt/spark-jars/commons-pool2-2.11.1.jar && \
    wget -q "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar" -O /opt/spark-jars/slf4j-api-1.7.30.jar && \
    wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.2.0/spark-token-provider-kafka-0-10_2.12-3.2.0.jar" -O /opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.2.0.jar && \
    wget -q "https://repo1.maven.org/maven2/org/apache/hbase/hbase-spark/1.4.7/hbase-spark-1.4.7.jar" -O /opt/spark-jars/hbase-spark-1.4.7.jar

RUN pip3 install --no-cache-dir happybase pandas mysql-connector-python confluent-kafka py4j==0.10.9.2 geopy  # Đảm bảo pandas và geopy được cài

COPY sqoop/ ./sqoop/
COPY src/ ./src/  
COPY data/ ./data/  

CMD ["sh", "-c", "until nc -z hbase 9090; do echo 'Waiting for HBase Thrift server...'; sleep 2; done; python3 src/init_hbase.py && /opt/bitnami/spark/bin/spark-submit --jars /opt/spark-jars/spark-sql-kafka-0-10_2.12-3.2.0.jar,/opt/spark-jars/spark-streaming-kafka-0-10_2.12-3.2.0.jar,/opt/spark-jars/kafka-clients-2.8.0.jar,/opt/spark-jars/commons-pool2-2.11.1.jar,/opt/spark-jars/slf4j-api-1.7.30.jar,/opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.2.0.jar,/opt/spark-jars/hbase-spark-1.4.7.jar src/task5_7_streaming.py"]