FROM flink:2.2.1-scala_2.12-java17

USER root

RUN apt-get update -y && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-dev \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir --break-system-packages \
    apache-flink==2.2.1

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar

USER flink