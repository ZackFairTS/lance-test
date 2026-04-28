FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-11-jdk-headless \
        curl ca-certificates python3 python3-pip gosu procps \
    && rm -rf /var/lib/apt/lists/*

ENV FLINK_HOME=/opt/flink
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$FLINK_HOME/bin:$PATH

COPY flink-1.16.3 /opt/flink
COPY lance-flink/target/flink-connector-lance-1.0.0-SNAPSHOT.jar /opt/flink/lib/

RUN pip3 install --break-system-packages --no-cache-dir pylance==4.0.1 pyarrow==21.0.0 boto3

RUN useradd -m -u 9999 flink && \
    mkdir -p /opt/flink/log /tmp/flink-checkpoints && \
    chown -R flink:flink /opt/flink /tmp/flink-checkpoints

WORKDIR /opt/flink

COPY scripts/docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
