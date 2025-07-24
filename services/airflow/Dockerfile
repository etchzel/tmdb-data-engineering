# install airflow dependencies. NOTE: should use ligther image, need to find constraints
FROM apache/airflow:slim-2.10.5-python3.12 AS python-builder

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# bookworm for build java and trino cli
FROM debian:bookworm-slim AS java-builder

RUN apt-get update -qq && \
    apt-get install -y wget \
    curl \
    apt-transport-https \
    gpg \
    unzip

# add Adoptium Temurin repo to install java
RUN wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null && \
    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
RUN apt-get update -qq && \
    apt-get install -y temurin-17-jdk

# install trino cli
RUN wget -qO /usr/local/bin/trino https://repo1.maven.org/maven2/io/trino/trino-cli/476/trino-cli-476-executable.jar && \
    chmod +x /usr/local/bin/trino

# install jq for parsing json
RUN wget -qO /usr/bin/jq https://github.com/jqlang/jq/releases/download/jq-1.8.1/jq-linux-amd64 && \
    chmod +x /usr/bin/jq

# cleanup
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# final image airflow
FROM apache/airflow:slim-2.10.5-python3.12

# copy build stages
COPY --from=python-builder /home/airflow/.local/lib/python3.12/site-packages /home/airflow/.local/lib/python3.12/site-packages
COPY --from=java-builder /usr/lib/jvm /usr/lib/jvm
COPY --from=java-builder /usr/local/bin/trino /usr/local/bin/trino
COPY --from=java-builder /usr/bin/jq /usr/bin/jq
COPY --from=java-builder /usr/bin/unzip /usr/bin/unzip

USER root

# create symlinks for java
RUN ln -sf /usr/lib/jvm/temurin-17-jdk-amd64/bin/java /etc/alternatives/java && \
    ln -sf /etc/alternatives/java /usr/bin/java

USER airflow
ENV JAVA_HOME=/usr/bin/java
ENV AIRFLOW_HOME=/opt/airflow

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME