FROM apache/airflow:2.10.4-python3.12

USER root

# Install OpenJDK-17
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
#RUN export JAVA_HOME

COPY requirements.txt .
COPY config/thesis-25-456305-079e7bf41602.json /tmp
COPY config/my_elestio_key /tmp
COPY config/my_elestio_key.pub /tmp

RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt