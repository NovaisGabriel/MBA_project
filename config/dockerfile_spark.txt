FROM gcr.io/spark-operator/spark-py:v3.0.0
LABEL MAINTAINER Carlos Barbosa
LABEL EMAIL carlos.barbosa@a3data.com.br 

USER root:root

RUN mkdir -p /app

COPY ./jars/ /opt/spark/jars

WORKDIR /app

USER root