FROM apache/airflow:2.9.1

USER root
RUN apt update && \
    apt install -y locales && \
	sed -i -e 's/# es_ES.UTF-8 UTF-8/es_ES.UTF-8 UTF-8/' /etc/locale.gen && \
	dpkg-reconfigure --frontend=noninteractive locales

ENV LANG es_ES.UTF-8
ENV LC_ALL es_ES.UTF-8

USER airflow
RUN pip install --upgrade pip setuptools wheel

COPY model_package .
RUN pip install -e .