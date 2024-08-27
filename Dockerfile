FROM python:3.11.3-slim AS build

ARG AWS_REGION=NULL
ENV AWS_REGION=${AWS_REGION}

ARG AWS_ACCESS_KEY_ID=NULL
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}

ARG AWS_SECRET_ACCESS_KEY=NULL
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

ARG AWS_AIRFLOW_NAME=NULL
ENV AWS_AIRFLOW_NAME=${AWS_AIRFLOW_NAME}

ARG AIRFLOW_URL=NULL
ENV AIRFLOW_URL=${AIRFLOW_URL}

ARG AIRFLOW_USERNAME=NULL
ENV AIRFLOW_USERNAME=${AIRFLOW_USERNAME}

ARG AIRFLOW_PASSWORD=NULL
ENV AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD}

COPY . /monitor/
WORKDIR /monitor
ENV PYTHONPATH /monitor

ENV TZ=America/Fortaleza

RUN apt update && \
    apt install -y gcc g++ python3-dev curl apt-utils apt-transport-https gnupg2 openssl nano && \
    apt upgrade -y && \
    pip install --disable-pip-version-check -r requirements.txt && \
    apt autoremove -y && apt clean && rm -rf ~/.cache/pip/ && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

FROM build

ENV PYTHONUNBUFFERED=True
