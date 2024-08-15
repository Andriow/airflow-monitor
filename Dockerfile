FROM python:3.11.4-slim AS build

COPY . /monitor/
WORKDIR /monitor

ENV AWS_REGION="YOUR-REGION" \
    AWS_ACCESS_KEY_ID="YOUR-KEY-ID" \
    AWS_SECRET_ACCESS_KEY="YOUR-SECRET-ACCESS-KEY" \
    AWS_AIRFLOW_NAME="YOUR AIRFLOW NAME ON MWAA" \
    AIRFLOW_URL="http://YOUR_AIRFLOW_URL" \
    AIRFLOW_USERNAME="YOUR AIRFLOW USER" \
    AIRFLOW_PASSWORD="YOUR AIRFLOW PASSWORD"

ENV TZ=America/Fortaleza

RUN apt update && \
    apt install -y gcc g++ python3-dev curl apt-utils apt-transport-https gnupg2 openssl && \
    apt upgrade -y && \
    pip install --disable-pip-version-check -r requirements.txt && \
    apt autoremove -y && apt clean && rm -rf ~/.cache/pip/ && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

FROM build

ENV PYTHONUNBUFFERED=True
