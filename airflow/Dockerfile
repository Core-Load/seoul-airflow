FROM python:3.10-slim

# 기본 패키지 + Docker CLI 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    ca-certificates \
    gnupg \
    lsb-release \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
      $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리
WORKDIR /opt/airflow

# requirements.txt 복사
COPY requirements.txt .

# Python 패키지 설치 (Airflow 포함)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Airflow 홈 설정
ENV AIRFLOW_HOME=/opt/airflow

# airflow 사용자 생성 및 필요한 디렉토리 생성
RUN useradd -ms /bin/bash -u 50000 airflow \
    && mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins \
    && chown -R airflow:airflow /opt/airflow