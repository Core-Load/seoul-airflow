FROM python:3.10-slim

# 작업 디렉토리
WORKDIR /usr/app

# requirements.txt 복사
COPY requirements.txt .

# 패키지 설치
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["dbt"]