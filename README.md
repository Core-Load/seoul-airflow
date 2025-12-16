# seoul-airflow
DE7 최종 팀 프로젝트

## 실행 방법
### 1. 환경 변수 설정 파일 .env 생성 (수동 작업)
- (local) sample.env 파일 참고하여 같은 위치에 .env 파일 생성
- (EC2) /home/ec2-user/.env 파일 생성

### 2. 이미지 빌드
    docker compose build

### 3. 서비스 실행
    docker compose up -d

### 4. Airflow 웹 UI 에서 등록
#### Variables
- s3_bucket_name
- seoul_api_key
#### Connections
- conn_aws
- conn_postgres
