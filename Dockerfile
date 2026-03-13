FROM python:3.11-slim-bookworm

WORKDIR /app

# 1. 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. 소스 코드 복사
COPY . .

# src 디렉토리를 PYTHONPATH에 추가하여 python -m src.xxx 형태로 실행 가능하게 함
ENV PYTHONPATH="/app:/app/src:${PYTHONPATH}"

# Airflow KubernetesPodOperator에서 유연하게 실행할 수 있도록 진입점 설정
ENTRYPOINT ["python", "-m"]
