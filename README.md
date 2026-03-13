# AIS ETL Project

AIS 데이터를 Iceberg(Nessie)에 적재하고 Spark Connect를 통해 효율적으로 수집하는 ETL 프로젝트입니다.

## 📌 현재 진행 상황

### 1. 데이터 소스 및 아키텍처
*   **실시간 수집:** [aisstream.io](https://aisstream.io) (WebSocket)
*   **과거 데이터(Backfill):** [IMF PortWatch] (ArcGIS REST API)
*   **적재 엔진:** **Spark Connect** (Remote Session)
    *   K8s에 상시 가동 중인 Spark Connect Server에 접속하여 가벼운 클라이언트로 고속 적재 수행.
    *   **Iceberg MERGE INTO:** 중복 방지 및 UPSERT 지원.
    *   **단일 커밋 최적화:** 백필 시 전 기간 데이터를 수집 후 단일 `MERGE` 쿼리를 실행하여 Nessie 커밋 부하 최소화 및 이력 정렬.

### 2. 구현된 모듈 (`src/`)
*   **`spark_manager.py`**: Spark Connect 전용 세션 관리 및 Nessie 커밋 메타데이터 주입.
*   **`imf_portwatch_manager.py`**: IMF API 인터페이스.
*   **`portwatch_chokepoints_upsert.py`**: 해상 요충지 통계 적재 (Spark Connect 기반).
*   **`portwatch_ports_upsert.py`**: 항구 통계 적재 (Single-Commit 최적화 적용).
*   **`ais_manager.py`**: `bbox.json` 구역의 실시간 AIS 수집 스트리밍.

### 3. Nessie 커밋 추적 (Commit History)
적재 시마다 Nessie 커밋 헤더에 다음 정보가 자동으로 기록됩니다:
- **Author**: `AIS ETL Pipeline`
- **Message**: 적재 대상 테이블, 데이터 범위(Start/End), 전체 행(Row) 개수 포함.
- **예시**: `Upsert 6099 rows into portwatch.ports (Range: 2026-03-01 to 2026-03-10)`

---

## ⚙️ 환경 설정

### 1. Conda 가상환경 및 라이브러리
```bash
conda activate ais
pip install pyspark==3.5.0 grpcio grpcio-status protobuf==3.20.3
```

### 2. 환경 변수 (.env)
- `SPARK_REMOTE_URL`: Spark Connect 서버 주소 (`sc://...`)
- `AISSTREAM_API_KEY`: AISStream API 키

---

## 🚀 실행 커맨드

### 1. 요충지 및 항구 통계 적재 (Daily/Backfill)
Spark Connect를 통해 원격 서버에서 적재가 수행됩니다.
```bash
# 기본 (최근 10일)
python -m src.portwatch_chokepoints_upsert
python -m src.portwatch_ports_upsert

# 과거 데이터 백필
python -m src.portwatch_ports_upsert --start_date 2026-01-01 --end_date 2026-03-12
```

### 2. 연결 테스트
```bash
python -m src.spark_manager
```

---

## 📅 데이터 업데이트 정보

### 1. IMF PortWatch 업데이트 주기
*   **매주 화요일 오전 9시 ET** (한국 시간 화요일 밤 10시~11시 KST) 업데이트.
*   주 단위 대량 업데이트되나 지연이 발생할 수 있어 **매일 1회 실행**을 권장합니다.

---

## ⚠️ 이슈 리포트
*   **AISStream Cloudflare Issue:** 서버 사이드 WebSocket 접속 시 Cloudflare WAF 차단 주의 (현재 `ais_manager.py`에서 대응 중).
*   **데이터 지연:** 항구별 통계는 IMF 측 상황에 따라 수일의 지연이 발생할 수 있습니다.
