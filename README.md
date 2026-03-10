# AIS ETL Project

AIS 데이터를 Iceberg(Trino)에 적재하고 Kafka를 통해 실시간 수집하는 ETL 프로젝트입니다.

## 📌 현재 진행 상황

### 1. 데이터 소스 특정
*   **실시간 수집:** [aisstream.io](https://aisstream.io) (WebSocket)
    *   현재 Cloudflare 차단 이슈(#170)로 인해 서버 사이드 접속 불안정 상태.
*   **과거 데이터(Backfill):** [IMF PortWatch](https://portwatch.imf.org) (ArcGIS REST API)
    *   28개 주요 병목 지점(Chokepoint) 및 항구(Port) 데이터 수집 및 Iceberg 적재 로직 완성.

### 2. 구현된 모듈 (`src/`)
*   **`imf_portwatch_manager.py`**: 28개 Chokepoint의 마스터 정보 및 일일 물동량 통계 데이터 추출.
*   **`trino_manager.py`**: Trino(Nessie/Iceberg) 연결 및 쿼리 실행 관리.
*   **`setup_trino_tables.py`**: `portwatch.chokepoints` Iceberg 테이블 생성/초기화 스크립트.
*   **`portwatch_etl.py`**: IMF 데이터를 추출(Extract)하여 Trino 테이블에 적재(Load)하는 전체 ETL 프로세스.
*   **`ais_manager.py`**: 실시간 AIS 수집 로직 (Cloudflare 대응 및 특정 지점 필터링).

### 3. 설정 파일
*   **`chokepoints.bbox`**: 28개 주요 해협 및 운하의 감시 구역(Bounding Box) 정의 완료.
*   **.env**: `AISSTREAM_API_KEY`, `TRINO_URL`, `TRINO_PORT` 등 환경 변수 관리.

---

## ⚙️ 환경 설정

### 1. Conda 가상환경
본 프로젝트는 `ais` 가상환경을 사용합니다.
```bash
conda activate ais
```

### 2. 의존성 설치
```bash
pip install -r requirements.txt
```

---

## 🚀 실행 커맨드

### 1. Trino 테이블 초기화 (최초 1회)
```bash
conda run -n ais python -m src.setup_trino_tables
```

### 2. IMF PortWatch ETL 실행 (과거 데이터 적재)
```bash
conda run -n ais python -m src.portwatch_etl --start_date 2026-03-01 --end_date 2026-03-05
```

### 3. 실시간 AIS 수집 테스트 (서비스 불안정 시 작동 안 함)
```bash
conda run -n ais python -m src.ais_manager
```

---

## ⚠️ 이슈 리포트
*   **AISStream Cloudflare Issue:** 현재 `aisstream.io` 서버가 Cloudflare WAF를 통해 서버 사이드 WebSocket 핸드셰이크를 차단하고 있음 ([GitHub Issue #170](https://github.com/aisstream/issues/issues/170)).
*   **현재 대응:** IMF PortWatch 데이터를 통한 백필 로직에 집중하여 Iceberg 파이프라인 구축 완료.
