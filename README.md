# AIS ETL Project

AIS 데이터를 Iceberg(Trino)에 적재하고 Kafka를 통해 실시간 수집하는 ETL 프로젝트입니다.

## 📌 현재 진행 상황

### 1. 데이터 소스 특정
*   **실시간 수집:** [aisstream.io](https://aisstream.io) (WebSocket)
    *   현재 Cloudflare 차단 이슈(#170)로 인해 서버 사이드 접속 불안정 상태.
*   **과거 데이터(Backfill):** [IMF PortWatch](https://portwatch.imf.org) (ArcGIS REST API)
    *   28개 주요 병목 지점(Chokepoint) 및 항구(Port) 데이터 수집 및 Iceberg 적재 로직 완성.
    *   **UPSERT 지원:** `(portid, portname, event_date)`를 키로 사용하여 데이터 중복 방지 및 변경 시에만 업데이트.
    *   **단일 커밋 최적화:** 모든 통계 데이터는 대량 적재 시 단일 `MERGE INTO` 쿼리를 사용하여 Nessie 커밋 부하를 최소화함.

### 2. 구현된 모듈 (`src/`)
*   **`imf_portwatch_manager.py`**: IMF PortWatch API와의 인터페이스. 페이지네이션 및 날짜별 최적화 쿼리 지원.
*   **`trino_manager.py`**: Trino(Nessie/Iceberg) 연결 및 쿼리 실행 관리.
*   **`setup_trino_tables.py`**: Trino 테이블(chokepoints, ports, master tables) 초기 생성 스크립트.
*   **`master_data_etl.py`**: 항구 및 요충지 마스터 정보를 수집하여 `chokepoints_master`, `ports_master` 테이블에 적재. (전체 데이터 UPSERT)
*   **`portwatch_chokepoints_upsert.py`**: 주요 해상 요충지(Chokepoint) 일별 통계 데이터를 적재.
*   **`portwatch_ports_upsert.py`**: 전 세계 주요 항구(Port) 일별 통계 데이터를 적재. (일자별 루프 백필 최적화)
*   **`ais_manager.py`**: 실시간 AIS 수집 로직 (Cloudflare 대응 및 특정 지점 필터링).

### 3. 설정 파일
*   **`chokepoints.bbox`**: 28개 주요 해협 및 운하의 감시 구역(Bounding Box) 정의 완료.
*   **.env**: `AISSTREAM_API_KEY`, `TRINO_URL`, `TRINO_PORT` 등 환경 변수 관리.

### 4. Trino 접속 및 쿼리
- **접속 명령어**:
  ```bash
  trino --server http://trino.hotel.svc.cluster.local:8080 --catalog nessie --user context_builder
  ```
- **테이블 스키마 변경 사항**:
    - 모든 통계 테이블에 `created_at`, `updated_at` 컬럼 존재 (TIMESTAMP WITH TIME ZONE).

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

### 1. 마스터 데이터 적재 (항구/요충지 정보)
전 세계 항구 및 요충지의 위치 정보를 한 번에 적재합니다.
```bash
conda run -n ais python -m src.master_data_etl
```
*   `--dry-run`: 실제 적재 없이 로그만 확인 가능.

### 2. 요충지 통계 적재 (Chokepoints)
기본적으로 **오늘(UTC)을 포함한 최근 10일치** 데이터를 적재합니다.
```bash
conda run -n ais python -m src.portwatch_chokepoints_upsert
```

### 3. 항구 통계 적재 (Ports)
기본적으로 **오늘(UTC)을 포함한 최근 10일치** 데이터를 일자별로 루프를 돌며 안정적으로 적재합니다.
```bash
conda run -n ais python -m src.portwatch_ports_upsert
```

### 4. 과거 데이터 백필 (Backfill)
특정 기간의 데이터를 일자별로 적재하고 싶을 때 사용합니다.
```bash
conda run -n ais python -m src.portwatch_ports_upsert --start_date 2026-01-01 --end_date 2026-03-12
```

---

## ⚠️ 이슈 리포트
*   **AISStream Cloudflare Issue:** 현재 `aisstream.io` 서버가 Cloudflare WAF를 통해 서버 사이드 WebSocket 핸드셰이크를 차단하고 있음.
*   **IMF 데이터 지연:** 항구별 통계 데이터는 IMF 측 업데이트 상황에 따라 수일의 지연이 발생할 수 있으나, 매일 실행 시 자동으로 최신화됩니다.
