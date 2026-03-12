# AIS ETL Project

AIS 데이터를 Iceberg(Trino)에 적재하고 Kafka를 통해 실시간 수집하는 ETL 프로젝트입니다.

## 📌 현재 진행 상황

### 1. 데이터 소스 특정
*   **실시간 수집:** [aisstream.io](https://aisstream.io) (WebSocket)
    *   **글로벌 모니터링:** 전 세계 28개 주요 요충지의 실시간 트래픽 수집 가능.
*   **과거 데이터(Backfill):** [IMF PortWatch](https://portwatch.imf.org) (ArcGIS REST API)
    *   28개 주요 요충지(Chokepoint) 및 2,000여 개 항구(Port) 데이터 수집 및 Iceberg 적재 완성.
    *   **UPSERT 지원:** `(portid, event_date)`를 키로 사용하여 데이터 중복 방지 및 변경 시에만 업데이트.
    *   **단일 커밋 최적화:** 대량 적재 시 단일 `MERGE INTO` 쿼리를 사용하여 Nessie 커밋 부하 최소화.

### 2. 구현된 모듈 (`src/`)
*   **`imf_portwatch_manager.py`**: IMF API 인터페이스. 페이지네이션 및 날짜별 최적화 쿼리 지원.
*   **`trino_manager.py`**: Trino(Nessie/Iceberg) 연결 및 쿼리 실행 관리.
*   **`setup_trino_tables.py`**: Trino 테이블(chokepoints, ports, master tables) 초기 생성.
*   **`master_data_etl.py`**: 항구/요충지 마스터 정보 적재. (전체 데이터 UPSERT)
*   **`portwatch_chokepoints_upsert.py`**: 해상 요충지 일별 통계 적재. (기본 최근 10일)
*   **`portwatch_ports_upsert.py`**: 전 세계 항구 일별 통계 적재. (일자별 루프 백필 최적화)
*   **`ais_manager.py`**: `bbox.json`에 정의된 구역의 실시간 AIS 수집 (WebSocket 스트리밍).

### 3. 설정 파일
*   **`bbox.json`**: 28개 주요 해협 및 운하의 감시 구역(Bounding Box) 좌표. `aisstream.io` 규격 준수.
*   **.env**: `AISSTREAM_API_KEY`, `TRINO_URL`, `TRINO_PORT` 등 환경 변수 관리.

### 4. Trino 접속 및 쿼리
- **접속 명령어**:
  ```bash
  trino --server http://trino.hotel.svc.cluster.local:8080 --catalog nessie --user context_builder
  ```
- **테이블 스키마 변경 사항**:
    - `created_at`, `updated_at` 컬럼 존재 (TIMESTAMP WITH TIME ZONE).

---

## ⚙️ 환경 설정

### 1. Conda 가상환경
본 프로젝트는 `ais` 또는 `oag` 가상환경을 사용합니다.
```bash
conda activate ais
```

---

## 🚀 실행 커맨드

### 1. 마스터 데이터 적재
```bash
conda run -n ais python -m src.master_data_etl
```

### 2. 요충지 및 항구 통계 적재 (Daily)
각각 최근 10일치 데이터를 UPSERT 방식으로 적재합니다.
```bash
conda run -n ais python -m src.portwatch_chokepoints_upsert
conda run -n ais python -m src.portwatch_ports_upsert
```

### 3. 실시간 AIS 글로벌 모니터링
`bbox.json`의 28개 구역 데이터를 실시간 스트리밍합니다.
```bash
conda run -n ais python -m src.ais_manager
```

### 4. 과거 데이터 백필 (Backfill)
```bash
conda run -n ais python -m src.portwatch_ports_upsert --start_date 2026-01-01 --end_date 2026-03-12
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
