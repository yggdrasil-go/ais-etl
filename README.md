# AIS ETL Project

AIS 데이터를 Iceberg(Trino)에 적재하고 Kafka를 통해 실시간 수집하는 ETL 프로젝트입니다.

## 📌 현재 진행 상황

### 1. 데이터 소스 특정
*   **실시간 수집:** [aisstream.io](https://aisstream.io) (WebSocket)
    *   현재 Cloudflare 차단 이슈(#170)로 인해 서버 사이드 접속 불안정 상태.
*   **과거 데이터(Backfill):** [IMF PortWatch](https://portwatch.imf.org) (ArcGIS REST API)
    *   28개 주요 병목 지점(Chokepoint) 및 항구(Port) 데이터 수집 가능 확인.

### 2. 구현된 모듈 (`src/`)
*   **`imf_portwatch_manager.py`**: 
    *   28개 Chokepoint의 마스터 정보(좌표, ID 등) 및 일일 물동량 통계 데이터 추출 로직 완성.
*   **`ais_manager.py`**: 
    *   `chokepoints.bbox` 기반의 실시간 AIS 수집 로직 구현 (특정 지점 필터링 기능 포함).
    *   Cloudflare 우회를 위한 헤더 최적화 및 타임아웃 강화 로직 적용.

### 3. 설정 파일
*   **`chokepoints.bbox`**: 28개 주요 해협 및 운하의 감시 구역(Bounding Box) 정의 완료.
*   **`.env`**: `AISSTREAM_API_KEY` 등 환경 변수 관리.

---

## ⚙️ 환경 설정

### 1. Conda 가상환경
본 프로젝트는 `ais` 가상환경을 사용합니다.
```bash
# 가상환경 활성화 (필요 시)
conda activate ais
```

### 2. 의존성 설치
```bash
pip install -r requirements.txt
```

---

## 🚀 실행 커맨드

### 1. IMF PortWatch 데이터 확인 (통계 및 좌표)
```bash
conda run -n ais python -m src.imf_portwatch_manager
```

### 2. 실시간 AIS 수집 테스트 (현재 서비스 불안정)
```bash
conda run -n ais python -m src.ais_manager
```

---

## ⚠️ 이슈 리포트
*   **AISStream Cloudflare Issue:** 현재 `aisstream.io` 서버가 Cloudflare WAF를 통해 서버 사이드 WebSocket 핸드셰이크를 차단하고 있음 ([GitHub Issue #170](https://github.com/aisstream/issues/issues/170)).
*   **대응:** 이슈 해결을 모니터링 중이며, 대안 소스(BarentsWatch, AISHub 등) 검토 중.
