[현재 데이터베이스 스키마 정보]
* DB 엔진: SQLite (Turso / libSQL 환경)
* 테이블 목록 (서로 독립적인 4개의 테이블):
  - apple_1p (1인용 리더보드)
  - apple_2p (2인용 리더보드)
  - apple_3p (3인용 리더보드)
  - apple_4p (4인용 리더보드)

* 각 테이블의 컬럼 구조 (4개 테이블 모두 동일):
  1. id (INTEGER) : Primary Key, Auto Increment
  2. player_names (TEXT) : 플레이어 이름 (Not Null)
  3. score (INTEGER) : 획득 점수 (Not Null)
  4. created_at (DATETIME) : 기록 생성일 (Default: CURRENT_TIMESTAMP)

  [DB연결정보]
  * .env에 기록