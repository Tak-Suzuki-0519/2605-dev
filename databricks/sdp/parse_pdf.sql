-- ============================================================================
-- Source: /Volumes/dev_catalog/raw/pdf_drop/         (UC Volume of PDFs)
-- Target: dev_catalog.ai_parse_document.parsed       (streaming table)
-- ============================================================================

-- TODO check "CLUSTER BY" or "CLUSTER BY AUTO" later

-- NOTE: 全列を明示宣言する（部分スキーマにしない）。
-- 同一 SDP パイプライン内で下流フロー（chunks）がこのテーブルを読むとき、
-- SDP は「明示スキーマ句で宣言した列」だけを下流の解析時スキーマとして伝播する。
-- document_id だけ宣言すると chunks 側で ai_parse_document / file_path / file_name が
-- 解決できず UNRESOLVED_COLUMN になるため、SELECT が生成する全列をここで宣言する。
CREATE OR REFRESH STREAMING TABLE dev_catalog.ai_parse_document.parsed (
    document_id       BIGINT NOT NULL,
    path              STRING,
    modificationTime  TIMESTAMP,
    length            BIGINT,
    file_path         STRING,
    file_name         STRING,
    file_block_start  BIGINT,
    ai_parse_document VARIANT,
    _ingested_at      TIMESTAMP,
      CONSTRAINT pk_parsed PRIMARY KEY (document_id) RELY
)
COMMENT 'ai_parse_document v2.0 test'
AS
SELECT
  abs(xxhash64(_metadata.file_path, modificationTime)) AS document_id,
  * EXCEPT (content),
  _metadata.file_path,
  _metadata.file_name,
  _metadata.file_block_start,
  ai_parse_document(content, map('version', '2.0')) AS ai_parse_document,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/dev_catalog/raw/pdf_drop/',
  format => 'binaryFile'
);