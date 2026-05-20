-- ============================================================================
-- Source: /Volumes/dev_catalog/raw/pdf_drop/         (UC Volume of PDFs)
-- Target: dev_catalog.ai_parse_document.parsed       (streaming table)
-- ============================================================================

-- TODO check "CLUSTER BY" or "CLUSTER BY AUTO" later

CREATE OR REFRESH STREAMING TABLE dev_catalog.ai_parse_document.parsed (
    document_id BIGINT NOT NULL,
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