-- ============================================================================
-- Source: /Volumes/dev_catalog/raw/pdf_drop/         (UC Volume of PDFs)
-- Target: dev_catalog.ai_parse_document.parsed       (streaming table)
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE dev_catalog.ai_parse_document.parsed
COMMENT 'ai_parse_document v2.0 test'
AS
SELECT
  * EXCEPT (content),
  _metadata.file_path,
  _metadata.file_name,
  _metadata.file_size,
  _metadata.file_block_start,
  ai_parse_document(content, map('version', '2.0')) AS ai_parse_document,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '/Volumes/dev_catalog/raw/pdf_drop/',
  format => 'binaryFile'
);
