# ============================================================================
# Layer:   SILVER
# Source:  dev_catalog.ai_parse_document.parsed   (bronze streaming table)
# Target:  dev_catalog.ai_parse_document.chunks   (materialized view)
# Purpose: Explode parsed-PDF elements into clean, per-element text chunks.
#          Drops parse-error documents and tiny/noise fragments.
#          RAG-ready: one row per retrievable chunk -> feeds Vector Search.
# ============================================================================

# TODO check cluster_by=[...] or cluster_by_auto=True later

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dev_catalog.ai_parse_document.chunks",
    comment='Silver: per-element text chunks from "dev_catalog.ai_parse_document.parsed"',
    # CDF required for Vector Search Delta Sync downstream
    table_properties={"delta.enableChangeDataFeed": "true"},
    # Python's schema= is the FULL schema (no partial-schema merge like SQL DDL).
    # Declare every column to match the query exactly, then attach the PK constraint.
    schema="""
        chunk_id      BIGINT    NOT NULL,
        document_id   BIGINT    NOT NULL,
        source_path   STRING,
        file_name     STRING,
        element_idx   INT       NOT NULL,
        page_no       INT,
        element_type  STRING,
        content       STRING,
        _ingested_at  TIMESTAMP NOT NULL,
        CONSTRAINT pk_chunks PRIMARY KEY (chunk_id) RELY
    """,
)
def chunks():
    elements = F.variant_get("ai_parse_document", "$.document.elements", "array<variant>")
    error_status = F.variant_get("ai_parse_document", "$.error_status", "array<variant>")
    content = F.variant_get("element", "$.content", "string")

    return (
        # batch read (not readStream) -> this @dp.table is a materialized view
        spark.read.table("dev_catalog.ai_parse_document.parsed")
        # drop documents that reported any parse error
        .where(error_status.isNull() | (F.size(error_status) == 0))
        # one row per element; null/empty element arrays are dropped (like LATERAL VIEW posexplode)
        .select(
            "document_id",
            "file_path",
            "file_name",
            F.posexplode(elements).alias("element_idx", "element"),
        )
        # drop empty / noise fragments (page numbers, stray headers, etc.)
        .where(content.isNotNull() & (F.length(F.trim(content)) > 10))
        .select(
            # deterministic & collision-free per (document, element position)
            F.abs(F.xxhash64("document_id", "element_idx")).alias("chunk_id"),
            "document_id",
            F.col("file_path").alias("source_path"),
            "file_name",
            "element_idx",
            F.coalesce(
                F.variant_get("element", "$.page_id", "string"),
                F.variant_get("element", "$.bbox[0].page_id", "string"),
            ).try_cast("int").alias("page_no"),
            F.variant_get("element", "$.type", "string").alias("element_type"),
            content.alias("content"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
