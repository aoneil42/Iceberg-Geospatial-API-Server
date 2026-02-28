-- =============================================================
-- DuckDB MCP Server Init SQL
-- Loads spatial + iceberg extensions and attaches the lakehouse
-- catalog via LakeKeeper + Garage (S3-compatible storage).
-- =============================================================

-- Install and load extensions
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
INSTALL spatial;
LOAD spatial;

-- Configure S3 credentials for Garage
-- Env vars are templated by entrypoint.sh at container startup
CREATE SECRET garage_s3 (
    TYPE S3,
    KEY_ID '${GARAGE_KEY_ID}',
    SECRET '${GARAGE_SECRET_KEY}',
    REGION 'garage',
    ENDPOINT 'garage:3900',
    URL_STYLE 'path',
    USE_SSL false
);

-- Iceberg REST catalog secret
-- LakeKeeper runs with AUTHZ_BACKEND=allowall in dev, so any token works.
CREATE SECRET lakekeeper_secret (
    TYPE ICEBERG,
    TOKEN 'dummy'
);

-- Attach the LakeKeeper catalog
-- All Iceberg namespaces/tables become queryable as:
--   SELECT * FROM lakehouse.<namespace>.<table>
ATTACH 'lakehouse' AS lakehouse (
    TYPE ICEBERG,
    ENDPOINT 'http://lakekeeper:8181/catalog',
    SECRET lakekeeper_secret
);
