%sql
-- 1. Set the active catalog
USE CATALOG api;

-- 2. Set the active schema (database)
USE SCHEMA ing;


-- 1. Source Configuration Table
CREATE TABLE IF NOT EXISTS ingestion_source_config (
    source_id INT NOT NULL,
    source_name STRING,
    source_type STRING,
    base_url STRING,
    auth_type STRING,
    is_active BOOLEAN
) USING DELTA;

-- 2. Entity Configuration Table
CREATE TABLE IF NOT EXISTS ingestion_entity_config (
    entity_id STRING NOT NULL,
    source_id INT,
    entity_name STRING,
    endpoint_url STRING,
    http_method STRING,
    pagination_type STRING,
    landing_zone_path STRING,
    watermark_column_name STRING,
    watermark_column_value TIMESTAMP,
    active_flag BOOLEAN
) USING DELTA;

-- 3. Audit Log Table
CREATE TABLE IF NOT EXISTS ingestion_audit_log (
    run_id STRING NOT NULL,
    source_id INT,
    entity_id STRING,
    run_start_time TIMESTAMP,
    run_end_time TIMESTAMP,
    status STRING,
    records_read BIGINT,
    records_loaded BIGINT,
    records_failed BIGINT,
    execution_duration_sec DOUBLE,
    notebook_name STRING,
    remarks STRING,
    file_path STRING
) USING DELTA;

-- Insert Seed Data into Source Config
INSERT INTO ingestion_source_config VALUES
    (1, 'Azure DevOps Boards', 'REST API', 'https://dev.azure.com/contoso', 'PAT', TRUE),
    (3, 'GitHub', 'REST API', 'https://api.github.com', 'OAuth2', TRUE),
    (5, 'ServiceNow', 'REST API', 'https://contoso.service-now.com/api', 'OAuth2', TRUE);

-- Insert Seed Data into Entity Config
INSERT INTO ingestion_entity_config VALUES
    ('ADO_BOARDS_WORKITEMS', 1, 'Work Items', '/_apis/wit/workitems', 'GET', 'ContinuationToken', 'landing/ado/boards/workitems/', 'ChangedDate', TIMESTAMP'2026-09-01 00:00:00', TRUE),
    ('GITHUB_REPOSITORIES', 3, 'Repositories', '/orgs/contoso/repos', 'GET', 'PageNumber', 'landing/github/repositories/', 'updated_at', TIMESTAMP'2026-09-01 00:00:00', TRUE),
    ('SNOW_INCIDENTS', 5, 'Incidents', '/table/incident', 'GET', 'OffsetPagination', 'landing/servicenow/incidents/', 'sys_updated_on', TIMESTAMP'2026-09-01 00:00:00', TRUE);
