-- ============================================================
-- Ingestion Framework — Sample INSERT scripts
-- Replace placeholder values:
--   {ado_org}        Azure DevOps organisation name
--   {ado_project}    Azure DevOps project name
--   {gh_owner}       GitHub organisation or user
--   {snow_instance}  ServiceNow instance subdomain (e.g. mycompany)
-- Auth secrets must be stored in Databricks secret scope "ingestion-secrets"
-- per the key convention in _lib/auth.py
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- ingestion_source_config  (4 rows — one per base URL)
-- ADO REST and ADO Analytics live on different hosts, so they
-- need separate source rows even though both use ADO_OAUTH.
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_source_config
    (source_id, source_name,              base_url,                                                 auth_type,   is_active)
VALUES
    (1,  'Azure DevOps REST',             'https://dev.azure.com/{ado_org}/{ado_project}',           'ADO_OAUTH', TRUE),
    (2,  'Azure DevOps Analytics',        'https://analytics.dev.azure.com/{ado_org}/{ado_project}', 'ADO_OAUTH', TRUE),
    (3,  'GitHub',                        'https://api.github.com',                                  'GITHUB_APP', TRUE),
    (4,  'ServiceNow',                    'https://{snow_instance}.service-now.com',                  'SNOW_OAUTH', TRUE);


-- ────────────────────────────────────────────────────────────
-- ADO REST  (source_id = 1)  — 5 entities via REST paginators
-- Secret prefix: "azure-devops-rest"
--   azure-devops-rest-tenant-id
--   azure-devops-rest-client-id
--   azure-devops-rest-client-secret
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                   pagination_type,    pagination_config,
     landing_zone_path,                                  watermark_column_name,  watermark_column_value, active_flag)
VALUES
-- 1. Epics — WIQL two-step with explicit field list (faster than $expand=all)
(   'ADO_EPICS',             1,         '/_apis/wit/workitems',          'WIQL',
    '{"wiql_where":"[System.WorkItemType] = ''Epic''","fields":["System.Id","System.Title","System.WorkItemType","System.State","System.AreaPath","System.IterationPath","System.AssignedTo","System.ChangedDate","Microsoft.VSTS.Common.Priority"]}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/epics',          'System.ChangedDate', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 2. Pull Requests — Descriptor continuation_header
(   'ADO_PULL_REQUESTS',     1,         '/_apis/git/pullrequests',       'Descriptor',
    '{"page_size_param":"$top","offset_mode":"none","records_path":"value","next_signal":"continuation_header","filter_mode":"odata_quoted","extra_params":{"searchCriteria.status":"all","api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/pull_requests',  'searchCriteria.minTime', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 3. Builds — Descriptor continuation_header
(   'ADO_BUILDS',            1,         '/_apis/build/builds',           'Descriptor',
    '{"page_size_param":"$top","offset_mode":"none","records_path":"value","next_signal":"continuation_header","filter_mode":"querystring","extra_params":{"statusFilter":"all","queryOrder":"startTimeAscending","api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/builds',         'minTime', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 4. Releases — Descriptor continuation_header
(   'ADO_RELEASES',          1,         '/_apis/release/releases',       'Descriptor',
    '{"page_size_param":"$top","offset_mode":"none","records_path":"value","next_signal":"continuation_header","filter_mode":"querystring","extra_params":{"$expand":"approvals,artifacts","queryOrder":"ascending","api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/releases',       'minCreatedTime', TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 5. Git Repositories — full load (no watermark filter), Descriptor empty_batch + offset
(   'ADO_REPOSITORIES',      1,         '/_apis/git/repositories',       'Descriptor',
    '{"page_size_param":"$top","offset_param":"$skip","offset_mode":"offset","records_path":"value","next_signal":"empty_batch","filter_mode":"querystring","extra_params":{"api-version":"7.1"}}',
    'abfss://landing@datalake.dfs.core.windows.net/ado/repositories',   'lastUpdateTime', TIMESTAMP '2000-01-01T00:00:00Z', TRUE);


-- ── Additional WIQL examples (add to source_id=1 as needed) ─────────────
-- Features:
--   pagination_config = '{"wiql_where":"[System.WorkItemType] = ''Feature''","fields":["System.Id","System.Title","System.State","System.AreaPath","System.ChangedDate","System.Parent"]}'
--
-- Bugs and User Stories (multi-type in one entity):
--   pagination_config = '{"wiql_where":"[System.WorkItemType] IN (''Bug'',''User Story'') AND [System.TeamProject] = ''@project''","fields":["System.Id","System.Title","System.WorkItemType","System.State","System.AssignedTo","System.ChangedDate"]}'
--
-- All types in an area path:
--   pagination_config = '{"wiql_where":"[System.AreaPath] UNDER ''MyProject\\MyTeam''","fields":["System.Id","System.Title","System.WorkItemType","System.State","System.ChangedDate"]}'

-- ────────────────────────────────────────────────────────────
-- ADO Analytics  (source_id = 2)  — 5 entities via OData
-- Secret prefix: "azure-devops-analytics"
-- Same Azure AD app registration can be reused — add a second
-- set of secrets under the "azure-devops-analytics" prefix or
-- share them by naming source_name identically and using one
-- source row (then source_id 1 and 2 collapse to one row).
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                        pagination_type, pagination_config,
     landing_zone_path,                                        watermark_column_name, watermark_column_value, active_flag)
VALUES
-- 6. Work Items — OData
(   'ADO_WORKITEMS_ODATA',   2,         '/_odata/v4.0/WorkItems',            'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/workitems_odata',   'ChangedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 7. Work Item Revisions — OData (full history)
-- NOTE: watermark must be ChangedDate (when the revision was created), NOT RevisedDate.
-- RevisedDate = 9999-12-31 for every current revision — watermarking on it re-fetches
-- all current-state records on every run regardless of the watermark value.
(   'ADO_WI_REVISIONS',      2,         '/_odata/v4.0/WorkItemRevisions',    'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/wi_revisions',      'ChangedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 8. Pipeline Runs — OData
(   'ADO_PIPELINE_RUNS',     2,         '/_odata/v4.0/PipelineRuns',         'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/pipeline_runs',     'CreatedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 9. Test Results Daily — OData (aggregated by day)
-- NOTE: watermark must be Date (a proper datetime), NOT DateSK.
-- DateSK is an integer surrogate key (20200101, 20200102 …); filtering
-- DateSK ge '2020-01-01T00:00:00Z' produces a type-mismatch 400 from Analytics.
(   'ADO_TEST_RESULTS',      2,         '/_odata/v4.0/TestResultsDaily',     'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/test_results_daily','Date',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 10. Test Runs — OData
(   'ADO_TEST_RUNS',         2,         '/_odata/v4.0/TestRuns',             'OData',         NULL,
    'abfss://landing@datalake.dfs.core.windows.net/ado/test_runs',         'CreatedDate',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE);


-- ────────────────────────────────────────────────────────────
-- GitHub  (source_id = 3)  — 10 entities via LinkHeader
-- Secret prefix: "github"
--   github-app-id
--   github-installation-id
--   github-private-key   (full PEM, newlines preserved in secret)
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                                        pagination_type, pagination_config,
     landing_zone_path,                                              watermark_column_name, watermark_column_value, active_flag)
VALUES
-- 11. Issues (all repos in org — repeat per repo or use GitHub GraphQL for cross-repo)
(   'GH_ISSUES',             3,         '/repos/{gh_owner}/{repo}/issues',   'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/issues',          'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 12. Pull Requests
(   'GH_PULL_REQUESTS',      3,         '/repos/{gh_owner}/{repo}/pulls',    'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/pull_requests',   'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 13. Commits
(   'GH_COMMITS',            3,         '/repos/{gh_owner}/{repo}/commits',  'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/commits',         'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 14. Workflow Runs (GitHub Actions)
(   'GH_WORKFLOW_RUNS',      3,         '/repos/{gh_owner}/{repo}/actions/runs', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":"workflow_runs","next_signal":"empty_batch","filter_mode":"querystring","extra_params":{"status":"completed"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/workflow_runs',   'created',       TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 15. Releases
(   'GH_RELEASES',           3,         '/repos/{gh_owner}/{repo}/releases', 'LinkHeader',    NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/releases',        'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 16. Code Scanning Alerts
(   'GH_CODE_SCANNING',      3,         '/repos/{gh_owner}/{repo}/code-scanning/alerts', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":null,"next_signal":"link_header","filter_mode":"querystring","extra_params":{"sort":"updated","direction":"asc"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/code_scanning',   'updated_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 17. Secret Scanning Alerts
(   'GH_SECRET_SCANNING',    3,         '/repos/{gh_owner}/{repo}/secret-scanning/alerts', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":null,"next_signal":"link_header","filter_mode":"querystring","extra_params":{"sort":"updated","direction":"asc"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/secret_scanning', 'created_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 18. Dependabot Alerts
(   'GH_DEPENDABOT',         3,         '/repos/{gh_owner}/{repo}/dependabot/alerts', 'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":null,"next_signal":"link_header","filter_mode":"querystring","extra_params":{"sort":"updated","direction":"asc"}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/dependabot',      'updated_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 19. Issue Comments (search API — nested under "items")
(   'GH_ISSUE_COMMENTS',     3,         '/repos/{gh_owner}/{repo}/issues/comments', 'LinkHeader', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/github/issue_comments',  'since',         TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 20. Check Runs (via workflow run sub-resource)
(   'GH_CHECK_RUNS',         3,         '/repos/{gh_owner}/{repo}/check-runs',     'Descriptor',
    '{"page_size_param":"per_page","offset_param":"page","offset_mode":"page","records_path":"check_runs","next_signal":"empty_batch","filter_mode":"querystring","extra_params":{}}',
    'abfss://landing@datalake.dfs.core.windows.net/github/check_runs',      'started_at',    TIMESTAMP '2020-01-01T00:00:00Z', TRUE);


-- ────────────────────────────────────────────────────────────
-- ServiceNow  (source_id = 4)  — 10 entities
-- Secret prefix: "servicenow"
--   servicenow-client-id
--   servicenow-client-secret
-- ────────────────────────────────────────────────────────────
INSERT INTO ingestion_entity_config
    (entity_id,              source_id, endpoint_url,                        pagination_type,   pagination_config,
     landing_zone_path,                                          watermark_column_name, watermark_column_value, active_flag)
VALUES
-- 21. Incidents
(   'SNOW_INCIDENTS',        4,         '/api/now/table/incident',           'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/incidents',   'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 22. Problems
(   'SNOW_PROBLEMS',         4,         '/api/now/table/problem',            'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/problems',    'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 23. Change Requests
(   'SNOW_CHANGES',          4,         '/api/now/table/change_request',     'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/changes',     'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 24. Change Tasks
(   'SNOW_CHANGE_TASKS',     4,         '/api/now/table/change_task',        'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/change_tasks','sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 25. CMDB Config Items — Descriptor with compound query and field selection
(   'SNOW_CMDB_CI',          4,         '/api/now/table/cmdb_ci',            'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"install_status!=7^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_exclude_reference_link":"true","sysparm_fields":"sys_id,name,sys_class_name,install_status,sys_updated_on"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/cmdb_ci',     'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 26. Users (sys_user)
(   'SNOW_USERS',            4,         '/api/now/table/sys_user',           'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"active=true^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_exclude_reference_link":"true","sysparm_fields":"sys_id,user_name,first_name,last_name,email,department,sys_updated_on"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/users',       'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 27. Service Requests (sc_request)
(   'SNOW_SC_REQUESTS',      4,         '/api/now/table/sc_request',         'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/sc_requests', 'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 28. Request Items (sc_req_item)
(   'SNOW_SC_REQ_ITEMS',     4,         '/api/now/table/sc_req_item',        'OffsetPagination', NULL,
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/sc_req_items','sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 29. Tasks (task — base table; use a subclass like sc_task or sn_si_task if needed)
(   'SNOW_TASKS',            4,         '/api/now/table/task',               'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"active=true^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_exclude_reference_link":"true"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/tasks',       'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE),

-- 30. User Groups (sys_user_group)
(   'SNOW_USER_GROUPS',      4,         '/api/now/table/sys_user_group',     'Descriptor',
    '{"page_size_param":"sysparm_limit","offset_param":"sysparm_offset","offset_mode":"offset","records_path":"result","next_signal":"empty_batch","filter_mode":"sysparm","filter_template":"active=true^{col}>={val}","extra_params":{"sysparm_display_value":"false","sysparm_fields":"sys_id,name,manager,type,sys_updated_on"}}',
    'abfss://landing@datalake.dfs.core.windows.net/servicenow/user_groups', 'sys_updated_on',  TIMESTAMP '2020-01-01T00:00:00Z', TRUE);


-- ────────────────────────────────────────────────────────────
-- Verification queries
-- ────────────────────────────────────────────────────────────
-- SELECT s.source_name, e.entity_id, e.pagination_type, e.watermark_column_name
-- FROM   ingestion_entity_config e
-- JOIN   ingestion_source_config s ON e.source_id = s.source_id
-- WHERE  e.active_flag = TRUE
-- ORDER  BY s.source_id, e.entity_id;
