

SELECT
    *,
    SPLIT_PART(HIERARCHYPATHNAME, '>>', 3) AS BRANCH_NAME,
    TRIM(SPLIT_PART(HIERARCHYPATHNAME, '>>', 4)) AS FIRM_NAME
FROM
    {{ ref('recursive_hierarchy_normalize_insurance') }}