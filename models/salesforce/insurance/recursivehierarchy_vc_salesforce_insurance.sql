{{  config(
    alias='recursivehierarchy_vc', 
    database='salesforce', 
    schema='insurance',
    grants = {'ownership': ['FH_READER']})   }}

SELECT *
FROM {{ ref('recursive_hierarchy_integrate_insurance') }}