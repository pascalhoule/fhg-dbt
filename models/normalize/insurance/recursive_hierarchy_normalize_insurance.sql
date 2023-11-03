{{			
    config (			
        materialized="table",			
        alias='recursive_hierarchy', 			
        database='normalize', 			
        schema='insurance'			
    )			
}}		

WITH 
RECURSIVE HierarchyPath (HierarchyLevel, ParentNodeID, NodeID, NodeName, HierarchyPath, HierarchyPathName) AS (
    SELECT
        1 ,
        CONCAT('^',PARENTNODEID,'^') AS PARENTNODEID,
        CONCAT('^',NODEID,'^') AS NODEID,
        NODENAME,
        CONCAT('^',NODEID,'^'),
        CONCAT('',NodeName,'')
    FROM {{ ref ('hierarchy_vc_clean_insurance') }}  H1
    WHERE PARENTNODEID IS NULL

    UNION ALL

    SELECT
        HierarchyLevel + 1,
        CONCAT('^',H1.PARENTNODEID,'^'),
        CONCAT('^',H1.NODEID,'^'),
        H1.NODENAME,
        CONCAT(HP.HIERARCHYPATH,'|',CONCAT('^',H1.NODEID,'^')),
        CONCAT(HP.HierarchyPathName,' >> ', CONCAT('',H1.NodeName,''))
    FROM HIERARCHYPATH HP JOIN {{ ref ('hierarchy_vc_clean_insurance') }} H1 ON CONCAT('^',H1.PARENTNODEID,'^') = HP.NODEID::VARCHAR  
)

SELECT * FROM HIERARCHYPATH
