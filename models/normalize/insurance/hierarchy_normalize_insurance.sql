{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='normalize', 			
        schema='insurance'			
    )			
}}	

WITH H1 AS (
SELECT
    DEPTH, 
    CONCAT('^',PARENTNODEID,'^')::VARCHAR AS PARENTNODEID,  
    CONCAT('^',NODEID,'^')::VARCHAR AS BRANCHID, 
    CASE 
	    WHEN 
            CHARINDEX('Branch Brokers', NodeName) > 0 AND
            (NodeName LIKE ('%Branch Brokers%') OR NodeName LIKE ('%DEPT%'))
            AND	NodeName NOT LIKE ('%DIVISION%')
            AND NodeName NOT LIKE ('%MAP%')
        THEN UPPER(REPLACE(NodeName, 'Branch Brokers', ''))
	    WHEN 
            CHARINDEX('DEPT', NodeName) > 0 AND
            (NodeName LIKE ('%Branch Brokers%') OR NodeName LIKE ('%DEPT%'))
            AND	NodeName NOT LIKE ('%DIVISION%')
            AND NodeName NOT LIKE ('%MAP%')
        THEN UPPER(REPLACE(NodeName, 'DEPT', ''))
        ELSE NODENAME
    END AS BRANCHNAME, 
    MGACODE, 
    HIERARCHYCODE, 
    STATUS 
FROM {{ ref ('hierarchy_vc_clean_insurance') }}
WHERE (NodeName LIKE ('%Branch Brokers%') OR NodeName LIKE ('%DEPT%'))
AND	NodeName NOT LIKE ('%DIVISION%')
AND NodeName NOT LIKE ('%MAP%')
),

HierarchyPathSubs AS
    (SELECT 
        HIERARCHYLEVEL,
        PARENTNODEID,
        NODEID,
        NODENAME,
        CASE 
            WHEN STARTSWITH(HIERARCHYPATH, '^m3^|^a605^|^a1802^') THEN  REPLACE(HIERARCHYPATH,'^m3^|^a605^|^a1802^','^m4^|^a605^|^a1802^') 
            WHEN STARTSWITH(HIERARCHYPATH, '^m3^|^a605^|^a1801^') THEN  REPLACE(HIERARCHYPATH,'^m3^|^a605^|^a1801^','^m4^|^a605^|^a1801^')
            WHEN STARTSWITH(HIERARCHYPATH, '^m3^|^a605^|^a146^')  THEN  REPLACE(HIERARCHYPATH,'^m3^|^a605^|^a146^', '^m4^|^a605^|^a146^')
            ELSE HIERARCHYPATH 
            END AS HIERARCHYPATH,
        HIERARCHYPATHNAME
    FROM {{ ref ('recursive_hierarchy_normalize_insurance') }}
    ),

Hierarchy AS(
 SELECT 
 		hps.ParentNodeID,
 		hps.NodeID,
        hps.NodeName,
 		hps.HierarchyPath,
        hps.HierarchyPathName,
 		hps.HierarchyLevel,
 		(SELECT MIN(b.BRANCHID) FROM H1 b WHERE hps.hierarchypath LIKE CONCAT('%',b.BRANCHID,'%')) AS BRANCHID,
 		LTRIM(RTRIM((SELECT MIN(b.BRANCHNAME) FROM H1 b WHERE hps.hierarchypath LIKE CONCAT('%',b.BRANCHID,'%')))) AS BRANCHNAME
 	FROM HierarchyPathSubs hps )

SELECT 
b.AgentCode,
b.AGENTNAME,
b.AGENTSTATUS,
b.AGENTTYPE,
b.BROKERID,
ba.USERDEFINED2,
h.ParentNodeID,
h.NodeID,
h.NodeName,
h.HierarchyPath, 
h.HierarchyPathName, 
h.BRANCHNAME,
CASE 
WHEN h.HierarchyPathName ilike '%MAP%' THEN 'MAP'
ELSE 'Non-MAP' 
END AS MAP_Segment,
CASE 
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m1^' THEN 'QC'
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m2^' THEN 'West'
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m3^' THEN 'Ontario'
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m4^' THEN 'Atlantic'
END AS MGA,
CASE 
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m1^' THEN 'QC'
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m2^' THEN 'West'
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m3^' THEN 'Ontario'
WHEN SUBSTRING(h.HierarchyPath, 1,4) = '^m4^' THEN 'Atlantic'
END AS REGION
FROM {{ ref ('broker_vc_clean_insurance') }} b 
LEFT JOIN {{ ref ('brokeradvanced_vc_clean_insurance') }} ba on ba.agentcode = b.agentcode
LEFT JOIN HIERARCHY h on CONCAT('^', b.parentnodeid, '^') = h.NodeID
WHERE b.agenttype <> 'Corporate' AND (ba.userdefined2 LIKE '3268%' or ba.userdefined2 like '3162%')
ORDER BY ba.userdefined2