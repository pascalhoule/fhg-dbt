{{ config( alias='a_dim', database='agt_comm', schema='contest', materialized = "view" ) }} 

SELECT
    B.AGENTCODE,
    BA.USERDEFINED2,
    B.BROKERID,
    B.FULLAGENTNAME,
    H.NODENAME AS BRANCHNAME,
    B.MGACODE,
    M.NAME AS MGANAME,
    FIRSTNAME,
    LASTNAME,
    SUBSTR(H.MODIFIED_NODE_ID, 2, LEN(H.MODIFIED_NODE_ID)) AS BRANCHID
FROM
    {{ ref('broker_vc_agt_comm_insurance') }} AS B
LEFT JOIN
    {{ ref('brokeradvanced_vc_agt_comm_insurance') }} AS BA
    ON B.AGENTCODE = BA.AGENTCODE
LEFT JOIN {{ ref('hierarchy_agt_comm_insurance') }} AS H
    ON
        B.PARENTNODEID = H.MODIFIED_NODE_ID
        AND B.AGENTCODE = H.AGENTCODE
LEFT JOIN {{ ref('mga_agt_comm_insurance') }} AS M ON B.MGACODE = M.MGACODE
WHERE
    B.AGENTSTATUS = 'Active'
ORDER BY
    B.AGENTSTATUS
