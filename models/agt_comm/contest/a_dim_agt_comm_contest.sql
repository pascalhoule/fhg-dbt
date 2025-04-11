{{ config( alias='a_dim', database='agt_comm', schema='contest', materialized = "view" ) }} 

SELECT
    B.AGENTCODE,
    BA.USERDEFINED2,
    B.BROKERID,
    B.AGENTNAME,
    FIRSTNAME,
    LASTNAME,
    B.AGENTSTATUS,
    COALESCE (BA.USERDEFINED2 = B.BROKERID, FALSE)
        AS IS_PRIMARY
FROM
    {{ ref('broker_fh_agt_comm_insurance') }} AS B
LEFT JOIN
    {{ ref('brokeradvanced_vc_agt_comm_insurance') }} AS BA
    ON B.AGENTCODE = BA.AGENTCODE
LEFT JOIN {{ ref('hierarchy_fh_agt_comm_insurance') }} AS H
    ON
        B.PARENTNODEID = H.NODEID
        AND B.AGENTCODE = H.AGENTCODE
LEFT JOIN {{ ref('mga_agt_comm_insurance') }} AS M ON B.MGACODE = M.MGACODE
WHERE
    B.PENDINGTERMINATION IS null
    AND B.TERMINATED IS null
    AND B.TRANSFERRINGOUT IS null
ORDER BY
    B.AGENTSTATUS
