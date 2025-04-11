{{ config( alias='a_dim_inactive', database='agt_comm', schema='contest', materialized = "view" ) }} 

SELECT
    B.AGENTCODE,
    BA.USERDEFINED2,
    B.BROKERID,
    B.AGENTNAME,
    FIRSTNAME,
    LASTNAME,
    COALESCE(BA.USERDEFINED2 = B.BROKERID, FALSE) AS IS_PRIMARY
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
    B.PENDINGTERMINATION IS NOT NULL
    OR B.TERMINATED IS NOT NULL
    OR B.TRANSFERRINGOUT IS NOT NULL
ORDER BY
    B.AGENTSTATUS
