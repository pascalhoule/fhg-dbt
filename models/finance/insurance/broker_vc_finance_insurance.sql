{{  config(alias='broker_vc', database='finance', schema='insurance', materialization = "view")  }} 

SELECT
    B.PARENTNODEID,
    B.AGENTCODE,
    B.FIRSTNAME,
    B.MIDDLENAME,
    B.LASTNAME,
    B.FULLAGENTNAME,
    B.AGENTNAME,
    B.COMPANYNAME,
    B.MGACODE,
    B.AGACODE,
    B.DATEOFBIRTH,
    B.PROVINCE,
    B.COMPANYPROVINCE,
    B.AGENTSTATUS,
    B.AGENTTYPE,
    B.LANGUAGEPREFERENCE,
    B.SERVICELEVEL,
    B.CREATEDDATE,
    B.SIN,
    B.BROKERID,
    B.LASTMODIFIEDDATE,
    CASE
        WHEN (
            B.SIN IS NOT NULL
            AND TRIM(B.SIN) NOT IN (
                '000 000 000',
                '0',
                '111 111 111',
                '999 999 999',
                '111 222 333',
                '123 456 789'
            )
        ) THEN SHA2(
            'b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2' || ':' || REGEXP_REPLACE(b.sin, '[^0-9]', '') || ':' || REVERSE('b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2'),
            256
        )
        ELSE NULL
    END AS HASHEDID,
    CASE
        WHEN (
            b.SIN IS NOT NULL
            AND TRIM(B.SIN) not in (
                '000 000 000',
                '0',
                '111 111 111',
                '999 999 999',
                '111 222 333',
                '123 456 789'
            )
        ) THEN UPPER(
            SHA2(
                'b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2' || ':' || REGEXP_REPLACE(b.sin, '[^0-9]', '') || ':' || REVERSE('b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2'),
                256
            )
        )
        ELSE null
    END AS UPPERCASE_HASHEDID
FROM {{ ref ('broker_vc_clean_insurance')  }}