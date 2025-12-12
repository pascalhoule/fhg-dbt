{{  config(alias='representatives_vc', 
    database='finance', 
    schema='investment', 
    materialized = "view")  }} 

SELECT
    R.REPRESENTIATIVECODE,
    R.INSAGENTCODE,
    R.REPID,
    R.FIRST_NAME,
    R.LAST_NAME,
    R.DOB,
    R.BRANCH_CODE,
    R.SIN,
    R.CODE,
    R.REPSTATUS,
    CASE
        WHEN (
            R.SIN IS NOT NULL
            AND TRIM(R.SIN) NOT IN (
                '000000000',
                '0',
                '111111111',
                '999999999',
                '111222333',
                '123456789'
            )
        ) THEN SHA2(
            'b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2' || ':' || REGEXP_REPLACE(R.SIN, '[^0-9]', '') || ':' || REVERSE('b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2'),
            256
        )
        ELSE NULL
    END AS HASHEDID,
    CASE
        WHEN (
            R.SIN IS NOT NULL
            AND TRIM(R.SIN) NOT IN (
                '000000000',
                '0',
                '111111111',
                '999999999',
                '111222333',
                '123456789'
            )
        ) THEN UPPER(
            SHA2(
                'b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2' || ':' || REGEXP_REPLACE(R.SIN, '[^0-9]', '') || ':' || REVERSE('b^.SG9h*W\\]7q3FEtP=7rB(fVt&JnA}2'),
                256
            )
        )
        ELSE NULL
    END AS UPPERCASE_HASHEDID  
FROM {{ ref ('representatives_vc_clean_investment')  }} R