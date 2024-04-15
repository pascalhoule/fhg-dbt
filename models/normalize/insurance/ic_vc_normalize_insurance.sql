{{  config(alias='ic_vc', database='normalize', schema='insurance')  }} 

SELECT
    ICCODE,
    ICID,
    SPLIT_PART(ICNAME, ' / ', 1) AS ICNAME_ENG,
    CASE
        WHEN SPLIT_PART(ICNAME, ' / ', 2) = '' THEN SPLIT_PART(ICNAME, ' / ', 1)
        ELSE SPLIT_PART(ICNAME, ' / ', 2)
    END AS ICNAME_FR
FROM {{ ref ('ic_vc_clean_insurance') }}