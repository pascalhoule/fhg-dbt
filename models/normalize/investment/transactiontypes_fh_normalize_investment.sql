 {{			
    config (			
        materialized="table",			
        alias='transactiontypes_fh', 			
        database='normalize', 			
        schema='insurance',
        tags=["transactiontypes_fh"]			
    )			
}}

SELECT
    T_VC.TRANSACTIONTYPECODE,
    T_VC.TRANSACTIONTYPENAME,
    T_VC.TRANSACTIONTYPENAMEFR,
    T.DBSIGN,
    T.REDEMPTION_FLAG
FROM
    {{ ref('transactiontypes_vc_clean_investment') }} T_VC
    JOIN {{ ref('transactiontypes_clean_investment') }} T ON T.EXT_TYPE_CODE = T_VC.TRANSACTIONTYPECODE