{{			
    config (			
        materialized="view",			
        alias='fundproducts_fh', 			
        database='normalize', 			
        schema='investment'  		
    )			
}}

SELECT
    VC.FUNDPRODUCTCODE,
    VC.SPONSORID,
    VC.FUNDID,
    VC.NAME,
    VC.LOADTYPE,
    VC.SUBTYPENAME,
    VC.SERVICEFEERATE,
    FP.CLASS AS CLASS_CODE,
    C.DESCRIPTION,
    AC.DESCRIPTION_FRENCH AS DESCRIPTIONFRENCH
FROM
    NORMALIZE.PROD_INVESTMENT.FUNDPRODUCTS_VC VC
    JOIN NORMALIZE.PROD_INVESTMENT.FUND_PRODUCTS FP ON VC.FUNDPRODUCTCODE = FP.CODE
    JOIN NORMALIZE.PROD_INVESTMENT.PRODUCT_CLASS C ON FP.CLASS = C.CODE
    JOIN {{ source('norm', 'asset_classes_fh') }} AC ON AC.DESCRIPTION = C.DESCRIPTION