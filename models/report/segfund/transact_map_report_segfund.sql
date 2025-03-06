{{			
    config (			
        materialized="view",			
        alias='transact_map', 			
        database='report', 			
        schema='segfund',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

SELECT
    *,
    CASE
        WHEN TRANSACTIONTYPECODE in (
            '372', '373', '353', '395', '352', '354', '317', '311', '318', '369', '415', '418', '416', '417'
        ) THEN 'REINVESTMENT_DIVIDEND_ADJUSTMENT'
        WHEN TRANSACTIONTYPECODE in (
            '310', '364', '321', '308', '365', '420', '424', '314', '315', '370', '316', '371', '429', '378'
        ) THEN 'DEPOSIT'
        WHEN TRANSACTIONTYPECODE in ('307') THEN 'TRANSFER_OUT'
        WHEN TRANSACTIONTYPECODE in (
            '338', '387', '367', '384', '2020', '324', '325', '326', '368', '426', '381', '379', '322', '359', '390'
        ) THEN 'REDEMPTION'
        WHEN TRANSACTIONTYPECODE in ('302') THEN 'TRANSFER_IN'
        ELSE 'OTHER'
    END AS FINANCE_CATEGORY
FROM
    {{ ref('transactiontypes_fh_report_investment') }} 