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
    TRANSACTIONTYPECODE,
    TRANSACTIONTYPENAME,
    TRANSACTIONTYPENAMEFR,
    DBSIGN,
    REDEMPTION_FLAG,
    CASE
        WHEN TRANSACTIONTYPECODE IN (
            '372',
            '373',
            '353',
            '395',
            '352',
            '354',
            '317',
            '311',
            '318',
            '369',
            '415',
            '418',
            '416',
            '417'
        ) THEN 'REINVESTMENT_DIVIDEND_ADJUSTMENT'
        WHEN TRANSACTIONTYPECODE IN (
            '310',
            '364',
            '321',
            '308',
            '365',
            '420',
            '424',
            '314',
            '315',
            '370',
            '316',
            '371',
            '429',
            '378'
        ) THEN 'DEPOSIT'
        WHEN TRANSACTIONTYPECODE IN ('307') THEN 'TRANSFER_OUT'
        WHEN TRANSACTIONTYPECODE IN (
            --'338', '387', '367', '384', '2020', '324', '325', '326', '368', '426', '381', '379', '322', '359', '390'
            '324',
            '325',
            '326',
            '332',
            '381',
            '390',
            '2020',
            '2025',
            '2026',
            '2030',
            '2031',
            '368',
            '322',
            '379',
            '387',
            '426',
            '384'
        ) THEN 'REDEMPTION'
        WHEN TRANSACTIONTYPECODE IN ('302') THEN 'TRANSFER_IN'
        ELSE 'OTHER'
    END AS FINANCE_CATEGORY,
    CASE
        WHEN TRANSACTIONTYPECODE IN (
            '384', '2020', '325', '379'
        ) THEN 'INCOME'
        WHEN TRANSACTIONTYPECODE IN (
            '324', '326', '368', '426', '381', '322'
        ) THEN 'ADHOC'
        ELSE ''
    END AS SUB_FINANCE_CATEGORY
FROM
    {{ ref('transactiontypes_fh_report_investment') }}
