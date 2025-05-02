{{  config(alias='__base_segfund_transtypes', database='agt_comm', schema='contest', materialized = "view")  }} 

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
    END AS FINANCE_CATEGORY,
    CASE
        WHEN TRANSACTIONTYPECODE in (
            '384', '2020', '325', '379'
        ) THEN 'INCOME'
        WHEN TRANSACTIONTYPECODE in (
            '324', '326', '368', '426', '381', '322'
        ) THEN 'ADHOC'
        ELSE ''
    END AS SUB_FINANCE_CATEGORY
FROM
   {{ ref('transactiontypes_fh_agt_comm_investment') }}  
WHERE
    --TRANSACTIONTYPECODE in ('308', '310', '314', '315', '316', '322', '324', '326', '365', '368', '370', '371', '378', '2030')
    TRANSACTIONTYPECODE in ('308', '310', '314', '315', '316', '321', '322', '324', '325', '326', '338', '359', '364', '365', '367', '368','370', '371',
'378', '379', '381', '384', '387', '390', '420', '424', '426', '429', '2020', '2030', '302', '307'
)