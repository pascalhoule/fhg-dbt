{{  config(alias='__base_segfund_transtypes', database='agt_comm', schema='contest', materialized = "view")  }} 

SELECT
    *
FROM
   {{ ref('transactiontypes_fh_agt_comm_investment') }}
WHERE
    TRANSACTIONTYPECODE in ('308', '310', '314', '315', '316', '322', '324', '326', '365', '368', '370', '371', '378', '2030')