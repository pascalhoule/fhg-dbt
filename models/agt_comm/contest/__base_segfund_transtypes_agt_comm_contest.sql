{{  config(alias='__base_segfund_transtypes', database='agt_comm', schema='contest', materialized = "view")  }} 

SELECT
    *
FROM
   {{ ref('transactiontypes_fh_agt_comm_investment') }}
WHERE
    TRANSACTIONTYPECODE in ('314', '315', '324', '378')