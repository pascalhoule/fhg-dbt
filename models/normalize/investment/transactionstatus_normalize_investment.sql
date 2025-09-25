 {{ config(
    alias='transactionstatus', 
    database='normalize', 
    schema='investment') }} 

SELECT
    MAPPEDTRANSACTIONSTATUS,
    COMMIT_TIMESTAMP,
    TRANSACTIONFLAG,
    TRANSACT_ID,
    STAGE_ID,
    CDC_OPERATION
FROM {{ ref('transactionstatus_clean_investment') }}
