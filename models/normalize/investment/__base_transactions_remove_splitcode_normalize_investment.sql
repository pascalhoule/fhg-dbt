 {{			
    config (					
        alias='__base_transactions_remove_splitcode', 			
        database='normalize', 			
        schema='investment',	
    )			
}}

    SELECT
        CODE,
        FUNDACCOUNT_CODE,
        SOURCE_NUMBER,
        JREP.INDIVIDUALREPRESENTATIVECODE AS REP_CODE,
        null AS CLIENTREPCODE,
        SETLEMENT_DATE,
        TRADE_DATE,
        SHARES_UNITS,
        UNIT_PRICE,
        AMOUNT * (JREP.SHARE / 100) AS AMOUNT,
        JREP.SHARE,
        SETTLEMENTAMOUNT * (JREP.SHARE / 100) AS SETTLEMENTAMOUNT,
        NET_AMOUNT * (JREP.SHARE / 100) AS NET_AMOUNT,
        DEALER_COMMISSION,
        DEPOSIT_DATE,
        CURRENCYNAME,
        PAYMENT_STATUS,
        EXT_TYPE_CODE,
        FUNDPRODUCT_CODE,
        MANUALENTRYFLAG,
        ENTEREDBY,
        APPROVAL_DATE,
        APPROVAL_BY
        
    FROM
        {{ ref('transactions_normalize_investment') }} T
        JOIN {{ ref('jointrepresentatives_vc_normalize_investment') }} JREP ON T.REP_CODE = JREP.JOINTREPRESENTATIVECODE
    UNION
    SELECT
        CODE,
        FUNDACCOUNT_CODE,
        SOURCE_NUMBER,
        REP_CODE,
        null as CLIENTREPCODE,
        SETLEMENT_DATE,
        TRADE_DATE,
        SHARES_UNITS,
        UNIT_PRICE,
        AMOUNT,
        JREP.SHARE,
        SETTLEMENTAMOUNT,
        NET_AMOUNT,
        DEALER_COMMISSION,
        DEPOSIT_DATE,
        CURRENCYNAME,
        PAYMENT_STATUS,
        EXT_TYPE_CODE,
        FUNDPRODUCT_CODE,
        MANUALENTRYFLAG,
        ENTEREDBY,
        APPROVAL_DATE,
        APPROVAL_BY  
    FROM
        {{ ref('transactions_normalize_investment') }} t
        LEFT JOIN {{ ref('jointrepresentatives_vc_normalize_investment') }} JREP ON T.REP_CODE = JREP.JOINTREPRESENTATIVECODE
    WHERE
        JREP.SHARE IS NULL
   


  