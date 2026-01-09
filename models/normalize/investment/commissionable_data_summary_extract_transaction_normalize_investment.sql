{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract_transaction', 			
        database='normalize', 			
        schema='investment'  		
    )			
}}

with tvc_status as (
    select
        transactioncode,
        lower(paymentstatus) as paymentstatus
    from (
        select
            transactioncode,
            paymentstatus,
            deposit_date,
            settlementdate,
            tradedate,
            row_number() over (
                partition by transactioncode
                order by
                    deposit_date   desc nulls last,
                    settlementdate desc nulls last,
                    tradedate      desc nulls last
            ) as rn
        from {{ ref('transactions_vc_normalize_investment') }} 
    )
    where rn = 1
),

base as (
    -- 1 row per transaction (TRANSACTION CODE)
    select
        T.CODE AS TRANSACTION_CODE,
        T.REP_CODE as BASE_REP_CODE,
        T.AMOUNT,
        coalesce(T.TICKET_CHARGE, 0)     as TICKET_CHARGE,
        coalesce(T.DEALER_COMMISSION, 0) as DEALER_COMMISSION,
        coalesce(T.HO_COMMISSION, 0)     as HO_COMMISSION,
        T.COMM_RUN_CODE,
        C.DATE                           as CHEQUE_DATE,
        min(C.DATE) over (partition by T.COMM_RUN_CODE) as RUN_START_DATE,
        max(C.DATE) over (partition by T.COMM_RUN_CODE) as RUN_END_DATE,
        tvc.paymentstatus,
        T_TYPE.DBSIGN,

        -- Signed investment amount at transaction level (replicated to all roles later)
        (T.AMOUNT * T_TYPE.DBSIGN)::NUMBER(18, 2) as INVESTMENT_AMOUNT_BASE,

        -- Commission shares and rep codes from PROCESSEDCOMMISSIONS_VC
        PC.TRX_REP_CODE,
        PC.OVR1_REP_CODE,
        PC.OVR2_REP_CODE,
        PC.BM_REP_CODE,
        PC.TRP_REP_SHARE,
        PC.OVR1_REP_SHARE,
        PC.OVR2_REP_SHARE,
        PC.BM_REP_SHARE,

        -- Transaction rep (constant within CODE)
        REP_TRX.REPID                           as TRANSACTION_REPID,
        trim(concat_ws(' ', REP_TRX.FIRST_NAME, REP_TRX.LAST_NAME)) as TRANSACTION_REP_NAME,
        BCH_TRX.NAME                            as TXN_BRANCH,
        SUBREG_TRX.NAME                         as TXN_SUBREGION,

        -- Client
        cast(CLI.CLIENTID as varchar)           as CLIENT_ID,
        coalesce(
            nullif(trim(CLI.ADJUSTED_STATE_CODE), ''),
            nullif(trim(CLI.STATE_CODE), ''),
            nullif(trim(RG.STATE_CODE), '')
        )                                       as CLIENT_STATE,
        coalesce(
            nullif(
                trim(
                    concat_ws(
                        ' ',
                        nullif(CLI.LAST_NAME, ''),
                        nullif(CLI.FIRST_NAME, '')
                    )
                ),
                ''
            ),
            nullif(trim(RG.LEGAL_NAME), '')
        )                                       as CLIENT_NAME,

        -- Account
        coalesce(
            FA.ACCOUNT_NUMBER,
            FA.FUNDACCOUNT_NUMBER,
            T.FUNDACCOUNT_NUMBER
        )                                       as ACCOUNT_NO,

        -- Product / sponsor
        SPONSOR.NAME                            as SPONSOR,
        SPONSOR.SPONSORID                       as SPONSOR_ID,
        FPROD.SUBTYPENAME                       as PRODUCT_TYPE,
        FPROD.FUNDID                            as CUSIP,
        FPROD.NAME                              as PRODUCT_NAME,
        FPROD.LOADTYPE                          as LOAD_TYPE

    from {{ ref('transactions_normalize_investment') }}   as T
    join {{ ref('cheque_normalize_investment') }}      as C
        on T.CHEQUELINK = C.CODE

    -- Commissions (1 commission row per transaction code)
    join {{ ref('processedcommissions_vc_normalize_investment') }}  as PC
        on PC.TRANSACTION_CODE = T.CODE

    left join tvc_status                                  as tvc
        on tvc.transactioncode = T.CODE

    join {{ ref('transactiontypes_fh_normalize_investment') }} as T_TYPE
        on T.EXT_TYPE_CODE = T_TYPE.TRANSACTIONTYPECODE

    -- Product / sponsor
    join {{ ref('fundaccount_vc_normalize_investment') }}  as FA_VC
        on T.FUNDACCOUNT_CODE = FA_VC.FUNDACCOUNTCODE
    join {{ ref('fundproducts_vc_normalize_investment') }} as FPROD
        on FA_VC.FUNDPRODUCT_CODE = FPROD.FUNDPRODUCTCODE
    join {{ ref('sponsors_vc_normalize_investment') }}     as SPONSOR
        on FPROD.SPONSORID = SPONSOR.SPONSORID

    -- Account / client
    left join {{ ref('fundaccount_normalize_investment') }} as FA
        on T.FUNDACCOUNT_CODE = FA.CODE
    left join {{ ref('registration_normalize_investment') }} as RG
        on FA.REGISTRATION_CODE = RG.CODE
    left join {{ ref('clients_normalize_investment') }}      as CLI
        on RG.KYC_CODE = CLI.CODE

    -- Transaction rep (note: REPRESENTIATIVECODE has an extra "i")
 left join {{ ref('representatives_vc_normalize_investment') }} as REP_TRX
    on REP_TRX.REPRESENTATIVECODE = to_varchar(coalesce(PC.TRX_REP_CODE, T.REP_CODE))

left join {{ ref('branches_vc_normalize_investment') }} as BCH_TRX
    on REP_TRX.BRANCH_CODE = BCH_TRX.CODE
left join {{ ref('region_vc_normalize_investment') }}  as SUBREG_TRX
    on BCH_TRX.SUBREGIONCODE = SUBREG_TRX.SUBREGIONCODE
),

roles_expanded as (
    -- Expand each transaction into up to 4 rows: TRX / OVR1 / OVR2 / BM
    select
        base.*,
        'TRX'                                   as role,
        coalesce(base.TRX_REP_CODE, base.BASE_REP_CODE) as rep_code,
        base.TRP_REP_SHARE                      as share
    from base

    union all

    select
        base.*,
        'OVR1'             as role,
        base.OVR1_REP_CODE as rep_code,
        base.OVR1_REP_SHARE as share
    from base

    union all

    select
        base.*,
        'OVR2'             as role,
        base.OVR2_REP_CODE as rep_code,
        base.OVR2_REP_SHARE as share
    from base

    union all

    select
        base.*,
        'BM'               as role,
        base.BM_REP_CODE   as rep_code,
        base.BM_REP_SHARE  as share
    from base
)

select
    -- 00 - transaction identifier
    r.TRANSACTION_CODE,

    -- 01 - type
    'trx' as TYPE,

    -- 02–05 - group leader per commission rep
    REP_COMM.REPID as GROUP_LEADER_REPID,

    coalesce(
    nullif(
        trim(concat_ws(
            ' ',
            nullif(trim(REP_COMM.FIRST_NAME), ''),
            nullif(trim(REP_COMM.LAST_NAME), '')
        )),
        ''
    ),
    nullif(trim(REP_COMM.LAST_NAME), ''),
    nullif(trim(BCH_COMM.NAME), '')
) as GROUP_LEADER_NAME,
    
    BCH_COMM.NAME as GROUP_LEADER_BRANCH_ID,
    
    -- 06–07 - transaction rep 
    r.TRANSACTION_REPID       as TRANSACTION_REPID,
    r.TRANSACTION_REP_NAME    as TRANSACTION_REP_NAME,

    -- 08–10 - client
    r.CLIENT_ID               as CLIENT_ID,
    r.CLIENT_STATE            as CLIENT_STATE,
    r.CLIENT_NAME             as CLIENT_NAME,

    -- 11 - account
    r.ACCOUNT_NO              as "Account #",

    -- 12–17 - product / sponsor
    r.SPONSOR                 as SPONSOR,
    r.SPONSOR_ID              as SPONSOR_ID,
    r.PRODUCT_TYPE            as PRODUCT_TYPE,
    r.CUSIP                   as CUSIP,
    r.PRODUCT_NAME            as PRODUCT_NAME,
    r.LOAD_TYPE               as LOAD_TYPE,

    -- 18–19 - commission rep (TRX / OVR1 / OVR2 / BM)
    REP_COMM.REPID as COMMISSION_REPID,
    
    coalesce(
        nullif(
            trim(concat_ws(
                ' ',
                nullif(trim(REP_COMM.FIRST_NAME), ''),
                nullif(trim(REP_COMM.LAST_NAME), '')
            )),
            ''
        ),
        nullif(trim(REP_COMM.LAST_NAME), ''),
        nullif(trim(BCH_COMM.NAME), '')
    ) as COMMISSION_REP_NAME,

    -- 20–21 - branch / sub-region of commission rep
    BCH_COMM.NAME             as BRANCH,
    SUBREG_COMM.NAME          as "Sub-Region",

    -- 22–24 - amounts
    0                              as ADJUSTMENT_AMOUNT,
    r.TICKET_CHARGE::NUMBER(18, 2) as TICKET_CHARGE,
    case
        when r.role = 'TRX'
            then r.DEALER_COMMISSION::NUMBER(18, 2)
        else 0::NUMBER(18, 2)
    end                           as GROSS_COMMISSION,

    -- 25 - net commission = share
    coalesce(r.share, 0)::NUMBER(18, 2) as NET_COMMISSION,

-- 26 - investment amount replicated across all roles within the same transaction (Dhaval RAW behavior)
    r.INVESTMENT_AMOUNT_BASE as INVESTMENT_AMOUNT,

    -- 27 - status
    r.paymentstatus              as STATUS,

    -- 28–31 - commission run / cheque
    r.COMM_RUN_CODE              as COMMISSION_RUN_ID,
    r.RUN_START_DATE::date       as COMMISSION_RUN_START_DATE,
    r.RUN_END_DATE::date         as COMMISSION_RUN_END_DATE,
    r.CHEQUE_DATE::date          as COMMISSION_CHEQUE_DATE

from roles_expanded r

-- Commission rep (note: REPRESENTIATIVECODE has an extra "i")
left join {{ ref('representatives_vc_normalize_investment') }} as REP_COMM
    on REP_COMM.REPRESENTATIVECODE = to_varchar(r.rep_code)
left join {{ ref('branches_vc_normalize_investment') }}    as BCH_COMM
    on REP_COMM.BRANCH_CODE = BCH_COMM.CODE
left join {{ ref('region_vc_normalize_investment') }}   as SUBREG_COMM
    on BCH_COMM.SUBREGIONCODE = SUBREG_COMM.SUBREGIONCODE

where
      (r.role = 'TRX'  and r.DEALER_COMMISSION <> 0)
   or (r.role in ('OVR1','OVR2','BM')
       and r.rep_code is not null
       and coalesce(r.share, 0) <> 0)