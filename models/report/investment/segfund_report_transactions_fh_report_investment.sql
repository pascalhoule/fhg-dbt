{{  
    config(alias='segfund_report_transactions_fh', 
    database='report', 
    schema='investment')  
}} 

SELECT * FROM {{ ref('segfund_report_transactions_fh_analyze_investment') }}