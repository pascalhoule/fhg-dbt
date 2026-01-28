{{  config( 
        alias='repmap_fh', 
        database='report', 
        schema='investment' )  }}

SELECT * FROM {{ ref('repmap_fh_analyze_investment') }}