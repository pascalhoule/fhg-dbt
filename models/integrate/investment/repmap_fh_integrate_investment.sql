{{  config( alias ='repmap_fh', 
            database = 'integrate', 
            schema = 'investment' )  
}}

SELECT * FROM {{ ref('repmap_fh_normalize_investment') }}