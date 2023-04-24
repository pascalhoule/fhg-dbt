 {{  config(alias='ic', database='normalize', schema='insurance')  }} 
 


SELECT 
activeuse,
  allowcommupload,
  allowfeedaccruals,
  allowmaxamount,
  allowstoploss,
  anniversarydate,
  category,
  cdc_operation,
  changedby,
  changedbycode,
  changedon,
  citscarriercode,
  city,
  commit_timestamp,
  contactperson,
  countryname,
  datalake_end_ts,
  datalake_start_ts,
  datalake_timestamp,
  dbxcompanyid,
  dbx_mgacode,
  description,
  email,
  fax,
  fixedeffectivedate,
  gstapp,
  hwtagreement,
  iccode,
  icid,
  icname,
  icshortname,
  is_first_known_record,
  is_last_known_record,
  negamount,
  notestatus,
  noteusercode,
  orgid,
  phone1,
  phone2,
  phone3,
  postalcode,
  printinvoice,
  province,
  pstapp,
  receiptsallowed,
  roundfyc,
  selfemployed,
  setupfee,
  sponsorcode,
  stage_id,
  stream_position,
  street,
  street2,
  street3,
  suppressemail,
  taxapp,
  transact_id,
  trustaccountnumber,
  uploaddir,
  website
  ,_infx_loaded_ts_utc  
  ,_infx_active_from_ts_utc 
  ,_infx_active_to_ts_utc  
  ,_infx_is_active  
  
  
  

from {{ ref ('ic_clean_insurance')  }}