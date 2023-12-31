 {{  config(alias='policy', database='normalize', schema='insurance')  }} 
 


SELECT aboveamount,
  accrualaccountflag,
  accvalueamount,
  adjcommprem,
  admincharge,
  agacode,
  ageissued,
  agentcode,
  agtservicefeelastaccrualon,
  annualpremiumamount,
  applicationdate,
  applicationnumber,
  approvaldate,
  appsigneddate,
  autoprocess,
  bankaccountnumber,
  bankaccounttype,
  bankname,
  banknumber,
  baseannualamount,
  basecsvamount,
  benefitperiod,
  benefitperiodtype,
  billday,
  boxnumber,
  branchcode,
  bulkloaddocname,
  cancelleddate,
  carrierstatementdate,
  cashwithappamount,
  cdc_operation,
  certificatetype,
  changedby,
  changedbycode,
  changedon,
  classquoted,
  clientcode,
  coi,
  coiamount,
  coitype,
  commagentcode,
  commissionablepremiumfactor,
  commissionprocessmode,
  commit_timestamp,
  contingentowner,
  contractdate,
  contractplantype,
  contracttype,
  conversionexpirydate,
  copypolicycode,
  covcommprem,
  coveragenumber,
  coveragetype,
  createaa_flag,
  createdby,
  createdbycode,
  createddate,
  creationsource,
  csvamount,
  csvdate,
  currency,
  datalake_end_ts,
  datalake_start_ts,
  datalake_timestamp,
  dbxagentusername,
  dbxcaseid,
  dbxclientid,
  dbxcompany,
  dbx_old_annualpremium,
  dbx_old_applicationnumber,
  dbx_old_balancedue,
  dbx_old_comm_year1,
  dbx_old_comm_year2,
  dbx_old_coveragetypecode,
  dbx_old_premiummin,
  dbx_old_premiumpayablefor,
  dbx_old_reviewdate,
  dealdirect,
  deductible,
  dob1,
  dob2,
  durationdesign,
  eliminationperiod,
  expirydate,
  faceamount,
  followupdate,
  fybc_flag,
  fycpaid,
  giccert,
  gicrate,
  gicterm,
  grossamount,
  hassplit,
  iccode,
  icpolicytype,
  icreqgenerateflag,
  importedifcoverageid,
  importednbcoveragecode,
  importednbpolicycode,
  importflag,
  importnote,
  insured,
  insurername1,
  insurername2,
  investtype,
  issuedate,
  issueprovince,
  is_first_known_record,
  is_last_known_record,
  jointownercode,
  jointownerrelation,
  lastaccrualon,
  linkedpolicynumber,
  loadtype,
  maileddate,
  matchingfuzzypolicynumber,
  matchingpolicynumber,
  maturitydate,
  maxpremiumamount,
  mgacode,
  mktvalueadjamount,
  mktvaluedate,
  modifydate,
  moneyorderflag,
  newbusinessadmin,
  notestatus,
  noteusercode,
  numemployees,
  occupationclass,
  offbook_flag,
  overfundedpremium,
  ownership,
  pacamount,
  pacdate,
  palreferencecode,
  paramediccode,
  paramedicdate,
  paymentdate,
  physicianaddress,
  physicianname,
  placeddate,
  plancode,
  planname,
  plantype,
  policyannualpremium,
  policycode,
  policyfee,
  policyid,
  policynumber,
  policypayor,
  policyrated,
  policyrating,
  policyservicemsupport,
  premiumamount,
  premiumduedate,
  premiummode,
  primaryreferrent,
  quickaccrualflag,
  quotecode,
  quoteno,
  rdcomment,
  receivedfromicdate,
  recordstatus,
  referencecode,
  registrationcode,
  renewaldate,
  reqgenerateflag,
  reqorderedby,
  requirementlanguage,
  riderprem,
  secondaryreferrent,
  senttoheadofficedate,
  senttoicdate,
  serviceagentcode,
  settlementdate,
  sin1,
  sin2,
  smoker,
  sponsorcode,
  staffreferral,
  stage_id,
  startdate,
  status,
  statuschangedate,
  stream_position,
  suminsured,
  sundryamount,
  surchargeamount,
  synctradeflag,
  sync_flag,
  taskassigned,
  team,
  totaldeposit,
  transact_id,
  transitnumber,
  underwriter,
  underwritingamount,
  underwritingstatus,
  userdefined1,
  userdefined2,
  verified,
  wspolicycode
  ,_infx_loaded_ts_utc  
  ,_infx_active_from_ts_utc 
  ,_infx_active_to_ts_utc  
  ,_infx_is_active  
  
  
  


from {{ ref ('policy_clean_insurance')  }}