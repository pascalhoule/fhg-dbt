 {{  config(alias='clients', database='report', schema='investment')  }} 



select 
INVESTMENT_CLIENT_CITY as "Client City",
INVESTMENT_CLIENT_CLIENTID as "Client Clientid",
INVESTMENT_CLIENT_CLIENTSINCE as "Client Clientsince",
INVESTMENT_CLIENT_CODE as "Client Code",
INVESTMENT_CLIENT_COUNTRY_CODE as "Client Country Code",
INVESTMENT_CLIENT_DOB as "Client DOB",
INVESTMENT_CLIENT_AGE_RANGE as "Client Age Range",
INVESTMENT_CLIENT_FIRST_NAME as "Client First Name",
INVESTMENT_CLIENT_LAST_NAME as "Client Last Name",
INVESTMENT_CLIENT_REP_CODE as "Client Rep Code",
INVESTMENT_CLIENT_SSN as "Client SSN",
INVESTMENT_CLIENT_STATE_CODE as "Client State Code",
INVESTMENT_CLIENT_STATUS as "Client Status",
investment_client_status_fr as "Client Status Fr",
INVESTMENT_CLIENT_STREET as "Client Street",
INVESTMENT_CLIENT_TAXID as "Client Tax Id",
INVESTMENT_CLIENT_ZIPCODE as "Client Zip Code",
INVESTMENT_STATE_NAME as "State Name",
investment_client_sex as "Gender",
investment_client_sex_fr as "Gender Fr",
investment_client_email as "Email",
investment_client_email2 as "Email2",
investment_client_cell_phone as "Cell Phone"
from {{ ref ('clients_analyze_investment')  }} 