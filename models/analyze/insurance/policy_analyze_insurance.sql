 {{  config(alias='policy', database='analyze', schema='insurance')  }} 
 

SELECT 

p.policycode
	-- ,pgl.policygroupcode
	-- ,ifnull(sp.agentcode, p.commagentcode) AS agentcode
	,ic.icname AS carrier
	,p.policynumber
	,p.plancode AS planid
	,p.plantype
	,p.planname
	,p.sundryamount AS premiumamount
	,p.policyannualpremium AS annualpremiumamount
	,p.baseannualamount AS commpremiumamount
	--,c_pm.description as paymentmode
	,p.faceamount
	,p.createdby AS createdby
	,cast(p.createddate AS DATE) AS createddate
	,cast(p.applicationdate AS DATE) AS applicationdate
	,cast(p.contractdate AS DATE) AS contractdate
	,cast(p.settlementdate AS DATE) AS settlementdate
	,cast(p.expirydate AS DATE) AS expirydate
	,cast(p.renewaldate AS DATE) AS renewaldate
	,cast(p.senttoicdate AS DATE) AS senttoicdate
	,cast(p.maileddate AS DATE) AS maileddate
	,p.STATUS
	-- ,ifnull(sp.rate, 100) / 100 AS splitrate
	-- ,pgi.ismainpolicy * ifnull(sp.rate, 100) / 100 AS appcount
	-- ,ifnull(fyctotals.accrualamount, 0) * ifnull(sp.rate, 100) / 100 AS fycamount
	,ifnull(mgafyototals.accrualamount, 0) AS mgafyoamount
	,issueprovince
	,CASE 
		WHEN c.importednbpolicycode IS NOT NULL
			THEN 'feed'
		ELSE 'manual'
		END AS appsource
	,CASE c.submissiontypetc
		WHEN 1
			THEN 'paper'
		WHEN 2
			THEN 'eapp'
		ELSE 'unknown'
		END AS apptype
	,commissionsprocesseddate.lastcommissionprocessdate
	,firstowner.ownerclientcode firstownerclientcode
	,firstinsured.insuredclientcode firstinsuredclientcode
	,p.clientcode as policy_clientcode
	,p.conversionexpirydate
	-- ,CASE WHEN pg.policycode = p.policycode	THEN 'yes' ELSE 'no' END AS ismaincoverage
	,p.GROSSAMOUNT
	,p.SUMINSURED
	,beneficiaries.beneficiaries

FROM dev_integrate.insurance.policy p
LEFT JOIN {{ ref ('ic_integrate_insurance')  }} ic ON ic.iccode = p.iccode
LEFT JOIN {{ ref ('policystatus_integrate_insurance')  }} ps ON ps.statusvalue ilike p.STATUS
-- LEFT JOIN {{ ref ('policysplit_integrate_insurance')  }} sp ON sp.policycode = p.policycode AND p.hassplit ilike 'y' AND sp.STATUS ilike 'a'
-- LEFT JOIN {{ ref ('mga_integrate_insurance')  }} mga ON mga.mgacode = p.mgacode
-- INNER JOIN dev_integrate.insurance.policygrouplinking pgl ON p.policycode = pgl.policycode
-- INNER JOIN dev_integrate.insurance.policygroup pg ON pgl.policygroupcode = pg.policygroupcode

LEFT JOIN (
	SELECT policycode
		,cast(max(changedon) AS DATE) lastcommissionprocessdate
	FROM {{ ref ('accruals_integrate_insurance')  }}
	GROUP BY policycode
	) commissionsprocesseddate
	ON p.policycode = commissionsprocesseddate.policycode
    
    
-- LEFT JOIN (
-- 	SELECT pgl.policycode
-- 		,pg.policygroupcode
-- 		,iff(pgl.policycode = pg.policycode, 1, 0) ismainpolicy
-- 		,pg.policycode mainpolicycode
-- 	FROM dev_integrate.insurance.policygrouplinking pgl
-- 	INNER JOIN dev_integrate.insurance.policygroup pg
-- 		ON pg.policygroupcode = pgl.policygroupcode
-- 	) pgi
-- 	ON pgi.policycode = p.policycode
    
LEFT JOIN (
	SELECT policycode
		,sum(amount) accrualamount
	FROM {{ ref ('accruals_integrate_insurance')  }}
	WHERE accruals.trxtype ilike 'fyc'
		AND accruals.subtype ilike 'ic'
		AND lower(accrualstatus) IN (
			'accrued'
			,'processed'
			)
	GROUP BY policycode
	) fyctotals
	ON fyctotals.policycode = p.policycode
    
    
LEFT JOIN (
	SELECT policycode
		,sum(amount) accrualamount
	FROM {{ ref ('accruals_integrate_insurance')  }} a
	WHERE a.ownertype ilike '1' --mga
		AND a.trxtype ilike 'fyb'
		AND lower(accrualstatus) IN (
			'accrued'
			,'processed'
			)
	GROUP BY policycode
	) mgafyototals
	ON mgafyototals.policycode = p.policycode

left join
          (
           select policycode, min(clientcode) as ownerclientcode
           from {{ ref ('policyclientlinking_integrate_insurance')  }}
           where type = 1 -- owner
           group by policycode
          )
          firstowner
          on firstowner.policycode = p.policycode

left join
          (
           select policycode , min(clientcode) insuredclientcode
           from {{ ref ('policyclientlinking_integrate_insurance')  }}
           where type = 2 -- insured
           group by policycode
          )
          firstinsured on firstinsured.policycode = p.policycode
LEFT JOIN 
    (
			SELECT importednbpolicycode
				,submissiontypetc
				,policycode
				,row_number() OVER (
					ORDER BY importednbpolicycode DESC
					) AS rownumber
			FROM {{ ref ('importednbpolicy_integrate_insurance')  }}  ip
			
			) c
	ON c.policycode = p.policycode
		AND c.rownumber = 1

LEFT JOIN 
    (
		select 
		policycode, 
		listagg(benefname||' - '||beneftype, ', ') beneficiaries 
		from FH_PROD.WEALTHSERV_INS_ODS_CURRENT_SECURE.BENEFICIARY 
		group by 1
	) beneficiaries
	ON beneficiaries.policycode = p.policycode

WHERE
	p.recordstatus ilike 'active'