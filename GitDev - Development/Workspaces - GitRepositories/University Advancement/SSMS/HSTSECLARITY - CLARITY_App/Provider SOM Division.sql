USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

	SET NOCOUNT ON;
	BEGIN
 
/************************************************************/

;WITH PT AS
(
SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	 dvsn.Epic_Financial_Division																			AS	'ucinn_ascendv2__Division__c'

FROM
(
SELECT DISTINCT
	csa.PAT_ENC_CSN_ID,
	csa.PAT_ID,
	csa.PROV_ID,
	csa.DEPARTMENT_ID,
	csa.HSP_ACCOUNT_ID,
	csa.ENC_TYPE_C,
	csa.ENC_TYPE_NAME,
	csa.ACCOUNT_ID,
	csa.ADMSN_TIME,
	csa.LOC_ID,
	csa.APPT_STATUS_C
FROM
(
-- OP/HOV
SELECT
	hsp.PAT_ENC_CSN_ID,
	hsp.PAT_ID,
	hsp.VISIT_PROV_ID AS PROV_ID,
	hsp.DEPARTMENT_ID,
	hsp.HSP_ACCOUNT_ID,
	hsp.ENC_TYPE_C,
	zdet.NAME AS ENC_TYPE_NAME,
	hsp.ACCOUNT_ID,
	COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE) AS ADMSN_TIME,
	hsp.PRIMARY_LOC_ID AS LOC_ID,
	hsp.APPT_STATUS_C
FROM  CLARITY.dbo.PAT_ENC hsp
LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet		ON zdet.DISP_ENC_TYPE_C = hsp.ENC_TYPE_C
LEFT OUTER JOIN CLARITY.dbo.V_SCHED_APPT appt	ON appt.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
UNION ALL
-- IP
SELECT
	hsp.PAT_ENC_CSN_ID,
	hsp.PAT_ID,
	hsp.ADMISSION_PROV_ID AS PROV_ID,
	hsp.DEPARTMENT_ID,
	hsp.HSP_ACCOUNT_ID,
	NULL AS ENC_TYPE_C,
	'IP-'+TRIM(zhat.NAME) AS ENC_TYPE_NAME,
	acc.GUARANTOR_ID AS ACCOUNT_ID,
	hsp.HOSP_ADMSN_TIME AS ADMSN_TIME,
	acc.ADM_LOC_ID AS LOC_ID,
	NULL AS APPT_STATUS_C
FROM  CLARITY.dbo.PAT_ENC_HSP hsp
LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat				ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C													
LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT acc				ON acc.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
) csa
) pe

			LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = pe.PROV_ID
			INNER JOIN CLARITY.dbo.PATIENT pt						ON pt.PAT_ID = pe.PAT_ID
			LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep				ON dep.DEPARTMENT_ID = pe.DEPARTMENT_ID
			--LEFT OUTER JOIN CLARITY.dbo.ZC_SEX sex					ON pt.SEX_C=sex.RCPT_MEM_SEX_C	
			--LEFT OUTER JOIN CLARITY.dbo.ZC_STATE st						ON st.STATE_C = pt.STATE_C
			--LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad1					ON ad1.PAT_ID = pt.PAT_ID
			--																 AND ad1.line=1
			--LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad2					ON ad2.PAT_ID = pt.PAT_ID
			--																 AND ad2.line=2
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT gacc						ON pe.ACCOUNT_ID=gacc.ACCOUNT_ID
			
			--LEFT OUTER JOIN CLARITY.dbo.ZC_STATE guarst				ON gacc.STATE_C =guarst.STATE_C															
			LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT acc				ON acc.HSP_ACCOUNT_ID = pe.HSP_ACCOUNT_ID
			--LEFT OUTER JOIN CLARITY.dbo.ZC_ACCT_BASECLS_HA bcls		ON bcls.ACCT_BASECLS_HA_C = acc.ACCT_BASECLS_HA_C
			--LEFT OUTER JOIN CLARITY.dbo.ACCOUNT_STATUS	accst		ON pe.HSP_ACCOUNT_ID=accst.ACCOUNT_ID 
			--LEFT OUTER JOIN CLARITY.dbo.HSP_ACCT_DX_LIST dx			ON pe.HSP_ACCOUNT_ID=dx.HSP_ACCOUNT_ID
			--													--AND dx.line=1
			--LEFT OUTER JOIN CLARITY.dbo.CLARITY_EDG edg				ON edg.DX_ID=dx.DX_ID
			--LEFT OUTER JOIN CLARITY.dbo.ACCOUNT_FPL_INFO fpl		ON pe.HSP_ACCOUNT_ID = fpl.ACCOUNT_ID
			--LEFT OUTER JOIN clarity_App.Rptg.ADT_Red_Folder_Extract Rd				ON rd.Acc_ID=acc.HSP_ACCOUNT_ID
			--LEFT OUTER JOIN CLARITY.dbo.PATIENT_4 pt4				ON pt.PAT_ID=pt4.PAT_ID
			--LEFT OUTER JOIN CLARITY.dbo.ACCT_GUAR_PAT_INFO rship	ON rship.ACCOUNT_ID = gacc.ACCOUNT_ID
			--LEFT OUTER JOIN CLARITY.dbo.ZC_GUAR_REL_TO_PAT rel		ON rel.GUAR_REL_TO_PAT_C = rship.GUAR_REL_TO_PAT_C
			LEFT OUTER JOIN CLARITY.dbo.PATIENT_TYPE AS pt_typ
			  ON pt_typ.PAT_ID = pt.PAT_ID
			  AND pt_typ.PATIENT_TYPE_C = '6'  --prisoner/inmate

			--LEFT OUTER JOIN CLARITY..HSP_ACCT_ATND_PROV	haatn ON haatn.HSP_ACCOUNT_ID = acc.HSP_ACCOUNT_ID		-- Admitting Provider
			--						AND	   haatn.LINE = 1

			LEFT OUTER JOIN CLARITY..CLARITY_LOC loc		ON loc.LOC_ID = pe.LOC_ID

			--LEFT OUTER JOIN CLARITY..CLARITY_EPM payor	ON payor.PAYOR_ID = acc.PRIMARY_PAYOR_ID

			--LEFT OUTER JOIN CLARITY..ZC_FIN_CLASS zfc	ON zfc.FIN_CLASS_C = payor.FINANCIAL_CLASS

			--LEFT OUTER JOIN CLARITY..CLARITY_EPP epp	ON epp.BENEFIT_PLAN_ID = acc.PRIMARY_PLAN_ID

			--LEFT OUTER JOIN CLARITY..ZC_COUNTRY zc		ON zc.COUNTRY_C = pt.COUNTRY_C

			LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_Pt AS clpt	ON clpt.Clrt_PAT_ID = pt.PAT_ID

			--LEFT OUTER JOIN CLARITY_App.Rptg.ADT_Red_Folder_Extract AS rf ON rf.MRN = CAST(pt.PAT_MRN_ID AS INTEGER)

			LEFT OUTER JOIN
			(
				SELECT
					Epic_Financial_Division_Code,
					Epic_Financial_Division,
                    Epic_Financial_Subdivision_Code,
					Epic_Financial_Subdivision,
                    Department,
                    Department_ID,
                    Organization,
                    Org_Number,
                    som_group_id,
                    som_group_name,
					som_hs_area_id,
					som_hs_area_name
				FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
				ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.RPT_GRP_SIX AS INT)
					AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.RPT_GRP_EIGHT AS INT))

WHERE 1 = 1
AND pe.HSP_ACCOUNT_ID IS NOT NULL
AND (
			(
				((pe.APPT_STATUS_C IS NOT NULL AND pe.APPT_STATUS_C IN ('2','6')) --ONLY COMPLETED/ARRIVED STATUS 
				OR (pe.APPT_STATUS_C IS NULL AND pe.ADMSN_TIME IS NOT NULL))
			)
		)
AND ser.PROV_TYPE <>'Resource'
--AND rf.Acc_ID IS NULL --not in red folder extract
AND pt_typ.PATIENT_TYPE_C IS NULL --not a prisoner/inmate
AND ((pe.ENC_TYPE_C IS NULL AND pe.ENC_TYPE_NAME IS NOT NULL) 
	OR
		  (pe.ENC_TYPE_C IS NOT NULL AND pe.ENC_TYPE_C IN (
    '1001' --Anti-coag visit
   ,'1003' --Procedure visit
   ,'101' --Office Visit
   ,'108' --Immunization
   ,'1200' --Routine Prenatal
   ,'1201' --Initial Prenatal
   ,'201' --Nurse Only
   ,'2100700001' --Office Visit / FC
   ,'2101' --Clinical Support
   ,'2103500001' --Home Visit
   ,'2103500002' --Home Visit - Nurse Only
   ,'2104200001' --Telemedicine Clinical Support
   ,'2105100001' --Therapy Visit
   ,'2105700001' --Prof Remote/Non Face-to-Face Encounter
   ,'213' --Dentistry Visit
   ,'2502' --Follow-Up
   ,'3' --Hospital Encounter
   ,'50' --Appointment
   ,'52' --Anesthesia
   ,'71' --Nurse Triage
   ,'72' --E-Consult
   ,'76' --Telemedicine
   ,'91' --Home Care Visit
)))
)

SELECT DISTINCT
	PT.ucinn_ascendv2__Division__c
FROM PT	

ORDER BY	PT.ucinn_ascendv2__Division__c
END

GO


