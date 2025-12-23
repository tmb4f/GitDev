USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*******************************************************************************************
WHAT:	Grateful Patient Program data extract 
WHO :	University Advancement
WHEN:	Daily
WHY :	Detail updates for completed inpatient encounters with "Person" resources
AUTHOR:	Tom Burgan
SPEC:	
--------------------------------------------------------------------------------------------
INPUTS: 	 
**		CLARITY.dbo.PAT_ENC_HSP hsp
**		CLARITY.dbo.PAT_ENC hsp
**		CLARITY.EPIC_UTIL.CSA_PAT_ENC_HSP AS csa
**		CLARITY.dbo.CLARITY_SER ser
**		CLARITY.dbo.PATIENT pt
**		CLARITY.dbo.clarity_ser_2 ser2
**		CLARITY.dbo.ZC_DISP_ENC_TYPE typ
**		CLARITY.dbo.CLARITY_DEP dep
**		CLARITY.dbo.ZC_SEX sex
**		CLARITY.dbo.ZC_STATE st
**		CLARITY.dbo.PAT_ADDRESS ad1
**		CLARITY.dbo.PAT_ADDRESS ad2
**		CLARITY.dbo.ACCOUNT gacc
**		CLARITY.dbo.ZC_STATE guarst	
**		CLARITY.dbo.HSP_ACCOUNT acc
**		CLARITY.dbo.ZC_ACCT_BASECLS_HA bcls
**		CLARITY.dbo.ACCOUNT_STATUS	accst
**		CLARITY.dbo.HSP_ACCT_DX_LIST dx
**		CLARITY.dbo.CLARITY_EDG edg
**		CLARITY.dbo.ACCOUNT_FPL_INFO fpl
**		clarity_App.Rptg.ADT_Red_Folder_Extract Rd
**		CLARITY.dbo.PATIENT_4 pt4
**		CLARITY.dbo.ACCT_GUAR_PAT_INFO rship
**		CLARITY.dbo.ZC_GUAR_REL_TO_PAT rel
**		CLARITY.dbo.PATIENT_TYPE AS pt_typ
**		CLARITY..HSP_ACCT_ATND_PROV	haatn
**		CLARITY..CLARITY_PRC prc
**		CLARITY..V_SCHED_APPT appt
**		CLARITY..CLARITY_LOC loc
**		CLARITY..CLARITY_EPM payor
**		CLARITY..ZC_FIN_CLASS zfc
**		CLARITY..PAT_ENC_3 enc3
**		CLARITY..CLARITY_EPP epp
**		CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_All AS mdm
**		[CLARITY_App].[Mapping].[Epic_Dept_Groupers] g
**		[CLARITY_App].[Mapping].Ref_Clinical_Area_Map c
**		[CLARITY_App].[Mapping].Ref_Service_Map s
**		[CLARITY_App].[Mapping].Ref_Organization_Map o
**		CLARITY..CLARITY_SER_SPEC spc
**		CLARITY..ZC_SPECIALTY zs
**		CLARITY_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv
	
OUTPUTS: 
   	1) SEQUENCE:				File 1 of 1
   	2) FILE NAMING CONVENTION:	Grateful_Patient_Program
   	3) OUTPUT TYPE:				TABLE 
   	4) TRANSFER METHOD:			sFTP/SSIS ETL
   	5) OUTPUT LOCATION:			HSTSECLARITY
								
   	6) FREQUENCY:				Daily
   	7) QUERY LOOKBACK PERIOD:	3 days
   	8) FILE SPECIFIC NOTES:	
		**     Pulls updated or created encounter records over the last three days
		
MODS: 
		**		09/19/2024  -Tom B.  Create stored procedure

********************************************************************************************/

-- =============================================
--EXEC [ETL].[uspSrc_UA_GratefulPatientProgram_IP]

--CREATE PROCEDURE [ETL].[uspSrc_UA_GratefulPatientProgram_IP] 
--	-- Add the parameters for the stored procedure here
--	--@StartDate DATETIME, --DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)), 
--	--@EndDate DATETIME --= GETDATE()
--	@LookBackDays INTEGER = 3
--AS

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	BEGIN
    
	--DECLARE @StartDate DATETIME;
	--DECLARE @EndDate DATETIME;
	DECLARE @LookBackDays INTEGER

		/*
	--set date parameter
	IF @Startdate IS NULL
    AND @Enddate IS NULL
    BEGIN
 
        SELECT @StartDate= DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)); --last five years
        SELECT  @EndDate	=GETDATE(); --Today
    END;
	*/
	SET @LookBackDays = 3
/************************************************************/

IF OBJECT_ID('tempdb..#ip ') IS NOT NULL
DROP TABLE #ip

SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	'UVAH-'+TRIM(pt.PAT_MRN_ID)																		AS	'ucinn_ascendv2__External_System_ID__c'
	,pt.PAT_MRN_ID																								AS	'ucinn_ascendv2__Patient_ID__c'
	,ser.PROV_NAME																							AS	'ucinn_ascendv2__Attending_Physician__c'
	,gacc.ACCOUNT_ID																						AS	'ucinn_ascendv2__Guarantor__c'
	,rel.NAME																										AS	'ucinn_ascendv2__Guarantor_Relationship__c'
	,appt.PRC_NAME																							AS	'ucinn_ascendv2__Appointment_Type__c'
	,loc.LOC_NAME																								AS	'ucinn_ascendv2__Appointment_Location__c'
	,dep.DEPARTMENT_NAME																			AS	'ucinn_ascendv2__Department_Formula__c'
	,dvsn.Epic_Financial_Division																			AS	'ucinn_ascendv2__Division__c'
	,CASE WHEN hsp.HOSP_DISCH_TIME IS NOT NULL THEN 'TRUE' ELSE 'FALSE' END	AS 'ucinn_ascendv2__Discharge_Flag__c'
	,typ.NAME																										AS	'ucinn_ascendv2__Encounter_Type__c'
	,dep.DEPARTMENT_NAME																			AS	'ucinn_ascendv2__Patient_Encounter_Dept_Override__c'
	,appt.APPT_STATUS_NAME																			AS	'ucinn_ascendv2__Appointment_Status__c'
	,typ.ABBR																										AS	'ucinn_ascendv2__Data_Source__c'
	,mdm.HOSPITAL_CODE																					AS	'ucinn_ascendv2__Hospital_Code__c'
	,CASE WHEN appt.APPT_STATUS_NAME = 'Canceled' THEN 'TRUE' ELSE 'FALSE' END AS 'ucinn_ascendv2__Is_Cancelled_Appointment_Formula__c'
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
	 END																												AS	'ucinn_ascendv2__Admit_Date_Time__c'
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
	 END																												AS	'ucinn_ascendv2__Appointment_Date_Time__c'
	,hsp.HOSP_DISCH_TIME																				AS	'ucinn_ascendv2__Discharge_Date_Time__c'
	,NULL																												AS	'ucinn_ascendv2__Is_Priority_Encounter__c'
	,acc.ACCT_FIN_CLASS_C
	,acc.ACCT_SLFPYST_HA_C
	,fpl.FPL_STATUS_CODE_C
	,fpl.FPL_EFF_DATE
	,accst.ACCOUNT_STATUS_C
	,pt.BIRTH_DATE
	,edg.ICD9_CODE
	--,x.UPDATE_DATE																							AS	'Update_Date'
	,GETDATE()																									AS	'Load_Dtm'

	,hsp.PAT_ENC_CSN_ID

INTO #ip

FROM  CLARITY.dbo.PAT_ENC_HSP hsp

--INNER JOIN
--(
--SELECT
--	csaseq.PAT_ENC_CSN_ID,
--    csaseq.UPDATE_DATE
--FROM
--(
--SELECT
--	csa.PAT_ENC_CSN_ID,
--    csa.UPDATE_DATE,
--	ROW_NUMBER() OVER(PARTITION BY csa.PAT_ENC_CSN_ID ORDER BY csa.UPDATE_DATE DESC) AS updseq
--FROM
--(
--SELECT csa.PAT_ENC_CSN_ID
--             , csa._UPDATE_DT AS UPDATE_DATE
--			FROM CLARITY.EPIC_UTIL.CSA_PAT_ENC_HSP AS csa
--			WHERE csa._UPDATE_DT >= CONVERT(DATE,DATEADD(dd,-@LookBackDays,GETDATE()))
--			GROUP BY csa.PAT_ENC_CSN_ID, csa._UPDATE_DT
--) csa
--) AS csaseq
--WHERE csaseq.updseq = 1
--) AS x
--  ON x.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

LEFT OUTER JOIN CLARITY.dbo.PAT_ENC enc			ON enc.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = hsp.ADMISSION_PROV_ID 
			INNER JOIN CLARITY.dbo.PATIENT pt						ON pt.PAT_ID = hsp.PAT_ID
			INNER JOIN CLARITY.dbo.clarity_ser_2 ser2					ON ser2.PROV_ID = ser.PROV_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE typ		ON typ.DISP_ENC_TYPE_C=enc.ENC_TYPE_C
			LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep				ON dep.DEPARTMENT_ID = hsp.DEPARTMENT_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_SEX sex					ON pt.SEX_C=sex.RCPT_MEM_SEX_C	
			LEFT OUTER JOIN CLARITY.dbo.ZC_STATE st						ON st.STATE_C = pt.STATE_C
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad1					ON ad1.PAT_ID = pt.PAT_ID
																			 AND ad1.line=1
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad2					ON ad2.PAT_ID = pt.PAT_ID
																			 AND ad2.line=2														
			LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT acc				ON acc.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT gacc						ON acc.GUARANTOR_ID=gacc.ACCOUNT_ID
			
			LEFT OUTER JOIN CLARITY.dbo.ZC_STATE guarst				ON gacc.STATE_C =guarst.STATE_C		
			LEFT OUTER JOIN CLARITY.dbo.ZC_ACCT_BASECLS_HA bcls		ON bcls.ACCT_BASECLS_HA_C = acc.ACCT_BASECLS_HA_C
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT_STATUS	accst		ON hsp.HSP_ACCOUNT_ID=accst.ACCOUNT_ID 
			LEFT OUTER JOIN CLARITY.dbo.HSP_ACCT_DX_LIST dx			ON hsp.HSP_ACCOUNT_ID=dx.HSP_ACCOUNT_ID
																--AND dx.line=1
			LEFT OUTER JOIN CLARITY.dbo.CLARITY_EDG edg				ON edg.DX_ID=dx.DX_ID
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT_FPL_INFO fpl		ON hsp.HSP_ACCOUNT_ID = fpl.ACCOUNT_ID
			LEFT OUTER JOIN clarity_App.Rptg.ADT_Red_Folder_Extract Rd				ON rd.Acc_ID=acc.HSP_ACCOUNT_ID
			LEFT OUTER JOIN CLARITY.dbo.PATIENT_4 pt4				ON pt.PAT_ID=pt4.PAT_ID
			LEFT OUTER JOIN CLARITY.dbo.ACCT_GUAR_PAT_INFO rship	ON rship.ACCOUNT_ID = gacc.ACCOUNT_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_GUAR_REL_TO_PAT rel		ON rel.GUAR_REL_TO_PAT_C = rship.GUAR_REL_TO_PAT_C
			LEFT OUTER JOIN CLARITY.dbo.PATIENT_TYPE AS pt_typ
			  ON pt_typ.PAT_ID = pt.PAT_ID
			  AND pt_typ.PATIENT_TYPE_C = '6'  --prisoner/inmate

			LEFT OUTER JOIN CLARITY..HSP_ACCT_ATND_PROV	haatn ON haatn.HSP_ACCOUNT_ID = acc.HSP_ACCOUNT_ID		-- Admitting Provider
									AND	   haatn.LINE = 1

			LEFT OUTER JOIN CLARITY..V_SCHED_APPT appt	ON appt.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY..CLARITY_LOC loc		ON loc.LOC_ID = acc.ADM_LOC_ID

			LEFT OUTER JOIN CLARITY..CLARITY_EPM payor	ON payor.PAYOR_ID = acc.PRIMARY_PAYOR_ID

			LEFT OUTER JOIN CLARITY..ZC_FIN_CLASS zfc	ON zfc.FIN_CLASS_C = payor.FINANCIAL_CLASS

			LEFT OUTER JOIN CLARITY..PAT_ENC_3 enc3	ON enc3.PAT_ENC_CSN = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY..CLARITY_EPP epp	ON epp.BENEFIT_PLAN_ID = acc.PRIMARY_PLAN_ID

			LEFT OUTER JOIN
			(
			SELECT
				hspc.EPIC_DEPARTMENT_ID,
                hspc.HOSPITAL_CODE
			FROM
			(
			SELECT DISTINCT	
				mdm.EPIC_DEPARTMENT_ID
			   ,mdm.HOSPITAL_CODE	
			FROM	CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_All AS mdm
			WHERE	mdm.EPIC_DEPARTMENT_ID IS NOT NULL
			) hspc
			) mdm
			ON mdm.EPIC_DEPARTMENT_ID = hsp.DEPARTMENT_ID

			LEFT JOIN [CLARITY_App].[Mapping].[Epic_Dept_Groupers] g ON hsp.DEPARTMENT_ID = g.epic_department_id
			LEFT JOIN [CLARITY_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
			LEFT JOIN [CLARITY_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
			LEFT JOIN [CLARITY_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id

			LEFT OUTER JOIN
			(
			SELECT
				PROV_ID,
                spc.SPECIALTY_C,
				zs.NAME AS SPECIALTY_NAME
			FROM CLARITY..CLARITY_SER_SPEC spc
			LEFT OUTER JOIN CLARITY..ZC_SPECIALTY zs
			ON zs.SPECIALTY_C = spc.SPECIALTY_C
			WHERE LINE = 1
			) sersp
			ON sersp.PROV_ID =
		CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
			THEN COALESCE(appt.PROV_ID, hsp.ADMISSION_PROV_ID)  --for Outpatient
			ELSE COALESCE(haatn.ATTENDING_PROV_ID, ser.PROV_ID)  --for Inpatient
		END

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
AND hsp.HSP_ACCOUNT_ID IS NOT NULL
AND ser.PROV_TYPE <>'Resource'
AND rd.Acc_ID IS NULL --not in red folder extract
AND pt_typ.PATIENT_TYPE_C IS NULL --not a prisoner/inmate
AND typ.DISP_ENC_TYPE_C NOT IN ('2505' -- Erroneous Encounter
                                                               ,'2506' -- Erroneous Telephone Encounter
															   ,'99' -- Billing Encounter
															   ,'52' -- Anesthesia'
															   ,'53' -- Anesthesia Event
															   )
AND pt.PAT_MRN_ID = '0014371'

SELECT

	enc.PAT_ENC_CSN_ID,

    CAST(enc.ucinn_ascendv2__External_System_ID__c AS VARCHAR(100))			 AS ucinn_ascendv2__External_System_ID__c,
    CAST(enc.ucinn_ascendv2__Patient_ID__c AS VARCHAR(255))				 AS ucinn_ascendv2__Patient_ID__c,
    CAST(enc.ucinn_ascendv2__Attending_Physician__c AS VARCHAR(255))			 AS ucinn_ascendv2__Attending_Physician__c,
    CAST(enc.ucinn_ascendv2__Guarantor__c AS VARCHAR(255))				 AS ucinn_ascendv2__Guarantor__c,
    CAST(enc.ucinn_ascendv2__Guarantor_Relationship__c AS VARCHAR(100))			 AS ucinn_ascendv2__Guarantor_Relationship__c,
    CAST(enc.ucinn_ascendv2__Appointment_Type__c AS VARCHAR(50))			 AS ucinn_ascendv2__Appointment_Type__c,
    CAST(enc.ucinn_ascendv2__Appointment_Location__c AS VARCHAR(255))			 AS ucinn_ascendv2__Appointment_Location__c,
    CAST(enc.ucinn_ascendv2__Department_Formula__c AS VARCHAR(255))			 AS ucinn_ascendv2__Department_Formula__c,
    CAST(enc.ucinn_ascendv2__Division__c AS VARCHAR(20))				 AS ucinn_ascendv2__Division__c,
    CAST(enc.ucinn_ascendv2__Discharge_Flag__c AS VARCHAR(10))				 AS ucinn_ascendv2__Discharge_Flag__c,
    CAST(enc.ucinn_ascendv2__Encounter_Type__c AS VARCHAR(255))				 AS ucinn_ascendv2__Encounter_Type__c,
    CAST(enc.ucinn_ascendv2__Patient_Encounter_Dept_Override__c AS VARCHAR(255))	 AS ucinn_ascendv2__Patient_Encounter_Dept_Override__c,
    CAST(enc.ucinn_ascendv2__Appointment_Status__c AS VARCHAR(255))			 AS ucinn_ascendv2__Appointment_Status__c,
    CAST(enc.ucinn_ascendv2__Data_Source__c AS VARCHAR(255))				 AS ucinn_ascendv2__Data_Source__c,
    CAST(enc.ucinn_ascendv2__Hospital_Code__c AS VARCHAR(255))				 AS ucinn_ascendv2__Hospital_Code__c,
    CAST(enc.ucinn_ascendv2__Is_Cancelled_Appointment_Formula__c AS VARCHAR(255))	 AS ucinn_ascendv2__Is_Cancelled_Appointment_Formula__c,
    ucinn_ascendv2__Admit_Date_Time__c,
    ucinn_ascendv2__Appointment_Date_Time__c,
    ucinn_ascendv2__Discharge_Date_Time__c,
    CAST(enc.ucinn_ascendv2__Is_Priority_Encounter__c AS VARCHAR(255))			 AS ucinn_ascendv2__Is_Priority_Encounter__c,
    --enc.Update_Date,
    enc.Load_Dtm
FROM #ip enc
WHERE 1 = 1
		AND (
				ISNULL(enc.ACCT_FIN_CLASS_C,1)<> '3' --K: Medicaid
					OR (
						ISNULL(enc.ACCT_SLFPYST_HA_C,1)<>'5'  -- 1,2,4,5,6,7,8: Bad Debt
					--indegent care flag***************
					OR (enc.FPL_STATUS_CODE_C NOT IN ('34','36','38','40','42') --verified 100%;95%;80%;55%;30% respectively- fpl assistance  for indegent care
									--AND (enc.FPL_EFF_DATE >=@StartDate AND enc.FPL_EFF_DATE <@EndDate))
									AND (enc.FPL_EFF_DATE >=CONVERT(DATE,DATEADD(dd,-@LookBackDays,GETDATE()))))
					OR ISNULL(enc.ACCOUNT_STATUS_C,1) <>'105'   --legal ; 3: collection ??
						)
				)

	AND (
				
						(DATEDIFF (YEAR, enc.BIRTH_DATE, CAST(enc.ucinn_ascendv2__Admit_Date_Time__c AS DATE)) >18 
								AND 
										(	enc.ICD9_CODE IS NULL 
										OR enc.ICD9_CODE NOT IN ('078.1','795.8','V08','V27.1','V27.3','V27.4','V27.6','V27.7') 
										OR enc.ICD9_CODE NOT BETWEEN '042' AND '044.9' 
										OR enc.ICD9_CODE NOT BETWEEN '054.10' AND '054.19' 
										OR enc.ICD9_CODE NOT BETWEEN '079.51' AND '079.53' 
										OR enc.ICD9_CODE NOT BETWEEN '090.0' AND '099.9' 
										OR enc.ICD9_CODE NOT BETWEEN '279.10' AND '279.19' 
										OR enc.ICD9_CODE NOT BETWEEN '632' AND '639.99' 
										)
						)
				
			
						OR
		
						(DATEDIFF (YEAR, enc.BIRTH_DATE, CAST(enc.ucinn_ascendv2__Admit_Date_Time__c AS DATE)) <=18
								AND 
										(	enc.ICD9_CODE IS NULL
										OR  enc.ICD9_CODE NOT BETWEEN '640.0'   AND '676.94' 
										OR	enc.ICD9_CODE NOT BETWEEN 'V22.0'   AND 'V25.9'
										OR  enc.ICD9_CODE NOT BETWEEN '042'	    AND '044.9' 
										OR  enc.ICD9_CODE NOT BETWEEN '054.10'  AND '054.19' 
										OR  enc.ICD9_CODE NOT BETWEEN '079.51'  AND '079.53'
										OR  enc.ICD9_CODE NOT BETWEEN '090.0'   AND '099.9' 
										OR  enc.ICD9_CODE NOT BETWEEN '279.10'  AND '279.19' 
										OR  enc.ICD9_CODE NOT BETWEEN '632'     AND '639.99' 
										OR  enc.ICD9_CODE NOT IN ('078.1','795.8','V08','V27.1','V27.3','V27.4','V27.6','V27.7') 
										)
						)
				)		

ORDER BY	CAST(enc.ucinn_ascendv2__Patient_ID__c AS VARCHAR(255))	, enc.ucinn_ascendv2__Admit_Date_Time__c
END

GO


