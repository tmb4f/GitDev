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
WHY :	Detail updates for patient information
AUTHOR:	Tom Burgan
SPEC:	
--------------------------------------------------------------------------------------------
INPUTS: 	 
**		CLARITY.EPIC_UTIL.CSA_PATIENT AS csa
**		CLARITY.dbo.PAT_ENC hsp
**		CLARITY.EPIC_UTIL.CSA_PAT_ENC AS csa	 
**		CLARITY.dbo.PAT_ENC_HSP hsp
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
**		CLARITY..ZC_COUNTRY zc
	
OUTPUTS: 
   	1) SEQUENCE:				File 1 of 1
   	2) FILE NAMING CONVENTION:	Grateful_Patient_Program
   	3) OUTPUT TYPE:				TABLE 
   	4) TRANSFER METHOD:			sFTP/SSIS ETL
   	5) OUTPUT LOCATION:			HSTSECLARITY
								
   	6) FREQUENCY:				Daily
   	7) QUERY LOOKBACK PERIOD:	3 days
   	8) FILE SPECIFIC NOTES:	
		**     Pulls updated or created patient records over the last three days
		
MODS: 
		**		09/21/2024  -Tom B.  Create stored procedure
				10/25/2024	-Tom B.  Update per spec changes
				11/04/2024	-Tom B.  Update per spec changes

********************************************************************************************/

-- =============================================
--EXEC [ETL].[uspSrc_UA_GratefulPatientProgram_Patient]

--ALTER   PROCEDURE [ETL].[uspSrc_UA_GratefulPatientProgram_Patient] 
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

;WITH PT AS
(
SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	 clpt.sk_Dim_Pt
	,CASE WHEN clpt.sk_Dim_Pt <> -1 THEN 'Patient' ELSE REPLACE(TRANSLATE(TRIM(pt.PAT_NAME),', ','__'	),'.','') END AS 	'ESI_3'
	,CASE WHEN clpt.sk_Dim_Pt <> -1 THEN 'New' ELSE 'Manual Review' END AS	'ucinn_ascendv2__Status__c'
	,'TRUE'																											AS	'uva_Is_Patient__c'
	,'FALSE'																											AS	'uva_Is_Guarantor__c'
	,'FALSE'																											AS	'uva_Is_Provider__c'
	,RIGHT('0000000'+CAST(clpt.sk_Dim_Pt AS VARCHAR(8)),8)						AS	'ucinn_ascendv2__MRN__c'
	,'Health System'																								AS	'ucinn_ascendv2__Data_Source__c'
	,pt.PAT_FIRST_NAME																					AS	'ucinn_ascendv2__First_Name__c'
	,pt.PAT_MIDDLE_NAME																					AS	'ucinn_ascendv2__Middle_Name__c'
	,pt.PAT_LAST_NAME																						AS	'ucinn_ascendv2__Last_Name__c'
	,'PHI'																												AS	'ucinn_ascendv2__Address_1_Type__c'
	,ad1.ADDRESS																								AS	'ucinn_ascendv2__Address_1_Line_1__c'
	,ad2.ADDRESS																								AS	'ucinn_ascendv2__Address_1_Line_2__c'
	,pt.CITY																											AS	'ucinn_ascendv2__City_1__c'
	,st.NAME																											AS	'ucinn_ascendv2__State_1__c'
	,pt.ZIP																												AS	'ucinn_ascendv2__Postal_Code_1__c'
	,zc.NAME																											AS	'ucinn_ascendv2__Country_1__c'
	,'PHI'																												AS	'ucinn_ascendv2__Phone_1_Type__c'
	,pt.HOME_PHONE																							AS	'ucinn_ascendv2__Phone_1__c'
	,'PHI'																												AS	'ucinn_ascendv2__Email_1_Type__c'
	,pt.EMAIL_ADDRESS																						AS	'ucinn_ascendv2__Email_1__c'
	,sex.NAME																										AS	'ucinn_ascendv2__Gender__c'
	,pt.BIRTH_DATE																								AS	'ucinn_ascendv2__Birthdate__c'
	,NULL																												AS	'ucinn_ascendv2__Deceased_Date__c'
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
	 END																												AS	'ucinn_ascendv2__Admit_Date_Time__c'
	,acc.ACCT_FIN_CLASS_C
	,acc.ACCT_SLFPYST_HA_C
	,fpl.FPL_STATUS_CODE_C
	,fpl.FPL_EFF_DATE
	,accst.ACCOUNT_STATUS_C
	,pt.BIRTH_DATE
	,edg.ICD9_CODE
	--,x.UPDATE_DATE																							AS	'Update_Date'
	,GETDATE()																									AS	'Load_Dtm'

FROM
--(
--SELECT
--	csaseq.PAT_ID,
--    csaseq.UPDATE_DATE
--FROM
--(
--SELECT
--	csa.PAT_ID,
--    csa.UPDATE_DATE,
--	ROW_NUMBER() OVER(PARTITION BY csa.PAT_ID ORDER BY csa.UPDATE_DATE DESC) AS updseq
--FROM
--(
--SELECT csa.PAT_ID
--             , csa._UPDATE_DT AS UPDATE_DATE
--			FROM CLARITY.EPIC_UTIL.CSA_PATIENT AS csa
--			WHERE csa._UPDATE_DT >= CONVERT(DATE,DATEADD(dd,-@LookBackDays,GETDATE()))
--			GROUP BY csa.PAT_ID, csa._UPDATE_DT
--) csa
--) AS csaseq
--WHERE csaseq.updseq = 1
--) AS x

--INNER JOIN CLARITY.dbo.PAT_ENC hsp
--  ON x.PAT_ID = hsp.PAT_ID

CLARITY.dbo.PAT_ENC hsp

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
--			FROM CLARITY.EPIC_UTIL.CSA_PAT_ENC AS csa
--			WHERE csa._UPDATE_DT >= CONVERT(DATE,DATEADD(dd,-@LookBackDays,GETDATE()))
--			GROUP BY csa.PAT_ENC_CSN_ID, csa._UPDATE_DT
--) csa
--) AS csaseq
--WHERE csaseq.updseq = 1
--) AS y
--  ON y.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
--	AND hsp.CONTACT_DATE >= '11/1/2024 00:00' -- Test

			LEFT OUTER JOIN CLARITY.dbo.PAT_ENC_HSP peh			ON peh.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
																										AND peh.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = COALESCE(hsp.VISIT_PROV_ID, peh.ADMISSION_PROV_ID)
			INNER JOIN CLARITY.dbo.PATIENT pt						ON pt.PAT_ID = hsp.PAT_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE typ		ON typ.DISP_ENC_TYPE_C=hsp.ENC_TYPE_C
			LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep				ON dep.DEPARTMENT_ID = hsp.DEPARTMENT_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_SEX sex					ON pt.SEX_C=sex.RCPT_MEM_SEX_C	
			LEFT OUTER JOIN CLARITY.dbo.ZC_STATE st						ON st.STATE_C = pt.STATE_C
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad1					ON ad1.PAT_ID = pt.PAT_ID
																			 AND ad1.line=1
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad2					ON ad2.PAT_ID = pt.PAT_ID
																			 AND ad2.line=2
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT gacc						ON hsp.ACCOUNT_ID=gacc.ACCOUNT_ID
			
			LEFT OUTER JOIN CLARITY.dbo.ZC_STATE guarst				ON gacc.STATE_C =guarst.STATE_C															
			LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT acc				ON acc.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
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

			LEFT OUTER JOIN CLARITY..CLARITY_PRC prc	ON prc.PRC_ID = hsp.APPT_PRC_ID

			LEFT OUTER JOIN CLARITY..V_SCHED_APPT appt	ON appt.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY..CLARITY_LOC loc		ON loc.LOC_ID = hsp.PRIMARY_LOC_ID

			LEFT OUTER JOIN CLARITY..CLARITY_EPM payor	ON payor.PAYOR_ID = acc.PRIMARY_PAYOR_ID

			LEFT OUTER JOIN CLARITY..ZC_FIN_CLASS zfc	ON zfc.FIN_CLASS_C = payor.FINANCIAL_CLASS

			LEFT OUTER JOIN CLARITY..PAT_ENC_3 enc3	ON enc3.PAT_ENC_CSN = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY..CLARITY_EPP epp	ON epp.BENEFIT_PLAN_ID = acc.PRIMARY_PLAN_ID

			LEFT OUTER JOIN CLARITY..ZC_COUNTRY zc		ON zc.COUNTRY_C = pt.COUNTRY_C

			LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_Pt AS clpt	ON clpt.Clrt_PAT_ID = pt.PAT_ID

WHERE 1 = 1
AND hsp.HSP_ACCOUNT_ID IS NOT NULL
AND (
			(
				((hsp.APPT_STATUS_C IS NOT NULL AND hsp.APPT_STATUS_C IN ('2','6')) --ONLY COMPLETED/ARRIVED STATUS 
				OR (hsp.APPT_STATUS_C IS NULL AND hsp.HOSP_ADMSN_TIME IS NOT NULL))
			)
		)
AND ser.PROV_TYPE <>'Resource'
AND rd.Acc_ID IS NULL --not in red folder extract
AND pt_typ.PATIENT_TYPE_C IS NULL --not a prisoner/inmate
AND typ.DISP_ENC_TYPE_C IN (
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
)
AND hsp.PAT_ID = 'Z2358311'
)

SELECT DISTINCT
	CAST('UVAH-'+RIGHT('0000000'+CAST(enc.sk_Dim_Pt AS VARCHAR(8)),8)+'-'+ESI_3+'-'+CONVERT(VARCHAR(8),CAST(enc.Load_Dtm AS DATE),112) AS VARCHAR(255)) AS 	ucinn_ascendv2__External_System_ID__c,
	CAST(enc.ucinn_ascendv2__Status__c AS VARCHAR(255))							AS ucinn_ascendv2__Status__c,
    CAST(enc.uva_Is_Patient__c AS VARCHAR(255))											AS uva_Is_Patient__c,
    CAST(enc.uva_Is_Guarantor__c AS VARCHAR(255))										AS uva_Is_Guarantor__c,
    CAST(enc.uva_Is_Provider__c AS VARCHAR(255))											AS uva_Is_Provider__c,
    CAST(enc.ucinn_ascendv2__MRN__c AS VARCHAR(100))								AS ucinn_ascendv2__MRN__c,
    CAST(enc.ucinn_ascendv2__Data_Source__c AS VARCHAR(255))				AS ucinn_ascendv2__Data_Source__c,
    CAST(enc.ucinn_ascendv2__First_Name__c AS VARCHAR(255))					AS ucinn_ascendv2__First_Name__c,
    CAST(enc.ucinn_ascendv2__Middle_Name__c AS VARCHAR(255))				AS ucinn_ascendv2__Middle_Name__c,
    CAST(enc.ucinn_ascendv2__Last_Name__c AS VARCHAR(255))					AS ucinn_ascendv2__Last_Name__c,
    CAST(enc.ucinn_ascendv2__Address_1_Type__c AS VARCHAR(40))			AS ucinn_ascendv2__Address_1_Type__c,
    CAST(enc.ucinn_ascendv2__Address_1_Line_1__c AS VARCHAR(255))		AS ucinn_ascendv2__Address_1_Line_1__c,
    CAST(enc.ucinn_ascendv2__Address_1_Line_2__c AS VARCHAR(255))		AS ucinn_ascendv2__Address_1_Line_2__c,
    CAST(enc.ucinn_ascendv2__City_1__c AS VARCHAR(255))							AS ucinn_ascendv2__City_1__c,
    CAST(enc.ucinn_ascendv2__State_1__c AS VARCHAR(255))							AS ucinn_ascendv2__State_1__c,
    CAST(enc.ucinn_ascendv2__Postal_Code_1__c AS VARCHAR(40))				AS ucinn_ascendv2__Postal_Code_1__c,
    CAST(enc.ucinn_ascendv2__Country_1__c AS VARCHAR(255))						AS ucinn_ascendv2__Country_1__c,
    CAST(enc.ucinn_ascendv2__Phone_1_Type__c AS VARCHAR(40))				AS ucinn_ascendv2__Phone_1_Type__c,
    CAST(enc.ucinn_ascendv2__Phone_1__c AS VARCHAR(255))						AS ucinn_ascendv2__Phone_1__c,
    CAST(enc.ucinn_ascendv2__Email_1_Type__c AS VARCHAR(40))					AS ucinn_ascendv2__Email_1_Type__c,
    CAST(enc.ucinn_ascendv2__Email_1__c AS VARCHAR(70))							AS ucinn_ascendv2__Email_1__c,
	CAST(enc.ucinn_ascendv2__Gender__c AS VARCHAR(40))							AS ucinn_ascendv2__Gender__c,
	enc.ucinn_ascendv2__Birthdate__c,
	enc.ucinn_ascendv2__Deceased_Date__c,
    enc.Load_Dtm
FROM pt enc
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

ORDER BY	CAST(enc.ucinn_ascendv2__MRN__c AS VARCHAR(100))	
END

GO


