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
**		CLARITY.dbo.PAT_ENC_HSP
**		CLARITY.EPIC_UTIL.CSA_PAT_ENC_HSP
**		CLARITY.dbo.PAT_ENC
**		CLARITY.dbo.CLARITY_SER
**		CLARITY.dbo.PATIENT
**		CLARITY.dbo.clarity_ser_2
**		CLARITY.dbo.ZC_DISP_ENC_TYPE
**		CLARITY.dbo.ZC_SEX
**		CLARITY.dbo.ZC_STATE
**		CLARITY.dbo.PAT_ADDRESS
**		CLARITY.dbo.HSP_ACCOUNT
**		CLARITY.dbo.ACCOUNT
**		CLARITY.dbo.ZC_STATE
**		CLARITY.dbo.ZC_ACCT_BASECLS_HA
**		CLARITY.dbo.ACCOUNT_STATUS
**		CLARITY.dbo.HSP_ACCT_DX_LIST
**		CLARITY.dbo.CLARITY_EDG
**		CLARITY.dbo.ACCOUNT_FPL_INFO
**		clarity_App.Rptg.ADT_Red_Folder_Extract
**		CLARITY.dbo.PATIENT_4
**		CLARITY.dbo.ACCT_GUAR_PAT_INFO
**		CLARITY.dbo.PATIENT_TYPE
**		CLARITY..HSP_ACCT_ATND_PROV
**		CLARITY..V_SCHED_APPT
**		CLARITY_App.Rptg.UA_GPP_Loc_to_Hosp
**		CLARITY..CLARITY_EPM
**		CLARITY..ZC_FIN_CLASS
**		CLARITY..PAT_ENC_3
**		CLARITY..CLARITY_EPP
**		CLARITY_App.Rptg.UA_GPP_Guar_Rltn_to_Pt
**		CLARITY_App.Rptg.UA_GPP_Department_Mapping
**		CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_All
**		[CLARITY_App].[Mapping].[Epic_Dept_Groupers]
**		[CLARITY_App].[Mapping].Ref_Clinical_Area_Map
**		[CLARITY_App].[Mapping].Ref_Service_Map
**		[CLARITY_App].[Mapping].Ref_Organization_Map
**		CLARITY..CLARITY_SER_SPEC
**		CLARITY..ZC_SPECIALTY
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
				10/25/2024	-Tom B.  Update per spec changes
				11/06/2024	-Tom B.  Update per spec changes
				12/01/2024	-Tom B.  Add @LOC_TO_HOSP mapping table; update logic for determining appointment status
				12/05/2024	-Tom B.  Use external mapping tables (Rptg.UA_GPP_Loc_to_Hosp, Rptg.UA_GPP_Guar_Rltn_to_Pt, Rptg.UA_GPP_Department_Mapping)
				12/06/2024	-Tom B.  Report 'Self' guarantor relationships
				12/11/2025	-Tom B.  Edit patient external system id; include Red Folder-assigned patients
				12/18/2025	-Tom B.  Edit provider external system id
				01/31/2025	-Tom B.  Transform generated NULL values
				02/05/2025	-Tom B.  Update Ascend column name for PROV_ID value
				02/28/2025	-Tom B.  Transform discharge datetime
				03/05/2025	-Tom B.  Add patient, guarantor, and physician identifiers
				08/18/2025	-Tom B.  Exclude encounters that do not have a documented admission date
				09/26/2025	-Tom B.  Correct join to table CLARITY.dbo.ACCT_GUAR_PAT_INFO; filter on UA_GPP_Department_Mapping values

********************************************************************************************/

-- =============================================
--EXEC [ETL].[uspSrc_UA_GratefulPatientProgram_IP]

ALTER   PROCEDURE [ETL].[uspSrc_UA_GratefulPatientProgram_IP] 
	-- Add the parameters for the stored procedure here
	--@StartDate DATETIME, --DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)), 
	--@EndDate DATETIME --= GETDATE()
	@LookBackDays INTEGER = 3
AS

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	BEGIN
    
	--DECLARE @StartDate DATETIME;
	--DECLARE @EndDate DATETIME;
	--DECLARE @LookBackDays INTEGER

		/*
	--set date parameter
	IF @Startdate IS NULL
    AND @Enddate IS NULL
    BEGIN
 
        SELECT @StartDate= DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)); --last five years
        SELECT  @EndDate	=GETDATE(); --Today
    END;
	*/
	--SET @LookBackDays = 3

/************************************************************/
;WITH IPCTE AS
(
SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	 RIGHT('000000000000'+CAST(hsp.PAT_ENC_CSN_ID AS VARCHAR(12)),12)		AS 	'ucinn_ascendv2__External_System_ID__c'
	,clpt.sk_Dim_Pt
	,CASE WHEN clpt.sk_Dim_Pt <> -1 THEN RIGHT('0000000'+CAST(clpt.sk_Dim_Pt AS VARCHAR(8)),8) ELSE REPLACE(TRANSLATE(TRIM(pt.PAT_NAME),', ','__'	),'.','') END AS 	'ESI_3'
	,RIGHT('0000000000'+CAST(gacc.ACCOUNT_ID AS VARCHAR(10)),10)	AS	'ucinn_ascendv2__Guarantor_ID__c'
	,ser.PROV_ID																									AS	'ucinn_ascendv2__Physician_ID__c'
	,CASE WHEN rship.GUAR_REL_TO_PAT_C = 15 THEN 'Self' ELSE ascrel.ASCEND_VALUE END 	AS	'ucinn_ascendv2__Guarantor_Relationship__c'
	,'In-Patient'																										AS	'ucinn_ascendv2__Appointment_Type__c'
	,CASE WHEN loc.LOC_ID IS NULL THEN 'UVA Hospital' ELSE loc.ASCEND_VALUE END AS 	'ucinn_ascendv2__Appointment_Location__c'
	,ascdep.ASCEND_VALUE																				AS	'ucinn_ascendv2__Department_Formula__c'
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL THEN 'Completed'
		WHEN hsp.HOSP_DISCH_TIME IS NULL THEN 'Admitted'
		ELSE 'Completed'
	 END																												AS	'ucinn_ascendv2__Appointment_Status__c'
	,'Health System'																								AS	'ucinn_ascendv2__Data_Source__c'
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
	 END																												AS	'ucinn_ascendv2__Admit_Date_Time__c'
	,hsp.HOSP_DISCH_TIME																				AS	'ucinn_ascendv2__Discharge_Date_Time__c'

	,acc.ACCT_FIN_CLASS_C
	,acc.ACCT_SLFPYST_HA_C
	,fpl.FPL_STATUS_CODE_C
	,fpl.FPL_EFF_DATE
	,accst.ACCOUNT_STATUS_C
	,pt.BIRTH_DATE
	,edg.ICD9_CODE
	,x.UPDATE_DATE																							AS	'Update_Date'
	,GETDATE()																									AS	'Load_Dtm'
	,enc.ENC_TYPE_C
	,typ.DISP_ENC_TYPE_C
	,typ.NAME AS ENC_TYPE_NAME

FROM  CLARITY.dbo.PAT_ENC_HSP hsp

INNER JOIN
(
SELECT
	csaseq.PAT_ENC_CSN_ID,
    csaseq.UPDATE_DATE
FROM
(
SELECT
	csa.PAT_ENC_CSN_ID,
    csa.UPDATE_DATE,
	ROW_NUMBER() OVER(PARTITION BY csa.PAT_ENC_CSN_ID ORDER BY csa.UPDATE_DATE DESC) AS updseq
FROM
(
SELECT csa.PAT_ENC_CSN_ID
             , csa._UPDATE_DT AS UPDATE_DATE
			FROM CLARITY.EPIC_UTIL.CSA_PAT_ENC_HSP AS csa
			WHERE csa._UPDATE_DT >= CONVERT(DATE,DATEADD(dd,-@LookBackDays,GETDATE()))
			GROUP BY csa.PAT_ENC_CSN_ID, csa._UPDATE_DT
) csa
) AS csaseq
WHERE csaseq.updseq = 1
) AS x
  ON x.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY.dbo.PAT_ENC enc			ON enc.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = hsp.ADMISSION_PROV_ID 
			INNER JOIN CLARITY.dbo.PATIENT pt						ON pt.PAT_ID = hsp.PAT_ID
			INNER JOIN CLARITY.dbo.clarity_ser_2 ser2					ON ser2.PROV_ID = ser.PROV_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE typ		ON typ.DISP_ENC_TYPE_C=enc.ENC_TYPE_C
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
				AND rship.PAT_ID = hsp.PAT_ID
			LEFT OUTER JOIN CLARITY.dbo.PATIENT_TYPE AS pt_typ
			  ON pt_typ.PAT_ID = pt.PAT_ID
			  AND pt_typ.PATIENT_TYPE_C = '6'  --prisoner/inmate

			LEFT OUTER JOIN CLARITY..HSP_ACCT_ATND_PROV	haatn ON haatn.HSP_ACCOUNT_ID = acc.HSP_ACCOUNT_ID		-- Admitting Provider
									AND	   haatn.LINE = 1

			LEFT OUTER JOIN CLARITY..V_SCHED_APPT appt	ON appt.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY_App.Rptg.UA_GPP_Loc_to_Hosp loc		ON loc.LOC_ID = acc.ADM_LOC_ID

			LEFT OUTER JOIN CLARITY..CLARITY_EPM payor	ON payor.PAYOR_ID = acc.PRIMARY_PAYOR_ID

			LEFT OUTER JOIN CLARITY..ZC_FIN_CLASS zfc	ON zfc.FIN_CLASS_C = payor.FINANCIAL_CLASS

			LEFT OUTER JOIN CLARITY..PAT_ENC_3 enc3	ON enc3.PAT_ENC_CSN = hsp.PAT_ENC_CSN_ID

			LEFT OUTER JOIN CLARITY..CLARITY_EPP epp	ON epp.BENEFIT_PLAN_ID = acc.PRIMARY_PLAN_ID

			LEFT OUTER JOIN CLARITY_App.Rptg.UA_GPP_Guar_Rltn_to_Pt ascrel	 ON ascrel.GUAR_REL_TO_PAT_C = rship.GUAR_REL_TO_PAT_C

			LEFT OUTER JOIN CLARITY_App.Rptg.UA_GPP_Department_Mapping ascdep	ON	ascdep.DEPARTMENT_ID = hsp.DEPARTMENT_ID

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

			LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_Pt AS clpt	ON clpt.Clrt_PAT_ID = pt.PAT_ID

WHERE 1 = 1
AND hsp.HSP_ACCOUNT_ID IS NOT NULL
AND hsp.HOSP_ADMSN_TIME IS NOT NULL -- TMB 20250818
AND ser.PROV_TYPE <>'Resource'
--AND rd.Acc_ID IS NULL --not in red folder extract
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
AND ascdep.ASCEND_VALUE NOT IN ('z_Not Translated because not in File - may add in Future State','zz_Not Translated - should not be in file')
)

SELECT
	CAST(ucinn_ascendv2__External_System_ID__c AS VARCHAR(255))						AS 	ucinn_ascendv2__External_System_ID__c,
    CAST('UVAH-'+ESI_3+'-Patient-'+CONVERT(VARCHAR(8),CAST(enc.Load_Dtm AS DATE),112) AS VARCHAR(255)) AS 	ucinn_ascendv2__Patient_Interim__c,
    ISNULL(CONVERT(VARCHAR(255),'UVAH-'+ucinn_ascendv2__Guarantor_ID__c+'-Guarantor-'+CONVERT(VARCHAR(8),CAST(enc.Load_Dtm AS DATE),112)	),'')	AS	ucinn_ascendv2__Guarantor_Interim__c,
    ISNULL(CONVERT(VARCHAR(255),'UVAH-'+enc.ucinn_ascendv2__Physician_ID__c+'-Provider-'+CONVERT(VARCHAR(8),CAST(enc.Load_Dtm AS DATE),112)),'') AS 	ucinn_ascendv2__Attending_Interim__c,
    CAST(ESI_3 AS VARCHAR(255))																										AS ucinn_ascendv2__Patient_ID__c,
    ISNULL(CONVERT(VARCHAR(255),ucinn_ascendv2__Guarantor_ID__c),'')									AS ucinn_ascendv2__Guarantor__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Physician_ID__c),'')								AS ucinn_ascendv2__Attending_Physician__c,
    ISNULL(CONVERT(VARCHAR(100),enc.ucinn_ascendv2__Guarantor_Relationship__c),'')			AS ucinn_ascendv2__Guarantor_Relationship__c,
    ISNULL(CONVERT(VARCHAR(50),enc.ucinn_ascendv2__Appointment_Type__c),'')						AS ucinn_ascendv2__Appointment_Type__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Appointment_Location__c),'')				AS ucinn_ascendv2__Appointment_Location__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Department_Formula__c),'')				AS ucinn_ascendv2__Department_Formula__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Appointment_Status__c),'')					AS ucinn_ascendv2__Appointment_Status__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Data_Source__c),'')								AS ucinn_ascendv2__Data_Source__c,
    ucinn_ascendv2__Admit_Date_Time__c,
    ISNULL(CONVERT(VARCHAR(23), CASE WHEN ucinn_ascendv2__Discharge_Date_Time__c IS NOT NULL
				THEN ucinn_ascendv2__Discharge_Date_Time__c
			   WHEN enc.ucinn_ascendv2__Appointment_Status__c IN ('Completed')
				THEN ucinn_ascendv2__Admit_Date_Time__c
			   ELSE NULL
				END, 121),'')																															AS ucinn_ascendv2__Discharge_Date_Time__c,
    enc.Load_Dtm
FROM IPCTE enc
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

--ORDER BY	 CAST('UVAH-'+ESI_3+'-Patient-'+CONVERT(VARCHAR(8),CAST(enc.Load_Dtm AS DATE),112) AS VARCHAR(255)), enc.ucinn_ascendv2__Admit_Date_Time__c
END

GO


