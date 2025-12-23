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
WHY :	Detail updates for patient encounter provider information
AUTHOR:	Tom Burgan
SPEC:	
--------------------------------------------------------------------------------------------
INPUTS: 
**		CLARITY.dbo.PAT_ENC
**		CLARITY.EPIC_UTIL.CSA_PAT_ENC
**		CLARITY.dbo.ZC_DISP_ENC_TYPE
**		CLARITY.dbo.V_SCHED_APPT
**		CLARITY.dbo.PAT_ENC_HSP
**		CLARITY.EPIC_UTIL.CSA_PAT_ENC_HSP
**		CLARITY.dbo.ZC_HOSP_ADMSN_TYPE
**		CLARITY.dbo.CLARITY_SER
**		CLARITY_App.Rptg.vwDim_Clrt_SERsrc
**		CLARITY_App.dbo.Dim_Physcn
**		CLARITY_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv
**		CLARITY_App.Rptg.UA_GPP_Provider_Division_Mapping
	
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
		**		09/20/2024  -Tom B.  Create stored procedure
				10/25/2024	-Tom B.  Update per spec changes
				11/04/2024	-Tom B.  Update per spec changes
				12/01/2024	-Tom B.  Update per spec changes
				12/18/2024	-Tom B.  Update External System Id
				01/28/2025	-Tom B.  Remove Atn_Dr_Last_Update_Dt, ATN_DR_NAME columns;
												    transform provider name, generated NULL values
				02/05/2025	-Tom B.  Update Ascend column name for PROV_ID value
				02/12/2025	-Tom B.  Add appointment statuses (1 Scheduled, 3 Canceled, 4 No Show, 5 Left without seen, 102 Walk-in)
				02/28/2025	-Tom B.  Remove column "Affiliation Data Source"
				04/28/2025	-Tom B.  Add provider credentials column (uva_Professional_Designation__c)
				12/04/2025	JSC3H	Changed query lookback to 1 day per Grateful patient team
********************************************************************************************/

-- =============================================
--EXEC [ETL].[uspSrc_UA_GratefulPatientProgram_Provider]

ALTER   PROCEDURE [ETL].[uspSrc_UA_GratefulPatientProgram_Provider] 
	-- Add the parameters for the stored procedure here
	--@StartDate DATETIME, --DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)), 
	--@EndDate DATETIME --= GETDATE()
	--@LookBackDays INTEGER = 3
	@LookBackDays INTEGER = 1
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

;WITH ptprov AS 
(
SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	 'New'																												AS	'ucinn_ascendv2__Status__c'
	,'FALSE'																											AS	'uva_Is_Patient__c'
	,'FALSE'																											AS	'uva_Is_Guarantor__c'
	,ser.PROV_ID																									AS	'ucinn_ascendv2__Physician_ID__c'
	,COALESCE(provdvsn.ASCEND_VALUE,'UNKNOWN')								AS	'uva_Dept_Division__c'
	,'TRUE'																											AS	'uva_Is_Provider__c'
	,'Health System'																								AS	'ucinn_ascendv2__Data_Source__c'
	,'UVA Health'																									AS	'Name Type'
	,dbo.fnToProperCase(
	 CAST (CASE
	    WHEN CHARINDEX(',',ser.PROV_NAME) > 2 THEN
	        CASE
	        WHEN CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME))) > 2 THEN
	            LTRIM(SUBSTRING(ser.PROV_NAME,CHARINDEX(',',ser.PROV_NAME)+1,CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME)))))
	        ELSE LTRIM(SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+1, LEN(ser.PROV_NAME)))
	        END
	    ELSE ''
	    END AS VARCHAR(255))
	 )																													AS 'ucinn_ascendv2__First_Name__c'
	,dbo.fnToProperCase(
	 CAST(CASE
	       WHEN CHARINDEX(',',ser.PROV_NAME) > 2 THEN -- found lastname, now look for space in first name to split middle name out
	         CASE
	           WHEN CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME))) > 2 THEN 
	             LTRIM(SUBSTRING(SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME)), 
	                       CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME)))+1,
	                       LEN(ser.PROV_NAME)))
	         ELSE ''
	       END  
	       ELSE ''
	      END AS VARCHAR(255))
	 )																															AS 'ucinn_ascendv2__Middle_Name__c'
	,dbo.fnToProperCase(
	 CAST(CASE                
	       WHEN CHARINDEX(',',ser.PROV_NAME) > 2 THEN
	         CASE 
	           WHEN CHARINDEX(' JR', SUBSTRING(ser.PROV_NAME,1, CHARINDEX(',', ser.PROV_NAME)-1)) > 2 THEN
	             LTRIM(SUBSTRING(ser.PROV_NAME,1, CHARINDEX(' JR', SUBSTRING(ser.PROV_NAME,1, CHARINDEX(',', ser.PROV_NAME)-1))-1))
	           WHEN CHARINDEX(' SR', SUBSTRING(ser.PROV_NAME,1, CHARINDEX(',', ser.PROV_NAME)-1)) > 2 THEN
	             LTRIM(SUBSTRING(ser.PROV_NAME,1, CHARINDEX(' SR', SUBSTRING(ser.PROV_NAME,1, CHARINDEX(',', ser.PROV_NAME)-1))-1))
	           WHEN CHARINDEX(' I', SUBSTRING(ser.PROV_NAME,1, CHARINDEX(',', ser.PROV_NAME)-1)) > 2 THEN
	             LTRIM(SUBSTRING(ser.PROV_NAME,1, CHARINDEX(' I', SUBSTRING(ser.PROV_NAME,1, CHARINDEX(',', ser.PROV_NAME)-1))-1))
	              ELSE LTRIM(SUBSTRING(ser.PROV_NAME, 1, CHARINDEX(',',ser.PROV_NAME)-1))
	         END
	       ELSE LTRIM(ser.PROV_NAME)
	     END AS VARCHAR(255))
	 )																														AS	'ucinn_ascendv2__Last_Name__c'
	,REPLACE(REPLACE(REPLACE(zld.NAME,', ',';'),'.',''),' ',';')							AS	'uva_Professional_Designation__c'	
	,COALESCE(CAST(ser.ACTIVE_STATUS AS VARCHAR(25)),'NA')				AS	'uva_Provider_Status__c'
	,'Employee'																										AS	'uva_Constituent_Role__c'
	,NULL																												AS	'ucinn_ascendv2__Employer_Account__c'
	,'Current'																											AS	'ucinn_ascendv2__Employment_Status__c'
	,'Business'																										AS	'ucinn_ascendv2__Email_1_Type__c'
	,physcn.Email																									AS	'ucinn_ascendv2__Email_1__c'
	,pe.UPDATE_DATE																							AS	'Update_Date'
	,GETDATE()																									AS	'Load_Dtm'

FROM
(
SELECT DISTINCT
	csa.PROV_ID,
	csa.ENC_TYPE_C,
	csa.ENC_TYPE_NAME,
	csa.ADMSN_TIME,
	csa.APPT_STATUS_C,
	csa.UPDATE_DATE
FROM
(
-- OP/HOV
SELECT
	hsp.VISIT_PROV_ID AS PROV_ID,
	hsp.ENC_TYPE_C,
	zdet.NAME AS ENC_TYPE_NAME,
	COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE) AS ADMSN_TIME,
	hsp.APPT_STATUS_C,
	x.UPDATE_DATE
FROM  CLARITY.dbo.PAT_ENC hsp
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
			FROM CLARITY.EPIC_UTIL.CSA_PAT_ENC AS csa
			WHERE csa._UPDATE_DT >= CONVERT(DATE,DATEADD(dd,-@LookBackDays,GETDATE()))
			GROUP BY csa.PAT_ENC_CSN_ID, csa._UPDATE_DT
) csa
) AS csaseq
WHERE csaseq.updseq = 1
) AS x
  ON x.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet		ON zdet.DISP_ENC_TYPE_C = hsp.ENC_TYPE_C
LEFT OUTER JOIN CLARITY.dbo.V_SCHED_APPT appt	ON appt.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
UNION ALL
-- IP
SELECT
	hsp.ADMISSION_PROV_ID AS PROV_ID,
	NULL AS ENC_TYPE_C,
	'IP-'+TRIM(zhat.NAME) AS ENC_TYPE_NAME,
	hsp.HOSP_ADMSN_TIME AS ADMSN_TIME,
	NULL AS APPT_STATUS_C,
	y.UPDATE_DATE
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
) AS y
  ON y.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat				ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C		
) csa
) pe

			LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = pe.PROV_ID

			LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER_2 ser2		ON ser2.PROV_ID = pe.PROV_ID

			LEFT OUTER JOIN CLARITY.dbo.ZC_LICENSE_DISPLAY zld		ON zld.LICENSE_DISPLAY_C = ser2.CUR_CRED_C 

			LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_SERsrc cser	ON cser.PROV_ID = ser.PROV_ID

			LEFT OUTER JOIN
			(
			SELECT
				sk_Dim_Physcn,
				Email
			FROM CLARITY_App.dbo.Dim_Physcn
			WHERE current_flag = 1
			) physcn		ON physcn.sk_Dim_Physcn = cser.sk_Dim_Physcn

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

			LEFT OUTER JOIN Rptg.UA_GPP_Provider_Division_Mapping provdvsn ON provdvsn.Epic_Financial_Division = dvsn.Epic_Financial_Division

WHERE 1 = 1
AND (
			(
				((pe.APPT_STATUS_C IS NOT NULL AND pe.APPT_STATUS_C IN ('1','2','3','4','5','6','102')) --ONLY SCHEDULED/COMPLETED/CANCELED/NO SHOW/LEFT WITHOUT SEEN/ARRIVED/WALK-IN STATUS
				OR (pe.APPT_STATUS_C IS NULL AND pe.ADMSN_TIME IS NOT NULL))
			)
		)
AND ser.Staff_Resource = 'Person'
AND ser.PROV_ID <> '000000'
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
	CAST('UVAH-'+enc.ucinn_ascendv2__Physician_ID__c+'-Provider-'+CONVERT(VARCHAR(8),CAST(enc.Load_Dtm AS DATE),112) AS VARCHAR(255)) AS 	ucinn_ascendv2__External_System_ID__c,
	ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Status__c),'')							AS ucinn_ascendv2__Status__c,
    ISNULL(CONVERT(VARCHAR(255),enc.uva_Is_Patient__c),'')											AS uva_Is_Patient__c,
    ISNULL(CONVERT(VARCHAR(255),enc.uva_Is_Guarantor__c),'')										AS uva_Is_Guarantor__c,
	ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Physician_ID__c),'')					AS ucinn_ascendv2__Physician_ID__c,
	ISNULL(CONVERT(VARCHAR(255),enc.uva_Dept_Division__c),'')										AS uva_Dept_Division__c,
    ISNULL(CONVERT(VARCHAR(255),enc.uva_Is_Provider__c),'')				 							AS uva_Is_Provider__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Data_Source__c),'')					AS ucinn_ascendv2__Data_Source__c,
    ISNULL(CONVERT(VARCHAR(255),enc.[Name Type]),'')				 										AS [Name Type],
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__First_Name__c),'')					AS ucinn_ascendv2__First_Name__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Middle_Name__c),'')				AS ucinn_ascendv2__Middle_Name__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Last_Name__c),'')					AS ucinn_ascendv2__Last_Name__c,
    ISNULL(CONVERT(VARCHAR(255),enc.uva_Professional_Designation__c),'')					AS uva_Professional_Designation__c,
    ISNULL(CONVERT(VARCHAR(255),enc.uva_Provider_Status__c),'')				 					AS uva_Provider_Status__c,
    ISNULL(CONVERT(VARCHAR(50),enc.uva_Constituent_Role__c),'')									AS uva_Constituent_Role__c,
    ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Employer_Account__c),'')		AS ucinn_ascendv2__Employer_Account__c,
    ISNULL(CONVERT(VARCHAR(30),enc.ucinn_ascendv2__Employment_Status__c),'')		AS ucinn_ascendv2__Employment_Status__c,
    ISNULL(CONVERT(VARCHAR(40),enc.ucinn_ascendv2__Email_1_Type__c),'')					AS ucinn_ascendv2__Email_1_Type__c,
    ISNULL(CONVERT(VARCHAR(70),enc.ucinn_ascendv2__Email_1__c),'')							AS ucinn_ascendv2__Email_1__c,
    enc.Load_Dtm
FROM ptprov enc

--ORDER BY ISNULL(CONVERT(VARCHAR(255),enc.ucinn_ascendv2__Physician_ID__c),'')
END

GO


