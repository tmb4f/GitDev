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
**		CLARITY_App.Rptg.vwDim_Clrt_SERsrc cser
	
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

********************************************************************************************/

-- =============================================
--EXEC [ETL].[uspSrc_UA_GratefulPatientProgram_Provider]

--ALTER   PROCEDURE [ETL].[uspSrc_UA_GratefulPatientProgram_Provider] 
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
	 ser.PROV_ID AS Provider_Id
	,ser.PROV_NAME AS Provider_Name
	,dvsn.Epic_Financial_Division
	,ser.PROV_TYPE AS Provider_Type
	,ser2.PRIMARY_DEPT_ID AS Primary_Department_Id
	,dep.DEPARTMENT_NAME AS Primary_Department_Name
	,o.organization_name AS Reporting_Hierarchy_Organization
	,s.service_name AS Reporting_Hierarchy_Service
	,c.clinical_area_name AS Reporting_Hierarchy_Clinical_Area

FROM
(
SELECT DISTINCT
	csa.PROV_ID,
	csa.PROV_TYPE,
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
	ser.PROV_TYPE,
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
             , csa.CONTACT_DATE AS UPDATE_DATE
			FROM CLARITY.dbo.PAT_ENC AS csa
			WHERE csa.CONTACT_DATE >= CONVERT(DATE,DATEADD(YEAR,-5,GETDATE()))
			GROUP BY csa.PAT_ENC_CSN_ID, csa.CONTACT_DATE) csa
) AS csaseq
WHERE csaseq.updseq = 1
) AS x
  ON x.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet		ON zdet.DISP_ENC_TYPE_C = hsp.ENC_TYPE_C
LEFT OUTER JOIN CLARITY.dbo.V_SCHED_APPT appt	ON appt.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser ON ser.PROV_ID = hsp.VISIT_PROV_ID
UNION ALL
-- IP
SELECT
	hsp.ADMISSION_PROV_ID AS PROV_ID,
	ser.PROV_TYPE,
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
             , csa.CONTACT_DATE AS UPDATE_DATE
			FROM CLARITY.dbo.PAT_ENC_HSP AS csa
			WHERE csa.CONTACT_DATE  >= CONVERT(DATE,DATEADD(YEAR,-5,GETDATE()))
			GROUP BY csa.PAT_ENC_CSN_ID, csa.CONTACT_DATE
) csa
) AS csaseq
WHERE csaseq.updseq = 1
) AS y
  ON y.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat				ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C	
LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser ON ser.PROV_ID = hsp.ADMISSION_PROV_ID	
) csa
) pe

			LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = pe.PROV_ID
			INNER JOIN CLARITY.dbo.clarity_ser_2 ser2					ON ser2.PROV_ID = ser.PROV_ID

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

			LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep	ON dep.DEPARTMENT_ID = ser2.PRIMARY_DEPT_ID

			LEFT JOIN [CLARITY_App].[Mapping].[Epic_Dept_Groupers] g ON ser2.PRIMARY_DEPT_ID = g.epic_department_id
			LEFT JOIN [CLARITY_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
			LEFT JOIN [CLARITY_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
			LEFT JOIN [CLARITY_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id

WHERE 1 = 1
AND (
			(
				((pe.APPT_STATUS_C IS NOT NULL AND pe.APPT_STATUS_C IN ('2','6')) --ONLY COMPLETED/ARRIVED STATUS 
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

SELECT --DISTINCT
	enc.Provider_Id,
    enc.Provider_Name,
    enc.Epic_Financial_Division,
    enc.Provider_Type,
    enc.Primary_Department_Id,
    enc.Primary_Department_Name,
    enc.Reporting_Hierarchy_Organization,
    enc.Reporting_Hierarchy_Service,
    enc.Reporting_Hierarchy_Clinical_Area
FROM ptprov enc

ORDER BY enc.Provider_Name
END

GO


