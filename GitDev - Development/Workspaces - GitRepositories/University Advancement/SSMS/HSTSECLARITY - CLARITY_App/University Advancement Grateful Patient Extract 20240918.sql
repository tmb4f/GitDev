USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================

/*******************************************************************************************
WHAT:	Grateful Patient Program data extract 
WHO :	University Advancement
WHEN:	Daily
WHY :	Detail updates for completed encounters with "Person" resources
AUTHOR:	Tom Burgan
SPEC:	
--------------------------------------------------------------------------------------------
INPUTS: 	        
**		dbo.PAT_ENC_HSP hsp	
**		dbo.PAT_ENC enc	
**		dbo.CLARITY_SER ser
**		
**		dbo.PATIENT pt						
**		dbo.clarity_ser_2 ser2				
**		ZC_DISP_ENC_TYPE typ		
**		dbo.ZC_SEX sex					
**		dbo.ZC_STATE st						
**		dbo.PAT_ADDRESS ad1					
**		dbo.PAT_ADDRESS ad2				
**		dbo.ACCOUNT gacc						
**		ZC_STATE guarst																		
**		dbo.HSP_ACCOUNT acc				
**		dbo.ZC_ACCT_BASECLS_HA bcls		
***		dbo.ACCOUNT_STATUS	accst		
**		dbo.HSP_ACCT_DX_LIST dx			
**		dbo.CLARITY_EDG edg				
**		dbo.ACCOUNT_FPL_INFO fpl		
**		clarity_App.[dbo].[ADT_Red_Folder_Extract] Rd				
**		dbo.PATIENT_4 pt4				
**		dbo.ACCT_GUAR_PAT_INFO rship	
**		dbo.ZC_GUAR_REL_TO_PAT rel
**		dbo.PATIENT_TYPE pt_typ
**		dbo.ZC_PATIENT_TYPE zpt

**		Strata.uspSrc_Clrt_HB_Encounters_Stage
	
	
		
OUTPUTS: 
   	1) SEQUENCE:				File 1 of 1
   	2) FILE NAMING CONVENTION:	Grateful_Patient_Program
   	3) OUTPUT TYPE:				TABLE 
   	4) TRANSFER METHOD:			sFTP
   	5) OUTPUT LOCATION:			HSTSDSSQLDM
								
   	6) FREQUENCY:				Daily
   	7) QUERY LOOKBACK PERIOD:	Last 5 years
   	8) FILE SPECIFIC NOTES:	
		**     Pulls last five years worth of data 
		**		search for last encounter
		**		replace the table values to reflect only last encounter for the patient 
		
		**		Added Identity_ser_id to get SMS_ID for physican  to be able to report backwards
		**		07/16/2018  -Mali A.  Check for last encounter per patient per provider (Provider level newly added)
		**		02/10/2019 - Mali A. Add filter to only consider last encounter that was completed or arrived status
		**		09/12/2023 - JSC - added filter to remove prisoner/inmate patient type

*********************************************************/

-- =============================================
--EXEC [ETL].[uspSrc_ADT_GratefulPatientProgram]

--ALTER PROCEDURE [ETL].[uspSrc_ADT_GratefulPatientProgram]
----CREATE PROCEDURE [HSCDOM\GHA4R].[uspSrc_GratefulPatientProgram]    
--	-- Add the parameters for the stored procedure here
--	--@StartDate DATETIME, --DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)), 
--	--@EndDate DATETIME --= GETDATE()
--AS

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	BEGIN
    
	DECLARE @StartDate DATETIME;
	DECLARE @EndDate DATETIME;

			--SET  @StartDate= DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)); --last five years
			--SET  @EndDate	=CONVERT(DATETIME,CONVERT(VARCHAR(10),DATEADD(dd,-1,GETDATE()),101) + ' 23:59:59'); --Yesterday

			SET  @StartDate= '9/1/2024 00:00:00'
			SET  @EndDate	= '9/17/2024 23:59:59'

		/*
	--set date parameter
	IF @Startdate IS NULL
    AND @Enddate IS NULL
    BEGIN
 
        SELECT @StartDate= DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)); --last five years
        SELECT  @EndDate	=GETDATE(); --Today
    END;
	*/
    -- Insert statements for procedure here
/*	
IF OBJECT_ID('CLARITY_App_Dev.dbo.ADT_GPP_Encounters') IS NOT NULL
DROP TABLE Clarity_App_Dev.dbo.ADT_GPP_Encounters 
CREATE TABLE Clarity_App_Dev.dbo.ADT_GPP_Encounters
			(
			ADm_Dt				DATETIME
			,Atn_Dr				VARCHAR(30)
			,Atn_Dr_SMS_ID		VARCHAR(30) --New column added 9/5/2017
			,Atn_Dr_LName		VARCHAR(30)
			,Atn_Dr_FName		VARCHAR(25)
			,Atn_Dr_MI			VARCHAR(12)
			,Atn_Dr_Type		VARCHAR(30)
			,Atn_Dr_Status		VARCHAR(25)
			,Atn_Dr_Last_Update_Dt	DATETIME
			,Source				VARCHAR(4)
			,IO_Flag			CHAR(1)
			,Sex				CHAR(1)
			,Birth_Dt			DATETIME
			,Age				SMALLINT
			,Age_Group			VARCHAR(5)
			,Pt_FName_MI		VARCHAR(30)
			,Pt_LName			VARCHAR(30)
			,CURR_PT_ADDR1		VARCHAR(20)
			,CURR_PT_ADDR2		VARCHAR(20)
			,CURR_PT_CITY		VARCHAR(15)
			,CURR_PT_STATE		VARCHAR(2)
			,CURR_PT_ZIP		VARCHAR(5)
			,CURR_PT_PHONE		VARCHAR(15)
			,GUAR_FNAME			VARCHAR(30)
			,GUAR_MNANE			VARCHAR(30)
			,GUAR_LNAME			VARCHAR(30)
			,GUAR_ADDR1			VARCHAR(20)
			,GUAR_ADDR2			VARCHAR(20)
			,GUAR_CITY			VARCHAR(15)
			,GUAR_STATE			VARCHAR(2)
			,GUAR_ZIP			VARCHAR(5)
			,GUAR_PHONE			VARCHAR(15)
			,GUAR_TO_PT			VARCHAR(1)
			,Status				VARCHAR(1)
			,Email				VARCHAR(30)
			,PAT_ID				VARCHAR(15)  --Changed from sk_Dim_Pt to PAT_ID
			,load_date_time		DATETIME
		
			)
*/
/*
/*******QUERY TO GET LAST ENCOUNTER*********************/
;WITH Lenc_cte AS 
(
SELECT 
	hsp.PAT_ID 'Pat_ID'
	,hsp.VISIT_PROV_ID 'Prov_ID'
	,MAX(hsp.PAT_ENC_DATE_REAL) 'Cntc_Dt'
FROM  CLARITY.dbo.PAT_ENC hsp		--ON hsp.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID

WHERE 
(hsp.CONTACT_DATE >=@StartDate		AND		hsp.CONTACT_DATE<@EndDate)
				OR 
				(hsp.HOSP_ADMSN_TIME  >=@StartDate	AND		hsp.HOSP_ADMSN_TIME <@EndDate)
				AND hsp.APPT_STATUS_C IN ('2','6') --ONLY COMPLETED/ARRIVED STATUS 

GROUP BY hsp.PAT_ID,hsp.VISIT_PROV_ID
)
*/
/**********************************************/

/******Atn_Dr_Last_Update_Dt************/
/*
,lst_cte AS 
(
SELECT 
	 enc.PAT_ID, 
	 enc.PAT_ENC_CSN_ID 'last_encounter'
	 ,enc.HSP_ACCOUNT_ID 'Hsp_Acct'
	 ,CASE WHEN enc.HOSP_ADMSN_TIME IS NULL
		THEN enc.CONTACT_DATE  --for Outpatient
		ELSE enc.HOSP_ADMSN_TIME  --for Inpatient
		END 'Adm_Dt'
	--,enc.CONTACT_DATE 'Contact_Dt'
	--,enc.HOSP_ADMSN_TIME 'Adm_Dt'
	,enc.VISIT_PROV_ID 'Prov_ID'
	,enc.ACCOUNT_ID 'Guar_ID'
	,enc.ENC_TYPE_C
FROM Lenc_cte
	INNER JOIN CLARITY.dbo.PAT_ENC enc		ON enc.PAT_ID = Lenc_cte.Pat_ID
										AND enc.VISIT_PROV_ID=Lenc_cte.Prov_ID
 										AND Lenc_cte.Cntc_Dt=enc.PAT_ENC_DATE_REAL
WHERE enc.APPT_STATUS_C IN ('2','6') --ONLY COMPLETED/ARRIVED STATUS 
)
*/
/************************************************************/
/* 9/17/2024 63858 */
--INSERT INTO [Clarity_App_Dev].[dbo].[ADT_GPP_Encounters]
SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	 hsp.HSP_ACCOUNT_ID
	,hsp.PAT_ENC_CSN_ID

	--,x.UPDATE_DATE

	,pt.PAT_ID

	,'UVAH-'+TRIM(pt.PAT_MRN_ID) 'External_System_ID' 
	,hsp.APPT_PRC_ID
	,hsp.APPT_STATUS_C
	,prc.PRC_NAME 'Hospital_Visit_Name'
	,appt.APPT_DTTM
	,hsp.CONTACT_DATE
	,hsp.HOSP_ADMSN_TIME
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
		END 'Admit_Date/Time'
	,hsp.ENC_TYPE_C
	,typ.NAME 'Encounter_Type_Name'
	,pt.PAT_MRN_ID 'Patient'
	,dep.DEPARTMENT_NAME 'Patient_Encounter_Dept_Override'
	,pt.PAT_ID 'Patient ID'
	--,haatn.ATTENDING_PROV_ID AS Adm_Dr
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.PROV_ID, hsp.VISIT_PROV_ID)  --for Outpatient
		ELSE COALESCE(haatn.ATTENDING_PROV_ID, ser.PROV_ID)  --for Inpatient
		END 'Admitting_Physician_Number'
	,CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
		END 'Appointment_ Date/Time'
	,hsp.HOSP_DISCHRG_TIME 'Discharge_Date/Time'
	,loc.LOC_NAME 'Appointment_Location'
	,loc.LOCATION_ABBR 'Appointment_Location_CD'
	,loc.LOC_ID 'Appointment_Location_ID'
	,appt.APPT_STATUS_NAME 'Appointment_Status'
	,appt.PRC_NAME 'Appointment_Type'
	,CASE WHEN appt.APPT_STATUS_NAME = 'Canceled' THEN 'True' ELSE 'False' END 'Is_Cancelled_Appointment'
	,CASE WHEN rship.GUAR_REL_TO_PAT_C = 15 THEN 'True' ELSE 'False' END 'Is_Self_Guarantor'
	,acc.PRIMARY_PAYOR_ID
	,payor.PAYOR_NAME
	,zfc.NAME AS FIN_CLASS_NAME
	,enc3.SELF_PAY_VISIT_YN
	,acc.PRIMARY_PLAN_ID
	,epp.BENEFIT_PLAN_NAME
	,CASE WHEN epp.BENEFIT_PLAN_NAME IS NULL THEN 'True' ELSE 'False' END 'Is_Self_Pay'
	,ser.PROV_ID 'Attending_Physician_Number'
	--,CAST(serid.SMS_ID AS VARCHAR(30)) 'Atn_Dr_SMS_ID'
	,dep.DEPARTMENT_NAME 'Department'
	,hsp.DEPARTMENT_ID 'Department_Code'
	,mdm.HOSPITAL_CODE 'Hospital_Code'
	,o.organization_name 'Organization'
	,gacc.ACCOUNT_NAME 'Guarantor'
	,rel.NAME 'Guarantor_Relationship'
	,CAST(CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
		END AS DATE) 'Admit_Date'
	,sersp.SPECIALTY_NAME 'Admitting_Physician_Specialty'
	,CAST(CASE WHEN hsp.HOSP_ADMSN_TIME IS NULL
		THEN COALESCE(appt.APPT_DTTM, hsp.CONTACT_DATE)  --for Outpatient
		ELSE hsp.HOSP_ADMSN_TIME  --for Inpatient
		END AS DATE) 'Appointment_Date'
	,ser2.NPI 'Attending_Physician_NPI'
	,appt.APPT_ENTRY_USER_ID
	,appt.APPT_ENTRY_USER_NAME_WID 'Created_By'
	,CAST(hsp.HOSP_DISCHRG_TIME AS DATE) 'Discharge_Date'
	,gacc.ACCOUNT_ID 'Guarantor ID'
	,payor.PAYOR_NAME 'Insurance_Name'
	,zfc.NAME 'Insurance_Type'
	,appt.SAME_DAY_YN 'Is_Same_Day_Appointment'
	,CASE WHEN appt.APPT_STATUS_NAME = 'Walk-In' THEN 'True' ELSE 'False' END 'Is_Walk-In'
	,hsp.IS_WALK_IN_YN
	,CAST(CASE                
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
	     END AS VARCHAR(25))                               AS Atn_Dr_LName
	    ,CAST (CASE
	       WHEN CHARINDEX(',',ser.PROV_NAME) > 2 THEN
	         CASE
	           WHEN CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME))) > 2 THEN
	             LTRIM(SUBSTRING(ser.PROV_NAME,CHARINDEX(',',ser.PROV_NAME)+1,CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME)))))
	           ELSE LTRIM(SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+1, LEN(ser.PROV_NAME)))
	         END
	       ELSE ''
	     END AS VARCHAR(25))                                AS Atn_Dr_FName
	    ,CAST(CASE
	       WHEN CHARINDEX(',',ser.PROV_NAME) > 2 THEN -- found lastname, now look for space in first name to split middle name out
	         CASE
	           WHEN CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME))) > 2 THEN 
	             LTRIM(SUBSTRING(SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME)), 
	                       CHARINDEX(' ', SUBSTRING(ser.PROV_NAME, CHARINDEX(',',ser.PROV_NAME)+2, LEN(ser.PROV_NAME)))+1,
	                       LEN(ser.PROV_NAME)))
	         ELSE ''
	       END  
	       ELSE ''
	      END AS VARCHAR(25))                               AS Atn_Dr_MI
	 
	
	,CAST(ser.PROV_TYPE AS VARCHAR(30))'Atn_Dr_Type'
	,COALESCE(CAST(ser.ACTIVE_STATUS AS VARCHAR(25)),'NA') 'Atn_Dr_Status'
	,ser2.INSTANT_OF_UPDATE_DTTM 'Atn_Dr_Last_Update_Dt'
	,CAST(typ.ABBR AS VARCHAR(4)) 'Source'
	,CASE WHEN acc.ACCT_BASECLS_HA_C ='1'
			THEN 'I'
			ELSE CASE WHEN acc.ACCT_BASECLS_HA_C ='2'
			THEN 'O' END 
			END 'IO_Flag'
	,CAST(sex.ABBR AS CHAR(1)) 'SEX'
	--,pt.BIRTH_DATE'Birth_Dt'
	--,CAST(DATEDIFF (YEAR, pt.BIRTH_DATE, COALESCE(lst_cte.Adm_Dt,0) )AS SMALLINT) 'Age'
	--,CASE WHEN DATEDIFF (YEAR, pt.BIRTH_DATE, lst_cte.Adm_Dt) >17
	--		THEN 'ADULT'
	--		ELSE 'PED'
	--	END 'Age_Group'
	,CONCAT(CAST(pt.PAT_FIRST_NAME AS VARCHAR(28)),' ',LEFT(pt.PAT_MIDDLE_NAME,1)) 'Pt_FName_MI'
	,CAST(pt.PAT_LAST_NAME AS VARCHAR(30)) 'Pt_LName'
	,CAST(ad1.ADDRESS AS VARCHAR(20))   'CURR_Pt_Addr1'
	,COALESCE(CAST(ad2.ADDRESS AS VARCHAR(20)),'NA') 'CURR_Pt_Addr2'
	,CAST (pt.CITY AS VARCHAR(15)) 'CURR_Pt_City'
	,CAST(st.ABBR AS VARCHAR(2)) 'CURR_PT_STATE'
	,CAST(pt.ZIP AS VARCHAR(5)) 'CURR_PT_ZIP'
	,COALESCE(CAST(pt.HOME_PHONE AS VARCHAR(15)),'NA') 'CURR_Pt_PHONE'	
	-- Split ser.Prov_Nme into Last name and First name by comma, remove JR,SR,I
	 ,CAST(CASE
	       WHEN CHARINDEX(',',gacc.ACCOUNT_NAME) > 2 THEN
	         CASE
	           WHEN CHARINDEX(' ', SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+2, LEN(gacc.ACCOUNT_NAME))) > 2 THEN
	             LTRIM(SUBSTRING(gacc.ACCOUNT_NAME,CHARINDEX(',',gacc.ACCOUNT_NAME)+1,CHARINDEX(' ', SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+2, LEN(gacc.ACCOUNT_NAME)))))
	           ELSE LTRIM(SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+1, LEN(gacc.ACCOUNT_NAME)))
	         END
	       ELSE ''
	     END AS VARCHAR(30))                               AS GUAR_FName
	,CAST(CASE
	       WHEN CHARINDEX(',',gacc.ACCOUNT_NAME) > 2 THEN -- found lastname, now look for space in first name to split middle name out
	         CASE
	           WHEN CHARINDEX(' ', SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+2, LEN(gacc.ACCOUNT_NAME))) > 2 THEN 
	             LTRIM(SUBSTRING(SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+2, LEN(gacc.ACCOUNT_NAME)), 
	                       CHARINDEX(' ', SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+2, LEN(gacc.ACCOUNT_NAME)))+1,
	                       LEN(gacc.ACCOUNT_NAME)))
	         ELSE ''
	       END  
	       ELSE ''
	      END AS VARCHAR(30) )                             AS GUAR_MName
	,CAST(CASE                
	       WHEN CHARINDEX(',',gacc.ACCOUNT_NAME) > 2 THEN
	         CASE 
	           WHEN CHARINDEX(' JR', SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(',', gacc.ACCOUNT_NAME)-1)) > 2 THEN
	             LTRIM(SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(' JR', SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(',', gacc.ACCOUNT_NAME)-1))-1))
	           WHEN CHARINDEX(' SR', SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(',', gacc.ACCOUNT_NAME)-1)) > 2 THEN
	             LTRIM(SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(' SR', SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(',', gacc.ACCOUNT_NAME)-1))-1))
	           WHEN CHARINDEX(' I', SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(',', gacc.ACCOUNT_NAME)-1)) > 2 THEN
	             LTRIM(SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(' I', SUBSTRING(gacc.ACCOUNT_NAME,1, CHARINDEX(',', gacc.ACCOUNT_NAME)-1))-1))
	              ELSE LTRIM(SUBSTRING(gacc.ACCOUNT_NAME, 1, CHARINDEX(',',gacc.ACCOUNT_NAME)-1))
	         END
	       ELSE LTRIM(gacc.ACCOUNT_NAME)
	     END AS VARCHAR(30))                           AS GUAR_LName
	   
	    
	,CAST(gacc.BILLING_ADDRESS_1 AS VARCHAR(20)) 'GUAR_ADDR1'
	,CAST (gacc.BILLING_ADDRESS_2 AS VARCHAR(20)) 'GUAR_ADDR2'
	,CAST (gacc.CITY AS VARCHAR(15)) 'GUAR_CITY'
	,CAST(guarst.ABBR AS VARCHAR (2)) 'GUAR_STATE'
	,CAST(gacc.ZIP AS VARCHAR (5)) 'GUAR_ZIP'
	,CAST (gacc.HOME_PHONE AS VARCHAR(15)) 'GUAR_PHONE'
	,CAST (rel.ABBR AS VARCHAR(1)) 'GAUR_TO_PT'
	,CAST(CASE WHEN pt4.PAT_LIVING_STAT_C='2' 
			THEN 'D' 
			ELSE '' END  AS VARCHAR(1))'Status'
	,CAST(pt.EMAIL_ADDRESS AS VARCHAR(30)) 'Email'
	,GETDATE() 'Load_Date_Time'

FROM  CLARITY.dbo.PAT_ENC hsp

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
--			WHERE csa._UPDATE_DT >= CONVERT(DATE,DATEADD(dd,-3,GETDATE()))
--			--WHERE csa._UPDATE_DT >= CONVERT(DATE,DATEADD(dd,-17,GETDATE()))
--			GROUP BY csa.PAT_ENC_CSN_ID, csa._UPDATE_DT
--) csa
--) AS csaseq
--WHERE csaseq.updseq = 1
--) AS x
--  ON x.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = hsp.VISIT_PROV_ID 
			INNER JOIN CLARITY.dbo.PATIENT pt						ON pt.PAT_ID = hsp.PAT_ID
			INNER JOIN CLARITY.dbo.clarity_ser_2 ser2					ON ser2.PROV_ID = ser.PROV_ID
			--INNER JOIN dbo.IDENTITY_ID idx					ON pt.PAT_ID=idx.PAT_ID
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
			LEFT OUTER JOIN (SELECT idxser.IDENTITY_ID 'SMS_ID'
			,idxser.PROV_ID
								FROM CLARITY.dbo.IDENTITY_SER_ID idxser
								WHERE  idxser.IDENTITY_TYPE_ID  ='6') serid		ON serid.PROV_ID=ser.PROV_ID
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
			THEN COALESCE(appt.PROV_ID, hsp.VISIT_PROV_ID)  --for Outpatient
			ELSE COALESCE(haatn.ATTENDING_PROV_ID, ser.PROV_ID)  --for Inpatient
		END

WHERE 1 = 1
AND hsp.HSP_ACCOUNT_ID IS NOT NULL
--AND hsp.APPT_STATUS_C IN ('2','6') --ONLY COMPLETED/ARRIVED STATUS 
--AND ((hsp.APPT_STATUS_C IS NOT NULL AND hsp.APPT_STATUS_C IN ('2','6')) --ONLY COMPLETED/ARRIVED STATUS 
--OR (hsp.APPT_STATUS_C IS NULL AND hsp.HOSP_ADMSN_TIME IS NOT NULL))
AND (
			((hsp.CONTACT_DATE >=@StartDate		AND		hsp.CONTACT_DATE<@EndDate)
			 OR 
			 (hsp.HOSP_ADMSN_TIME  >=@StartDate	AND		hsp.HOSP_ADMSN_TIME <@EndDate)
			)
				--AND hsp.APPT_STATUS_C IN ('2','6') --ONLY COMPLETED/ARRIVED STATUS
			AND (
						((hsp.APPT_STATUS_C IS NOT NULL AND hsp.APPT_STATUS_C IN ('2','6')) --ONLY COMPLETED/ARRIVED STATUS 
						OR (hsp.APPT_STATUS_C IS NULL))
					)
		)
AND ser.PROV_TYPE <>'Resource'
AND rd.Acc_ID IS NULL --not in red folder extract
AND pt_typ.PATIENT_TYPE_C IS NULL --not a prisoner/inmate
AND typ.DISP_ENC_TYPE_C NOT IN ('2505' -- Erroneous Encounter
                                                               ,'2506' -- Erroneous Telephone Encounter
															   ,'99' -- Billing Encounter
															   ,'52' -- Anesthesia'
															   ,'53' -- Anesthesia Event
															   )

--AND hsp.DEPARTMENT_ID = 10295006
--AND hsp.HOSP_ADMSN_TIME IS NOT NULL
--AND pt.PAT_ID = 'Z102087'
/*
		AND (
				ISNULL(acc.ACCT_FIN_CLASS_C,1)<> '3' --K: Medicaid
					OR (
						ISNULL(acc.ACCT_SLFPYST_HA_C,1)<>'5'  -- 1,2,4,5,6,7,8: Bad Debt
					--indegent care flag***************
					OR (fpl.FPL_STATUS_CODE_C NOT IN ('34','36','38','40','42') --verified 100%;95%;80%;55%;30% respectively- fpl assistance  for indegent care
									AND (fpl.FPL_EFF_DATE >=@StartDate AND fpl.FPL_EFF_DATE <@EndDate))
					OR ISNULL(accst.ACCOUNT_STATUS_C,1) <>'105'   --legal ; 3: collection ??
						)
				)

	AND (
				
						(DATEDIFF (YEAR, pt.BIRTH_DATE, lst_cte.Adm_Dt) >18 
								AND 
										(	edg.ICD9_CODE IS NULL 
										OR edg.ICD9_CODE NOT IN ('078.1','795.8','V08','V27.1','V27.3','V27.4','V27.6','V27.7') 
										OR edg.ICD9_CODE NOT BETWEEN '042' AND '044.9' 
										OR edg.ICD9_CODE NOT BETWEEN '054.10' AND '054.19' 
										OR edg.ICD9_CODE NOT BETWEEN '079.51' AND '079.53' 
										OR edg.ICD9_CODE NOT BETWEEN '090.0' AND '099.9' 
										OR edg.ICD9_CODE NOT BETWEEN '279.10' AND '279.19' 
										OR edg.ICD9_CODE NOT BETWEEN '632' AND '639.99' 
										)
						)
				
			
						OR
		
						(DATEDIFF (YEAR, pt.BIRTH_DATE, lst_cte.Adm_Dt) <=18
								AND 
										(	edg.ICD9_CODE IS NULL
										OR  edg.ICD9_CODE NOT BETWEEN '640.0'   AND '676.94' 
										OR	edg.ICD9_CODE NOT BETWEEN 'V22.0'   AND 'V25.9'
										OR  edg.ICD9_CODE NOT BETWEEN '042'	    AND '044.9' 
										OR  edg.ICD9_CODE NOT BETWEEN '054.10'  AND '054.19' 
										OR  edg.ICD9_CODE NOT BETWEEN '079.51'  AND '079.53'
										OR  edg.ICD9_CODE NOT BETWEEN '090.0'   AND '099.9' 
										OR  edg.ICD9_CODE NOT BETWEEN '279.10'  AND '279.19' 
										OR  edg.ICD9_CODE NOT BETWEEN '632'     AND '639.99' 
										OR  edg.ICD9_CODE NOT IN ('078.1','795.8','V08','V27.1','V27.3','V27.4','V27.6','V27.7') 
										)
						)
				)		
*/
--ORDER BY	CAST(ser.PROV_ID AS VARCHAR(30))
		--	,CAST(idx.IDENTITY_ID AS VARCHAR(12))   DESC
--ORDER BY	hsp.HSP_ACCOUNT_ID, hsp.PAT_ENC_CSN_ID, CAST(ser.PROV_ID AS VARCHAR(30))
--ORDER BY	hsp.HSP_ACCOUNT_ID, hsp.PAT_ENC_CSN_ID, ser.PROV_ID
--ORDER BY	dep.DEPARTMENT_NAME, hsp.HSP_ACCOUNT_ID, hsp.PAT_ENC_CSN_ID, ser.PROV_ID
ORDER BY	pt.PAT_ID, hsp.PAT_ENC_CSN_ID, ser.PROV_ID
END
/*
FROM CLARITY..HSP_ACCOUNT
LEFT JOIN CLARITY..ZC_STATE				ON HSP_ACCOUNT.PAT_STATE_C = ZC_STATE.STATE_C

/*[RK] New join for hospital encounters and patient table*/
INNER JOIN clarity..PAT_ENC_HSP			ON hsp_account.PRIM_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
INNER JOIN clarity..PATIENT				ON hsp_account.PAT_ID = patient.PAT_ID
INNER JOIN clarity..PAT_ENC				ON pat_enc_hsp.PAT_ENC_CSN_ID = pat_enc.PAT_ENC_CSN_ID
LEFT JOIN clarity.dbo.HSP_ACCT_SBO sbo  ON sbo.HSP_ACCOUNT_ID=HSP_ACCOUNT.HSP_ACCOUNT_ID

/*[RK] Removing join of origional ABCO code*/
--LEFT JOIN CLARITY..PAT_ENC_HSP			ON HSP_ACCOUNT.PAT_ID = PAT_ENC_HSP.PAT_ID 
--									AND	   HSP_ACCOUNT.HSP_ACCOUNT_ID = PAT_ENC_HSP.HSP_ACCOUNT_ID 
--									AND	   PAT_ENC_HSP.PAT_ENC_CSN_ID = HSP_ACCOUNT.PRIM_ENC_CSN_ID
--LEFT JOIN CLARITY..PAT_ENC				ON PAT_ENC.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
--INNER JOIN CLARITY..PATIENT				ON PAT_ENC.PAT_ID = PATIENT.PAT_ID

LEFT JOIN CLARITY..ZC_SEX				ON HSP_ACCOUNT.PAT_SEX_C = ZC_SEX.RCPT_MEM_SEX_C
LEFT JOIN CLARITY..PATIENT_RACE			ON PATIENT.PAT_ID = PATIENT_RACE.PAT_ID
LEFT JOIN CLARITY..ZC_PATIENT_RACE		ON PATIENT_RACE.PATIENT_RACE_C = ZC_PATIENT_RACE.PATIENT_RACE_C

/*[RK] Unnecessary join to coverage multiple return item in chronicles/clarity*/
--LEFT JOIN CLARITY..COVERAGE				ON HSP_ACCOUNT.COVERAGE_ID = COVERAGE.COVERAGE_ID						-- Insurance Info
LEFT JOIN CLARITY..CLARITY_FC			ON HSP_ACCOUNT.ACCT_FIN_CLASS_C = CLARITY_FC.FINANCIAL_CLASS			-- Payer Class
LEFT JOIN CLARITY..ZC_ACCT_CLASS_HA		ON HSP_ACCOUNT.ACCT_CLASS_HA_C = ZC_ACCT_CLASS_HA.ACCT_CLASS_HA_C		-- Patient Type
LEFT JOIN CLARITY..ZC_ACCT_BASECLS_HA	ON HSP_ACCOUNT.ACCT_BASECLS_HA_C = ZC_ACCT_BASECLS_HA.ACCT_BASECLS_HA_C	-- Patient Base Class
LEFT JOIN CLARITY..ZC_ADMISSION_SRC		ON HSP_ACCOUNT.ADMISSION_SOURCE_C = ZC_ADMISSION_SRC.INTERNAL_ID		-- Admission Source
LEFT JOIN CLARITY..ZC_ADM_SOURCE		ON HSP_ACCOUNT.ADMISSION_TYPE_C = ZC_ADM_SOURCE.INTERNAL_ID
LEFT JOIN CLARITY..HSP_ACCT_ATND_PROV	ON HSP_ACCT_ATND_PROV.HSP_ACCOUNT_ID = HSP_ACCOUNT.HSP_ACCOUNT_ID		-- Admitting Provider
									AND	   HSP_ACCT_ATND_PROV.LINE = 1
LEFT JOIN CLARITY..CLARITY_LOC			ON HSP_ACCOUNT.LOC_ID = CLARITY_LOC.LOC_ID */





GO


