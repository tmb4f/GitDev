USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================

/*******************************************************************************************
WHAT:	Grateful_Patient_Program
WHO :	Health Foundation
WHEN:	Daily
WHY :	Last encounter census for last five years  
AUTHOR:	Mali Amarasinghe
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

IF OBJECT_ID('tempdb..#gpp ') IS NOT NULL
DROP TABLE #gpp

IF OBJECT_ID('tempdb..#gpp2 ') IS NOT NULL
DROP TABLE #gpp2

	BEGIN
    
	DECLARE @StartDate DATETIME;
	DECLARE @EndDate DATETIME;

			SET  @StartDate= DATEADD(yy,-5,DATEADD(yy, DATEDIFF(yy,0,getdate()), 0)); --last five years
			SET  @EndDate	=CONVERT(DATETIME,CONVERT(VARCHAR(10),DATEADD(dd,-1,GETDATE()),101) + ' 23:59:59'); --Yesterday

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
/*******QUERY TO GET ENCOUNTER*********************/
;WITH Lenc_cte AS 
(
SELECT DISTINCT
	hsp.PAT_ID 'Pat_ID'
	,hsp.VISIT_PROV_ID 'Prov_ID'
	,hsp.PAT_ENC_DATE_REAL 'Cntc_Dt'
	,hsp.DEPARTMENT_ID
	,dep.DEPARTMENT_NAME
	,dep.EXTERNAL_NAME
	,dep.SPECIALTY
	,dep2.ADDRESS_CITY
	,dep2.ADDRESS_ZIP_CODE
	,dep2.ADDRESS_HOUSE_NUM
	,dep2.ADDRESS_STATE_C
	,zts.NAME AS ADDRESS_STATE
	,dep2.ADDRESS_COUNTY_C
	,zc2.NAME AS ADDRESS_COUNTY
FROM  CLARITY.dbo.PAT_ENC hsp		--ON hsp.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	ON dep.DEPARTMENT_ID = hsp.DEPARTMENT_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP_2 dep2
	ON dep2.DEPARTMENT_ID = hsp.DEPARTMENT_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_TAX_STATE zts
ON zts.TAX_STATE_C = dep2.ADDRESS_STATE_C
LEFT OUTER JOIN CLARITY.dbo.ZC_COUNTY_2 zc2
ON zc2.COUNTY_2_C = dep2.ADDRESS_COUNTY_C
WHERE 1=1
AND
(hsp.CONTACT_DATE >=@StartDate		AND		hsp.CONTACT_DATE<@EndDate)
				OR 
				(hsp.HOSP_ADMSN_TIME  >=@StartDate	AND		hsp.HOSP_ADMSN_TIME <@EndDate)
				AND hsp.APPT_STATUS_C IN ('2','6') --ONLY COMPLETED/ARRIVED STATUS 
)
/**********************************************/

/******Atn_Dr_Last_Update_Dt************/

,lst_cte AS 
(
SELECT 
	 enc_cte.PAT_ID, 
	 enc.PAT_ENC_CSN_ID 'last_encounter'
	 ,enc.HSP_ACCOUNT_ID 'Hsp_Acct'
	 ,CASE WHEN enc.HOSP_ADMSN_TIME IS NULL
		THEN enc.CONTACT_DATE  --for Outpatient
		ELSE enc.HOSP_ADMSN_TIME  --for Inpatient
		END 'Adm_Dt'
	--,enc.CONTACT_DATE 'Contact_Dt'
	--,enc.HOSP_ADMSN_TIME 'Adm_Dt'
	,enc_cte.Prov_ID 'Prov_ID'
	,enc.ACCOUNT_ID 'Guar_ID'
	,enc.ENC_TYPE_C
	,enc_cte.DEPARTMENT_ID
	,enc_cte.DEPARTMENT_NAME
	,enc_cte.EXTERNAL_NAME
	,enc_cte.SPECIALTY
	,enc_cte.ADDRESS_CITY
	,enc_cte.ADDRESS_ZIP_CODE
	,enc_cte.ADDRESS_HOUSE_NUM
	,enc_cte.ADDRESS_STATE
	,enc_cte. ADDRESS_COUNTY

FROM Lenc_cte enc_cte
	LEFT OUTER JOIN CLARITY.dbo.PAT_ENC enc		ON enc.PAT_ID = enc_cte.Pat_ID
										AND enc.VISIT_PROV_ID=enc_cte.Prov_ID
 										AND enc_cte.Cntc_Dt=enc.PAT_ENC_DATE_REAL
WHERE enc.APPT_STATUS_C IN ('2','6') --ONLY COMPLETED/ARRIVED STATUS 
)
/************************************************************/

--INSERT INTO [Clarity_App_Dev].[dbo].[ADT_GPP_Encounters]
SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	--lst_cte.Hsp_Acct  'Acct'
	--,CAST(idx.IDENTITY_ID AS VARCHAR(12))  'MRN'
	lst_cte.Adm_Dt  'Adm_Dt'
	,CAST(ser.PROV_ID AS VARCHAR(30)) 'Atn_Dr'
	,CAST(serid.SMS_ID AS VARCHAR(30)) 'Atn_Dr_SMS_ID'
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
	,typ.NAME AS ENC_TYPE_NAME
	,CAST(typ.ABBR AS VARCHAR(4)) 'Source'
	,CASE WHEN acc.ACCT_BASECLS_HA_C ='1'
			THEN 'I'
			ELSE CASE WHEN acc.ACCT_BASECLS_HA_C ='2'
			THEN 'O' END 
			END 'IO_Flag'
	,CAST(sex.ABBR AS CHAR(1)) 'SEX'
	,pt.BIRTH_DATE'Birth_Dt'
	,CAST(DATEDIFF (YEAR, pt.BIRTH_DATE, COALESCE(lst_cte.Adm_Dt,0) )AS SMALLINT) 'Age'
	,CASE WHEN DATEDIFF (YEAR, pt.BIRTH_DATE, lst_cte.Adm_Dt) >17
			THEN 'ADULT'
			ELSE 'PED'
		END 'Age_Group'
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
	,CAST(pt.PAT_ID AS VARCHAR(20)) 'PAT_ID'
	,GETDATE() 'Load_Date_Time'

	,lst_cte.DEPARTMENT_ID
	,lst_cte.DEPARTMENT_NAME
	,lst_cte.EXTERNAL_NAME
	,lst_cte.SPECIALTY
	,lst_cte.ADDRESS_CITY
	,lst_cte.ADDRESS_ZIP_CODE
	,lst_cte.ADDRESS_HOUSE_NUM
	,lst_cte.ADDRESS_STATE
	,lst_cte. ADDRESS_COUNTY

INTO #gpp

FROM CLARITY.dbo.CLARITY_SER ser
			INNER JOIN lst_cte								ON lst_cte.Prov_ID = ser.PROV_ID
			INNER JOIN CLARITY.dbo.PATIENT pt						ON pt.PAT_ID = lst_cte.PAT_ID
			INNER JOIN CLARITY.dbo.clarity_ser_2 ser2					ON ser2.PROV_ID = ser.PROV_ID
			--INNER JOIN dbo.IDENTITY_ID idx					ON pt.PAT_ID=idx.PAT_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE typ		ON typ.DISP_ENC_TYPE_C=lst_cte.ENC_TYPE_C
			LEFT OUTER JOIN CLARITY.dbo.ZC_SEX sex					ON pt.SEX_C=sex.RCPT_MEM_SEX_C	
			LEFT OUTER JOIN CLARITY.dbo.ZC_STATE st						ON st.STATE_C = pt.STATE_C
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad1					ON ad1.PAT_ID = pt.PAT_ID
																			 AND ad1.line=1
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad2					ON ad2.PAT_ID = pt.PAT_ID
																			 AND ad2.line=2
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT gacc						ON lst_cte.Guar_ID=gacc.ACCOUNT_ID
			
			LEFT OUTER JOIN CLARITY.dbo.ZC_STATE guarst				ON gacc.STATE_C =guarst.STATE_C															
			LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT acc				ON acc.HSP_ACCOUNT_ID = lst_cte.Hsp_Acct
			LEFT OUTER JOIN CLARITY.dbo.ZC_ACCT_BASECLS_HA bcls		ON bcls.ACCT_BASECLS_HA_C = acc.ACCT_BASECLS_HA_C
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT_STATUS	accst		ON lst_cte.Hsp_Acct= accst.ACCOUNT_ID 
			LEFT OUTER JOIN CLARITY.dbo.HSP_ACCT_DX_LIST dx			ON lst_cte.Hsp_Acct=dx.HSP_ACCOUNT_ID
																--AND dx.line=1
			LEFT OUTER JOIN CLARITY.dbo.CLARITY_EDG edg				ON edg.DX_ID=dx.DX_ID
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT_FPL_INFO fpl		ON lst_cte.Hsp_Acct = fpl.ACCOUNT_ID
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
WHERE ser.PROV_TYPE <>'Resource'
		--AND idx.IDENTITY_TYPE_ID='14'
	
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
			
				AND rd.Acc_ID IS NULL --not in red folder extract
				AND pt_typ.PATIENT_TYPE_C IS NULL --not a prisoner/inmate
/*
--SELECT DISTINCT
--	ENC_TYPE_NAME	
SELECT
	Adm_Dt,
    Atn_Dr,
    Atn_Dr_SMS_ID,
    Atn_Dr_LName,
    Atn_Dr_FName,
    Atn_Dr_MI,
    Atn_Dr_Type,
    Atn_Dr_Status,
    Atn_Dr_Last_Update_Dt,
    ENC_TYPE_NAME,
    Source,
    IO_Flag,
    SEX,
    Birth_Dt,
    Age,
    Age_Group,
    Pt_FName_MI,
    Pt_LName,
    CURR_Pt_Addr1,
    CURR_Pt_Addr2,
    CURR_Pt_City,
    CURR_PT_STATE,
    CURR_PT_ZIP,
    CURR_Pt_PHONE,
    GUAR_FName,
    GUAR_MName,
    GUAR_LName,
    GUAR_ADDR1,
    GUAR_ADDR2,
    GUAR_CITY,
    GUAR_STATE,
    GUAR_ZIP,
    GUAR_PHONE,
    GAUR_TO_PT,
    Status,
    Email,
    PAT_ID,
    Load_Date_Time
FROM #gpp

--ORDER BY	CAST(pt.PAT_ID AS VARCHAR(20)), lst_cte.Adm_Dt, CAST(ser.PROV_ID AS VARCHAR(30))
--		--	,CAST(idx.IDENTITY_ID AS VARCHAR(12))   DESC

ORDER BY	PAT_ID, Adm_Dt, Atn_Dr

--ORDER BY	ENC_TYPE_NAME
*/
--/*
SELECT
	   rptg.PAT_ID
	  ,rptg.Adm_Dt
      ,rptg.Atn_Dr_Prov_Id
	  ,rptg.Atn_Dr_SMS_Id
	  ,rptg.Atn_Dr_LName
	  ,rptg.Atn_Dr_FName
	  ,rptg.Atn_Dr_MI
	  ,rptg.Atn_Dr_Type
	  ,rptg.Atn_Dr_Status
	  ,rptg.Atn_Dr_Last_Update_Dt
	  ,rptg.Atn_Dr_Financial_Division_Name
	  ,rptg.[Source]
	  ,rptg.IO_Flag
	  ,rptg.Sex
	  ,rptg.Birth_Dt
	  ,rptg.Age
	  ,rptg.Age_Group
	  ,rptg.Pt_FName_MI
	  ,rptg.Pt_LName
	  ,rptg.CURR_PT_ADDR1
	  ,rptg.CURR_PT_ADDR2
	  ,rptg.CURR_PT_CITY
	  ,rptg.CURR_PT_STATE
	  ,rptg.CURR_PT_ZIP
	  ,rptg.CURR_PT_PHONE
	  ,rptg.GUAR_FNAME
	  ,rptg.GUAR_MName
	  ,rptg.GUAR_LNAME
	  ,rptg.GUAR_ADDR1
	  ,rptg.GUAR_ADDR2
	  ,rptg.GUAR_CITY
	  ,rptg.GUAR_STATE
	  ,rptg.GUAR_ZIP
	  ,rptg.GUAR_PHONE
	  ,rptg.GAUR_TO_PT
	  ,rptg.[Status]
      ,rptg.sk_Dim_Pt
	  ,rptg.Load_Date_Time
	  ,rptg.Email
	  ,rptg.sk_Dim_Physcn
	  ,rptg.DEPARTMENT_ID
	  ,rptg.DEPARTMENT_NAME
	  ,rptg.EXTERNAL_NAME
	  ,rptg.SPECIALTY
	  ,rptg.ADDRESS_CITY
	  ,rptg.ADDRESS_ZIP_CODE
	  ,rptg.ADDRESS_HOUSE_NUM
	  ,rptg.ADDRESS_STATE
	  ,rptg. ADDRESS_COUNTY

INTO #gpp2

FROM (
SELECT seq.*
      ,ROW_NUMBER() OVER(PARTITION BY seq.Seq_Value, seq.sk_Dim_Pt ORDER BY seq.Adm_Dt DESC) Seq_Num
FROM (
SELECT gpp.*
      ,CASE WHEN gpp.sk_Dim_Physcn <= 0 AND gpp.Atn_Dr_SMS_Id IS NOT NULL THEN CAST(gpp.Atn_Dr_SMS_Id AS VARCHAR(30))
			WHEN gpp.sk_Dim_Physcn <= 0 AND gpp.Atn_Dr_SMS_Id IS NULL THEN CAST(gpp.Atn_Dr_Prov_Id AS VARCHAR(30))
		    ELSE CAST(gpp.sk_Dim_Physcn AS VARCHAR(30))
	   END AS Seq_Value
FROM (
SELECT clrt.ADm_Dt AS Adm_Dt
      ,clrt.Atn_Dr AS Atn_Dr_Prov_Id
	  ,clrt.Atn_Dr_SMS_ID AS Atn_Dr_SMS_Id
	  ,CASE WHEN dphyscn1.LastName NOT IN ('Invalid','Unk') THEN dphyscn1.LastName
	        WHEN dphyscn2.LastName IS NOT NULL THEN dphyscn2.LastName
	        ELSE clrt.Atn_Dr_LName
	   END AS Atn_Dr_LName
	  ,CASE WHEN dphyscn1.LastName NOT IN ('Invalid','Unk') THEN dphyscn1.FirstName
	        WHEN dphyscn2.LastName IS NOT NULL THEN dphyscn2.FirstName
	        ELSE clrt.Atn_Dr_FName
	   END AS Atn_Dr_FName
	  ,CASE WHEN dphyscn1.LastName NOT IN ('Invalid','Unk') THEN dphyscn1.MI
	        WHEN dphyscn2.LastName IS NOT NULL THEN dphyscn2.MI
	        ELSE clrt.Atn_Dr_MI
	   END AS Atn_Dr_MI
	  ,clrt.Atn_Dr_Type
	  ,clrt.Atn_Dr_Status
	  ,clrt.Atn_Dr_Last_Update_Dt
	  ,ser.Financial_Division_Name AS Atn_Dr_Financial_Division_Name
	  ,clrt.[Source]
	  ,clrt.IO_Flag
	  ,clrt.Sex
	  ,clrt.Birth_Dt
	  ,clrt.Age
	  ,clrt.Age_Group
	  ,clrt.Pt_FName_MI
	  ,clrt.Pt_LName
	  ,clrt.CURR_PT_ADDR1
	  ,clrt.CURR_PT_ADDR2
	  ,clrt.CURR_PT_CITY
	  ,clrt.CURR_PT_STATE
	  ,clrt.CURR_PT_ZIP
	  ,clrt.CURR_PT_PHONE
	  ,clrt.GUAR_FNAME
	  ,clrt.GUAR_MName
	  ,clrt.GUAR_LNAME
	  ,clrt.GUAR_ADDR1
	  ,clrt.GUAR_ADDR2
	  ,clrt.GUAR_CITY
	  ,clrt.GUAR_STATE
	  ,clrt.GUAR_ZIP
	  ,clrt.GUAR_PHONE
	  ,clrt.GAUR_TO_PT
	  ,clrt.[Status]
      ,dpt.sk_Dim_Pt
	  ,clrt.load_date_time AS Load_Date_Time
	  ,clrt.Email
	  ,CASE WHEN dphyscn1.sk_Dim_Physcn > 0 THEN dphyscn1.sk_Dim_Physcn
	        WHEN dphyscn2.sk_Dim_Physcn IS NOT NULL AND dphyscn2.sk_Dim_Physcn > 0 THEN dphyscn2.sk_Dim_Physcn
	        ELSE ser.sk_Dim_Physcn
	   END AS sk_Dim_Physcn
	  ,clrt.PAT_ID
	  ,clrt.DEPARTMENT_ID
	  ,clrt.DEPARTMENT_NAME
	  ,clrt.EXTERNAL_NAME
	  ,clrt.SPECIALTY
	  ,clrt.ADDRESS_CITY
	  ,clrt.ADDRESS_ZIP_CODE
	  ,clrt.ADDRESS_HOUSE_NUM
	  ,clrt.ADDRESS_STATE
	  ,clrt. ADDRESS_COUNTY
FROM #gpp clrt
LEFT JOIN CLARITY_App.[Rptg].[vwDim_Clrt_Pt] dpt
ON (dpt.Clrt_PAT_ID = clrt.PAT_ID)
LEFT JOIN CLARITY_App.[Rptg].[vwDim_Clrt_SERsrc] ser
ON (ser.PROV_ID = clrt.Atn_Dr)
LEFT OUTER JOIN (SELECT sk_Dim_Physcn, LastName, FirstName, MI
                 FROM CLARITY_App.dbo.Dim_Physcn
				 WHERE current_flag = 1) dphyscn1
ON dphyscn1.sk_Dim_Physcn = ser.sk_Dim_Physcn
LEFT OUTER JOIN (SELECT sk_Dim_Physcn, LastName, FirstName, MI, NPINumber
                 FROM CLARITY_App.dbo.Dim_Physcn
				 WHERE current_flag = 1
				 AND ((NPINumber IS NOT NULL) AND (NPINumber <> 'Unknown') AND (CAST(NPINumber AS INTEGER) > 0))) dphyscn2
ON dphyscn2.NPINumber = CAST(ser.NPI AS VARCHAR(10))
) gpp
) seq
) rptg
WHERE rptg.Seq_Num = 1

--SELECT
--	*
--FROM #gpp2
--ORDER BY	PAT_ID, Adm_Dt, Atn_Dr_Prov_Id

SELECT DISTINCT
	gpp.DEPARTMENT_ID,
	gpp.DEPARTMENT_NAME,
    gpp.EXTERNAL_NAME,
    gpp.SPECIALTY,
    gpp.ADDRESS_CITY,
    gpp.ADDRESS_ZIP_CODE,
    gpp.ADDRESS_HOUSE_NUM,
    gpp.ADDRESS_STATE,
    gpp.ADDRESS_COUNTY,
    --mdm.EPIC_DEPARTMENT_ID,
    mdm.EPIC_DEPT_TYPE,
    mdm.EPIC_SPCLTY,
    mdm.organization_id,
    mdm.organization_name,
    mdm.service_id,
    mdm.service_name,
    mdm.clinical_area_id,
    mdm.clinical_area_name,
    mdm.ambulatory_flag,
    mdm.childrens_flag,
    mdm.childrens_ambulatory_name,
    mdm.mc_ambulatory_name,
    mdm.ambulatory_operation_name,
    mdm.serviceline_division_name,
    mdm.mc_operation_name,
    mdm.inpatient_adult_name,
    mdm.FINANCE_COST_CODE,
    mdm.PEOPLESOFT_NAME,
    mdm.SERVICE_LINE,
    mdm.SUB_SERVICE_LINE,
    mdm.OPNL_SERVICE_NAME,
    mdm.CORP_SERVICE_LINE,
    mdm.RL_LOCATION_BESAFE,
    mdm.LOC_ID,
    mdm.REV_LOC_NAME,
    mdm.A2K3_NAME,
    mdm.A2K3_CLINIC_CARE_AREA_DESCRIPTION,
    mdm.AMB_PRACTICE_GROUP,
    mdm.HS_AREA_ID,
    mdm.HS_AREA_NAME,
    mdm.TJC_FLAG,
    mdm.NDNQI_NAME,
    mdm.NHSN_NAME,
    mdm.PRACTICE_GROUP_NAME,
    mdm.DIVISION_DESC,
    mdm.ADMIN_DESC,
    mdm.BUSINESS_UNIT,
    mdm.PFA_POD,
    mdm.HUB,
    mdm.PBB_POD,
    mdm.PG_SURVEY_DESIGNATOR,
    mdm.upg_practice_flag,
    mdm.upg_practice_region_name,
    mdm.upg_practice_id,
    mdm.upg_practice_name,
    mdm.HOSPITAL_CODE,
    mdm.LOC_RPT_GRP_NINE_NAME,
    mdm.community_health_flag,
    mdm.deleted_flag
FROM
(
SELECT DISTINCT
	DEPARTMENT_ID
	,DEPARTMENT_NAME
	,EXTERNAL_NAME
	,SPECIALTY
	,ADDRESS_CITY
	,ADDRESS_ZIP_CODE
	,ADDRESS_HOUSE_NUM
	,ADDRESS_STATE
	,ADDRESS_COUNTY
FROM #gpp2
) gpp
--ON grouper.epic_department_id = gpp.DEPARTMENT_ID
LEFT OUTER JOIN
(
SELECT DISTINCT
 mdm.EPIC_DEPARTMENT_ID
--,mdm.EPIC_DEPT_NAME
--,mdm.EPIC_EXT_NAME
,mdm.EPIC_DEPT_TYPE
,mdm.EPIC_SPCLTY
,org.organization_id
,org.organization_name
,org.service_id
,org.[service_name]
,org.clinical_area_id
,org.clinical_area_name
,grouper.ambulatory_flag
,grouper.childrens_flag
,grouper.childrens_ambulatory_name
,grouper.mc_ambulatory_name
,grouper.ambulatory_operation_name
--,grouper.childrens_name
,grouper.serviceline_division_name
,grouper.mc_operation_name
,grouper.inpatient_adult_name
,mdm.FINANCE_COST_CODE
,mdm.PEOPLESOFT_NAME
,mdm.SERVICE_LINE
,mdm.SUB_SERVICE_LINE
,mdm.OPNL_SERVICE_NAME
,mdm.CORP_SERVICE_LINE
,mdm.RL_LOCATION [RL_LOCATION_BESAFE]
,COALESCE(mdm.LOC_ID,'0')  LOC_ID 
,COALESCE(mdm.REV_LOC_NAME,'Null') REV_LOC_NAME
,mdm.A2K3_NAME
,mdm.A2K3_CLINIC_CARE_AREA_DESCRIPTION
,mdm.AMB_PRACTICE_GROUP
,mdm.HS_AREA_ID
,COALESCE(REPLACE(mdm.HS_AREA_NAME,'upg','Null'),'Null') HS_AREA_NAME  --- some have null string
,mdm.TJC_FLAG
,mdm.NDNQI_NAME
,mdm.NHSN_NAME
,mdm.PRACTICE_GROUP_NAME
--,mdm.PRESSGANEY_NAME
,mdm.DIVISION_DESC
,mdm.ADMIN_DESC
,mdm.BUSINESS_UNIT
--,mdm.RPT_RUN_DT
,mdm.PFA_POD
,mdm.HUB
,mdm.PBB_POD
,mdm.PG_SURVEY_DESIGNATOR
,grouper.UPG_PRACTICE_FLAG
,grouper.UPG_PRACTICE_REGION_NAME
,grouper.UPG_PRACTICE_ID
,grouper.UPG_PRACTICE_NAME
,mdm.[HOSPITAL_CODE]
,mdm.[LOC_RPT_GRP_NINE_NAME]
,grouper.community_health_flag
,CASE WHEN mdmcurr.EPIC_DEPARTMENT_ID IS NULL THEN 1 ELSE 0 END AS deleted_flag

FROM
(
SELECT DISTINCT ids.EPIC_DEPARTMENT_ID, ids.sk_Ref_Clinical_Area_Map, ids.clinical_area_id, ids.clinical_area_name, ids.sk_Ref_Service_Map, ids.service_id, ids.[service_name], ids.organization_id, COALESCE(organization.organization_name,'Unmapped') AS organization_name
FROM
(
SELECT DISTINCT ca.EPIC_DEPARTMENT_ID, ca.sk_Ref_Clinical_Area_Map, ca.clinical_area_id, ca.clinical_area_name, ca.sk_Ref_Service_Map, COALESCE([service].service_id,0) AS service_id, COALESCE([service].[service_name], 'Unmapped') AS [service_name], COALESCE([service].organization_id,0) AS organization_id
FROM
(
SELECT DISTINCT mapping.EPIC_DEPARTMENT_ID, mapping.sk_Ref_Clinical_Area_Map, COALESCE(clinical_area.clinical_area_id,0) AS clinical_area_id, COALESCE(clinical_area.clinical_area_name,'Unmapped') AS clinical_area_name, COALESCE(clinical_area.sk_Ref_Service_Map,0) AS sk_Ref_Service_Map
FROM
(
SELECT DISTINCT mdm.EPIC_DEPARTMENT_ID, COALESCE(grouper.sk_Ref_Clinical_Area_Map,0) AS sk_Ref_Clinical_Area_Map
FROM
(
SELECT
	mdmhx.EPIC_DEPARTMENT_ID
FROM
(
SELECT
    EPIC_DEPARTMENT_ID,
	MDM_BATCH_ID,
	ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
FROM CLARITY_App.dbo.Ref_MDM_Location_Master_History
) mdmhx
WHERE mdmhx.seq =1
) mdm
LEFT JOIN CLARITY_App.Mapping.Epic_Dept_Groupers grouper
ON grouper.EPIC_DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID
) mapping
LEFT JOIN CLARITY_App.Mapping.Ref_Clinical_Area_Map clinical_area
ON mapping.sk_Ref_Clinical_Area_Map = clinical_area.sk_Ref_Clinical_Area_Map
) ca
LEFT JOIN CLARITY_App.Mapping.Ref_Service_Map [service]
ON ca.sk_Ref_Service_Map = [service].sk_Ref_Service_Map
) ids
LEFT JOIN CLARITY_App.Mapping.Ref_Organization_Map organization
ON organization.organization_id = ids.organization_id
) org
LEFT OUTER JOIN
(
SELECT
	mdmhx.EPIC_DEPARTMENT_ID,
    mdmhx.EPIC_DEPT_NAME,
    mdmhx.EPIC_EXT_NAME,
    mdmhx.EPIC_DEPT_TYPE,
    mdmhx.EPIC_SPCLTY,
    mdmhx.FINANCE_COST_CODE,
    mdmhx.PEOPLESOFT_NAME,
    mdmhx.SERVICE_LINE,
    mdmhx.SUB_SERVICE_LINE,
    mdmhx.OPNL_SERVICE_NAME,
    mdmhx.CORP_SERVICE_LINE,
    mdmhx.RL_LOCATION,
    mdmhx.LOC_ID,
    mdmhx.REV_LOC_NAME,
    mdmhx.A2K3_NAME,
    mdmhx.A2K3_CLINIC_CARE_AREA_DESCRIPTION,
    mdmhx.AMB_PRACTICE_GROUP,
    mdmhx.HS_AREA_ID,
    mdmhx.HS_AREA_NAME,
    mdmhx.TJC_FLAG,
    mdmhx.NDNQI_NAME,
    mdmhx.NHSN_NAME,
    mdmhx.PRACTICE_GROUP_NAME,
    mdmhx.PRESSGANEY_NAME,
    mdmhx.DIVISION_DESC,
    mdmhx.ADMIN_DESC,
    mdmhx.BUSINESS_UNIT,
    mdmhx.RPT_RUN_DT,
    mdmhx.PFA_POD,
    mdmhx.HUB,
    mdmhx.PBB_POD,
    mdmhx.PG_SURVEY_DESIGNATOR,
    mdmhx.HOSPITAL_CODE,
    mdmhx.LOC_RPT_GRP_NINE_NAME
FROM
(
SELECT
    EPIC_DEPARTMENT_ID,
    EPIC_DEPT_NAME,
    EPIC_EXT_NAME,
    EPIC_DEPT_TYPE,
    EPIC_SPCLTY,
    FINANCE_COST_CODE,
    PEOPLESOFT_NAME,
    SERVICE_LINE,
    SUB_SERVICE_LINE,
    OPNL_SERVICE_NAME,
    CORP_SERVICE_LINE,
    RL_LOCATION,
    LOC_ID,
    REV_LOC_NAME,
    A2K3_NAME,
    A2K3_CLINIC_CARE_AREA_DESCRIPTION,
    AMB_PRACTICE_GROUP,
    HS_AREA_ID,
    HS_AREA_NAME,
    TJC_FLAG,
    NDNQI_NAME,
    NHSN_NAME,
    PRACTICE_GROUP_NAME,
    PRESSGANEY_NAME,
    DIVISION_DESC,
    ADMIN_DESC,
    BUSINESS_UNIT,
	CAST(NULL AS DATETIME) AS RPT_RUN_DT,
    PFA_POD,
    HUB,
    PBB_POD,
    PG_SURVEY_DESIGNATOR,
    HOSPITAL_CODE,
    LOC_RPT_GRP_NINE_NAME,
	MDM_BATCH_ID,
	ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
FROM CLARITY_App.dbo.Ref_MDM_Location_Master_History
) mdmhx
WHERE mdmhx.seq =1
) mdm
ON mdm.EPIC_DEPARTMENT_ID = org.EPIC_DEPARTMENT_ID
LEFT JOIN
(
SELECT DISTINCT
	epic_department_id
FROM CLARITY_App.dbo.Ref_MDM_Location_Master
) mdmcurr
ON mdmcurr.EPIC_DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID
LEFT JOIN CLARITY_App.Mapping.Epic_Dept_Groupers grouper
ON grouper.epic_department_id = org.EPIC_DEPARTMENT_ID
--INNER JOIN @ClinicalArea clinicalarea
--ON org.sk_Ref_Clinical_Area_Map = clinicalarea.sk_Ref_Clinical_Area_Map
----INNER JOIN
--LEFT OUTER JOIN
--(
--SELECT DISTINCT
--	DEPARTMENT_ID
--FROM #gpp2
--) gpp
--ON grouper.epic_department_id = gpp.DEPARTMENT_ID
) mdm
ON mdm.EPIC_DEPARTMENT_ID = gpp.DEPARTMENT_ID
--ORDER BY org.organization_id
--                  , org.service_id
--				  , org.clinical_area_id
--				  , mdm.EPIC_DEPARTMENT_ID;
ORDER BY gpp.DEPARTMENT_ID
                  , gpp.DEPARTMENT_NAME
				  , gpp.EXTERNAL_NAME;
--GO
--*/
END

GO


