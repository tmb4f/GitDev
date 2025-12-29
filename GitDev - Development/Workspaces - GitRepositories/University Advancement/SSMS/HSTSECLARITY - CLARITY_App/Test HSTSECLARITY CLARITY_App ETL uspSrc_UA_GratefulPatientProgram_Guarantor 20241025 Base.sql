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
WHY :	Detail updates for patient account guarantor information
AUTHOR:	Tom Burgan
SPEC:	
--------------------------------------------------------------------------------------------
INPUTS:         
**		CLARITY.dbo.PAT_ENC hsp
**		CLARITY.EPIC_UTIL.CSA_PAT_ENC AS csa
**		CLARITY.dbo.PAT_ENC_HSP peh
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
--EXEC [ETL].[uspSrc_UA_GratefulPatientProgram_Guarantor]

--ALTER   PROCEDURE [ETL].[uspSrc_UA_GratefulPatientProgram_Guarantor] 
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

--IF OBJECT_ID('tempdb..#ptguar ') IS NOT NULL
--DROP TABLE #ptguar
;WITH ptguar AS 
(

--/*
-- 00:58 48,339 20241025 1647
SELECT DISTINCT  --without distinct, results multiple rows becasue diagnosis code in multiple line
	'UVAH-'+TRIM(pt.PAT_MRN_ID)																		AS	'ucinn_ascendv2__External_System_ID__c'
	,'Patient'																											AS	'RecordTypeId'
	,pt.PAT_MRN_ID																								AS	'ucinn_ascendv2__MRN__c'
	,NULL																												AS	'ucinn_ascendv2__Donor_ID__c'
	,'TRUE'																											AS	'Is Guarantor'
	,'UVA Health'																									AS	'Constituent Type'
	,'Health System'																								AS	'ucinn_ascendv2__Data_Source__c'
	,'UVA Health'																									AS	'Name Type'
	,CAST(CASE
	    WHEN CHARINDEX(',',gacc.ACCOUNT_NAME) > 2 THEN
	        CASE
	        WHEN CHARINDEX(' ', SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+2, LEN(gacc.ACCOUNT_NAME))) > 2 THEN
	            LTRIM(SUBSTRING(gacc.ACCOUNT_NAME,CHARINDEX(',',gacc.ACCOUNT_NAME)+1,CHARINDEX(' ', SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+2, LEN(gacc.ACCOUNT_NAME)))))
	        ELSE LTRIM(SUBSTRING(gacc.ACCOUNT_NAME, CHARINDEX(',',gacc.ACCOUNT_NAME)+1, LEN(gacc.ACCOUNT_NAME)))
	        END
	    ELSE ''
	    END AS VARCHAR(255))																			AS 'ucinn_ascendv2__First_Name__c'
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
	      END AS VARCHAR(255) )																		AS 'ucinn_ascendv2__Middle_Name__c'
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
	     END AS VARCHAR(255))																			AS	'ucinn_ascendv2__Last_Name__c'
	,'UVA Health'																									AS	'ucinn_ascendv2__Address_1_Type__c'
	,'Current'																											AS	'uva_Address_1_Status__c'
	,'Health System'																								AS	'uva_Address_1_Source__c'
	,CAST(CASE WHEN rel.NAME = 'Self' THEN ad1.ADDRESS ELSE gacc.BILLING_ADDRESS_1 END AS VARCHAR(255))   'ucinn_ascendv2__Address_1_Line_1__c'
	,CAST(CASE WHEN rel.NAME = 'Self' THEN ad2.ADDRESS ELSE gacc.BILLING_ADDRESS_2 END AS VARCHAR(255))   'ucinn_ascendv2__Address_1_Line_2__c'
	,CAST(gacc.CITY AS VARCHAR(255))															AS	'ucinn_ascendv2__City_1__c'
	,CAST(guarst.NAME AS VARCHAR (255))														AS	'ucinn_ascendv2__State_1__c'
	,CAST(gacc.ZIP AS VARCHAR (40))																AS	'ucinn_ascendv2__Postal_Code_1__c'
	,'UVA Health'																									AS	'ucinn_ascendv2__Phone_1_Type__c'
	,'Current'																											AS	'uva_Phone_1_Status__c'
	,'Health System'																								AS	'uva_Phone_1_Source__c'
	,CAST(gacc.HOME_PHONE AS VARCHAR(15))											AS	'ucinn_ascendv2__Phone_1__c'
	,'UVA Health'																									AS	'ucinn_ascendv2__Email_1_Type__c'
	,'Current'																											AS	'uva_Email_1_Status__c'
	,'Health System'																								AS	'uva_Email_1_Source__c'
	,CAST(CASE WHEN rel.NAME = 'Self' THEN pt.EMAIL_ADDRESS ELSE NULL END AS VARCHAR(70)) AS 'ucinn_ascendv2__Email_1__c'
	,rel.NAME																										AS	'ucinn_ascendv2__Relationship_1_Related_Contact__c'
	,NULL																												AS	'ucinn_ascendv2__Relationship_1_Related_Role__c'
	,NULL																												AS	'ucinn_ascendv2__Relationship_Type_Formula__c'
	,CASE WHEN rship.GUAR_REL_TO_PAT_C = 15 THEN 'TRUE' ELSE 'FALSE' END AS 'ucinn_ascendv2__Is_Self_Guarantor__c'
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
	,x.UPDATE_DATE																							AS	'Update_Date'
	,GETDATE()																									AS	'Load_Dtm'
--*/
--INTO #ptguar

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
	
  LEFT OUTER JOIN CLARITY.dbo.PAT_ENC_HSP peh			ON peh.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
																										--AND peh.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID

LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser				ON ser.PROV_ID = COALESCE(hsp.VISIT_PROV_ID, peh.ADMISSION_PROV_ID)
			INNER JOIN CLARITY.dbo.PATIENT pt						ON pt.PAT_ID = hsp.PAT_ID
			--INNER JOIN CLARITY.dbo.clarity_ser_2 ser2					ON ser2.PROV_ID = ser.PROV_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE typ		ON typ.DISP_ENC_TYPE_C=hsp.ENC_TYPE_C
			LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep				ON dep.DEPARTMENT_ID = hsp.DEPARTMENT_ID
			LEFT OUTER JOIN CLARITY.dbo.ZC_SEX sex					ON pt.SEX_C=sex.RCPT_MEM_SEX_C	
			LEFT OUTER JOIN CLARITY.dbo.ZC_STATE st						ON st.STATE_C = pt.STATE_C
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad1					ON ad1.PAT_ID = pt.PAT_ID
																			 AND ad1.line=1
			LEFT OUTER JOIN CLARITY.dbo.PAT_ADDRESS ad2					ON ad2.PAT_ID = pt.PAT_ID
																			 AND ad2.line=2
			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT gacc						ON hsp.ACCOUNT_ID=gacc.ACCOUNT_ID

			LEFT OUTER JOIN CLARITY.dbo.ACCOUNT_2 gacc2				ON hsp.ACCOUNT_ID = gacc2.ACCT_ID
			
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

			LEFT OUTER JOIN CLARITY.dbo.ZC_PATIENT_TYPE zpt		ON zpt.PATIENT_TYPE_C = pt_typ.PATIENT_TYPE_C

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
			SELECT DISTINCT
				pt.PAT_ID,
				pt.PAT_NAME,
				pt.ADD_LINE_1,
				pt.ADD_LINE_2,
				pt.CITY,
				pt.STATE_C,
				--zs.NAME AS STATE_NAME,
				--zs.ABBR AS STATE_ABBR,
				pt.ZIP,
				pt.HOME_PHONE,
				pt.EMAIL_ADDRESS
			FROM CLARITY.dbo.PATIENT pt
			--LEFT OUTER JOIN CLARITY.dbo.ZC_STATE zs
			--	ON zs.STATE_C = pt.STATE_C
			) guar
				ON guar.PAT_ID = gacc.PAT_REC_OF_GUAR_ID

			LEFT OUTER JOIN CLARITY.dbo.MYC_PATIENT mycpt	ON mycpt.MYPT_ID = gacc2.MYPT_ID

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
AND typ.DISP_ENC_TYPE_C NOT IN ('2505' -- Erroneous Encounter
                                                               ,'2506' -- Erroneous Telephone Encounter
															   ,'99' -- Billing Encounter
															   ,'52' -- Anesthesia'
															   ,'53' -- Anesthesia Event
															   )
AND rship.GUAR_REL_TO_PAT_C <> 15 -- Self Guarantor
)

SELECT
	*
FROM ptguar
ORDER BY
	ucinn_ascendv2__MRN__c,
	ucinn_ascendv2__Admit_Date_Time__c
/*
SELECT DISTINCT
    CAST(enc.ucinn_ascendv2__External_System_ID__c AS VARCHAR(255))		AS ucinn_ascendv2__External_System_ID__c,
    CAST(enc.RecordTypeId AS VARCHAR(255))					AS RecordTypeId,
    CAST(enc.ucinn_ascendv2__MRN__c AS VARCHAR(100))				AS ucinn_ascendv2__MRN__c,
    CAST(enc.ucinn_ascendv2__Donor_ID__c AS VARCHAR(20))			AS ucinn_ascendv2__Donor_ID__c,
    CAST(enc.[Is Guarantor] AS VARCHAR(255))					AS [Is Guarantor],
    CAST(enc.[Constituent Type] AS VARCHAR(255))				AS [Constituent Type],
    CAST(enc.ucinn_ascendv2__Data_Source__c AS VARCHAR(255))			AS ucinn_ascendv2__Data_Source__c,
    CAST(enc.[Name Type] AS VARCHAR(255))					AS [Name Type],
    CAST(enc.ucinn_ascendv2__First_Name__c AS VARCHAR(255))			AS ucinn_ascendv2__First_Name__c,
    CAST(enc.ucinn_ascendv2__Middle_Name__c AS VARCHAR(255))			AS ucinn_ascendv2__Middle_Name__c,
    CAST(enc.ucinn_ascendv2__Last_Name__c AS VARCHAR(255))			AS ucinn_ascendv2__Last_Name__c,
    CAST(enc.ucinn_ascendv2__Address_1_Type__c AS VARCHAR(40))			AS ucinn_ascendv2__Address_1_Type__c,
    CAST(enc.uva_Address_1_Status__c AS VARCHAR(40))				AS uva_Address_1_Status__c,
    CAST(enc.uva_Address_1_Source__c AS VARCHAR(255))				AS uva_Address_1_Source__c,
    CAST(enc.ucinn_ascendv2__Address_1_Line_1__c AS VARCHAR(255))		AS ucinn_ascendv2__Address_1_Line_1__c,
    CAST(enc.ucinn_ascendv2__Address_1_Line_2__c AS VARCHAR(255))		AS ucinn_ascendv2__Address_1_Line_2__c,
    CAST(enc.ucinn_ascendv2__City_1__c AS VARCHAR(255))				AS ucinn_ascendv2__City_1__c,
    CAST(enc.ucinn_ascendv2__State_1__c AS VARCHAR(255))			AS ucinn_ascendv2__State_1__c,
    CAST(enc.ucinn_ascendv2__Postal_Code_1__c AS VARCHAR(40))			AS ucinn_ascendv2__Postal_Code_1__c,
    CAST(enc.ucinn_ascendv2__Phone_1_Type__c AS VARCHAR(40))			AS ucinn_ascendv2__Phone_1_Type__c,
    CAST(enc.uva_Phone_1_Status__c AS VARCHAR(40))				AS uva_Phone_1_Status__c,
    CAST(enc.uva_Phone_1_Source__c AS VARCHAR(255))				AS uva_Phone_1_Source__c,
    CAST(enc.ucinn_ascendv2__Phone_1__c AS VARCHAR(255))			AS ucinn_ascendv2__Phone_1__c,
    CAST(enc.ucinn_ascendv2__Email_1_Type__c AS VARCHAR(40))			AS ucinn_ascendv2__Email_1_Type__c,
    CAST(enc.uva_Email_1_Status__c AS VARCHAR(40))				AS uva_Email_1_Status__c,
    CAST(enc.uva_Email_1_Source__c AS VARCHAR(255))				AS uva_Email_1_Source__c,
    CAST(enc.ucinn_ascendv2__Email_1__c AS VARCHAR(70))			AS ucinn_ascendv2__Email_1__c,
    CAST(enc.ucinn_ascendv2__Relationship_1_Related_Contact__c AS VARCHAR(255))	AS ucinn_ascendv2__Relationship_1_Related_Contact__c,
    CAST(enc.ucinn_ascendv2__Relationship_1_Related_Role__c AS VARCHAR(255))	AS ucinn_ascendv2__Relationship_1_Related_Role__c,
    CAST(enc.ucinn_ascendv2__Relationship_Type_Formula__c AS VARCHAR(255))	AS ucinn_ascendv2__Relationship_Type_Formula__c,
    CAST(enc.ucinn_ascendv2__Is_Self_Guarantor__c AS VARCHAR(255))		AS ucinn_ascendv2__Is_Self_Guarantor__c,
    enc.Load_Dtm
--FROM #ptguar enc
FROM ptguar enc
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
*/
END

GO


