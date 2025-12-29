USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

----exec [ETL].[uspSrc_Mercury_V1_MyChart_Membership_List]
--ALTER PROCEDURE [ETL].[uspSrc_Mercury_V1_MyChart_Membership_List]

--AS

SET NOCOUNT ON

/*******************************************************************************************
WHAT:	Epic MyChart Membership List detail for CRM application implementation project. Recommend sending
three full fiscal years plus current.
WHO :	Mercury Healthcare
WHY :	Mercury Patient Engagement ingests a wide range of data and applies healthcare-specific, data science-based modeling and insights to build consumer/patient 360°
profiles that activate and inform consumer-driven, healthcare-oriented engagement strategies.
OWNER:	Tom Burgan (TMB4F)
SPEC:	[O:\Computing Services\INFSUP_S\Documentation\Projects\Mercury Healthcare]
--------------------------------------------------------------------------------------------
INPUTS FOR PROD:
	1)	None 
OUTPUTS: 
   	1)	SEQUENCE:				Member List Data Source Requirements-V1
   	2)	FILE NAMING CONVENTION:	{Source}_{File Type}_{YYYYMMDD} Ex. MYCHART_MEMBERSHIP_LIST_20140801 (append _TEST for files from TST)
   	3)	OUTPUT TYPE:			Pipe-delim ASCII text file
   	4)	TRANSFER METHOD:		sFTP
   	5)	OUTPUT LOCATION:
   	6)	FREQUENCY:				Monthly, 3rd of each month
   	7)	QUERY LOOKBACK PERIOD:	Three full fiscal years plus current
   	8)	FILE SPECIFIC NOTES:	
--------------------------------------------------------------------------------------------
MODS: 
	03/23/23 - tmb4f -	Initial creation per Mercury Healthcare per Member List Data Source Requirements-V1 Document
	03/28/23 - tmb4f -	remove patient data missing required column values; transform gender values
	04/12/23 - tmb4f -	Transform home_phone values, street address, and primary email
	04/25/23 - tmb4f -	Transform home_phone values, street address, and primary email
--------------------------------------------------------------------------------------------
INPUTS:
	CLARITY.dbo.PATIENT_MYC
	CLARITY.dbo.PATIENT
	CLARITY.dbo.ZC_STATE
	CLARITY.dbo.ZC_SEX
--------------------------------------------------------------------------------------------
OUTPUTS:
	[ETL].[uspSrc_Mercury_V1_MyChart_Membership_List]
--------------------------------------------------------------------------------------------
RUNS:    na
TO DO:	
	Col 5
******************************************************************************************/

IF OBJECT_ID('tempdb..#myc ') IS NOT NULL
DROP TABLE #myc

SELECT
	   m.PAT_ID,
	   pt.PAT_MRN_ID,
	   CASE WHEN ISNULL(pt.PAT_FIRST_NAME,'')	= '' THEN 'Unknown' ELSE ETL.RemoveSpecialChars(pt.PAT_FIRST_NAME) END AS			first_name,
	   CASE WHEN ISNULL(pt.PAT_MIDDLE_NAME,'') = '' THEN 'Unknown' ELSE pt.PAT_MIDDLE_NAME END AS			middle_name,
	   CASE WHEN ISNULL(pt.PAT_LAST_NAME,'')	= '' THEN 'Unknown' ELSE ETL.RemoveSpecialChars(pt.PAT_LAST_NAME) END AS			last_name,
	   pt.PAT_NAME,
	   CAST(NULL AS VARCHAR(10))													AS			prefix,
	   CAST(NULL AS VARCHAR(10))													AS			suffix,
	   ETL.RemoveSpecialChars(pt.ADD_LINE_1)								AS			street_address_1,
	   ETL.RemoveSpecialChars(pt.ADD_LINE_2)								AS			street_address_2,
	   CASE WHEN ISNULL(pt.CITY	,'') = '' THEN 'Unknown' ELSE pt.CITY END AS			 city,
	   CASE WHEN ISNULL(st.ABBR,'') = '' THEN 'Unknown' ELSE st.ABBR END AS			state_province,
	   CASE WHEN ISNULL(pt.ZIP,'') = '' THEN 'Unknown' ELSE pt.ZIP END AS			postal_code,
	   ge.NAME 																						AS			gender,
	   CONVERT(VARCHAR(10), pt.BIRTH_DATE, 23)						AS			birth_date,
	   DATEDIFF(yy, pt.BIRTH_DATE, GETDATE()) - 
						   CASE WHEN (MONTH(pt.BIRTH_DATE) = MONTH(GETDATE()) 
									  AND DAY(pt.BIRTH_DATE) > DAY(GETDATE()) 
									   OR MONTH (pt.BIRTH_DATE) > MONTH (GETDATE()) ) 
								THEN 1 
								ELSE 0 
								END														AS CURRENT_PT_AGE,

	   RIGHT(
	   CASE WHEN ISNULL(pt.HOME_PHONE,'') = '' THEN 'Unknown'
				  WHEN pt.HOME_PHONE = '000-000-0000' THEN 'Unknown'
				  WHEN pt.HOME_PHONE = '999-999-9999' THEN 'Unknown'
				  WHEN pt.HOME_PHONE = '9999999999' THEN 'Unknown'
				  WHEN pt.HOME_PHONE = 'None' THEN 'Unknown'
				  WHEN pt.HOME_PHONE LIKE '+%' THEN 'Unknown'
				  WHEN pt.HOME_PHONE LIKE '%dad%' THEN 'Unknown'
				  WHEN pt.HOME_PHONE LIKE '%mom%' THEN 'Unknown'
				  ELSE REPLACE(pt.HOME_PHONE,'-','')
	   END
	   ,10)																								AS			home_phone,
	   CAST(NULL AS VARCHAR(10))													AS			mobile_phone,
	   ETL.RemoveSpecialChars(TRIM('"#+-.;!()]''\ ' FROM pt.EMAIL_ADDRESS)) AS			primary_email,
	   CAST(NULL AS VARCHAR(30))													AS			secondary_email,
	   CAST(NULL AS VARCHAR(30))													AS			employer_name,
	   'Active MyChart User'																	AS			list_name,
	   ptrel.LINE,
	   ptrel.PAT_REL_RELATION_C,
	   zepr.NAME AS PAT_REL_RELATION_NAME,
	   ptrel.PAT_REL_NAME,
	   ptrel.PAT_REL_LGL_GUAR_YN

  INTO #myc

  FROM [CLARITY].[dbo].[PATIENT_MYC] m
  INNER JOIN [CLARITY].[dbo].[PAT_RELATIONSHIPS] ptrel ON m.PAT_ID = ptrel.PAT_ID
  LEFT JOIN [CLARITY].[dbo].[ZC_EMERG_PAT_REL] zepr ON zepr.EMERG_PAT_REL_C = ptrel.PAT_REL_RELATION_C
  LEFT JOIN CLARITY.dbo.PATIENT pt WITH(NOLOCK) ON pt.PAT_ID = m.PAT_ID
  LEFT JOIN CLARITY.dbo.ZC_STATE st WITH(NOLOCK) ON st.STATE_C = pt.STATE_C
  LEFT JOIN CLARITY.dbo.ZC_SEX ge WITH(NOLOCK) ON ge.RCPT_MEM_SEX_C = pt.SEX_C
  WHERE m.MYCHART_STATUS_C = 1 -- 'Activated'
	AND ptrel.PAT_REL_RELATION_C = 19 -- 'GUARDIAN'
	AND  DATEDIFF(yy, pt.BIRTH_DATE, GETDATE()) - 
						   CASE WHEN (MONTH(pt.BIRTH_DATE) = MONTH(GETDATE()) 
									  AND DAY(pt.BIRTH_DATE) > DAY(GETDATE()) 
									   OR MONTH (pt.BIRTH_DATE) > MONTH (GETDATE()) ) 
								THEN 1 
								ELSE 0 
								END	 > 18

SELECT
	PAT_ID AS Patient_Id,
    PAT_MRN_ID AS MRN,
    --first_name,
    --middle_name,
    --last_name,
    PAT_NAME AS Patient_Name,
    --prefix,
    --suffix,
    --street_address_1,
    --street_address_2,
    --city,
    --state_province,
    --postal_code,
    --gender,
	'Active'	AS Patient_MyChart_Status,
    birth_date AS Patient_Birth_Date,
    CURRENT_PT_AGE AS Patient_Age,
    --home_phone,
    --mobile_phone,
    --primary_email,
    --secondary_email,
    --employer_name,
    --list_name,
    --LINE,
    --PAT_REL_RELATION_C,
    PAT_REL_RELATION_NAME AS Relation_To_Patient,
    PAT_REL_NAME AS Relation_Name--,
    --PAT_REL_LGL_GUAR_YN
FROM #myc
ORDER BY
	PAT_ID
	
 /*
 SELECT 
	ISNULL(CONVERT(VARCHAR(100),[first_name]),'') AS first_name,
	ISNULL(CONVERT(VARCHAR(100),[middle_name]),'') AS middle_name,
	ISNULL(CONVERT(VARCHAR(100),[last_name]),'') AS last_name,
	ISNULL(CONVERT(VARCHAR(100),[prefix]),'') AS prefix,
	ISNULL(CONVERT(VARCHAR(100),[suffix]),'') AS suffix,
	ISNULL(CONVERT(VARCHAR(100),[street_address_1]),'') AS street_address_1,
	ISNULL(CONVERT(VARCHAR(100),[street_address_2]),'') AS street_address_2,
	ISNULL(CONVERT(VARCHAR(100),[city]),'') AS city,
	ISNULL(CONVERT(VARCHAR(100),[state_province]),'') AS state_province,
	ISNULL(CONVERT(VARCHAR(100),[postal_code]),'') AS postal_code,
	ISNULL(CONVERT(VARCHAR(100),[gender]),'') AS gender,
	ISNULL(CONVERT(VARCHAR(100),[birth_date]),'') AS birth_date,
	ISNULL(CONVERT(VARCHAR(100),[home_phone]),'') AS home_phone,
	ISNULL(CONVERT(VARCHAR(100),[mobile_phone]),'') AS mobile_phone,
	ISNULL(CONVERT(VARCHAR(100),[primary_email]),'') AS primary_email,
	ISNULL(CONVERT(VARCHAR(100),[secondary_email]),'') AS secondary_email,
	ISNULL(CONVERT(VARCHAR(100),[employer_name]),'') AS employer_name,
	ISNULL(CONVERT(VARCHAR(100),[list_name]),'') AS list_name
 FROM #myc
 WHERE [first_name] <> 'Unknown'
 AND [last_name] <> 'Unknown'
 AND [street_address_1] <> 'Unknown'
 AND [city] <> 'Unknown'
 AND [state_province] <> 'Unknown'
 AND [postal_code] <> 'Unknown'
 AND [home_phone] <> 'Unknown'
 ORDER BY last_name, first_name, middle_name
 */
GO


