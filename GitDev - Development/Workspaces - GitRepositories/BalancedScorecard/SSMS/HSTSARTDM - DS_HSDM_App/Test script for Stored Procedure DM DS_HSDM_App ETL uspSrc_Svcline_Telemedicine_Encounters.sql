USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =====================================================================================
-- Create procedure uspSrc_Svcline_Telemedicine_Encounters
-- =====================================================================================

--ALTER PROCEDURE [ETL].[uspSrc_Svcline_Telemedicine_Encounters]

--/*
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--*/

--AS
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_Svcline_Telemedicine_Encounters
--WHO : Brian Costello
--WHEN: 4/8/2020
--WHY : Report Telemedicine Encounter Detailed Data for dashboard and other related reporting
--
--------------------------------------------------------------------------------------------------------------------------
--MODS:       
--			04/08/2020	- BJC	-	Create stored procedure
--			04/09/2020	- BJC	-	Added more fields for Data Portal
--			04/13/2020	- BJC	-	Added logic for Video / Phone identification including Circleback = Phone if prior to Feb 2019
--			04/16/2020	- BJC	-	Re-categorize Appt Note-related encounters as 'Non-Standard'
--			04/21/2020	- BJC	-	Join to DS_HSDM_App.dbo.Ref_vwClarity_Dep in order to pull in Null Department Names (MDM excludes Deleted DEPs relevant to data)
--			04/23/2020	- BJC	-	Pull in new column for Appt_Note, update logic for assigning Video and Phone
--			04/24/2020	- BJC	-	Update logic for assigning Video and Phone
--			05/12/2020	- BJC	-	Add logic to account for Urgent Care as Event Types, update join to Dim_Clrt_Pt
--			05/13/2020	- BJC	-	Add new Column for SDE UVA#8336
--			05/27/2020	- BJC	-	Add new Column for Pt_Home_Resource, update logic for Event Type, Communication_Type
--			06/08/2020	- BJC	-	Add Postal Code, County, State, Country
--			07/15/2020	- BJC	-	Source Appt_Note from Stage table, Add Guarantor
--          01/07/2022  - TMB  -   Add Telehealth_Mode and Telehealth_Mode_Name
--          03/10/2022  - TMB  -   Replace Teleheath_Mode column with Telehealth_Mode_C, add columns UVaID and CANCEL_REASON_NAME
--			12/02/2022  - TMB  -	Add columns APPT_DTTM, EMPlye_Nme, and EMPlye_Systm_Login

--************************************************************************************************************************

    SET NOCOUNT ON;

SELECT DISTINCT 
		TMED.sk_Telemedicine_Encounters 
		,CAST(TMED.Encounter_CSN AS NUMERIC(18,0))																	AS Encounter_CSN
		,CAST(TMED.Event_Date AS DATETIME)																			AS Event_Date
		,CAST(TMED.Encounter_CSN AS BIGINT)																			AS Event_ID
		,CAST('Telehealth Encounter' AS VARCHAR(150))																AS Event_Category
		,CASE WHEN (CONVERT(INT,DATEDIFF(d, TMED.Event_Date,GETDATE())) < 4 AND CONVERT(INT,DATEDIFF(d, TMED.Event_Date,GETDATE())) > 0) THEN 'Y'								
			ELSE 'N' END																							AS Event_Date_Within_Nightly_Processing
		,CAST(TMED.Person_ID AS INT)																				AS Person_ID
		,CAST(TMED.Person_Name AS VARCHAR(200))																		AS Person_Name
		,CAST(TMED.Encounter_Status_Category AS VARCHAR(200))														AS Encounter_Status_Category
		,CAST(TMED.Encounter_Status AS VARCHAR(200))																AS Encounter_Status
		,CAST(TMED.Event_Count AS NUMERIC(18,0))																	AS Event_Count
		,CASE WHEN TMED.Circleback =1 THEN CAST('Circleback' AS VARCHAR(50))	
			WHEN TMED.EConsult =1 THEN CAST('EConsult'  AS VARCHAR(50))
			WHEN TMED.Urgent_Care = 1 THEN CAST('Virtual Urgent Care'  AS VARCHAR(50))
			WHEN (TMED.Telemed_Sched =1 AND TMED.Pt_Home_Resource =1) THEN CAST('DTC Telemedicine Scheduled' AS VARCHAR(50))	 					
			WHEN (TMED.Telemed_Sched =1 AND TMED.Epic_Department_ID=10242001) THEN CAST('Contracted Telemedicine Scheduled' AS VARCHAR(50))
			WHEN TMED.Telemed_Unsecure =1 THEN CAST('DTC Prof Remote Scheduled' AS VARCHAR(50))
			ELSE CAST('Other' AS VARCHAR(50)) END																	AS Event_Type
		,CASE WHEN TMED.EConsult =1 THEN CAST('EConsult'  AS VARCHAR(50)) 
			WHEN TMED.Comm_Type_Billing IS NOT NULL THEN CAST(Comm_Type_Billing AS VARCHAR(50)) 
			WHEN TMED.SMARTPHRASE_ID = 543071 THEN CAST('Video' AS VARCHAR(50))
			WHEN TMED.SMARTPHRASE_ID = 543052 THEN CAST('Phone' AS VARCHAR(50))	
			WHEN TMED.Smartphrase_Name LIKE ('%TELMEDVIDEO%') THEN CAST('Video' AS VARCHAR(50))
			WHEN TMED.Smartphrase_Name LIKE ('%TELPHCALL%') THEN CAST('Phone' AS VARCHAR(50))
			WHEN TMED.Telemed_Sched =1 THEN CAST('Video' AS VARCHAR(50))
			WHEN (TMED.Circleback =1 AND TMED.Event_Date<'2019-02-01') THEN CAST('Phone'  AS VARCHAR(50))
			ELSE CAST('Phone or Video' AS VARCHAR(50)) END															AS Communication_Type
		,CAST(TMED.Visit_Type AS VARCHAR(200))																		AS Visit_Type	
		,CAST(TMED.Encounter_Type AS VARCHAR(200))																	AS Encounter_Type
		,CAST(TMED.Epic_Department_ID AS NUMERIC(18,0))																AS Epic_Department_ID  																 
		,CASE WHEN MDM.EPIC_DEPT_NAME IS NULL THEN CAST(DEP.DEPARTMENT_NAME AS VARCHAR(255))	
			ELSE CAST(MDM.EPIC_DEPT_NAME AS VARCHAR(255)) END														AS Epic_Department_Name	
		--,DDEP.Clrt_DEPt_Nme AS Lst_Logn_DEPt_Nme -- VARCHAR(254)	
		,CAST(MDM.EPIC_EXT_NAME AS VARCHAR(255))																	AS Epic_Department_Name_External																	 		
		,CAST(MDM.EPIC_SPCLTY AS VARCHAR(200))																		AS Epic_Department_Specialty
		,CAST(TMED.Prov_ID AS VARCHAR(18))																			AS Prov_ID
		,CAST(PHY.Prov_Nme AS VARCHAR(200))																			AS Prov_Name
		,CAST(PHY.sk_dim_physcn AS INT)																				AS sk_dim_physcn
		,CAST(PHY.UHC_Specialty AS VARCHAR(200))																	AS Prov_Specialty
		,CAST(PHY.Prov_Typ AS VARCHAR(200))																			AS Prov_Type
		,CAST(PHY.Staff_Resource AS VARCHAR(200))																	AS Prov_Resource_Type
		,CAST(TMED.Pt_Home_Resource AS INT)																			AS Pt_Home_Resource
		,CAST(TMED.Smartdata_Element AS VARCHAR(200))																AS Smartdata_Element
		,CAST(TMED.Smartphrase_ID AS NUMERIC(18,0))																	AS Smartphrase_ID
		,CAST(TMED.Smartphrase_Name AS VARCHAR(200))																AS Smartphrase_Name 
		,CAST(TMED.HB_HAR AS NUMERIC(18,0))																			AS HB_HAR
		,CAST(TMED.PB_HAR AS NUMERIC(18,0))																			AS PB_HAR
		,CAST(MyChart_Status AS VARCHAR(150))																		AS MyChart_Status
		,CAST(Visit_Coverage AS VARCHAR(150))																		AS Visit_Coverage
		,CAST(PAT.BIRTH_DATE AS DATETIME)																			AS person_birth_date
		,CAST(PAT.Clrt_Sex AS VARCHAR(255))																			AS person_gender
		,CASE WHEN CONVERT(INT,DATEDIFF(d, PAT.BIRTH_DATE, TMED.Event_Date)/365.25) < 18 THEN 1							    
		 	ELSE 0 END																								AS peds
  		,CAST(TMED.transplant AS SMALLINT)																			AS transplant
  		,CAST(NULL AS SMALLINT)																						AS oncology
		,CAST(PAT.sk_Dim_Pt AS INT)																					AS sk_Dim_Pt
		,CAST(ENC.sk_Fact_Pt_Acct AS BIGINT)																		AS sk_Fact_Pt_Acct 
		,CAST(ENC.sk_Fact_Pt_Enc_Clrt AS INT)																		AS sk_Fact_Pt_Enc_Clrt 
		,CASE WHEN TMED.transplant=1 THEN 10
		 	WHEN CONVERT(INT,DATEDIFF(d, PAT.BIRTH_DATE, TMED.Event_Date)/365.25) < 18 THEN 11									    
		 	ELSE CAST(MDM2.service_line_id AS INT) END																AS service_line_id
		,CASE WHEN TMED.transplant=1 THEN 'Transplant'
		 	WHEN CONVERT(INT,DATEDIFF(d, PAT.BIRTH_DATE, TMED.Event_Date)/365.25) < 18 THEN 'Womens and Childrens'									    
		 	ELSE CAST(MDM2.service_line AS VARCHAR(150)) END														AS service_line
		,CASE WHEN CONVERT(INT,DATEDIFF(d, PAT.BIRTH_DATE, TMED.Event_Date)/365.25) < 18 THEN 1	
		 	ELSE CAST(MDM2.sub_service_line_id AS INT) END															AS sub_service_line_id 
		,CASE WHEN CONVERT(INT,DATEDIFF(d, PAT.BIRTH_DATE, TMED.Event_Date)/365.25) < 18 THEN 'Children'	
		 	ELSE CAST(MDM2.sub_service_line AS VARCHAR(150)) END													AS sub_service_line
		,CAST(MDM.opnl_service_id AS INT)																			AS opnl_service_id
		,CAST(MDM.opnl_service_name AS VARCHAR(150))																AS opnl_service_name
		,CAST(MDM.corp_service_line_id AS INT)																		AS corp_service_line_id
		,CAST(MDM.corp_service_line AS VARCHAR(150))																AS corp_service_line
		,CAST(MDM.hs_area_id AS SMALLINT)																			AS hs_area_id
		,CAST(MDM.hs_area_name AS VARCHAR(150))																		AS hs_area_name
		,CAST(MDM.POD_ID AS VARCHAR(66))																			AS POD_ID
		,CAST(MDM.PFA_POD AS VARCHAR(100))																			AS POD_Name
		,CAST(MDM.hub_id AS VARCHAR(66))																			AS Hub_ID
		,CAST(MDM.HUB AS VARCHAR(100))																				AS Hub_Name
		,CAST(MDM.LOC_ID AS INT)																					AS rev_location_id		
		,CAST(MDM.REV_LOC_NAME AS VARCHAR(150))																		AS rev_location	
		,CAST(SOM.som_hs_area_id AS SMALLINT)																		AS som_hs_area_id
		,CAST(SOM.som_hs_area_name	AS VARCHAR(150))																AS som_hs_area_name		
		,CAST(SOM.som_group_id AS INT)																				AS som_group_id
		,CAST(SOM.som_group_name AS VARCHAR(150))																	AS som_group_name
		,CAST(SOM.Department_ID AS SMALLINT)																		AS som_department_id
		,CAST(SOM.Department AS VARCHAR(150))																		AS som_department_name		
		,CAST(SOM.Org_Number AS INT)																				AS som_division_id
		,CAST(SOM.Organization AS VARCHAR(150))																		AS som_division_name
		,CAST(MDM.UPG_PRACTICE_FLAG	 AS INT)																		AS upg_practice_flag
		,CAST(MDM.UPG_PRACTICE_REGION_ID AS INTEGER)																AS upg_practice_region_id
		,CAST(MDM.UPG_PRACTICE_REGION_NAME AS VARCHAR(150))															AS upg_practice_region_name
		,CAST(MDM.UPG_PRACTICE_ID AS INTEGER)																		AS upg_practice_id
		,CAST(MDM.UPG_PRACTICE_NAME AS VARCHAR(150))																AS upg_practice_name																	
		,CAST(TRY_CONVERT(INT,PHY.Financial_Division,0) AS INT)														AS Financial_Division_ID
		,CAST(PHY.Financial_Division_Name	AS VARCHAR(150))														AS Financial_Division_Name
		,CAST(TRY_CONVERT(INT,PHY.Financial_SubDivision,0) AS INT)													AS Financial_Sub_Division_ID
		,CAST(PHY.Financial_SubDivision_Name AS VARCHAR(150))														AS Financial_Sub_Division_Name
		,CAST(LEFT(DATENAME(MM, TMED.Event_Date), 3) + ' ' + CAST(DAY(DMDT.day_date) AS VARCHAR(2))	AS VARCHAR(10))	AS report_period
		,CAST(CAST(TMED.Event_Date AS DATE) AS SMALLDATETIME)														AS report_date
		,CAST(DMDT.fmonth_num AS SMALLINT)																			AS fmonth_num
		,CAST(DMDT.fmonth_name AS VARCHAR(10))																		AS fmonth_name
		,CAST(DMDT.fyear_num AS SMALLINT)																			AS fyear_num
		,CAST(DMDT.fyear_name AS VARCHAR(10))																		AS fyear_name
		,CAST(NOTES.APPT_NOTE AS VARCHAR(1000))																		AS Appt_Note
		,CAST(PAT.POSTALCODE AS VARCHAR(300))																		AS Postal_Code
		,CAST(PAT.COUNTY AS VARCHAR(300))																			AS County
		,CAST(PAT.STATEORPROVINCE AS VARCHAR(300))																	AS StateorProvince
		,CAST(PAT.COUNTRY AS VARCHAR(300))																			AS Country
		,CAST(CONCAT(GUAR.Account_Name,' [',CAST(GUAR.ACCOUNT_ID AS VARCHAR),']') AS VARCHAR(300))					AS Guarantor
		,TMED.Telehealth_Mode_C
		,TMED.Telehealth_Mode_Name
		,PHYS.UVaID
		,APPTS.CANCEL_REASON_NAME
		,APPTS.APPT_DTTM -- DATETIME
		,EMP.EMPlye_Nme -- VARCHAR(160)
		,EMP.EMPlye_Systm_Login -- VARCHAR(254)

FROM DS_HSDM_App.Stage.Telemedicine_Encounters AS TMED
INNER JOIN	DS_HSDW_Prod..Dim_Date													AS  DMDT	ON CAST(dmdt.day_date AS DATE) = CAST(TMED.Event_Date AS DATE)
LEFT JOIN	DS_HSDW_Prod..Ref_MDM_Location_Master									AS  MDM		ON MDM.epic_department_id=TMED.Epic_Department_ID
LEFT JOIN	DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc						AS  MDM2	ON MDM2.epic_department_id=TMED.Epic_Department_ID
LEFT JOIN	DS_HSDM_App..Ref_vwClarity_Dep											AS  DEP		ON DEP.Department_id=TMED.Epic_Department_ID
LEFT JOIN	DS_HSDW_Prod..Dim_Clrt_SERsrc											AS  PHY		ON PHY.PROV_ID=TMED.Prov_ID
LEFT JOIN    DS_HSDW_Prod..Dim_Physcn													AS  PHYS		ON PHYS.sk_Dim_Physcn = PHY.sk_Dim_Physcn
LEFT JOIN	DS_HSDM_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv					AS  SOM		ON SOM.Epic_Financial_Subdivision_Code=PHY.Financial_SubDivision
LEFT JOIN	DS_HSDW_Prod..Fact_Pt_Enc_Clrt											AS  ENC		ON ENC.PAT_ENC_CSN_ID=TMED.Encounter_CSN
LEFT JOIN	DS_HSDW_Prod..Dim_Clrt_Pt												AS	PAT		ON PAT.sk_Dim_Clrt_Pt=ENC.sk_Dim_Clrt_Pt AND PAT.Pt_Rec_Merged_Out<>1
LEFT JOIN	DS_HSDM_App.Stage.Telemedicine_Appt_Notes								AS	NOTES	ON NOTES.Encounter_CSN=TMED.Encounter_CSN
LEFT JOIN   DS_HSDW_Prod.Rptg.vwFact_AccountGuarantor								AS  GUAR	ON GUAR.ACCOUNT_ID=TMED.Guarantor_ID
LEFT JOIN	DS_HSDM_App.Stage.Scheduled_Appointment									AS  APPTS	ON APPTS.PAT_ENC_CSN_ID = TMED.Encounter_CSN
LEFT JOIN  DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye								AS  EMP	ON EMP.EMPlye_Usr_ID = APPTS.APPT_ENTRY_USER_ID
--LEFT JOIN  DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt									AS  DDEP ON DDEP.sk_Dim_Clrt_DEPt = EMP.sk_Lst_Logn_DEP

GO


