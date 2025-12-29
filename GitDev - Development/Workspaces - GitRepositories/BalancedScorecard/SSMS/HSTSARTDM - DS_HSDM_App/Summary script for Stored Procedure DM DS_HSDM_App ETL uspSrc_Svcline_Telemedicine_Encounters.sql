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
--          01/07/2022   - TMB   -   Add Telehealth_Mode and Telehealth_Mode_Name
--          03/10/2022   - TMB   -   Replace Teleheath_Mode column with Telehealth_Mode_C, add columns UVaID and CANCEL_REASON_NAME

--************************************************************************************************************************

    SET NOCOUNT ON;

SELECT DISTINCT
   --       TMED.Encounter_CSN
		 --,TMED.Event_Date
         --,TMED.EConsult
          TMED.EConsult
		 ,TMED.Comm_Type_Billing
		 ,TMED.Smartphrase_ID
		 ,TMED.Smartphrase_Name
		 ,TMED.Telemed_Sched
		 ,TMED.Circleback
		 ,CASE WHEN TMED.EConsult =1 THEN CAST('EConsult'  AS VARCHAR(50)) 
			WHEN TMED.Comm_Type_Billing IS NOT NULL THEN CAST(Comm_Type_Billing AS VARCHAR(50)) 
			WHEN TMED.SMARTPHRASE_ID = 543071 THEN CAST('Video' AS VARCHAR(50))
			WHEN TMED.SMARTPHRASE_ID = 543052 THEN CAST('Phone' AS VARCHAR(50))	
			WHEN TMED.Smartphrase_Name LIKE ('%TELMEDVIDEO%') THEN CAST('Video' AS VARCHAR(50))
			WHEN TMED.Smartphrase_Name LIKE ('%TELPHCALL%') THEN CAST('Phone' AS VARCHAR(50))
			WHEN TMED.Telemed_Sched =1 THEN CAST('Video' AS VARCHAR(50))
			WHEN (TMED.Circleback =1 AND TMED.Event_Date<'2019-02-01') THEN CAST('Phone'  AS VARCHAR(50))
			ELSE CAST('Phone or Video' AS VARCHAR(50)) END															AS Communication_Type

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
LEFT JOIN	DS_HSDM_App.Stage.Scheduled_Appointment									AS	APPTS	ON APPTS.PAT_ENC_CSN_ID = TMED.Encounter_CSN

--ORDER BY Communication_Type, TMED.Event_Date, TMED.Encounter_CSN
ORDER BY Communication_Type

GO


