USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

---- =====================================================================================
---- Alter procedure uspSrc_Telemedicine Encounters
---- =====================================================================================

ALTER PROCEDURE [ETL].[uspSrc_Telemedicine_Encounters]

/*
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
    )
*/

AS
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_Telemedicine Encounters
--WHO : Brian Costello
--WHEN: 4/6/2020
--WHY : Report Telemedicine Encounters for dashboard and other related reporting
--
--------------------------------------------------------------------------------------------------------------------------
--MODS:       
--			04/04/2020	- BJC	-	Create stored procedure
--			04/08/2020	- BJC	-	Re-do query for performance
--			04/09/2020	- BJC	-	Removal of duplicates
--			04/13/2020	- BJC	-	Added Encounters with Appt Notes related to Confirmed Video and flags for Video / Phone identification
--			04/13/2020	- BJC	-	Added joins to Encounters with Appt Notes related to Video and Phone (Lara Oktay reviewed comments and determined whether to associated them with Video or Phone)
--			04/17/2020	- BJC	-	Added date filters for contact dates >= 07-01-2017 (Epic Phase 2 go-live) for better performance
--			04/21/2020	- BJC	-	Encounters with Appt Notes no longer an inclusion criteria for Telemed encoutners. Used solely for enhancing existing encoutner data
--			04/23/2020	- BJC	-	Add Appt_Note column, add inclusion of SDE #8336 when Unsecure Smartphrases not present.
--			04/24/2020	- BJC	-	Display Smartphrase name for Telemed or Telmed spartphrases.
--			05/06/2020	- BJC	-	Add two new visit types per J Fuchs and L Oktay:: 11702811 Telemedicine Sick, 11702813 Telemedicine Nurse Prenatal
--			05/07/2020	- BJC	-	Add two new visit types per D Wolf and L Oktay: 11702808 ViTel Net Adult, 11702809 ViTel Net Peds
--			05/08/2020	- BJC	-	Add new Column for Urgent Care encounters. 
--			05/13/2020	- BJC	-	Add new Column for SDE UVA#8336
--			05/22/2020	- BJC	-	Add new Comm_Type_Billing column per L Oktay. No longer assign Video or Phone assigned based on Scheduling Notes. Add new Prof Remote Initial Visit visit type. Update logic for Video / Audio. 
--			07/15/2020	- BJC	-	Filter erroneous encounter types, Null Appt_Note, add Guarantor_ID
--          12/15/2021  - TMB   -   Add TELEHEALTH_MODE_C and name columns
--			01/27/2022	- ARD	-	Add new CTE to begin counting Telehealth Mode as a valid telehealth encounter. Also introduced logic to cease counting telehealth encounters using the old standard as of 10/12/2021.
--          05/16/2022   - TMB   -   Remove logic that assigns provider to encounter based on resource type

--************************************************************************************************************************

/*	
 As of 05/06/2020:
  This script is used as a building block for downstream stored procedure on HSTSARTDM DS_HSDM_App ETL.uspSrc_Svcline_Telemedicine_Encounters
*/

--DECLARE @p_DateStart AS DATE
--SET @p_DateStart = '20210701'

; WITH TELEMED_PT_HOME_PROVIDER AS
		(
		SELECT 
		PAT_ENC_CSN_ID										AS Encounter_CSN
		
		FROM CLARITY..PAT_ENC_APPT							AS PE
		
		WHERE  1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND PE.PROV_ID='1301496' --TELEMEDICINE PATIENT HOME 
		)


,TRANSPLANT_ENCS AS  -- to identify Transplant-related encounters
		(
		SELECT DISTINCT
		PAT_ENC_CSN_ID										AS Encounter_CSN
		
		FROM CLARITY..APPT_UTIL_SNAPSHOT					AS UTIL
		INNER JOIN CLARITY..CLARITY_DEP 					AS DEP ON DEP.DEPARTMENT_ID=UTIL.UTIL_SNAP_DEPT_ID
		
		WHERE  1=1
		AND UTIL.CONTACT_DATE>='07-01-2017'
		--AND UTIL.CONTACT_DATE>=@p_DateStart
		AND util.CONTACT_DATE < '20211013'
		AND DEP.RPT_GRP_THIRTY='Transplant' --TRANSPLANT SERVICE LINE 
		)


,CIRCLEBACK_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT DISTINCT 
		hno.PAT_ENC_CSN_ID							AS Encounter_CSN
		,dphr.SMARTPHRASES_ID						AS SMARTPHRASES_ID
		
		FROM CLARITY..NOTE_SMARTPHRASE_IDS			AS dphr
		LEFT JOIN CLARITY..CL_SPHR					AS sphr ON sphr.SMARTPHRASE_ID=dphr.SMARTPHRASES_ID
		LEFT JOIN CLARITY..HNO_INFO					AS hno	ON hno.NOTE_ID=dphr.NOTE_ID
		INNER JOIN (SELECT 
					hno.PAT_ENC_CSN_ID
					,MAX(LINE) MAXIMUM
					
					FROM CLARITY..NOTE_SMARTPHRASE_IDS			AS phr
					INNER JOIN CLARITY..CL_SPHR					AS sphr ON sphr.SMARTPHRASE_ID=phr.SMARTPHRASES_ID
					LEFT JOIN CLARITY..HNO_INFO					AS hno	ON hno.NOTE_ID=phr.NOTE_ID
					LEFT JOIN CLARITY..PAT_ENC					AS pe	ON pe.PAT_ENC_CSN_ID=hno.PAT_ENC_CSN_ID

					WHERE 1=1
					AND pe.CONTACT_DATE>='07-01-2017'
					--AND pe.CONTACT_DATE >= @p_DateStart
					AND pe.CONTACT_DATE < '20211013'
					AND phr.SMARTPHRASES_ID IN	(
												413317		--CIRCLEBACK
												)
					GROUP BY hno.PAT_ENC_CSN_ID
					) UNIQUE_NOTES ON (UNIQUE_NOTES.PAT_ENC_CSN_ID = hno.PAT_ENC_CSN_ID AND UNIQUE_NOTES.MAXIMUM = dphr.LINE)

		
		WHERE  1=1
		AND SMARTPHRASES_ID IN	(
								413317		--CIRCLEBACK
								) 	
		GROUP BY 
		hno.PAT_ENC_CSN_ID
		,dphr.SMARTPHRASES_ID
		)


,ECONSULT_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT
		pe.PAT_ENC_CSN_ID							AS Encounter_CSN
		,NULL										AS SMARTPHRASES_ID
		
		FROM CLARITY..PAT_ENC						AS pe
		
		WHERE 1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND pe.ENC_TYPE_C='72' --ECONSULT
		)


,URGENT_CARE_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT
		pe.PAT_ENC_CSN_ID							AS Encounter_CSN
		,NULL										AS SMARTPHRASES_ID
		
		FROM CLARITY..PAT_ENC						AS pe
		
		WHERE 1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND pe.APPT_PRC_ID IN 
		(
		'11702808' --ViTel Net Adult
		,'11702809' --ViTel Net Peds
		)
		)


,SCHED_DTC_VISIT_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT
		pe.PAT_ENC_CSN_ID							AS Encounter_CSN
		,NULL										AS SMARTPHRASES_ID
		
		FROM CLARITY..PAT_ENC						AS pe
		
		WHERE 1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND pe.APPT_PRC_ID IN 
		(

            '11702624' --TELEMEDICINE CLINIC SUPPORT FV
            ,'11702623' --TELEMEDICINE CLINIC SUPPORT IV
            ,'1170025' --TELEMEDICINE EDUCATION VISIT
            ,'1170017' --TELEMEDICINE FOLLOW-UP
            ,'11702797' --TELEMEDICINE GRANT FOLLOW-UP
            ,'11702796' --TELEMEDICINE GRANT INITIAL
            ,'11701853' --TELEMEDICINE INITIAL PRENATAL
            ,'1170016' --TELEMEDICINE INITIAL VISIT
            ,'1170024' --TELEMEDICINE NURSE VISIT
            ,'11702499' --TELEMEDICINE NUTRITION
            ,'11700380' --TELEMEDICINE POST-OP
            ,'11700379' --TELEMEDICINE PRE-OP
            ,'11702854' --TELEMEDICINE ROUTINE PRENATAL
            ,'11702500' --TELEMEDICINE SOCIAL WORK
			,'11702811' --Telemedicine Sick
			,'11702813' --Telemedicine Nurse Prenatal
		)
		)


,UNSECURE_SMARTPHRASE_NOTES AS  -- to identify encounters with Telemed Smartphrases
		(
		SELECT DISTINCT 
		hno.PAT_ENC_CSN_ID							AS Encounter_CSN
		,hno.NOTE_ID								AS Note_ID
		,phr.SMARTPHRASES_ID						AS SMARTPHRASES_ID
		,MAX(phr.LINE) MAXIMUM
					
		FROM CLARITY..NOTE_SMARTPHRASE_IDS			AS phr
		LEFT JOIN CLARITY..HNO_INFO					AS hno	ON hno.NOTE_ID=phr.NOTE_ID
		LEFT JOIN CLARITY..PAT_ENC					AS pe	ON pe.PAT_ENC_CSN_ID=hno.PAT_ENC_CSN_ID

		WHERE 1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND phr.SMARTPHRASES_ID IN	(
									543071		--TELMEDVIDEO - created for COVID reporting per Chris Keith
									,543052		--TELPHCALL - created for COVID reporting per Chris Keith
									)
		GROUP BY 
		hno.PAT_ENC_CSN_ID
		,hno.NOTE_ID
		,phr.SMARTPHRASES_ID		
		)


,UNSECURE_SMARTPHRASE_DUPS AS   -- to order rows for encounters with multiple rows of Smartphrases
		(
		SELECT 
		Encounter_CSN								AS Encounter_CSN
		,SMARTPHRASES_ID							AS SMARTPHRASES_ID
        ,ROW_NUMBER() OVER (PARTITION BY Encounter_CSN ORDER BY MAXIMUM) row_num
		
		FROM UNSECURE_SMARTPHRASE_NOTES
		)


,UNSECURE_SMARTPHRASE_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT 
		Encounter_CSN								AS Encounter_CSN
		,SMARTPHRASES_ID							AS SMARTPHRASES_ID
      		
		FROM UNSECURE_SMARTPHRASE_DUPS

		WHERE ROW_NUM=1
		)


,UNSECURE_SDE_NOTES AS      -- to identify encounters with Telemed SDE
		(
		SELECT DISTINCT	sed.CONTACT_SERIAL_NUM				AS Encounter_CSN
		,sed.SRC_NOTE_ID									AS Note_ID
		,CASE 
			WHEN phr.SMARTPHRASES_ID IN	
									(
									543071		--TELMEDVIDEO - created for COVID reporting per Chris Keith
									,543052		--TELPHCALL - created for COVID reporting per Chris Keith
									) THEN phr.SMARTPHRASES_ID	
			WHEN sphr.SMARTPHRASE_NAME LIKE ('%TELMEDVIDEO%') THEN phr.SMARTPHRASES_ID							
			WHEN sphr.SMARTPHRASE_NAME LIKE ('%TELPHCALL%') THEN phr.SMARTPHRASES_ID 
			ELSE NULL END									AS SMARTPHRASES_ID
			,sphr.SMARTPHRASE_NAME
		FROM CLARITY..SMRTDTA_ELEM_DATA						AS sed										
		LEFT JOIN CLARITY..SMRTDTA_ELEM_VALUE				AS sev	ON sed.HLV_ID = sev.HLV_ID  
		LEFT JOIN CLARITY..CLARITY_CONCEPT					AS cc	ON sed.ELEMENT_ID = cc.CONCEPT_ID
		LEFT JOIN CLARITY..NOTE_SMARTPHRASE_IDS				AS phr  ON phr.NOTE_ID=sed.SRC_NOTE_ID
		LEFT JOIN CLARITY..CL_SPHR							AS sphr ON sphr.SMARTPHRASE_ID=phr.SMARTPHRASES_ID	
		LEFT JOIN CLARITY..PAT_ENC							AS pe	ON pe.PAT_ENC_CSN_ID=sed.CONTACT_SERIAL_NUM	
		
		WHERE 1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND sed.context_name='encounter'
		AND sed.element_id IN ('UVA#8336') 
		)


,UNSECURE_SDE_DUPS AS   -- to order rows for encounters with multiple rows of Smartphrases
		(
		SELECT 
		Encounter_CSN								AS Encounter_CSN
		,SMARTPHRASES_ID							AS SMARTPHRASES_ID
        ,ROW_NUMBER() OVER (PARTITION BY Encounter_CSN ORDER BY SMARTPHRASES_ID DESC) row_num
		
		FROM UNSECURE_SDE_NOTES
		)


,UNSECURE_SDE_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT 
		Encounter_CSN								AS Encounter_CSN
		,SMARTPHRASES_ID							AS SMARTPHRASES_ID
      		
		FROM UNSECURE_SDE_DUPS

		WHERE ROW_NUM=1
		)


,UNSECURE_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT
		pe.PAT_ENC_CSN_ID							AS Encounter_CSN
		,NULL										AS SMARTPHRASES_ID
		
		FROM CLARITY..PAT_ENC						AS pe
		
		WHERE 1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND pe.ENC_TYPE_C='2105700001' --Prof Remot Visit Type
		)		


,UNSECURE_VISIT_PROF_REMOTE_ENCs AS  -- COUNTS AS TELEMEDICINE ENCOUNTER
		(
		SELECT
		pe.PAT_ENC_CSN_ID							AS Encounter_CSN
		,NULL										AS SMARTPHRASES_ID
		
		FROM CLARITY..PAT_ENC						AS pe
		
		WHERE 1=1
		AND pe.CONTACT_DATE>='07-01-2017'
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe.CONTACT_DATE < '20211013'
		AND pe.APPT_PRC_ID IN
			(
			'11702814' --PROF REMOTE INITIAL VISIT
			,'11702802' --PROF REMOTE VISIT
			)
		)		


,APPT_NOTES AS 
		(
		SELECT DISTINCT
		pen.Encounter_CSN							AS Encounter_CSN
		,pen.APPT_NOTE								AS Appt_Note
		,pen.COMM_TYPE

		FROM [CLARITY_App].[Stage].[Telemedicine_Appt_Notes]		AS pen	
		)

---- ==================================================================================================================================
---- AD - 01/27/22 -  Added to introduce specific telehealth modes as an event that should count towards overall telehealth encounters.
---- ==================================================================================================================================
,MODES_ENC AS	--Counts as Telehealth Encounters
		(
		SELECT
		pe.PAT_ENC_CSN_ID							AS Encounter_CSN
		,NULL										AS SMARTPHRASES_ID

		FROM CLARITY..PAT_ENC pe
		INNER JOIN CLARITY..PAT_ENC_6			AS pe6		ON pe6.PAT_ENC_CSN_ID = pe.PAT_ENC_CSN_ID

		WHERE 1=1
		AND pe.CONTACT_DATE >='20210101' --Per M.Kragie, begin including telehealth modes as of 01/01/2021
		--AND pe.CONTACT_DATE >= @p_DateStart
		AND pe6.TELEHEALTH_MODE_C IN ('2','3','4','5') --Video [2], Telephone [3], Clinic to Clinic Video [4], E-Visit [5] --AD - Confirmed by M.Kragie with Telehealth Department.
		)

SELECT 

DISTINCT
		 CAST(DATASET.Encounter_CSN AS NUMERIC(18,0))																AS Encounter_CSN
		,CAST(idn.IDENTITY_ID AS INT)																				AS Person_ID
		,CAST(PT.PAT_NAME AS VARCHAR(200))																			AS Person_Name
		,CASE WHEN PE.CONTACT_DATE>=(GETDATE()-1) AND PE.CALCULATED_ENC_STAT_C=2 THEN 'Possible' ELSE
				ests.NAME END																						AS Encounter_Status_Category
		,CASE WHEN PE.CONTACT_DATE>=(GETDATE()-1) AND PE.CALCULATED_ENC_STAT_C=2 THEN 'Possible-Scheduled' 
				WHEN PE.CALCULATED_ENC_STAT_C IN (1,3) THEN CONCAT(ests.NAME,'-',asts.NAME)
						ELSE ests.NAME END																			AS Encounter_Status
		,CAST(PE.CONTACT_DATE AS DATETIME)																			AS Event_Date
		, 1																											AS Event_Count
 		,CAST(prc.PRC_NAME AS VARCHAR(200))																			AS Visit_Type
		,CAST(ENC.NAME AS VARCHAR(200))																				AS Encounter_Type
		,CAST(PE.EFFECTIVE_DEPT_ID AS NUMERIC(18,0))																AS Epic_Department_ID		 																 
		--,CASE WHEN SER.STAFF_RESOURCE_C=2 THEN CAST(hap.PROV_ID AS VARCHAR(18))
		--	WHEN pe.VISIT_PROV_ID IS NULL THEN  CAST(hap.PROV_ID AS VARCHAR(18))
		--	ELSE CAST(pe.VISIT_PROV_ID AS VARCHAR(18)) END															AS Prov_ID			 																 
		,CAST(pe.VISIT_PROV_ID AS VARCHAR(18))										AS Prov_ID																		 
		--,CASE WHEN SER.STAFF_RESOURCE_C=2 THEN CAST(SERH.PROV_NAME AS VARCHAR(200))
		--	WHEN pe.VISIT_PROV_ID IS NULL THEN  CAST(SERH.PROV_NAME AS VARCHAR(200))
		--	ELSE CAST(SER.PROV_NAME AS VARCHAR(200)) END															AS Prov_Name																 
		,CAST(SER.PROV_NAME AS VARCHAR(200))										AS Prov_Name
		,CASE WHEN UNSECURE_SDE_ENCs.Encounter_CSN IS NOT NULL THEN CAST('UVA#8336' AS VARCHAR(200))
			ELSE NULL END																							AS Smartdata_Element
		,CASE WHEN UNSECURE_SMARTPHRASE_ENCs.SMARTPHRASES_ID IS NOT NULL THEN UNSECURE_SMARTPHRASE_ENCs.SMARTPHRASES_ID	
			WHEN UNSECURE_SDE_ENCs.SMARTPHRASES_ID IS NOT NULL THEN UNSECURE_SDE_ENCs.SMARTPHRASES_ID
			WHEN CIRCLEBACK_ENCs.SMARTPHRASES_ID IS NOT NULL THEN CIRCLEBACK_ENCs.SMARTPHRASES_ID
 			ELSE NULL END																							AS Smartphrase_ID
		,CASE WHEN UNSECURE_SMARTPHRASE_ENCs.SMARTPHRASES_ID IS NOT NULL THEN CAST(USPHR.SMARTPHRASE_NAME AS VARCHAR(200))
			WHEN UNSECURE_SDE_ENCs.SMARTPHRASES_ID IS NOT NULL THEN CAST(USSDE.SMARTPHRASE_NAME AS VARCHAR(200))
			WHEN CIRCLEBACK_ENCs.SMARTPHRASES_ID IS NOT NULL THEN CAST(CSPHR.SMARTPHRASE_NAME AS VARCHAR(200))
			ELSE NULL END																							AS Smartphrase_Name 
		,CAST(pe.HSP_ACCOUNT_ID AS NUMERIC(18,0))																	AS HB_HAR
		,CAST(pe4.PB_VISIT_HAR_ID AS NUMERIC(18,0))																	AS PB_HAR
		,CAST(MYCH.NAME AS VARCHAR(150))																			AS MyChart_Status
		,CASE WHEN PE.COVERAGE_ID IS NOT NULL THEN CAST(FIN.NAME AS VARCHAR(150))
			ELSE 'N/A' END																							AS Visit_Coverage
		,CASE WHEN TRANSPLANT_ENCs.Encounter_CSN IS NOT NULL THEN 1
				ELSE 0 END																							AS transplant
		,CASE WHEN TELEMED_PT_HOME_PROVIDER.Encounter_CSN IS NOT NULL THEN 1
				ELSE 0 END																							AS Pt_Home_Resource		
		,CASE WHEN CIRCLEBACK_ENCs.Encounter_CSN IS NOT NULL THEN 1
				ELSE 0 END																							AS Circleback	
		,CASE WHEN UNSECURE_SMARTPHRASE_ENCs.Encounter_CSN IS NOT NULL THEN 1
				WHEN UNSECURE_SDE_ENCs.Encounter_CSN IS NOT NULL THEN 1
				WHEN UNSECURE_ENCs.Encounter_CSN IS NOT NULL THEN 1
				WHEN UNSECURE_VISIT_PROF_REMOTE_ENCs.Encounter_CSN IS NOT NULL THEN 1
				ELSE 0 END																							AS Telemed_Unsecure
		,CASE WHEN SCHED_DTC_VISIT_ENCs.Encounter_CSN IS NOT NULL THEN 1
				ELSE 0 END																							AS Telemed_Sched	
		,CASE WHEN URGENT_CARE_ENCs.Encounter_CSN IS NOT NULL THEN 1
				ELSE 0 END																							AS Urgent_Care
		,CASE WHEN ECONSULT_ENCs.Encounter_CSN IS NOT NULL THEN 1
				ELSE 0 END																							AS EConsult	
		,CASE WHEN MODES_ENC.Encounter_CSN IS NOT NULL AND pE.CONTACT_DATE >= '20211013' THEN 1
				ELSE 0 END																							AS Telehealth_Mode	--AD - Added 01/27/22
		,NULL																										AS Appt_Notes
		,NULL																										AS Appt_Notes_Video		
		,NULL																										AS Appt_Notes_Phone		
		,NULL																										AS Appt_Note																
		,CAST(PB.COMM_TYPE AS VARCHAR(200))																			AS Comm_Type_Billing
		,CASE WHEN PE.ACCOUNT_ID IS NOT NULL THEN CAST(PE.ACCOUNT_ID AS NUMERIC)
				ELSE CAST(GUAR.ACCOUNT_ID AS NUMERIC) END															AS Guarantor_ID
		,CAST(pe6.TELEHEALTH_MODE_C AS INTEGER)																		AS Telehealth_Mode_C
		,CAST(ztm.NAME AS VARCHAR(254))																				AS Telehealth_Mode_Name

FROM 

(

		SELECT ECONSULT_ENCs.Encounter_CSN,
               ECONSULT_ENCs.SMARTPHRASES_ID 
		FROM ECONSULT_ENCs

		UNION
		SELECT CIRCLEBACK_ENCs.Encounter_CSN,
               CIRCLEBACK_ENCs.SMARTPHRASES_ID 
		FROM CIRCLEBACK_ENCs


		UNION
		SELECT URGENT_CARE_ENCs.Encounter_CSN,
               URGENT_CARE_ENCs.SMARTPHRASES_ID 
		FROM URGENT_CARE_ENCs
		

		UNION
		SELECT SCHED_DTC_VISIT_ENCs.Encounter_CSN,
               SCHED_DTC_VISIT_ENCs.SMARTPHRASES_ID 
		FROM SCHED_DTC_VISIT_ENCs
		
		
		UNION					
		SELECT UNSECURE_SMARTPHRASE_ENCs.Encounter_CSN,
               UNSECURE_SMARTPHRASE_ENCs.SMARTPHRASES_ID 
		FROM UNSECURE_SMARTPHRASE_ENCs
			
		UNION
		SELECT UNSECURE_SDE_ENCs.Encounter_CSN,
               UNSECURE_SDE_ENCs.SMARTPHRASES_ID 
		FROM UNSECURE_SDE_ENCs

		UNION
		SELECT UNSECURE_ENCs.Encounter_CSN,
               UNSECURE_ENCs.SMARTPHRASES_ID 
		FROM UNSECURE_ENCs

		UNION
		SELECT UNSECURE_VISIT_PROF_REMOTE_ENCs.Encounter_CSN,
               UNSECURE_VISIT_PROF_REMOTE_ENCs.SMARTPHRASES_ID 
		FROM UNSECURE_VISIT_PROF_REMOTE_ENCs
---- =====================================================================================
---- AD - 01/27/22 - Added to union Telehealth Modes to overall dataset
---- =====================================================================================		
		UNION
		SELECT MODES_ENC.Encounter_CSN, 
			   MODES_ENC.SMARTPHRASES_ID
		FROM MODES_ENC

				
) AS DATASET

INNER JOIN CLARITY..PAT_ENC															AS  PE		ON PE.PAT_ENC_CSN_ID=DATASET.Encounter_CSN
INNER JOIN CLARITY..PAT_ENC_4														AS	PE4		ON PE4.PAT_ENC_CSN_ID=PE.PAT_ENC_CSN_ID
INNER JOIN CLARITY..PATIENT															AS  PT		ON PT.PAT_ID=PE.PAT_ID
LEFT JOIN CLARITY..VALID_PATIENT													AS	VAL		ON VAL.PAT_ID=PE.PAT_ID
LEFT JOIN CLARITY..CLARITY_DEP														AS  DEP		ON DEP.DEPARTMENT_ID=pe.EFFECTIVE_DEPT_ID
LEFT JOIN CLARITY..CLARITY_SER														AS  SER		ON SER.PROV_ID=pe.VISIT_PROV_ID
LEFT JOIN CLARITY..HSP_ATND_PROV													AS  HAP		ON HAP.PAT_ENC_CSN_ID=pe.PAT_ENC_CSN_ID AND hap.LINE=1
LEFT JOIN CLARITY..CLARITY_SER														AS  SERH	ON SERH.PROV_ID=hap.PROV_ID
LEFT JOIN CLARITY..ZC_CALCULATED_ENC_STAT											AS  ests	ON ests.CALCULATED_ENC_STAT_C=pe.CALCULATED_ENC_STAT_C
LEFT JOIN CLARITY..ZC_APPT_STATUS													AS  asts	ON asts.APPT_STATUS_C=pe.APPT_STATUS_C
LEFT JOIN CLARITY..CLARITY_PRC														AS  PRC		ON prc.PRC_ID=pe.APPT_PRC_ID
LEFT JOIN CLARITY..PAT_ENC_2														AS  PE2		ON PE2.PAT_ENC_CSN_ID=PE.PAT_ENC_CSN_ID
LEFT JOIN TRANSPLANT_ENCS														 				ON TRANSPLANT_ENCS.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN TELEMED_PT_HOME_PROVIDER												 				ON TELEMED_PT_HOME_PROVIDER.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN UNSECURE_SMARTPHRASE_ENCs															 	ON UNSECURE_SMARTPHRASE_ENCs.Encounter_CSN=DATASET.Encounter_CSN 
LEFT JOIN UNSECURE_SDE_ENCs															 			ON UNSECURE_SDE_ENCs.Encounter_CSN=DATASET.Encounter_CSN 
LEFT JOIN UNSECURE_ENCs													 						ON UNSECURE_ENCs.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN UNSECURE_VISIT_PROF_REMOTE_ENCs													 	ON UNSECURE_VISIT_PROF_REMOTE_ENCs.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN SCHED_DTC_VISIT_ENCs																	ON SCHED_DTC_VISIT_ENCs.Encounter_CSN = DATASET.Encounter_CSN
LEFT JOIN URGENT_CARE_ENCs																		ON URGENT_CARE_ENCs.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN CIRCLEBACK_ENCs														 				ON CIRCLEBACK_ENCs.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN ECONSULT_ENCs															 				ON ECONSULT_ENCs.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN APPT_NOTES														 				    ON APPT_NOTES.Encounter_CSN=DATASET.Encounter_CSN
LEFT JOIN MODES_ENC																				ON MODES_ENC.Encounter_CSN = DATASET.Encounter_CSN --AD - Added 01/27/2022
LEFT JOIN CLARITY..CL_SPHR															AS  USPHR	ON USPHR.SMARTPHRASE_ID=UNSECURE_SMARTPHRASE_ENCs.SMARTPHRASES_ID
LEFT JOIN CLARITY..CL_SPHR															AS  USSDE	ON USSDE.SMARTPHRASE_ID=UNSECURE_SDE_ENCs.SMARTPHRASES_ID
LEFT JOIN CLARITY..CL_SPHR															AS  CSPHR	ON CSPHR.SMARTPHRASE_ID=CIRCLEBACK_ENCs.SMARTPHRASES_ID
LEFT JOIN CLARITY..IDENTITY_ID														AS  idn		ON IDN.PAT_ID=PE.PAT_ID AND IDN.IDENTITY_TYPE_ID=14
LEFT JOIN CLARITY..PATIENT_MYC														AS  MYC		ON MYC.PAT_ID=PE.PAT_ID
LEFT JOIN CLARITY..ZC_MYCHART_STATUS												AS  MYCH	ON MYCH.MYCHART_STATUS_C=MYC.MYCHART_STATUS_C
LEFT JOIN CLARITY..IDENTITY_SER_ID													AS  ISI		ON ISI.PROV_ID=SER.PROV_ID 
LEFT JOIN CLARITY..ZC_DISP_ENC_TYPE													AS  ENC		ON ENC.DISP_ENC_TYPE_C=PE.ENC_TYPE_C
LEFT JOIN CLARITY..COVERAGE															AS  CVG		ON CVG.COVERAGE_ID=PE.COVERAGE_ID
LEFT JOIN CLARITY..CLARITY_EPM														AS  EPM		ON EPM.PAYOR_ID=CVG.PAYOR_ID
LEFT JOIN CLARITY..ZC_FIN_CLASS														AS  FIN		ON FIN.FIN_CLASS_C=EPM.FINANCIAL_CLASS
LEFT JOIN CLARITY..PAT_ACCT_CVG														AS GUAR		ON GUAR.PAT_ID=PT.PAT_ID AND GUAR.LINE=1
LEFT JOIN  
	(
	SELECT DISTINCT PAT_ENC_CSN_ID
	       ,COMM_TYPE
			FROM
			(
				SELECT
				PAT_ENC_CSN_ID
				,CPT_CODE
				,CASE WHEN CPT_CODE IN ('99441','99942','99443','99999') THEN 'Phone'
				ELSE 'Video'  END AS COMM_TYPE
				,ROW_NUMBER() OVER (PARTITION BY PAT_ENC_CSN_ID ORDER BY CPT_CODE DESC) row_num

				FROM CLARITY..ARPB_TRANSACTIONS
				WHERE CPT_CODE
				IN 
					(
						'99441','99942','99443','99999'  --PHONE
						,'99201','99202','99203','99204','99205','99206','99207','99208','99209'			--- VIDEO
						,'99210','99211','99212','99213','99214','99215','99216','99217','99218','99219'	--- VIDEO
						,'99220','99221','99222','99223','99224','99225','99226','99227','99228','99229'	--- VIDEO
						,'99230','99231','99232','99233','99234','99235','99236','99237','99238','99239'	--- VIDEO
						,'99240','99241','99242','99243','99244','99245'									--- VIDEO
					) 
			)
			PB_TX 
			WHERE ROW_NUM=1
			) PB ON PB.PAT_ENC_CSN_ID=DATASET.Encounter_CSN
LEFT OUTER JOIN CLARITY..PAT_ENC_6											AS PE6		ON PE6.PAT_ENC_CSN_ID=PE.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY..ZC_TELEHEALTH_MODE						AS ZTM	ON ZTM.TELEHEALTH_MODE_C = PE6.TELEHEALTH_MODE_C


WHERE 1=1
AND PT.PAT_NAME NOT LIKE 'testpatient%' 
AND VAL.IS_VALID_PAT_YN<>'N'
AND PE.ENC_TYPE_C<>'2505' --Erroneous Encounter
AND PE.ENC_TYPE_C<>'2506' --Erroneous Telephone Encounter
--AND pe.CONTACT_DATE >= @p_DateStart

GO


