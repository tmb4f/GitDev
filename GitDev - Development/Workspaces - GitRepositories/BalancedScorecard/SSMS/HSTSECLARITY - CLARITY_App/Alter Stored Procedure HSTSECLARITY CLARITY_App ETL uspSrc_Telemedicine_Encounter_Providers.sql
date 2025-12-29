USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =====================================================================================
-- Create procedure uspSrc_Telemedicine_Encounter_Providers
-- =====================================================================================

ALTER PROCEDURE [ETL].[uspSrc_Telemedicine_Encounter_Providers]

/*
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
    )
*/

AS
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_Telemedicine_Encounter_Providers
--WHO : Brian Costello
--WHEN: 6/23/2020
--WHY : Report Providers associated with Telemedicine Appointments
--
--------------------------------------------------------------------------------------------------------------------------
--MODS:       
--			06/23/2020	- BJC	-	Create stored procedure
--          05/17/2021  - TMB  -   Add columns Pt_Home_Resource_Pool, Prov_1_Pool, Prov_2_Pool, Prov_3_Pool,
--       	                                    Prov_4_Pool, and Prov_5_Pool (VARCHAR(200)) to extract

--************************************************************************************************************************



; WITH PROV_1 AS
		(
		SELECT
		PEA.PAT_ENC_CSN_ID										
		,PEA.PROV_ID			
		,SPI.POOL_NAME						
		
		FROM CLARITY_App.Stage.Telemedicine_Encounters  AS TMED 
		LEFT JOIN CLARITY..PAT_ENC_APPT					AS PEA ON PEA.PAT_ENC_CSN_ID=TMED.Encounter_CSN
		LEFT JOIN CLARITY..SCHED_POOL_INFO              AS SPI ON SPI.POOL_ID=PEA.APPT_PROV_POOL_ID
				
		WHERE  1=1
		AND PEA.LINE=1
		AND PEA.PROV_ID IS NOT NULL
		)


,PROV_2  AS
		(
		SELECT
		PEA.PAT_ENC_CSN_ID										
		,PEA.PROV_ID	
		,SPI.POOL_NAME								
		
		FROM CLARITY_App.Stage.Telemedicine_Encounters  AS TMED 
		LEFT JOIN CLARITY..PAT_ENC_APPT					AS PEA ON PEA.PAT_ENC_CSN_ID=TMED.Encounter_CSN
		LEFT JOIN CLARITY..SCHED_POOL_INFO              AS SPI ON SPI.POOL_ID=PEA.APPT_PROV_POOL_ID
				
		WHERE  1=1
		AND PEA.LINE=2
		AND PEA.PROV_ID IS NOT NULL
		)

,PROV_3  AS
		(
		SELECT
		PEA.PAT_ENC_CSN_ID										
		,PEA.PROV_ID		
		,SPI.POOL_NAME							
		
		FROM CLARITY_App.Stage.Telemedicine_Encounters  AS TMED 
		LEFT JOIN CLARITY..PAT_ENC_APPT					AS PEA ON PEA.PAT_ENC_CSN_ID=TMED.Encounter_CSN
		LEFT JOIN CLARITY..SCHED_POOL_INFO              AS SPI ON SPI.POOL_ID=PEA.APPT_PROV_POOL_ID
				
		WHERE  1=1
		AND PEA.LINE=3
		AND PEA.PROV_ID IS NOT NULL
		)

,PROV_4  AS
		(
		SELECT
		PEA.PAT_ENC_CSN_ID										
		,PEA.PROV_ID		
		,SPI.POOL_NAME							
		
		FROM CLARITY_App.Stage.Telemedicine_Encounters  AS TMED 
		LEFT JOIN CLARITY..PAT_ENC_APPT					AS PEA ON PEA.PAT_ENC_CSN_ID=TMED.Encounter_CSN
		LEFT JOIN CLARITY..SCHED_POOL_INFO              AS SPI ON SPI.POOL_ID=PEA.APPT_PROV_POOL_ID
				
		WHERE  1=1
		AND PEA.LINE=4
		AND PEA.PROV_ID IS NOT NULL
		)

,PROV_5  AS
		(
		SELECT
		PEA.PAT_ENC_CSN_ID										
		,PEA.PROV_ID	
		,SPI.POOL_NAME								
		
		FROM CLARITY_App.Stage.Telemedicine_Encounters  AS TMED 
		LEFT JOIN CLARITY..PAT_ENC_APPT					AS PEA ON PEA.PAT_ENC_CSN_ID=TMED.Encounter_CSN
		LEFT JOIN CLARITY..SCHED_POOL_INFO              AS SPI ON SPI.POOL_ID=PEA.APPT_PROV_POOL_ID
				
		WHERE  1=1
		AND PEA.LINE=5
		AND PEA.PROV_ID IS NOT NULL
		)

,PT_HOME_RESOURCE AS
		(
		SELECT 
		PEA.PAT_ENC_CSN_ID										
		,PEA.PROV_ID	
		,SPI.POOL_NAME								
		
		FROM CLARITY_App.Stage.Telemedicine_Encounters  AS TMED 
		LEFT JOIN CLARITY..PAT_ENC_APPT					AS PEA ON PEA.PAT_ENC_CSN_ID=TMED.Encounter_CSN		
		LEFT JOIN CLARITY..SCHED_POOL_INFO              AS SPI ON SPI.POOL_ID=PEA.APPT_PROV_POOL_ID						
		
		WHERE  1=1
		AND PEA.PROV_ID='1301496' 
		)


SELECT 
		CAST(PROV_1.PAT_ENC_CSN_ID AS NUMERIC(18,0))														AS Encounter_CSN
		,CASE WHEN PT_HOME_RESOURCE.PAT_ENC_CSN_ID IS NOT NULL THEN 1
				ELSE 0 END																					AS Pt_Home_Resource		
		,CASE WHEN PT_HOME_RESOURCE.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PT_HOME_RESOURCE.POOL_NAME AS VARCHAR(200)) 
			ELSE NULL END																					AS Pt_Home_Resource_Pool					
		,CASE WHEN PROV_1.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_1.PROV_ID AS VARCHAR(18)) 
			ELSE NULL END																					AS Prov_1				
		,CASE WHEN PROV_1.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_1.POOL_NAME AS VARCHAR(200)) 
			ELSE NULL END																					AS Prov_1_Pool		
		,CASE WHEN PROV_2.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_2.PROV_ID AS VARCHAR(18)) 
			ELSE NULL END																					AS Prov_2		
		,CASE WHEN PROV_2.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_2.POOL_NAME AS VARCHAR(200)) 
			ELSE NULL END																					AS Prov_2_Pool	
		,CASE WHEN PROV_3.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_3.PROV_ID AS VARCHAR(18)) 
			ELSE NULL END																					AS Prov_3				
		,CASE WHEN PROV_3.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_3.POOL_NAME AS VARCHAR(200)) 
			ELSE NULL END																					AS Prov_3_Pool	
		,CASE WHEN PROV_4.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_4.PROV_ID AS VARCHAR(18)) 	
			ELSE NULL END																					AS Prov_4		
		,CASE WHEN PROV_4.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_4.POOL_NAME AS VARCHAR(200)) 
			ELSE NULL END																					AS Prov_4_Pool	
		,CASE WHEN PROV_5.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_5.PROV_ID AS VARCHAR(18)) 
			ELSE NULL END																					AS Prov_5				
		,CASE WHEN PROV_5.PAT_ENC_CSN_ID IS NOT NULL THEN CAST(PROV_5.POOL_NAME AS VARCHAR(200)) 
			ELSE NULL END																					AS Prov_5_Pool			
									
FROM PROV_1
LEFT JOIN PROV_2 ON PROV_2.PAT_ENC_CSN_ID = PROV_1.PAT_ENC_CSN_ID
LEFT JOIN PROV_3 ON PROV_3.PAT_ENC_CSN_ID = PROV_1.PAT_ENC_CSN_ID
LEFT JOIN PROV_4 ON PROV_4.PAT_ENC_CSN_ID = PROV_1.PAT_ENC_CSN_ID
LEFT JOIN PROV_5 ON PROV_5.PAT_ENC_CSN_ID = PROV_1.PAT_ENC_CSN_ID
LEFT JOIN PT_HOME_RESOURCE ON PT_HOME_RESOURCE.PAT_ENC_CSN_ID=PROV_1.PAT_ENC_CSN_ID					

ORDER BY PROV_1.PAT_ENC_CSN_ID

GO


