USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-------------------------------------------------------------------------------
/**********************************************************************************************************************
WHAT: Data Portal Metric for Missing Registration Items Workqueue User Productivity
WHO : Tom Burgan
WHEN: 2025/02/21
WHY : Monitoring user activity in the Missing Reg Items workqueues
-----------------------------------------------------------------------------------------------------------------------
INFO: 
	Metric showing elapsed times for released workqueue items 
	Duration between the creation instant and the end/released time of a workqueue item.
	This is filtered specifically to user activity in the Missing Reg Items workqueues.
     
		INPUTS:	  


		OUTPUTS:
			Granularity at the workqueue item level, query pulls the items that have been released from the workqueue..  
			Includes items in the "Patient" workqueues that contain the phrase "MISSING REG ITEMS".
			Elapsed time is the number of minutes between the START_TIME and END_TIME for released workqueue items.
			The EXIT_USER-level count of released workqueue items measures workqueue activity productivity.
-----------------------------------------------------------------------------------------------------------------------
MODS: 	
	2025/02/21 - TMB-	Initital Creation

**********************************************************************************************************************/

--ALTER PROCEDURE [ETL].[uspSrc_Missing_Reg_Item_WQ_Monitoring.sql]
--AS 

DECLARE @startdate		SMALLDATETIME 
DECLARE @enddate		SMALLDATETIME 

/*----Get default Balanced Scorecard date range*/
IF			@startdate 	IS NULL
    	AND @enddate 	IS NULL
EXEC	Clarity_App.ETL.usp_Get_Dash_Dates_BalancedScorecard 		@startdate 	OUTPUT
																,	@enddate 	OUTPUT;  
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#wqi ') IS NOT NULL
DROP TABLE #wqi

IF OBJECT_ID('tempdb..#pwr ') IS NOT NULL
DROP TABLE #pwr

IF OBJECT_ID('tempdb..#pwrs ') IS NOT NULL
DROP TABLE #pwrs

IF OBJECT_ID('tempdb..#uhx ') IS NOT NULL
DROP TABLE #uhx

IF OBJECT_ID('tempdb..#mri ') IS NOT NULL
DROP TABLE #mri

SELECT DISTINCT
	pwi.ITEM_ID,
	wi.WORKQUEUE_ID,
	wi.WORKQUEUE_NAME,
	wi.DESCRIPTION
INTO #wqi
FROM CLARITY..PAT_WQ_ITEMS	pwi
INNER JOIN CLARITY..WORKQUEUE_INFO wi ON pwi.WORKQUEUE_ID = wi.WORKQUEUE_ID
AND wi.WORKQUEUE_TYPE_C = 3 -- PATIENT WQS ONLY
WHERE 1 = 1
AND wi.WORKQUEUE_TYPE_C = 3 -- PATIENT WQS ONLY
AND wi.DESCRIPTION LIKE '%missing reg items for appointments%'
--AND pwi.WORKQUEUE_ID = '2555'
AND CAST(pwi.RELEASE_DATE AS DATE)  BETWEEN @startdate AND @enddate
ORDER BY
	pwi.ITEM_ID

CREATE UNIQUE CLUSTERED INDEX IX_wqi ON #wqi (ITEM_ID)

SELECT DISTINCT
	wqi.WORKQUEUE_ID,
	wqi.WORKQUEUE_NAME
FROM #wqi wqi
ORDER BY
	wqi.WORKQUEUE_ID

GO


