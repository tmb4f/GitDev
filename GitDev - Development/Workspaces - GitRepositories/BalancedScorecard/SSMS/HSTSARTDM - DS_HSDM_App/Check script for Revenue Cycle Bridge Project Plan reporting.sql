USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL
                 ,@enddate SMALLDATETIME = NULL

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Pod_CallServiceLevel]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Pod_CallServiceLevel
--WHO : Tom Burgan
--WHEN: 3/28/19
--WHY : Report ACC call system call servie level rates.
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
--              DS_HSDW_App.ETL.usp_Get_Dash_Dates_BalancedScorecard
--              DS_HSDW_Prod.Rptg.vwDim_Date
--              DS_HSDW_Prod.CallCenter.ACC_PhoneData_QueueSummary
--              DS_HSDW_App.CallCenter.ACC_PhoneData_Pod_Mapping
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Pod_CallServiceLevel]
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--       03/28/2019 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;

DECLARE @locstartdate SMALLDATETIME
                 ,@locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate
-------------------------------------------------------------------------------
---declare and set some variables to improve the query performance
DECLARE @bod SMALLDATETIME,                       ----to prevent reuse of getdate() to get beginning of day in where clauses
        @currdate SMALLDATETIME                   ----to prevent reuse of getdate() in where clauses
SET @currdate = CAST(GETDATE() AS SMALLDATETIME)
SET @bod = CAST(CAST(@currdate AS DATE) AS SMALLDATETIME)

----------------------------------------------------------

if OBJECT_ID('tempdb..#datetable') is not NULL
DROP TABLE #datetable

if OBJECT_ID('tempdb..#podmapping') is not NULL
DROP TABLE #podmapping

if OBJECT_ID('tempdb..#acc') is not NULL
DROP TABLE #acc

if OBJECT_ID('tempdb..#check') is not NULL
DROP TABLE #check

if OBJECT_ID('tempdb..#accsum') is not NULL
DROP TABLE #accsum

--if OBJECT_ID('tempdb..#allacc') is not NULL
--DROP TABLE #allacc

--if OBJECT_ID('tempdb..#accdatetable') is not NULL
--DROP TABLE #accdatetable

if OBJECT_ID('tempdb..#AmbOpt_Dash_CallAnswerRate') is not NULL
DROP TABLE #AmbOpt_Dash_CallAnswerRate

if OBJECT_ID('tempdb..#PhoneMetrics') is not NULL
DROP TABLE #PhoneMetrics

IF OBJECT_ID('tempdb..#cgcahps ') IS NOT NULL
DROP TABLE #cgcahps

IF OBJECT_ID('tempdb..#hcahps ') IS NOT NULL
DROP TABLE #hcahps

IF OBJECT_ID('tempdb..#chcahps ') IS NOT NULL
DROP TABLE #chcahps

IF OBJECT_ID('tempdb..#er ') IS NOT NULL
DROP TABLE #er

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

IF OBJECT_ID('tempdb..#numerator_MD ') IS NOT NULL
DROP TABLE #numerator_MD

IF OBJECT_ID('tempdb..#denominator_MD ') IS NOT NULL
DROP TABLE #denominator_MD

IF OBJECT_ID('tempdb..#numerator_IN ') IS NOT NULL
DROP TABLE #numerator_IN

IF OBJECT_ID('tempdb..#denominator_IN ') IS NOT NULL
DROP TABLE #denominator_IN

IF OBJECT_ID('tempdb..#numerator_PD ') IS NOT NULL
DROP TABLE #numerator_PD

IF OBJECT_ID('tempdb..#denominator_PD ') IS NOT NULL
DROP TABLE #denominator_PD

IF OBJECT_ID('tempdb..#numerator_ERP ') IS NOT NULL
DROP TABLE #numerator_ERP

IF OBJECT_ID('tempdb..#denominator_ERP ') IS NOT NULL
DROP TABLE #denominator_ERP

IF OBJECT_ID('tempdb..#numerator_ERA ') IS NOT NULL
DROP TABLE #numerator_ERA

IF OBJECT_ID('tempdb..#denominator_ERA ') IS NOT NULL
DROP TABLE #denominator_ERA

if OBJECT_ID('tempdb..#PatExp_CGCAHPS_TopBoxPct') is not NULL
DROP TABLE #PatExp_CGCAHPS_TopBoxPct

if OBJECT_ID('tempdb..#PatExp_HCAHPS_TopBoxPct') is not NULL
DROP TABLE #PatExp_HCAHPS_TopBoxPct

if OBJECT_ID('tempdb..#PatExp_CHCAHPS_TopBoxPct') is not NULL
DROP TABLE #PatExp_CHCAHPS_TopBoxPct

if OBJECT_ID('tempdb..#PatExp_ERP_TopBoxPct') is not NULL
DROP TABLE #PatExp_ERP_TopBoxPct

if OBJECT_ID('tempdb..#PatExp_ERA_TopBoxPct') is not NULL
DROP TABLE #PatExp_ERA_TopBoxPct

if OBJECT_ID('tempdb..#PatientSatisfactionHighlights') is not NULL
DROP TABLE #PatientSatisfactionHighlights

--SELECT date_dim.day_date
--      ,date_dim.fmonth_num
--	  ,date_dim.month_name
--	  --,date_dim.fmonth_name
--      ,date_dim.Fyear_num
--      --,date_dim.FYear_name
--INTO #datetable
--FROM DS_HSDW_Prod.Rptg.vwDim_Date AS date_dim
--WHERE date_dim.day_date >= @locstartdate
--AND date_dim.day_date < @locenddate
--ORDER BY date_dim.day_date                 ---BDD 5/14/2018 added order by to reduce data reording in subsequent cluster index step
SELECT date_dim.day_date
      ,date_dim.month_num
	  ,date_dim.month_name
	  --,date_dim.fmonth_name
      ,date_dim.year_num
	  ,date_dim.Fyear_num
      --,date_dim.FYear_name
INTO #datetable
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS date_dim
WHERE date_dim.day_date >= @locstartdate
AND date_dim.day_date < @locenddate
ORDER BY date_dim.day_date                 ---BDD 5/14/2018 added order by to reduce data reording in subsequent cluster index step

  -- Create index for temp table #datetable
  CREATE UNIQUE CLUSTERED INDEX IX_datetable ON #datetable ([day_date])

SELECT DISTINCT
       Workgroup
	 , Pod
	 , OperationalOwner
INTO #podmapping
FROM DS_HSDW_App.CallCenter.ACC_Workgroup_Pod_Mapping
ORDER BY Workgroup

  -- Create index for temp table #podmapping
  CREATE UNIQUE CLUSTERED INDEX IX_podmapping ON #podmapping ([Workgroup])

--SELECT *
--FROM #podmapping
--ORDER BY Pod
--                   ,Workgroup

------------------------------------------------------------------------------------------

SELECT [cName]
      ,CAST([cReportGroup] AS VARCHAR(20)) AS ReportGroup
      ,CASE
		 WHEN pod.[Pod] IS NULL THEN 'Non-Pod'
		 ELSE pod.[Pod]
	   END AS Pod
      ,CAST([I3TimeStampGMT] AS DATE) AS I3TimeStampGMT
      ,CAST(CAST([I3TimeStampGMT] AS DATE) AS SMALLDATETIME) AS day_date
      ,SUM([nEnteredAcd]) AS nEnteredAcd
      ,SUM([nAbandonedAcd]) AS nAbandonedAcd
      ,SUM([nGrabbedAcd]) AS nGrabbedAcd
	  ,SUM([nLocalDisconnectAcd]) AS nLocalDisconnectAcd
      ,SUM([nAlertedAcd]) AS nAlertedAcd
      ,SUM([nAnsweredAcd]) AS nAnsweredAcd
      ,SUM([tAnsweredAcd]) AS tAnsweredAcd
	  ,AVG([nAcdSvcLvl]) AS nAcdSvcLvl
      ,SUM([nAnsweredAcdSvcLvl]) AS nAnsweredAcdSvcLvl
      ,SUM([nAnsweredAcdSvcLvl1]) AS nAnsweredAcdSvcLvl1
      ,SUM([nAnsweredAcdSvcLvl2]) AS nAnsweredAcdSvcLvl2
      ,SUM([nAnsweredAcdSvcLvl3]) AS nAnsweredAcdSvcLvl3
      ,SUM([nAnsweredAcdSvcLvl4]) AS nAnsweredAcdSvcLvl4
      ,SUM([nAnsweredAcdSvcLvl5]) AS nAnsweredAcdSvcLvl5
      ,SUM([nAnsweredAcdSvcLvl6]) AS nAnsweredAcdSvcLvl6
      ,SUM([nAbandonAcdSvcLvl]) AS nAbandonAcdSvcLvl
      ,SUM([nAbandonAcdSvcLvl1]) AS nAbandonAcdSvcLvl1
      ,SUM([nAbandonAcdSvcLvl2]) AS nAbandonAcdSvcLvl2
      ,SUM([nAbandonAcdSvcLvl3]) AS nAbandonAcdSvcLvl3
      ,SUM([nAbandonAcdSvcLvl4]) AS nAbandonAcdSvcLvl4
      ,SUM([nAbandonAcdSvcLvl5]) AS nAbandonAcdSvcLvl5
      ,SUM([nAbandonAcdSvcLvl6]) AS nAbandonAcdSvcLvl6
	  ,AVG(CAST([tAnsweredAcd] AS NUMERIC(10,2))) AS avgAnsweredAcd
	  ,AVG(CAST([tAbandonedAcd] AS NUMERIC(10,2))) AS tAbandonedAcd
	  ,AVG(CAST([tTalkAcd] AS NUMERIC(10,2))) AS tTalkAcd
	  ,AVG(CAST([tTalkCompleteAcd] AS NUMERIC(10,2))) AS tTalkCompleteAcd
	  ,SUM([nHoldAcd]) AS nHoldAcd
	  ,AVG(CAST([tHoldAcd] AS NUMERIC(10,2))) AS tHoldAcd
	  ,SUM([nTransferedAcd]) AS nTransferedAcd
	  ,SUM([nNotAnsweredAcd]) AS nNotAnsweredAcd 
	  ,AVG(CAST([tAlertedAcd] AS NUMERIC(10,2))) AS tAlertedAcd
	  ,SUM([nDisconnectAcd]) AS nDisconnectAcd
	  ,AVG(CAST([tAgentTalk] AS NUMERIC(10,2))) AS tAgentTalk
	  ,AVG([nServiceLevel]) AS nServiceLevel
	  ,pod.[OperationalOwner]
  INTO #acc
  FROM [DS_HSDM_I3_IC_P_callcenter_rptg_shadow].[dbo].[IWrkgrpQueueStats] accqs
  LEFT OUTER JOIN #podmapping pod
  ON pod.Workgroup = accqs.cName
  WHERE cHKey3 = 'Call'
  AND cHKey4 = '*'
  --AND CAST(I3TimeStampGMT AS DATE) BETWEEN '3/1/2021' AND '3/31/2021'
  GROUP BY cName, CASE WHEN pod.[Pod] IS NULL THEN 'Non-Pod' ELSE pod.[Pod] END, pod.[OperationalOwner], cReportGroup, CAST(I3TimeStampGMT AS DATE)
  ORDER BY cName, CASE WHEN pod.[Pod] IS NULL THEN 'Non-Pod' ELSE pod.[Pod] END, pod.[OperationalOwner], cReportGroup, CAST(I3TimeStampGMT AS DATE)

  --SELECT cName
  --             , SUM(nEnteredAcd) AS nEnteredAcd
  --             , SUM(nAnsweredAcd) AS nAnsweredAcd
  --             , SUM(tAnsweredAcd) AS tAnsweredAcd
  --FROM #check
  --GROUP BY cName
  --ORDER BY cName

/*SELECT [cName]
      ,CAST([cReportGroup] AS VARCHAR(20)) AS ReportGroup
      ,CASE
		 WHEN pod.[Pod] IS NULL THEN 'Non-Pod'
		 ELSE pod.[Pod]
	   END AS Pod
      ,CAST([I3TimeStampGMT] AS SMALLDATETIME) AS day_date
      ,[nEnteredAcd]
      ,[nAbandonedAcd]
      ,[nGrabbedAcd]
      ,[nLocalDisconnectAcd]
      ,[nAlertedAcd]
      ,[nAnsweredAcd]
      ,[nAcdSvcLvl]
      ,[nAnsweredAcdSvcLvl]
      ,[nAnsweredAcdSvcLvl1]
      ,[nAnsweredAcdSvcLvl2]
      ,[nAnsweredAcdSvcLvl3]
      ,[nAnsweredAcdSvcLvl4]
      ,[nAnsweredAcdSvcLvl5]
      ,[nAnsweredAcdSvcLvl6]
      ,[nAbandonAcdSvcLvl]
      ,[nAbandonAcdSvcLvl1]
      ,[nAbandonAcdSvcLvl2]
      ,[nAbandonAcdSvcLvl3]
      ,[nAbandonAcdSvcLvl4]
      ,[nAbandonAcdSvcLvl5]
      ,[nAbandonAcdSvcLvl6]
      ,[tAnsweredAcd]
      ,[tAbandonedAcd]
      ,[tTalkAcd]
      ,[tTalkCompleteAcd]
      ,[nHoldAcd]
      ,[tHoldAcd]
      ,[nTransferedAcd]
      ,[nNotAnsweredAcd]
      ,[tAlertedAcd]
      ,[nDisconnectAcd]
      ,[tAgentTalk]
      ,[nServiceLevel]
	  ,pod.[OperationalOwner]
	  --,CAST(accqs.nAnsweredAcd AS NUMERIC(10,2)) * CAST(accqs.tAnsweredAcd AS NUMERIC(10,2)) AS tAnsweredAcd
  INTO #acc
  FROM DS_HSDW_Prod.[CallCenter].[ACC_PhoneData_QueueSummary] accqs
  LEFT OUTER JOIN #podmapping pod
  ON pod.Workgroup = accqs.cName*/

--  SELECT *

--  INTO #check

--  FROM #acc
--  --WHERE cName IN ('Dentistry','Otolaryngology')
--  WHERE day_date >= '3/1/2021 00:00 AM' AND day_date < '4/1/2021 00:00 AM'
--  AND Pod = 'Digestive Health'

--  SELECT *
--  FROM #check
--  --ORDER BY cName
--  --                  , day_date
--  ORDER BY Pod
--                    , cName
--                    , day_date

--SELECT SUM(nEnteredAcd) AS nEnteredAcd,
--               SUM(nAnsweredAcd) AS nAnsweredAcd,
--			   AVG(tAnsweredAcd) AS tAnsweredAcd
--FROM #check
--GROUP BY cName
--ORDER BY cName

SELECT
       acc.day_date
      ,CASE
	     WHEN ref.POD_ID IS NULL THEN -1
		 ELSE ref.POD_ID
	   END AS pod_id
	  ,acc.Pod AS pod_name
	  ,acc.ReportGroup
	  ,acc.OperationalOwner
	  ,acc.cName
      ,acc.nEnteredAcd
	  ,acc.nAbandonedAcd
	  ,acc.nAnsweredAcd
	  ,acc.tAnsweredAcd
      ,acc.nAnsweredAcdSvcLvl1 + acc.nAnsweredAcdSvcLvl2 + acc.nAnsweredAcdSvcLvl3 + acc.nAnsweredAcdSvcLvl4 + acc.nAnsweredAcdSvcLvl5 AS nAnsweredAcdSvcLvl_30
  INTO #accsum
  FROM #acc acc
  LEFT OUTER JOIN (SELECT DISTINCT
                          POD_ID
						, PFA_POD
                   FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
				   WHERE PFA_POD IS NOT NULL) ref ON ref.PFA_POD = acc.Pod

  -- Create index for temp table #accsum

  CREATE NONCLUSTERED INDEX IX_accsum ON #accsum ([day_date], [pod_id], [pod_name], [cName], [ReportGroup])

  --SELECT *
  --FROM #accsum
  --WHERE pod_id = 1
  --AND day_date BETWEEN '7/1/2020' AND '7/31/2020'
  --ORDER BY day_date
  --                  , cName
  --                  , ReportGroup

--SELECT DISTINCT
--       pod_id
--	 , pod_name
--	 , cName
--	 , ReportGroup
--INTO #allacc
--FROM #accsum

  -- Create index for temp table #allacc

  --CREATE NONCLUSTERED INDEX IX_allacc ON #allacc ([pod_id], [pod_name], [cName], [ReportGroup])

--SELECT acc.pod_id
--     , acc.pod_name
--	 , acc.cName
--	 , acc.ReportGroup
--     , dt.day_date
--	 , dt.month_num
--	 , dt.year_num
--	 , dt.FYear_name
--INTO #accdatetable
--FROM #allacc acc
--CROSS JOIN #datetable dt

  -- Create index for temp table #accdatetable

  --CREATE NONCLUSTERED INDEX IX_accdatetable ON #accdatetable ([day_date], [pod_id], [pod_name], [cName], [ReportGroup])

-----------------------------------------------------------------------------------------------------------

---BDD 7/27/2018 added insert to stage. Assumes truncate is handled in the SSIS package
--INSERT INTO DS_HSDM_App.TabRptg.Dash_AmbOpt_CallServiceLevel_Pod_Tiles
--           ([event_type]
--           ,[event_count]
--           ,[event_date]
--           ,[event_category]
--           ,[pod_id]
--           ,[pod_name]
--           ,[hub_id]
--           ,[hub_name]
--           ,[epic_department_id]
--           ,[epic_department_name]
--           ,[epic_department_name_external]
--           ,[month_num]
--           ,[year_num]
--           ,[FYear_name]
--           ,[report_period]
--           ,[report_date]
--           ,[peds]
--           ,[transplant]
--           ,[sk_Dim_pt]
--           ,[sk_Fact_Pt_Acct]
--           ,[sk_Fact_Pt_Enc_Clrt]
--           ,[person_birth_date]
--           ,[person_gender]
--           ,[person_id]
--           ,[person_name]
--           ,[practice_group_id]
--           ,[practice_group_name]
--           ,[provider_id]
--           ,[provider_name]
--           ,[service_line_id]
--           ,[service_line]
--           ,[sub_service_line_id]
--           ,[sub_service_line]
--           ,[opnl_service_id]
--           ,[opnl_service_name]
--           ,[corp_service_line_id]
--           ,[corp_service_line_name]
--           ,[hs_area_id]
--           ,[hs_area_name]
--           ,[nEnteredAcd]
--           ,[nAbandonedAcd]
--           ,[nAnsweredAcd]
--           ,[nAnsweredAcdSvcLvl_20]
--		   ,[ReportGroup]
--		   ,[OperationalOwner]
--		   )
    SELECT	DISTINCT
            CAST('Call Service Level' AS VARCHAR(50)) AS event_type
           ,CASE WHEN main.day_date IS NOT NULL THEN 1 ELSE 0 END AS event_count
           ,main.day_date AS event_date
           ,CAST(main.cName AS VARCHAR(150)) AS event_category
		   ,CASE WHEN main.pod_id = -1 THEN NULL ELSE main.pod_id END AS pod_id
		   ,CASE WHEN main.pod_name = 'Unknown' THEN CAST(NULL AS VARCHAR(100)) ELSE CAST(main.pod_name AS VARCHAR(100)) END AS pod_name
		   --,CAST(NULL AS VARCHAR(66)) AS hub_id
		   --,CAST(NULL AS VARCHAR(100)) AS hub_name
		   --,CAST(NULL AS NUMERIC(18,0)) AS epic_department_id
		   --,CAST(NULL AS VARCHAR(254)) AS epic_department_name
		   --,CAST(NULL AS VARCHAR(254)) AS epic_department_name_external
           ,date_dim.month_num
	       ,date_dim.month_name
	       --,date_dim.fmonth_name
           ,date_dim.year_num
	       ,date_dim.Fyear_num
           --,date_dim.FYear_name
     --      ,CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
     --      ,CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date
		   --,CAST(NULL AS SMALLINT) AS peds
		   --,CAST(NULL AS SMALLINT) AS transplant
		   --,CAST(NULL AS INTEGER) AS sk_Dim_pt
		   --,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Acct
		   --,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
		   --,CAST(NULL AS DATE) AS person_birth_date
		   --,CAST(NULL AS VARCHAR(254)) AS person_gender
		   --,CAST(NULL AS INTEGER) AS person_id
		   --,CAST(NULL AS VARCHAR(200)) AS person_name
		   --,CAST(NULL AS INT) AS practice_group_id
		   --,CAST(NULL AS VARCHAR(150)) AS practice_group_name
		   --,CAST(NULL AS VARCHAR(18)) AS provider_id
		   --,CAST(NULL AS VARCHAR(200)) AS provider_name
			-- MDM
		   --,CAST(NULL AS SMALLINT) AS service_line_id
		   --,CAST(NULL AS VARCHAR(150)) AS service_line
		   --,CAST(NULL AS SMALLINT) AS sub_service_line_id
		   --,CAST(NULL AS VARCHAR(150)) AS sub_service_line
		   --,CAST(NULL AS SMALLINT) AS opnl_service_id
		   --,CAST(NULL AS VARCHAR(150)) AS opnl_service_name
		   --,CAST(NULL AS SMALLINT) AS corp_service_line_id
		   --,CAST(NULL AS VARCHAR(150)) AS corp_service_line
		   --,CAST(NULL AS SMALLINT) AS hs_area_id
		   --,CAST(NULL AS VARCHAR(150)) AS hs_area_name
		   
		   ,CASE WHEN main.nEnteredAcd IS NULL THEN 0 ELSE main.nEnteredAcd END AS nEnteredAcd
		   ,CASE WHEN main.nAbandonedAcd IS NULL THEN 0 ELSE main.nAbandonedAcd END AS nAbandonedAcd
		   ,CASE WHEN main.nAnsweredAcd IS NULL THEN 0 ELSE main.nAnsweredAcd END AS nAnsweredAcd
		   ,CASE WHEN main.tAnsweredAcd IS NULL THEN 0 ELSE main.tAnsweredAcd END AS tAnsweredAcd
		   ,CASE WHEN main.nAnsweredAcdSvcLvl_30 IS NULL THEN 0 ELSE main.nAnsweredAcdSvcLvl_30 END AS nAnsweredAcdSvcLvl_30
		   ,main.ReportGroup
		   ,main.OperationalOwner

		INTO #AmbOpt_Dash_CallAnswerRate

        FROM
        --    #accdatetable AS date_dim
        --LEFT OUTER JOIN (
		(
                         --main

							SELECT
									acc.pod_id
								   ,acc.pod_name
								   ,acc.cName
								   ,acc.ReportGroup
								   ,acc.OperationalOwner

							--Select
								   ,acc.day_date
							       ,SUM(acc.nEnteredAcd) AS nEnteredAcd
							       ,SUM(acc.nAbandonedAcd) AS nAbandonedAcd
							       ,SUM(acc.nAnsweredAcd) AS nAnsweredAcd
							       ,SUM(acc.tAnsweredAcd) AS tAnsweredAcd
								   ,SUM(acc.nAnsweredAcdSvcLvl_30) AS nAnsweredAcdSvcLvl_30
								FROM
									#accsum acc
								GROUP BY acc.pod_id
								       , acc.pod_name
									   , acc.cName
									   , acc.ReportGroup
									   , acc.OperationalOwner
									   , acc.day_date
	         ) main
        LEFT OUTER JOIN #datetable AS date_dim
    --    ON  ((date_dim.day_date = main.day_date)
		  --   AND (date_dim.pod_id = main.pod_id)
		  --   AND (date_dim.pod_name = main.pod_name)
			 --AND (date_dim.cName = main.cName)
			 --AND (date_dim.ReportGroup = main.ReportGroup))
        ON  date_dim.day_date = main.day_date
        --LEFT OUTER JOIN #podmapping pod
        --ON date_dim.cName = pod.Workgroup
        WHERE
            main.day_date >= @locstartdate
            AND main.day_date < @locenddate
		ORDER BY CASE WHEN main.day_date IS NOT NULL THEN 1 ELSE 0 END DESC
		       , CASE WHEN main.pod_id = -1 THEN NULL ELSE main.pod_id END 
			   , CASE WHEN main.pod_name = 'Unknown' THEN CAST(NULL AS VARCHAR(100)) ELSE CAST(main.pod_name AS VARCHAR(100)) END
			   , CAST(main.cName AS VARCHAR(150))
			   , main.ReportGroup
			   , main.day_date

	--SELECT *
	--FROM #AmbOpt_Dash_CallAnswerRate
	--ORDER BY pod_id
	--                 , year_num
	--				 , month_num
	--				 --, fmonth_name
	--				 , event_category
	--				 , ReportGroup
	--				 , event_date

	SELECT
			 acc.pod_id
			,acc.pod_name
			,acc.year_num
			,acc.month_num
			,acc.month_name
	        ,acc.Fyear_num
			--,acc.event_date

	--Select
			,SUM(acc.nEnteredAcd) AS nEnteredAcd
			,SUM(acc.nAbandonedAcd) AS nAbandonedAcd
			,SUM(acc.nAnsweredAcd) AS nAnsweredAcd
			,SUM(acc.tAnsweredAcd) AS tAnsweredAcd
			,SUM(acc.nAnsweredAcdSvcLvl_30) AS nAnsweredAcdSvcLvl_30

		INTO #PhoneMetrics

		FROM
			#AmbOpt_Dash_CallAnswerRate acc
		GROUP BY acc.pod_id
				, acc.pod_name
			    , acc.year_num
			    , acc.month_num
			    , acc.month_name
	            ,acc.Fyear_num
			    --, acc.event_date

	--SELECT pod_id,
 --          pod_name,
 --          year_num,
 --          month_num,
 --          month_name,
	--	   Fyear_num,
 --          nEnteredAcd,
 --          nAbandonedAcd,
 --          nAnsweredAcd,
 --          tAnsweredAcd,
 --          nAnsweredAcdSvcLvl_30
	--FROM #PhoneMetrics
	--ORDER BY pod_id
 --                     , pod_name
	--		          , year_num
	--		          , month_num
	--		          , month_name
	--                  , Fyear_num
	--		          --, event_date

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    SELECT DISTINCT
            CASE WHEN pm.VALUE IS NULL THEN 0
                      ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text
		   ,pm.AGE
		   ,pm.Svc_Cde
           ,rec.year_num
           ,rec.month_num	
           ,rec.month_name
		   ,rec.Fyear_num
           ,pm.hs_area_id
           ,pm.hs_area_name
           ,COALESCE(pm.pod_id,-1) AS pod_id
		   ,COALESCE(pm.pod_name,'Non-Pod') AS pod_name
           ,pm.epic_department_id
           ,pm.epic_department_name
		   ,CASE WHEN pm.sk_Dim_PG_Question IN ('805','809') AND pm.VALUE = 'Yes, definitely' THEN 1 -- Yes definitely, Yes somewhat, No scale questions
						ELSE CASE WHEN pm.sk_Dim_PG_Question IN ('707','721','731') AND pm.VALUE = 'Yes' THEN 1 -- Yes/No scale questions
						                     ELSE 0
						          END
			END AS TOP_BOX

    INTO #cgcahps

    --FROM    #datetable AS rec
    --LEFT OUTER JOIN
	FROM
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,DISDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
				,loc_master.POD_ID AS pod_id
		        ,loc_master.PFA_POD AS pod_name
				,mdm.hs_area_id
				,mdm.hs_area_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,Resp_Age.AGE
				,resp.Svc_Cde
		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
		LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
							  , sk_Dim_Clrt_SERsrc
							  , sk_Dim_Physcn
							  , ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												  WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												  ELSE Atn_End_Dtm
																												END DESC) AS 'Atn_Seq'
						 FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						 WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient; ER = '326'; IN = '4'; PD = '2092'
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
        LEFT OUTER JOIN
        (
            SELECT DISTINCT
                   EPIC_DEPARTMENT_ID,
                   SERVICE_LINE,
				   POD_ID,
                   PFA_POD,
				   HUB_ID,
                   HUB,
			       BUSINESS_UNIT,
				   LOC_ID,
				   REV_LOC_NAME,
				   UPG_PRACTICE_FLAG,
				   UPG_PRACTICE_REGION_ID,
				   UPG_PRACTICE_REGION_NAME,
				   UPG_PRACTICE_ID,
				   UPG_PRACTICE_NAME
            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
		WHERE   resp.Svc_Cde='MD'
		         AND qstn.sk_Dim_PG_Question IN (
				 809, --	CG_28CL, During your most recent visit, did clerks and receptionists at this provider's office treat you with courtesy and respect?; Yes definitely/Yes somewhat/No
				 707, --	ACO_02C, When you made this appointment for care you needed right away, did you get this appointment as soon as you thought you needed?; No/Yes
				 721, --	ACO_09C, During this visit, did you see this provider within 15 minutes of your appointment time?; No/Yes
				 731, --	ACO_14C, During this visit, did this provider have your medical records?; No/Yes
				 805  --	CG_26CL, Would you recommend this provider's office to your family and friends?; Yes definitely/Yes somewhat/No
				 )
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
	) AS pm
    LEFT OUTER JOIN #datetable AS rec
    ON rec.day_date=pm.RECDATE

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    --ORDER BY rec.day_date;

  -- Create index for temp table #cgcahps
  CREATE UNIQUE CLUSTERED INDEX IX_cgcahps ON #cgcahps ([pod_id], [epic_department_id], [PG_Question_Variable], [event_date], [event_id])

  --SELECT *
  --FROM #cgcahps
  --ORDER BY pod_id
  --                   ,epic_department_id
		--			 ,PG_Question_Variable
		--			 ,event_date
		--			 ,event_id

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    SELECT DISTINCT
            CASE WHEN pm.VALUE IS NULL THEN 0
                      ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text
		   ,pm.AGE
		   ,pm.Svc_Cde
           ,rec.year_num
           ,rec.month_num	
           ,rec.month_name
		   ,rec.Fyear_num
           ,pm.hs_area_id
           ,pm.hs_area_name
           ,COALESCE(pm.pod_id,-1) AS pod_id
		   ,COALESCE(pm.pod_name,'Non-Pod') AS pod_name
           ,pm.epic_department_id
           ,pm.epic_department_name
		   ,CASE WHEN pm.VARNAME IN ('CMS_23') AND pm.VALUE IN ('9','10-Best possible') THEN 1 -- 1-10-Best possible scale questions
						--ELSE CASE WHEN pm.VARNAME IN ('O3') AND pm.VALUE = '5' THEN 1 -- 'Very Good', 'Good', 'Average', 'Poor', 'Very Poor' scale questions
						ELSE CASE WHEN pm.VARNAME IN ('CMS_24') AND pm.VALUE = 'Definitely Yes' THEN 1 -- 'Definitely no', 'Definitely yes', 'Probably no', 'Probably yes' scale questions
						                     ELSE 0
						          END
			END AS TOP_BOX

    INTO #hcahps

    --FROM    #datetable AS rec
    --LEFT OUTER JOIN
	FROM
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,DISDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
				,loc_master.POD_ID AS pod_id
		        ,loc_master.PFA_POD AS pod_name
				,mdm.hs_area_id
				,mdm.hs_area_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,Resp_Age.AGE
				,resp.Svc_Cde
		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
		LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
							  , sk_Dim_Clrt_SERsrc
							  , sk_Dim_Physcn
							  , ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												  WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												  ELSE Atn_End_Dtm
																												END DESC) AS 'Atn_Seq'
						 FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						 WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '4' -- Age question for Outpatient; ER = '326'; IN = '4'; PD = '2092'
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
        LEFT OUTER JOIN
        (
            SELECT DISTINCT
                   EPIC_DEPARTMENT_ID,
                   SERVICE_LINE,
				   POD_ID,
                   PFA_POD,
				   HUB_ID,
                   HUB,
			       BUSINESS_UNIT,
				   LOC_ID,
				   REV_LOC_NAME,
				   UPG_PRACTICE_FLAG,
				   UPG_PRACTICE_REGION_ID,
				   UPG_PRACTICE_REGION_NAME,
				   UPG_PRACTICE_ID,
				   UPG_PRACTICE_NAME
            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
		WHERE   resp.Svc_Cde='IN'
		         AND qstn.VARNAME IN (
				 'CMS_23', --	CMS_23, Using any number from 0 to 10, where 0 is the worst hospital possible and 10 is the best hospital possible, what number would you use to rate this hospital?; 1 - 10-Best possible
				 --'O3' --	O3, Likelihood of your recommending this hospital to others; Very Good/Good/Average/Poor/Very Poor
				 'CMS_24' --	CMS_24, Would you recommend this hospital to your friends and family?; Definitely no/Definitely yes/Probably no/Probably yes
				 )
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
	) AS pm
    LEFT OUTER JOIN #datetable AS rec
    ON rec.day_date=pm.RECDATE

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    --ORDER BY rec.day_date;

  -- Create index for temp table #hcahps
  CREATE UNIQUE CLUSTERED INDEX IX_hcahps ON #hcahps ([pod_id], [epic_department_id], [PG_Question_Variable], [event_date], [event_id])

  --SELECT *
  --FROM #hcahps
  --ORDER BY pod_id
  --                   ,epic_department_id
		--			 ,PG_Question_Variable
		--			 ,event_date
		--			 ,event_id
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    SELECT DISTINCT
            CASE WHEN pm.VALUE IS NULL THEN 0
                      ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text
		   ,pm.AGE
		   ,pm.Svc_Cde
           ,rec.year_num
           ,rec.month_num	
           ,rec.month_name
		   ,rec.Fyear_num
           ,pm.hs_area_id
           ,pm.hs_area_name
           ,COALESCE(pm.pod_id,-1) AS pod_id
		   ,COALESCE(pm.pod_name,'Non-Pod') AS pod_name
           ,pm.epic_department_id
           ,pm.epic_department_name
		   ,CASE WHEN pm.VARNAME IN ('CH_48') AND pm.VALUE IN ('9','10-Best hosp') THEN 1 -- 1-10-Best hosp scale questions
						ELSE CASE WHEN pm.VARNAME IN ('CH_49') AND pm.VALUE = 'Definitely Yes' THEN 1 -- 'Definitely no', 'Definitely yes', 'Probably no', 'Probably yes' scale questions
						                     ELSE 0
						          END
			END AS TOP_BOX

    INTO #chcahps

    --FROM    #datetable AS rec
    --LEFT OUTER JOIN
	FROM
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,DISDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
				,loc_master.POD_ID AS pod_id
		        ,loc_master.PFA_POD AS pod_name
				,mdm.hs_area_id
				,mdm.hs_area_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,Resp_Age.AGE
				,resp.Svc_Cde
		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
		LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
							  , sk_Dim_Clrt_SERsrc
							  , sk_Dim_Physcn
							  , ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												  WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												  ELSE Atn_End_Dtm
																												END DESC) AS 'Atn_Seq'
						 FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						 WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '2092' -- Age question for Outpatient; ER = '326'; IN = '4'; PD = '2092'
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
        LEFT OUTER JOIN
        (
            SELECT DISTINCT
                   EPIC_DEPARTMENT_ID,
                   SERVICE_LINE,
				   POD_ID,
                   PFA_POD,
				   HUB_ID,
                   HUB,
			       BUSINESS_UNIT,
				   LOC_ID,
				   REV_LOC_NAME,
				   UPG_PRACTICE_FLAG,
				   UPG_PRACTICE_REGION_ID,
				   UPG_PRACTICE_REGION_NAME,
				   UPG_PRACTICE_ID,
				   UPG_PRACTICE_NAME
            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
		WHERE   resp.Svc_Cde='PD'
		         AND qstn.VARNAME IN (
				 'CH_48', --	CH_48, Using any number from 0 to 10, where 0 is the worst hospital possible and 10 is the best hospital possible, what number would you use to rate this hospital during your child's stay?; 1 - 10-Best hosp
				 'CH_49'  --	CH_49, Would you recommend this hospital to your friends and family?; Definitely no/Definitely yes/Probably no/Probably yes
				 )
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
	) AS pm
    LEFT OUTER JOIN #datetable AS rec
    ON rec.day_date=pm.RECDATE

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    --ORDER BY rec.day_date;

  -- Create index for temp table #chcahps
  CREATE UNIQUE CLUSTERED INDEX IX_chcahps ON #chcahps ([pod_id], [epic_department_id], [PG_Question_Variable], [event_date], [event_id])

  --SELECT *
  --FROM #chcahps
  --ORDER BY pod_id
  --                   ,epic_department_id
		--			 ,PG_Question_Variable
		--			 ,event_date
		--			 ,event_id
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    SELECT DISTINCT
            CASE WHEN pm.VALUE IS NULL THEN 0
                      ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text
		   ,pm.AGE
		   ,pm.Svc_Cde
           ,rec.year_num
           ,rec.month_num	
           ,rec.month_name
		   ,rec.Fyear_num
           ,pm.hs_area_id
           ,pm.hs_area_name
           ,COALESCE(pm.pod_id,-1) AS pod_id
		   ,COALESCE(pm.pod_name,'Non-Pod') AS pod_name
           ,pm.epic_department_id
           ,pm.epic_department_name
		   ,CASE WHEN pm.VARNAME IN ('EDCP36') AND pm.VALUE IN ('9','10-Best care') THEN 1 -- 1-10-Best care scale questions
						ELSE CASE WHEN pm.VARNAME IN ('EDCP37') AND pm.VALUE = 'Definitely Yes' THEN 1 -- 'Definitely no', 'Definitely yes', 'Probably no', 'Probably yes' scale questions
						                     ELSE 0
						          END
			END AS TOP_BOX

    INTO #er

    --FROM    #datetable AS rec
    --LEFT OUTER JOIN
	FROM
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,DISDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
				,loc_master.POD_ID AS pod_id
		        ,loc_master.PFA_POD AS pod_name
				,mdm.hs_area_id
				,mdm.hs_area_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,Resp_Age.AGE
				,resp.Svc_Cde
		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
		LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
							  , sk_Dim_Clrt_SERsrc
							  , sk_Dim_Physcn
							  , ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												  WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												  ELSE Atn_End_Dtm
																												END DESC) AS 'Atn_Seq'
						 FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						 WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '326' -- Age question for Outpatient; ER = '326'; IN = '4'; PD = '2092'
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
        LEFT OUTER JOIN
        (
            SELECT DISTINCT
                   EPIC_DEPARTMENT_ID,
                   SERVICE_LINE,
				   POD_ID,
                   PFA_POD,
				   HUB_ID,
                   HUB,
			       BUSINESS_UNIT,
				   LOC_ID,
				   REV_LOC_NAME,
				   UPG_PRACTICE_FLAG,
				   UPG_PRACTICE_REGION_ID,
				   UPG_PRACTICE_REGION_NAME,
				   UPG_PRACTICE_ID,
				   UPG_PRACTICE_NAME
            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
		WHERE   resp.Svc_Cde='ER'
		         AND qstn.VARNAME IN (
				 'EDCP36', --	EDCP36, Using any number from 0 to 10, where 0 is the worst emergency room care possible and 10 is the best emergency room care possible, what number would you use to rate your care during this emergency room visit?; 1 - 10-Best care
				 'EDCP37'  --	EDCP37, Would you recommend this emergency room to your friends and family?; Definitely no/Definitely yes/Probably no/Probably yes
				 )
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
	) AS pm
    LEFT OUTER JOIN #datetable AS rec
    ON rec.day_date=pm.RECDATE

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    --ORDER BY rec.day_date;

  -- Create index for temp table #chcahps
  CREATE UNIQUE CLUSTERED INDEX IX_er ON #er ([pod_id], [epic_department_id], [PG_Question_Variable], [event_date], [event_id])

  --SELECT *
  --FROM #er
  --ORDER BY pod_id
  --                   ,epic_department_id
		--			 ,PG_Question_Variable
		--			 ,event_date
		--			 ,event_id

SELECT
       [Proc].Svc_Cde
     , [Proc].pod_id
     , [Proc].pod_name
     , [Proc].epic_department_id
	 , [Proc].epic_department_name
     , [Proc].year_num
	 , [Proc].month_num
	 , [Proc].month_name
	 , [Proc].Fyear_num
	 --, [Proc].event_date
	 , [Proc].PG_Question_Variable
	 , [Proc].PG_Question_Text
     , SUM(CASE WHEN [Proc].event_count = 1 AND [Proc].TOP_BOX = 1 THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS Denominator

INTO #summary

FROM
(
SELECT Svc_Cde
             , pod_id
             , pod_name
			 , epic_department_id
			 , epic_department_name
			 , year_num
			 , month_num
			 , month_name
	         , Fyear_num
			 --, event_date
			 , PG_Question_Variable
			 , PG_Question_Text
			 , event_count
			 , TOP_BOX
FROM #cgcahps
WHERE event_count = 1
--AND hs_area_id = 1
UNION ALL
SELECT Svc_Cde
             , pod_id
             , pod_name
			 , epic_department_id
			 , epic_department_name
			 , year_num
			 , month_num
			 , month_name
	         , Fyear_num
			 --, event_date
			 , PG_Question_Variable
			 , PG_Question_Text
			 , event_count
			 , TOP_BOX
FROM #hcahps
WHERE event_count = 1
UNION ALL
SELECT Svc_Cde
             , pod_id
             , pod_name
			 , epic_department_id
			 , epic_department_name
			 , year_num
			 , month_num
			 , month_name
	         , Fyear_num
			 --, event_date
			 , PG_Question_Variable
			 , PG_Question_Text
			 , event_count
			 , TOP_BOX
FROM #chcahps
WHERE event_count = 1
UNION ALL
SELECT 'ERP' AS Svc_Cde
             , pod_id
             , pod_name
			 , epic_department_id
			 , epic_department_name
			 , year_num
			 , month_num
			 , month_name
	         , Fyear_num
			 --, event_date
			 , PG_Question_Variable
			 , PG_Question_Text
			 , event_count
			 , TOP_BOX
FROM #er
WHERE event_count = 1
AND AGE < 18
UNION ALL
SELECT 'ERA' AS Svc_Cde
             , pod_id
             , pod_name
			 , epic_department_id
			 , epic_department_name
			 , year_num
			 , month_num
			 , month_name
	         , Fyear_num
			 --, event_date
			 , PG_Question_Variable
			 , PG_Question_Text
			 , event_count
			 , TOP_BOX
FROM #er
WHERE event_count = 1
AND AGE >= 18
) [Proc]
GROUP BY [Proc].Svc_Cde
                  , [Proc].pod_id
                  , [Proc].pod_name
                  , [Proc].epic_department_id
				  , [Proc].epic_department_name
			      , [Proc].year_num
			      , [Proc].month_num
			      , [Proc].month_name
	              , [Proc].Fyear_num
			      --, [Proc].event_date
				  , [Proc].PG_Question_Variable
				  , [Proc].PG_Question_Text

--SELECT *
--FROM #summary
--ORDER BY Svc_Cde
--                  , pod_id
--                  , pod_name
--                  , epic_department_id
--				  , epic_department_name
--			      , year_num
--			      , month_num
--			      , month_name
--			      --, event_date
--				  , PG_Question_Variable
--				  , PG_Question_Text

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.CG_26CL,0) AS CG_26CL_Numerator,
       COALESCE(p.ACO_14C,0) AS ACO_14C_Numerator,
       COALESCE(p.ACO_09C,0) AS ACO_09C_Numerator,
       COALESCE(p.ACO_02C,0) AS ACO_02C_Numerator,
       COALESCE(p.CG_28CL,0) AS CG_28CL_Numerator

INTO #numerator_MD

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Numerator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'MD'
) AS pivoted PIVOT ( MAX(Numerator) FOR PG_Question_Variable IN (CG_28CL, ACO_02C, ACO_09C, ACO_14C, CG_26CL) )
AS p;

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.CG_26CL,0) AS CG_26CL_Denominator,
       COALESCE(p.ACO_14C,0) AS ACO_14C_Denominator,
       COALESCE(p.ACO_09C,0) AS ACO_09C_Denominator,
       COALESCE(p.ACO_02C,0) AS ACO_02C_Denominator,
       COALESCE(p.CG_28CL,0) AS CG_28CL_Denominator

INTO #denominator_MD

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Denominator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'MD'
) AS pivoted PIVOT ( MAX(Denominator) FOR PG_Question_Variable IN (CG_28CL, ACO_02C, ACO_09C, ACO_14C, CG_26CL) )
AS p;

SELECT numerator.Svc_Cde,
       numerator.pod_id,
       numerator.pod_name,
       numerator.epic_department_id,
       numerator.epic_department_name,
       numerator.year_num,
       numerator.month_num,
       numerator.month_name,
	   numerator.Fyear_num,
       --numerator.event_date,
       numerator.CG_26CL_Numerator,
       denominator.CG_26CL_Denominator,
       numerator.ACO_14C_Numerator,
       denominator.ACO_14C_Denominator,
       numerator.ACO_09C_Numerator,
       denominator.ACO_09C_Denominator,
       numerator.ACO_02C_Numerator,
       denominator.ACO_02C_Denominator,
       numerator.CG_28CL_Numerator,
       denominator.CG_28CL_Denominator

INTO #PatExp_CGCAHPS_TopBoxPct

FROM
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(CG_26CL_Numerator) AS CG_26CL_Numerator,
			  SUM(ACO_14C_Numerator) AS ACO_14C_Numerator,
			  SUM(ACO_09C_Numerator) AS ACO_09C_Numerator,
			  SUM(ACO_02C_Numerator) AS ACO_02C_Numerator,
			  SUM(CG_28CL_Numerator) AS CG_28CL_Numerator 
FROM #numerator_MD
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) numerator
LEFT OUTER JOIN
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(CG_26CL_Denominator) AS CG_26CL_Denominator,
			  SUM(ACO_14C_Denominator) AS ACO_14C_Denominator,
			  SUM(ACO_09C_Denominator) AS ACO_09C_Denominator,
			  SUM(ACO_02C_Denominator) AS ACO_02C_Denominator,
			  SUM(CG_28CL_Denominator) AS CG_28CL_Denominator 
FROM #denominator_MD
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) denominator
ON denominator.pod_id = numerator.pod_id
AND denominator.epic_department_id = numerator.epic_department_id
--AND denominator.event_date = numerator.event_date
AND denominator.year_num = numerator.year_num
AND denominator.month_num = numerator.month_num

----------------------------------------------------------------------------------------------------------

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.CMS_23,0) AS CMS_23_Numerator,
       --COALESCE(p.O3,0) AS O3_Numerator
       COALESCE(p.CMS_24,0) AS CMS_24_Numerator

INTO #numerator_IN

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Numerator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'IN'
--) AS pivoted PIVOT ( MAX(Numerator) FOR PG_Question_Variable IN (CMS_23, O3) )
) AS pivoted PIVOT ( MAX(Numerator) FOR PG_Question_Variable IN (CMS_23, CMS_24) )
AS p;

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.CMS_23,0) AS CMS_23_Denominator,
       --COALESCE(p.O3,0) AS O3_Denominator
       COALESCE(p.CMS_24,0) AS CMS_24_Denominator

INTO #denominator_IN

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Denominator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'IN'
--) AS pivoted PIVOT ( MAX(Denominator) FOR PG_Question_Variable IN (CMS_23, O3) )
) AS pivoted PIVOT ( MAX(Denominator) FOR PG_Question_Variable IN (CMS_23, CMS_24) )
AS p;

SELECT numerator.Svc_Cde,
       numerator.pod_id,
       numerator.pod_name,
       numerator.epic_department_id,
       numerator.epic_department_name,
       numerator.year_num,
       numerator.month_num,
       numerator.month_name,
	   numerator.Fyear_num,
       --numerator.event_date,
       numerator.CMS_23_Numerator,
       denominator.CMS_23_Denominator,
       --numerator.O3_Numerator,
       --denominator.O3_Denominator
       numerator.CMS_24_Numerator,
       denominator.CMS_24_Denominator

INTO #PatExp_HCAHPS_TopBoxPct

FROM
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(CMS_23_Numerator) AS CMS_23_Numerator,
			  --SUM(O3_Numerator) AS O3_Numerator 
			  SUM(CMS_24_Numerator) AS CMS_24_Numerator 
FROM #numerator_IN
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) numerator
LEFT OUTER JOIN
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(CMS_23_Denominator) AS CMS_23_Denominator,
			  --SUM(O3_Denominator) AS O3_Denominator
			  SUM(CMS_24_Denominator) AS CMS_24_Denominator
FROM #denominator_IN
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) denominator
ON denominator.pod_id = numerator.pod_id
AND denominator.epic_department_id = numerator.epic_department_id
--AND denominator.event_date = numerator.event_date
AND denominator.year_num = numerator.year_num
AND denominator.month_num = numerator.month_num

----------------------------------------------------------------------------------------------------------

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.CH_48,0) AS CH_48_Numerator,
       COALESCE(p.CH_49,0) AS CH_49_Numerator

INTO #numerator_PD

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Numerator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'PD'
) AS pivoted PIVOT ( MAX(Numerator) FOR PG_Question_Variable IN (CH_48, CH_49) )
AS p;

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.CH_48,0) AS CH_48_Denominator,
       COALESCE(p.CH_49,0) AS CH_49_Denominator

INTO #denominator_PD

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Denominator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'PD'
) AS pivoted PIVOT ( MAX(Denominator) FOR PG_Question_Variable IN (CH_48, CH_49) )
AS p;

SELECT numerator.Svc_Cde,
       numerator.pod_id,
       numerator.pod_name,
       numerator.epic_department_id,
       numerator.epic_department_name,
       numerator.year_num,
       numerator.month_num,
       numerator.month_name,
	   numerator.Fyear_num,
       --numerator.event_date,
       numerator.CH_48_Numerator,
       denominator.CH_48_Denominator,
       numerator.CH_49_Numerator,
       denominator.CH_49_Denominator

INTO #PatExp_CHCAHPS_TopBoxPct

FROM
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(CH_48_Numerator) AS CH_48_Numerator,
			  SUM(CH_49_Numerator) AS CH_49_Numerator 
FROM #numerator_PD
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) numerator
LEFT OUTER JOIN
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(CH_48_Denominator) AS CH_48_Denominator,
			  SUM(CH_49_Denominator) AS CH_49_Denominator
FROM #denominator_PD
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) denominator
ON denominator.pod_id = numerator.pod_id
AND denominator.epic_department_id = numerator.epic_department_id
--AND denominator.event_date = numerator.event_date
AND denominator.year_num = numerator.year_num
AND denominator.month_num = numerator.month_num

----------------------------------------------------------------------------------------------------------

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.EDCP36,0) AS EDCP36_Numerator,
       COALESCE(p.EDCP37,0) AS EDCP37_Numerator

INTO #numerator_ERP

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Numerator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'ERP'
) AS pivoted PIVOT ( MAX(Numerator) FOR PG_Question_Variable IN (EDCP36, EDCP37) )
AS p;

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.EDCP36,0) AS EDCP36_Denominator,
       COALESCE(p.EDCP37,0) AS EDCP37_Denominator

INTO #denominator_ERP

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Denominator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'ERP'
) AS pivoted PIVOT ( MAX(Denominator) FOR PG_Question_Variable IN (EDCP36, EDCP37) )
AS p;

SELECT numerator.Svc_Cde,
       numerator.pod_id,
       numerator.pod_name,
       numerator.epic_department_id,
       numerator.epic_department_name,
       numerator.year_num,
       numerator.month_num,
       numerator.month_name,
	   numerator.Fyear_num,
       --numerator.event_date,
       numerator.EDCP36_Numerator,
       denominator.EDCP36_Denominator,
       numerator.EDCP37_Numerator,
       denominator.EDCP37_Denominator

INTO #PatExp_ERP_TopBoxPct

FROM
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(EDCP36_Numerator) AS EDCP36_Numerator,
			  SUM(EDCP37_Numerator) AS EDCP37_Numerator 
FROM #numerator_ERP
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) numerator
LEFT OUTER JOIN
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(EDCP36_Denominator) AS EDCP36_Denominator,
			  SUM(EDCP37_Denominator) AS EDCP37_Denominator
FROM #denominator_ERP
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) denominator
ON denominator.pod_id = numerator.pod_id
AND denominator.epic_department_id = numerator.epic_department_id
--AND denominator.event_date = numerator.event_date
AND denominator.year_num = numerator.year_num
AND denominator.month_num = numerator.month_num

----------------------------------------------------------------------------------------------------------

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.EDCP36,0) AS EDCP36_Numerator,
       COALESCE(p.EDCP37,0) AS EDCP37_Numerator

INTO #numerator_ERA

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Numerator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'ERA'
) AS pivoted PIVOT ( MAX(Numerator) FOR PG_Question_Variable IN (EDCP36, EDCP37) )
AS p;

SELECT p.Svc_Cde,
       p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
	   p.year_num,
	   p.month_num,
	   p.month_name,
	   p.Fyear_num,
	   --p.event_date,
       COALESCE(p.EDCP36,0) AS EDCP36_Denominator,
       COALESCE(p.EDCP37,0) AS EDCP37_Denominator

INTO #denominator_ERA

FROM
(
SELECT DISTINCT
	Svc_Cde
  , pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , year_num
  , month_num
  , month_name
  , Fyear_num
  --, event_date
  , Denominator
  , PG_Question_Variable
FROM #summary
WHERE Svc_Cde = 'ERA'
) AS pivoted PIVOT ( MAX(Denominator) FOR PG_Question_Variable IN (EDCP36, EDCP37) )
AS p;

SELECT numerator.Svc_Cde,
       numerator.pod_id,
       numerator.pod_name,
       numerator.epic_department_id,
       numerator.epic_department_name,
       numerator.year_num,
       numerator.month_num,
       numerator.month_name,
	   numerator.Fyear_num,
       --numerator.event_date,
       numerator.EDCP36_Numerator,
       denominator.EDCP36_Denominator,
       numerator.EDCP37_Numerator,
       denominator.EDCP37_Denominator

INTO #PatExp_ERA_TopBoxPct

FROM
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(EDCP36_Numerator) AS EDCP36_Numerator,
			  SUM(EDCP37_Numerator) AS EDCP37_Numerator 
FROM #numerator_ERA
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) numerator
LEFT OUTER JOIN
(
SELECT Svc_Cde,
              pod_id,
              pod_name,
			  epic_department_id,
			  epic_department_name,
			  year_num,
			  month_num,
			  month_name,
			  Fyear_num,
			  --event_date,
			  SUM(EDCP36_Denominator) AS EDCP36_Denominator,
			  SUM(EDCP37_Denominator) AS EDCP37_Denominator
FROM #denominator_ERA
GROUP BY Svc_Cde
                   ,pod_id
                   ,pod_name
                   ,epic_department_id
                   ,epic_department_name
				   ,year_num
				   ,month_num
				   ,month_name
	               ,Fyear_num
				   --,event_date
) denominator
ON denominator.pod_id = numerator.pod_id
AND denominator.epic_department_id = numerator.epic_department_id
--AND denominator.event_date = numerator.event_date
AND denominator.year_num = numerator.year_num
AND denominator.month_num = numerator.month_num

--SELECT *
--FROM #PatExp_CGCAHPS_TopBoxPct
--ORDER BY Svc_Cde
--                  , pod_id
--                  , pod_name
--			      , year_num
--			      , month_num
--			      , month_name
--	              , Fyear_num
--			      --, event_date

--SELECT *
--FROM #PatExp_HCAHPS_TopBoxPct
--ORDER BY Svc_Cde
--                  , pod_id
--                  , pod_name
--			      , year_num
--			      , month_num
--			      , month_name
--	              , Fyear_num
--			      --, event_date

--SELECT *
--FROM #PatExp_CHCAHPS_TopBoxPct
--ORDER BY Svc_Cde
--                  , pod_id
--                  , pod_name
--			      , year_num
--			      , month_num
--			      , month_name
--	              , Fyear_num
--			      --, event_date

--SELECT *
--FROM #PatExp_ERP_TopBoxPct
--ORDER BY Svc_Cde
--                  , pod_id
--                  , pod_name
--			      , year_num
--			      , month_num
--			      , month_name
--	              , Fyear_num
--			      --, event_date

--SELECT *
--FROM #PatExp_ERA_TopBoxPct
--ORDER BY Svc_Cde
--                  , pod_id
--                  , pod_name
--			      , year_num
--			      , month_num
--			      , month_name
--	              , Fyear_num
--			      --, event_date
/*
SELECT PatientSatisfactionHighlights.pod_id,
       PatientSatisfactionHighlights.pod_name,
       PatientSatisfactionHighlights.year_num,
       PatientSatisfactionHighlights.month_num,
       PatientSatisfactionHighlights.month_name,
	   PatientSatisfactionHighlights.Fyear_num,
       PhoneMetrics.nEnteredAcd AS Calls_Entered,
       PhoneMetrics.nAbandonedAcd AS Calls_Abandoned,
       PhoneMetrics.nAnsweredAcd AS Calls_Answered,
       PhoneMetrics.tAnsweredAcd AS Calls_Answered_Time,
       PhoneMetrics.nAnsweredAcdSvcLvl_30 AS Calls_Answered_Within_30,
/*
				 809, --	CG_28CL, During your most recent visit, did clerks and receptionists at this provider's office treat you with courtesy and respect?; Yes definitely/Yes somewhat/No
				 707, --	ACO_02C, When you made this appointment for care you needed right away, did you get this appointment as soon as you thought you needed?; No/Yes
				 721, --	ACO_09C, During this visit, did you see this provider within 15 minutes of your appointment time?; No/Yes
				 731, --	ACO_14C, During this visit, did this provider have your medical records?; No/Yes
				 805  --	CG_26CL, Would you recommend this provider's office to your family and friends?; Yes definitely/Yes somewhat/No
*/
       PatientSatisfactionHighlights.CG_26CL_Numerator AS Recommend_Provider_TopBox,
       PatientSatisfactionHighlights.CG_26CL_Denominator AS Recommend_Provider_Response,
       PatientSatisfactionHighlights.ACO_14C_Numerator AS Provider_Have_Records_TopBox,
       PatientSatisfactionHighlights.ACO_14C_Denominator AS Provider_Have_Records_Response,
       PatientSatisfactionHighlights.ACO_09C_Numerator AS See_Provider_15_Minutes_TopBox,
       PatientSatisfactionHighlights.ACO_09C_Denominator AS See_Provider_15_Minutes_Response,
       PatientSatisfactionHighlights.ACO_02C_Numerator AS Appt_As_Soon_As_Needed_TopBox,
       PatientSatisfactionHighlights.ACO_02C_Denominator AS Appt_As_Soon_As_Needed_Response,
       PatientSatisfactionHighlights.CG_28CL_Numerator AS Courtesy_Clerks_Receptionists_TopBox,
       PatientSatisfactionHighlights.CG_28CL_Denominator  AS Courtesy_Clerks_Receptionists_Response
FROM
(
SELECT pod_id,
       pod_name,
       year_num,
       month_num,
       month_name,
	   Fyear_num,
       CG_26CL_Numerator,
       CG_26CL_Denominator,
       ACO_14C_Numerator,
       ACO_14C_Denominator,
       ACO_09C_Numerator,
       ACO_09C_Denominator,
       ACO_02C_Numerator,
       ACO_02C_Denominator,
       CG_28CL_Numerator,
       CG_28CL_Denominator
FROM #PatExp_CGCAHPS_TopBoxPct
WHERE pod_name <> 'Non-Pod'
AND Fyear_num = 2021
) PatientSatisfactionHighlights
LEFT OUTER JOIN
(
SELECT pod_id,
           pod_name,
           year_num,
           month_num,
           month_name,
		   Fyear_num,
           nEnteredAcd,
           nAbandonedAcd,
           nAnsweredAcd,
           tAnsweredAcd,
           nAnsweredAcdSvcLvl_30
FROM #PhoneMetrics
WHERE pod_name <> 'Non-Pod'
AND Fyear_num = 2021
) PhoneMetrics
ON PhoneMetrics.pod_id = PatientSatisfactionHighlights.pod_id
AND PhoneMetrics.year_num = PatientSatisfactionHighlights.year_num
AND PhoneMetrics.month_num = PatientSatisfactionHighlights.month_num
*/

SELECT PhoneMetrics.pod_id,
       PhoneMetrics.pod_name,
       PhoneMetrics.year_num,
       PhoneMetrics.month_num,
       PhoneMetrics.month_name,
	   PhoneMetrics.Fyear_num,
       PhoneMetrics.nEnteredAcd AS Calls_Entered,
       PhoneMetrics.nAbandonedAcd AS Calls_Abandoned,
       PhoneMetrics.nAnsweredAcd AS Calls_Answered,
       PhoneMetrics.tAnsweredAcd AS Calls_Answered_Time,
       PhoneMetrics.nAnsweredAcdSvcLvl_30 AS Calls_Answered_Within_30
FROM
(
SELECT pod_id,
           pod_name,
           year_num,
           month_num,
           month_name,
		   Fyear_num,
           nEnteredAcd,
           nAbandonedAcd,
           nAnsweredAcd,
           tAnsweredAcd,
           nAnsweredAcdSvcLvl_30
FROM #PhoneMetrics
WHERE pod_name <> 'Non-Pod'
AND Fyear_num = 2021
) PhoneMetrics
ORDER BY PhoneMetrics.pod_id
                  , PhoneMetrics.year_num
				  , PhoneMetrics.month_num

SELECT PatientSatisfactionHighlights.pod_id,
       PatientSatisfactionHighlights.pod_name,
	   PatientSatisfactionHighlights.epic_department_id,
	   PatientSatisfactionHighlights.epic_department_name,
       PatientSatisfactionHighlights.year_num,
       PatientSatisfactionHighlights.month_num,
       PatientSatisfactionHighlights.month_name,
	   PatientSatisfactionHighlights.Fyear_num,
/*
				 809, --	CG_28CL, During your most recent visit, did clerks and receptionists at this provider's office treat you with courtesy and respect?; Yes definitely/Yes somewhat/No
				 707, --	ACO_02C, When you made this appointment for care you needed right away, did you get this appointment as soon as you thought you needed?; No/Yes
				 721, --	ACO_09C, During this visit, did you see this provider within 15 minutes of your appointment time?; No/Yes
				 731, --	ACO_14C, During this visit, did this provider have your medical records?; No/Yes
				 805  --	CG_26CL, Would you recommend this provider's office to your family and friends?; Yes definitely/Yes somewhat/No
*/
       PatientSatisfactionHighlights.CG_26CL_Numerator AS Recommend_Provider_TopBox,
       PatientSatisfactionHighlights.CG_26CL_Denominator AS Recommend_Provider_Response,
       PatientSatisfactionHighlights.ACO_14C_Numerator AS Provider_Have_Records_TopBox,
       PatientSatisfactionHighlights.ACO_14C_Denominator AS Provider_Have_Records_Response,
       PatientSatisfactionHighlights.ACO_09C_Numerator AS See_Provider_15_Minutes_TopBox,
       PatientSatisfactionHighlights.ACO_09C_Denominator AS See_Provider_15_Minutes_Response,
       PatientSatisfactionHighlights.ACO_02C_Numerator AS Appt_As_Soon_As_Needed_TopBox,
       PatientSatisfactionHighlights.ACO_02C_Denominator AS Appt_As_Soon_As_Needed_Response,
       PatientSatisfactionHighlights.CG_28CL_Numerator AS Courtesy_Clerks_Receptionists_TopBox,
       PatientSatisfactionHighlights.CG_28CL_Denominator  AS Courtesy_Clerks_Receptionists_Response
FROM
(
SELECT pod_id,
       pod_name,
	   epic_department_id,
	   epic_department_name,
       year_num,
       month_num,
       month_name,
	   Fyear_num,
       CG_26CL_Numerator,
       CG_26CL_Denominator,
       ACO_14C_Numerator,
       ACO_14C_Denominator,
       ACO_09C_Numerator,
       ACO_09C_Denominator,
       ACO_02C_Numerator,
       ACO_02C_Denominator,
       CG_28CL_Numerator,
       CG_28CL_Denominator
FROM #PatExp_CGCAHPS_TopBoxPct
WHERE pod_name <> 'Non-Pod'
AND Fyear_num = 2021
) PatientSatisfactionHighlights
ORDER BY PatientSatisfactionHighlights.pod_id
                  , PatientSatisfactionHighlights.epic_department_id
                  , PatientSatisfactionHighlights.year_num
				  , PatientSatisfactionHighlights.month_num

SELECT PatientSatisfactionHighlights.pod_id,
       PatientSatisfactionHighlights.pod_name,
	   PatientSatisfactionHighlights.epic_department_id,
	   PatientSatisfactionHighlights.epic_department_name,
       PatientSatisfactionHighlights.year_num,
       PatientSatisfactionHighlights.month_num,
       PatientSatisfactionHighlights.month_name,
	   PatientSatisfactionHighlights.Fyear_num,
/*
				 'CMS_23', --	CMS_23, Using any number from 0 to 10, where 0 is the worst hospital possible and 10 is the best hospital possible, what number would you use to rate this hospital?; 1 - 10-Best possible
				 'CMS_24'  --	CMS_24, Would you recommend this hospital to your friends and family?; Definitely no/Definitely yes/Probably no/Probably yes
*/
       PatientSatisfactionHighlights.CMS_23_Numerator AS Rate_Hospital_TopBox,
       PatientSatisfactionHighlights.CMS_23_Denominator AS Rate_Hospital_Response,
       PatientSatisfactionHighlights.CMS_24_Numerator AS Recommend_Hospital_TopBox,
       PatientSatisfactionHighlights.CMS_24_Denominator AS Recommend_Hospital_Response
FROM
(
SELECT pod_id,
       pod_name,
	   epic_department_id,
	   epic_department_name,
       year_num,
       month_num,
       month_name,
	   Fyear_num,
       CMS_23_Numerator,
       CMS_23_Denominator,
       CMS_24_Numerator,
       CMS_24_Denominator
FROM #PatExp_HCAHPS_TopBoxPct
--WHERE pod_name <> 'Non-Pod'
--AND Fyear_num = 2021
WHERE Fyear_num = 2021
) PatientSatisfactionHighlights
ORDER BY PatientSatisfactionHighlights.pod_id
                  , PatientSatisfactionHighlights.epic_department_id
                  , PatientSatisfactionHighlights.year_num
				  , PatientSatisfactionHighlights.month_num

SELECT PatientSatisfactionHighlights.pod_id,
       PatientSatisfactionHighlights.pod_name,
	   PatientSatisfactionHighlights.epic_department_id,
	   PatientSatisfactionHighlights.epic_department_name,
       PatientSatisfactionHighlights.year_num,
       PatientSatisfactionHighlights.month_num,
       PatientSatisfactionHighlights.month_name,
	   PatientSatisfactionHighlights.Fyear_num,
/*
				 'CH_48', --	CH_48, Using any number from 0 to 10, where 0 is the worst hospital possible and 10 is the best hospital possible, what number would you use to rate this hospital during your child's stay?; 1 - 10-Best hosp
				 'CH_49'  --	CH_49, Would you recommend this hospital to your friends and family?; Definitely no/Definitely yes/Probably no/Probably yes
*/
       PatientSatisfactionHighlights.CH_48_Numerator AS Rate_Hospital_TopBox,
       PatientSatisfactionHighlights.CH_48_Denominator AS Rate_Hospital_Response,
       PatientSatisfactionHighlights.CH_49_Numerator AS Recommend_Hospital_TopBox,
       PatientSatisfactionHighlights.CH_49_Denominator AS Recommend_Hospital_Response
FROM
(
SELECT pod_id,
       pod_name,
	   epic_department_id,
	   epic_department_name,
       year_num,
       month_num,
       month_name,
	   Fyear_num,
       CH_48_Numerator,
       CH_48_Denominator,
       CH_49_Numerator,
       CH_49_Denominator
FROM #PatExp_CHCAHPS_TopBoxPct
--WHERE pod_name <> 'Non-Pod'
--AND Fyear_num = 2021
WHERE Fyear_num = 2021
) PatientSatisfactionHighlights
ORDER BY PatientSatisfactionHighlights.pod_id
                  , PatientSatisfactionHighlights.epic_department_id
                  , PatientSatisfactionHighlights.year_num
				  , PatientSatisfactionHighlights.month_num

SELECT PatientSatisfactionHighlights.pod_id,
       PatientSatisfactionHighlights.pod_name,
	   PatientSatisfactionHighlights.epic_department_id,
	   PatientSatisfactionHighlights.epic_department_name,
       PatientSatisfactionHighlights.year_num,
       PatientSatisfactionHighlights.month_num,
       PatientSatisfactionHighlights.month_name,
	   PatientSatisfactionHighlights.Fyear_num,
/*
				 'EDCP36', --	EDCP36, Using any number from 0 to 10, where 0 is the worst emergency room care possible and 10 is the best emergency room care possible, what number would you use to rate your care during this emergency room visit?; 1 - 10-Best care
				 'EDCP37'  --	EDCP37, Would you recommend this emergency room to your friends and family?; Definitely no/Definitely yes/Probably no/Probably yes
*/
       PatientSatisfactionHighlights.EDCP36_Numerator AS Rate_ER_TopBox,
       PatientSatisfactionHighlights.EDCP36_Denominator AS Rate_ER_Response,
       PatientSatisfactionHighlights.EDCP37_Numerator AS Recommend_ER_TopBox,
       PatientSatisfactionHighlights.EDCP37_Denominator AS Recommend_ER_Response
FROM
(
SELECT pod_id,
       pod_name,
	   epic_department_id,
	   epic_department_name,
       year_num,
       month_num,
       month_name,
	   Fyear_num,
       EDCP36_Numerator,
       EDCP36_Denominator,
       EDCP37_Numerator,
       EDCP37_Denominator
FROM #PatExp_ERP_TopBoxPct
--WHERE pod_name <> 'Non-Pod'
--AND Fyear_num = 2021
WHERE Fyear_num = 2021
) PatientSatisfactionHighlights
ORDER BY PatientSatisfactionHighlights.pod_id
                  , PatientSatisfactionHighlights.epic_department_id
                  , PatientSatisfactionHighlights.year_num
				  , PatientSatisfactionHighlights.month_num

SELECT PatientSatisfactionHighlights.pod_id,
       PatientSatisfactionHighlights.pod_name,
	   PatientSatisfactionHighlights.epic_department_id,
	   PatientSatisfactionHighlights.epic_department_name,
       PatientSatisfactionHighlights.year_num,
       PatientSatisfactionHighlights.month_num,
       PatientSatisfactionHighlights.month_name,
	   PatientSatisfactionHighlights.Fyear_num,
/*
				 'EDCP36', --	EDCP36, Using any number from 0 to 10, where 0 is the worst emergency room care possible and 10 is the best emergency room care possible, what number would you use to rate your care during this emergency room visit?; 1 - 10-Best care
				 'EDCP37'  --	EDCP37, Would you recommend this emergency room to your friends and family?; Definitely no/Definitely yes/Probably no/Probably yes
*/
       PatientSatisfactionHighlights.EDCP36_Numerator AS Rate_ER_TopBox,
       PatientSatisfactionHighlights.EDCP36_Denominator AS Rate_ER_Response,
       PatientSatisfactionHighlights.EDCP37_Numerator AS Recommend_ER_TopBox,
       PatientSatisfactionHighlights.EDCP37_Denominator AS Recommend_ER_Response
FROM
(
SELECT pod_id,
       pod_name,
	   epic_department_id,
	   epic_department_name,
       year_num,
       month_num,
       month_name,
	   Fyear_num,
       EDCP36_Numerator,
       EDCP36_Denominator,
       EDCP37_Numerator,
       EDCP37_Denominator
FROM #PatExp_ERA_TopBoxPct
--WHERE pod_name <> 'Non-Pod'
--AND Fyear_num = 2021
WHERE Fyear_num = 2021
) PatientSatisfactionHighlights
ORDER BY PatientSatisfactionHighlights.pod_id
                  , PatientSatisfactionHighlights.epic_department_id
                  , PatientSatisfactionHighlights.year_num
				  , PatientSatisfactionHighlights.month_num

GO


