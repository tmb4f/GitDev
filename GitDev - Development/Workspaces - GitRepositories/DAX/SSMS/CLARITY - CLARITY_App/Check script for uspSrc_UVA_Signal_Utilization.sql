USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROC [ETL].[uspSrc_UVA_Signal_Utilization]
--AS

/*******************************************************************************************
WHAT:	[Rptg].[uspUVA_Signal_Utilization]
WHO :	Gian P Simone
WHEN:	4/1/2023
WHY :	Signal Utilization - Data Portal Dashboard
		- Requested by Dr. Lyman & Dr. Lepsch, this data source brings overall utilization of the 4 main areas within Epic Signal (InBasket, Notes, Orders, Chart Review).

*******************************************************************************************/

----HISTORY------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
-- Manual pull from Danny Davis from Signal web-app. Data includes all basic/in scope metrics discussed
-- with the team. Range from 3/13/2022 - 3/1/2023.
-----------------------------------------------------------------------------------------------------

SET NOCOUNT ON 

-----set date ranges BDD 5/10/2023 -------------
DECLARE @startdate SMALLDATETIME, @enddate SMALLDATETIME

EXEC etl.usp_Get_Dash_Dates_BalancedScorecard @startdate  OUTPUT, @enddate  OUTPUT    

---only pull through yesterday
SET @enddate = CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME)

DECLARE @histenddate SMALLDATETIME, @livestartdate SMALLDATETIME
SET @histenddate = '02/01/2023'-- Only need history previous to February 2023. UAL ETL went live 1/31/2023.
--SET @livestartdate = '01/01/2023' -- Data UAL Tables started refreshing on ETL
SET @livestartdate = '05/12/2024' -- Data UAL Tables started refreshing on ETL
------------------------------------------------

IF OBJECT_ID('tempdb..#HISTORY') IS NOT NULL
DROP TABLE #HISTORY

IF OBJECT_ID('tempdb..#UAL') IS NOT NULL
DROP TABLE #UAL

IF OBJECT_ID('tempdb..#BASE') IS NOT NULL
DROP TABLE #BASE

IF OBJECT_ID('tempdb..#DETAILS') IS NOT NULL
DROP TABLE #DETAILS

IF OBJECT_ID('tempdb..#DATES') IS NOT NULL
DROP TABLE #DATES

IF OBJECT_ID('tempdb..#PROVIDERS') IS NOT NULL
DROP TABLE #PROVIDERS

IF OBJECT_ID('tempdb..#INBASKET') IS NOT NULL
DROP TABLE #INBASKET

IF OBJECT_ID('tempdb..#NOTES') IS NOT NULL
DROP TABLE #NOTES

IF OBJECT_ID('tempdb..#chart') IS NOT NULL
DROP TABLE #chart

IF OBJECT_ID('tempdb..#ORD') IS NOT NULL
DROP TABLE #ORD

--SELECT DISTINCT
--	X_EMP_MAP.USER_NUMBER_ID USER_ID
--	,DATE_DIMENSION.CALENDAR_DT ACTIVITY_DATE -- Grouping all minutes by User/Activity
--	,HIST.Metric_ID ACTIVITY_ID 
--	,SUM(CAST(HIST.Numerator AS NUMERIC)) Numerator
--	,MAX(CAST(HIST.Denominator AS NUMERIC)) Denominator
--INTO #HISTORY
--FROM
--	CLARITY_App.ETL.SIGNAL_HISTORY_LOAD HIST
--INNER JOIN Clarity.dbo.X_EMP_MAP ON X_EMP_MAP.COMMUNITY_ID = HIST.EMP_CID
--LEFT OUTER JOIN Clarity.dbo.DATE_DIMENSION ON DATE_DIMENSION.CALENDAR_DT = CAST(HIST.Reporting_Period_Start_Date AS DATE)
--WHERE
--    DATE_DIMENSION.CALENDAR_DT > @startdate
--	AND
--	DATE_DIMENSION.CALENDAR_DT < @histenddate 
--GROUP BY
--	X_EMP_MAP.USER_NUMBER_ID 
--	,DATE_DIMENSION.CALENDAR_DT 
--	,HIST.Metric_ID 

----LIVE DATA / ETL TABLES---------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
-- UAL  Clarity tables added to ETL on 1/31/2023 - No previous data available for Backfills.
-----------------------------------------------------------------------------------------------------

SELECT DISTINCT
	UAL.USER_ID
	,DATE_DIMENSION.CALENDAR_DT ACTIVITY_DATE
	,UAL.ACTIVITY_ID
	,SUM(UAL.NUMBER_OF_SECONDS_ACTIVE)/60.0 Numerator
	,COUNT(DISTINCT DATE_DIMENSION.CALENDAR_DT) Denominator
INTO #UAL
FROM
	Clarity.dbo.UAL_ACTIVITY_HOURS UAL
LEFT OUTER JOIN Clarity.dbo.DATE_DIMENSION ON DATE_DIMENSION.CALENDAR_DT = CAST(UAL.ACTIVITY_HOUR_DTTM AS DATE)
WHERE
	DATE_DIMENSION.CALENDAR_DT >= @livestartdate
	AND
    DATE_DIMENSION.CALENDAR_DT < @enddate

	AND UAL.USER_ID = '815'

GROUP BY
	UAL.USER_ID
	,DATE_DIMENSION.CALENDAR_DT 
	,UAL.ACTIVITY_ID

-- Combining both historical and current Live information for continuous data visualization on tableau.

SELECT DISTINCT 
	* 
INTO #BASE
FROM
(
--	SELECT DISTINCT 
--		USER_ID
--		,ACTIVITY_DATE
--		,ACTIVITY_ID
--		,CASE WHEN Denominator = 0 THEN 0 ELSE Numerator/Denominator END Numerator 
--	--	,CASE WHEN Numerator = 0 THEN 0 ELSE Numerator/Denominator END Numerator 
--	FROM #HISTORY
--UNION ALL
	SELECT DISTINCT 
		USER_ID
		,ACTIVITY_DATE
		,ACTIVITY_ID
		,Numerator
		--,CASE WHEN Numerator = 0 THEN 0 ELSE Numerator/Denominator END Numerator 
	FROM #UAL
) A
	
----DETAILS------------------------------------------------------------------------------------------

SELECT DISTINCT
	CLARITY_EMP.USER_ID
	,CLARITY_EMP.NAME USER_NAME
	,CLARITY_SER.PROV_ID
	,CLARITY_SER.PROV_TYPE
	,ZC_SPECIALTY.NAME PROV_SPECIALTY
	,CLARITY_DEP.DEPARTMENT_ID
	,CLARITY_DEP.DEPARTMENT_NAME
	,#BASE.ACTIVITY_ID
	,DESKTOP_ACTIVITY.ACTIVITY_NAME
	,DATE_DIMENSION.CALENDAR_DT
	,DATE_DIMENSION.DAY_OF_WEEK
	,DATE_DIMENSION.WEEK_NUMBER
	,DATE_DIMENSION.DAY_OF_MONTH
	,DATE_DIMENSION.MONTH_NUMBER
	,DATE_DIMENSION.MONTH_NAME
	,DATE_DIMENSION.WEEKEND_YN
	,DATE_DIMENSION.HOLIDAY_YN
	,#BASE.Numerator
INTO #DETAILS
FROM 
	#BASE
LEFT OUTER JOIN Clarity.dbo.CLARITY_EMP ON CLARITY_EMP.USER_ID = #BASE.USER_ID
LEFT OUTER JOIN Clarity.dbo.CLARITY_SER ON CLARITY_EMP.PROV_ID = CLARITY_SER.PROV_ID
LEFT OUTER JOIN Clarity.dbo.CLARITY_SER_SPEC ON CLARITY_SER_SPEC.PROV_ID = CLARITY_SER.PROV_ID
			AND CLARITY_SER_SPEC.LINE = 1
LEFT OUTER JOIN Clarity.dbo.CLARITY_SER_2 ON CLARITY_SER_2.PROV_ID = CLARITY_SER.PROV_ID
LEFT OUTER JOIN Clarity.dbo.CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = CLARITY_SER_2.PRIMARY_DEPT_ID
LEFT OUTER JOIN Clarity.dbo.ZC_SPECIALTY ON CLARITY_SER_SPEC.SPECIALTY_C = ZC_SPECIALTY.SPECIALTY_C
LEFT OUTER JOIN Clarity.dbo.DESKTOP_ACTIVITY ON DESKTOP_ACTIVITY.ACTIVITY_ID = #BASE.ACTIVITY_ID
LEFT OUTER JOIN Clarity.dbo.DATE_DIMENSION ON DATE_DIMENSION.CALENDAR_DT = #BASE.ACTIVITY_DATE

WHERE CLARITY_EMP.PROV_ID = '29582'

--Dates------------------------------------------------------------------------------------------------
-- For denominator calculations we need distinct dates each user logged into hyperspace, regardless on the activity

SELECT DISTINCT
	USER_ID
	,CALENDAR_DT
INTO #DATES
FROM
	#DETAILS

--Provider List----------------------------------------------------------------------------------------

SELECT DISTINCT
	USER_ID
	,USER_NAME
	,PROV_ID
	,PROV_TYPE
	,PROV_SPECIALTY
	,DEPARTMENT_ID
	,DEPARTMENT_NAME
INTO #PROVIDERS
FROM
	#DETAILS

--In-Basket----------------------------------------------------------------------------------------
-- Average number of minutes (Active) a provider spent in In Basket per day. 

SELECT DISTINCT
	#DETAILS.USER_ID
	,#DETAILS.CALENDAR_DT
	,SUM(COALESCE(#DETAILS.NUMERATOR,0)) Numerator
INTO #INBASKET
FROM 
	#DETAILS
WHERE
	(#DETAILS.ACTIVITY_ID = '185' OR (#DETAILS.ACTIVITY_ID IN (SELECT GROUPER_RECORDS.GRP_REC_LIST_VALUE FROM Clarity.dbo.GROUPER_RECORDS WHERE GROUPER_ID IN ('1748175','1748176','1748575'))))
	-- Epic released Groupers to capture InBasket related activities.
GROUP BY
	USER_ID
	,CALENDAR_DT

--Notes----------------------------------------------------------------------------------------
-- Average amount of time a provider spent in Notes per day. 

SELECT DISTINCT
	USER_ID
	,CALENDAR_DT
	,SUM(NUMERATOR) Numerator
INTO #NOTES
FROM 
	#DETAILS
WHERE
	(#DETAILS.ACTIVITY_ID = '218' OR (#DETAILS.ACTIVITY_ID IN (SELECT GROUPER_RECORDS.GRP_REC_LIST_VALUE FROM Clarity.dbo.GROUPER_RECORDS WHERE GROUPER_ID IN ('1748200','1748201','1748600'))))
	-- Epic released Groupers to capture Note related activities.
GROUP BY
	USER_ID
	,CALENDAR_DT

--Chart Review----------------------------------------------------------------------------------------
-- Average number of minutes a provider spent in clinical review activities, such as Chart Review, per day.. 

SELECT DISTINCT
	USER_ID
	,ACTIVITY_ID
	,ACTIVITY_NAME
	,CALENDAR_DT
	,SUM(NUMERATOR) Numerator
INTO #chart
FROM 
	#DETAILS
WHERE
	(#DETAILS.ACTIVITY_ID = '11' OR (#DETAILS.ACTIVITY_ID IN (SELECT GROUPER_RECORDS.GRP_REC_LIST_VALUE FROM Clarity.dbo.GROUPER_RECORDS WHERE GROUPER_ID IN ('1748125','1748126','1748525'))))
	-- Epic released Groupers to capture Note related activities.
GROUP BY
	USER_ID
	,ACTIVITY_ID
	,ACTIVITY_NAME
	,CALENDAR_DT

SELECT
	*
FROM #chart
ORDER BY
	CALENDAR_DT

--SELECT DISTINCT
--	ACTIVITY_ID,
--	ACTIVITY_NAME
--FROM #chart
--ORDER BY
--	ACTIVITY_ID

--Orders----------------------------------------------------------------------------------------
-- Average amount of time a provider spent in orders per day. 

SELECT DISTINCT
	USER_ID
	,CALENDAR_DT
	,SUM(NUMERATOR) Numerator
INTO #ORD
FROM 
	#DETAILS
WHERE
	(#DETAILS.ACTIVITY_ID = '206' OR (#DETAILS.ACTIVITY_ID IN (SELECT GROUPER_RECORDS.GRP_REC_LIST_VALUE FROM Clarity.dbo.GROUPER_RECORDS WHERE GROUPER_ID IN ('1748205','1748206','1748605'))))
	-- Epic released Groupers to capture Order related activities.	
GROUP BY
	USER_ID
	,CALENDAR_DT

--Final--------------------------------------------------------------------------------------------
-- Including Data Portal joins/filters

---load to stage table prior to move to DM server 
--TRUNCATE TABLE Stage.Dash_Signal_Utilization

--INSERT Stage.Dash_Signal_Utilization
--           ([user_id]
--           ,[provider_id]
--           ,[provider_name]
--           ,[provider_type]
--           ,[provider_specialty]
--           ,[sk_dim_physcn]
--           ,[Cost_Code]
--           ,[rev_location_id]
--           ,[rev_location]
--           ,[epic_department_id]
--           ,[epic_department_name]
--           ,[epic_department_name_external]
--           ,[opnl_service_id]
--           ,[opnl_service_name]
--           ,[corp_service_id]
--           ,[corp_service_name]
--           ,[hs_area_id]
--           ,[hs_area_name]
--           ,[hospital_code]
--           ,[financial_division_id]
--           ,[financial_division_name]
--           ,[financial_sub_division_id]
--           ,[financial_sub_division_name]
--           ,[som_group_id]
--           ,[som_group_name]
--           ,[som_department_id]
--           ,[som_department_name]
--           ,[som_division_id]
--           ,[som_division_name]
--           ,[Event_date]
--           ,[fmonth_num]
--           ,[Fyear_num]
--           ,[FYear_name]
--           ,[month_begin_date]
--           ,[Minutes_In_Basket]
--           ,[Minutes_Orders]
--           ,[Minutes_Notes]
--           ,[Minutes_Chart_Review]
--           ,[event_count]
--		   )
SELECT DISTINCT
	CAST(#PROVIDERS.USER_ID AS VARCHAR(18))													AS user_id
	,CAST(#PROVIDERS.prov_id AS VARCHAR(18))												AS provider_id
	,CAST(#PROVIDERS.USER_NAME AS VARCHAR(200))												AS provider_name
	,CAST(#PROVIDERS.PROV_TYPE AS VARCHAR(200))												AS provider_type
	,CAST(#PROVIDERS.PROV_SPECIALTY AS VARCHAR(200))										AS provider_specialty
	, physsvc.sk_dim_physcn																	AS sk_dim_physcn
	,CAST(ISNULL(LOC_INFO.FINANCE_COST_CODE,'') AS VARCHAR(30))								AS Cost_Code
	,CLARITY_LOC.LOC_ID																		AS rev_location_id
	,CLARITY_LOC.LOC_NAME																	AS rev_location
	,CAST(LOC_INFO.EPIC_DEPARTMENT_ID AS NUMERIC(18,0))										AS epic_department_id
	,CAST(LOC_INFO.EPIC_DEPT_NAME AS VARCHAR(255))											AS epic_department_name
	,CAST(LOC_INFO.EPIC_EXT_NAME AS VARCHAR(255))											AS epic_department_name_external
	,CAST(LOC_INFO.opnl_service_id AS INT)													AS opnl_service_id
	,CAST(LOC_INFO.opnl_service_name	AS VARCHAR(150))									AS opnl_service_name
	,CAST(LOC_INFO.corp_service_line_id AS INT)												AS corp_service_id
	,CAST(LOC_INFO.corp_service_line AS VARCHAR(150))										AS corp_service_name
	,CAST(LOC_INFO.hs_area_id AS SMALLINT)													AS hs_area_id
	,CAST(LOC_INFO.hs_area_name AS VARCHAR(150))											AS hs_area_name
	,CAST(mdm.HOSPITAL_CODE	AS VARCHAR(150))												AS hospital_code
	, TRY_CAST(vwDim_Clrt_SERsrc.Financial_Division AS INT)																										AS financial_division_id
    , CASE WHEN vwDim_Clrt_SERsrc.Financial_Division_Name <> 'na' THEN CAST(vwDim_Clrt_SERsrc.Financial_Division_Name AS VARCHAR(150)) ELSE NULL END			AS financial_division_name
	, TRY_CAST(vwDim_Clrt_SERsrc.Financial_SubDivision AS INT)																									AS financial_sub_division_id
	, CASE WHEN vwDim_Clrt_SERsrc.Financial_SubDivision_Name <> 'na' THEN CAST(vwDim_Clrt_SERsrc.Financial_SubDivision_Name AS VARCHAR(150)) ELSE NULL END		AS financial_sub_division_name
	, dvsn.som_group_id																		AS som_group_id
	, dvsn.som_group_name																	AS som_group_name
	, dvsn.Department_ID																	AS som_department_id
	, CAST(dvsn.Department AS VARCHAR(150))													AS som_department_name
	, CAST(dvsn.Org_Number AS INT)															AS som_division_id
	, CAST(dvsn.Organization AS VARCHAR(150))												AS som_division_name	
	,CAST(#DATES.CALENDAR_DT AS DATETIME)													AS Event_date
	,dd.fmonth_num
	,dd.Fyear_num
	,dd.FYear_name
	,dd.month_begin_date
	,CAST(COALESCE(#INBASKET.Numerator,0)	AS NUMERIC)										AS Minutes_In_Basket
	,CAST(COALESCE(#ORD.Numerator,0)			AS NUMERIC)									AS Minutes_Orders
	,CAST(COALESCE(#NOTES.Numerator,0)			AS NUMERIC)									AS Minutes_Notes
	,CAST(COALESCE(#chart.Numerator,0)			AS NUMERIC)									AS Minutes_Chart_Review
	,1 AS event_count
FROM 
	#PROVIDERS
INNER JOIN #DATES ON #DATES.USER_ID = #PROVIDERS.USER_ID
LEFT OUTER JOIN #INBASKET ON #INBASKET.USER_ID = #PROVIDERS.USER_ID AND #INBASKET.CALENDAR_DT = #DATES.CALENDAR_DT
LEFT OUTER JOIN #NOTES ON #NOTES.USER_ID = #PROVIDERS.USER_ID AND #NOTES.CALENDAR_DT = #DATES.CALENDAR_DT
LEFT OUTER JOIN #ORD ON #ORD.USER_ID = #PROVIDERS.USER_ID AND #ORD.CALENDAR_DT = #DATES.CALENDAR_DT
LEFT OUTER JOIN #chart ON #chart.USER_ID = #PROVIDERS.USER_ID AND #chart.CALENDAR_DT = #DATES.CALENDAR_DT
LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Date dd ON dd.day_date = #DATES.CALENDAR_DT
LEFT OUTER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY EPIC_DEPARTMENT_ID ORDER BY hs_area_id DESC) seq FROM CLARITY_App.Rptg.vwRef_MDM_Location_Master) map ON map.EPIC_DEPARTMENT_ID = #PROVIDERS.DEPARTMENT_ID AND map.seq = 1
LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL_History MDM ON MDM.EPIC_DEPARTMENT_ID = #PROVIDERS.DEPARTMENT_ID 
LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_SERsrc ON vwDim_Clrt_SERsrc.PROV_ID = #PROVIDERS.PROV_ID 
LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_Physcn_Combined physsvc ON physsvc.sk_Dim_Physcn = vwDim_Clrt_SERsrc.sk_Dim_Physcn
LEFT OUTER JOIN Clarity.dbo.CLARITY_DEP ON CLARITY_DEP.DEPARTMENT_ID = #PROVIDERS.DEPARTMENT_ID
LEFT OUTER JOIN Clarity.dbo.CLARITY_LOC ON CLARITY_LOC.LOC_ID = CLARITY_DEP.REV_LOC_ID
LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv dvsn ON (CAST(dvsn.Epic_Financial_Division_Code AS INT) = TRY_CAST(vwDim_Clrt_SERsrc.Financial_Division AS INT) AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INT) = TRY_CAST(vwDim_Clrt_SERsrc.Financial_SubDivision AS INT))
LEFT OUTER JOIN (SELECT 
					* 
				FROM Clarity_App.rptg.vwref_mdm_location_master_history mdm_hist 
				WHERE 
				  mdm_hist.load_dtm =	(SELECT MAX(mdm_hist2.load_dtm) 
										FROM Clarity_App.rptg.vwRef_MDM_Location_Master_History mdm_hist2 
										WHERE mdm_hist2.load_dtm <= GETDATE() AND mdm_hist2.epic_department_id = mdm_hist.epic_department_id)) LOC_INFO ON LOC_INFO.EPIC_DEPARTMENT_ID = #PROVIDERS.DEPARTMENT_ID
ORDER BY
	#PROVIDERS.USER_ID, #DATES.CALENDAR_DT ASC

GO


