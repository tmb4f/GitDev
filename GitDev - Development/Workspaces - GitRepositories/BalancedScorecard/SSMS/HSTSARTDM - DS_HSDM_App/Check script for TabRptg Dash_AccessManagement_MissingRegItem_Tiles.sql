USE DS_HSDM_APP

IF OBJECT_ID('tempdb..#wq ') IS NOT NULL
DROP TABLE #wq

IF OBJECT_ID('tempdb..#datetable') IS NOT NULL
DROP TABLE #datetable

IF OBJECT_ID('tempdb..#util') IS NOT NULL
DROP TABLE #util

IF OBJECT_ID('tempdb..#utildatetable') IS NOT NULL
DROP TABLE #utildatetable

IF OBJECT_ID('tempdb..#util2') IS NOT NULL
DROP TABLE #util2

IF OBJECT_ID('tempdb..#utildatetable2') IS NOT NULL
DROP TABLE #utildatetable2

SELECT date_dim.day_date
      ,date_dim.fmonth_num
      ,date_dim.Fyear_num
      ,date_dim.FYear_name
INTO #datetable
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS date_dim
WHERE date_dim.day_date >= '4/7/2025'
AND date_dim.day_date < '4/10/2025'

  -- Create index for temp table #datetable

  CREATE UNIQUE CLUSTERED INDEX IX_datetable ON #datetable ([day_date])

SELECT [sk_Dash_AccessManagement_MissingRegItem_Tiles]
      ,[event_type]
      ,[event_count]
      ,[event_date]
      ,[event_id]
      ,[event_category]
      ,[epic_department_id]
      ,[epic_department_name]
      ,[epic_department_name_external]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[peds]
      ,[transplant]
      ,[oncology]
      ,[App_Flag]
      ,[sk_Dim_Pt]
      ,[sk_Fact_Pt_Acct]
      ,[sk_Fact_Pt_Enc_Clrt]
      ,[sk_dim_physcn]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[provider_id]
      ,[provider_name]
      ,[prov_typ]
      ,[hs_area_id]
      ,[hs_area_name]
      ,[pod_id]
      ,[pod_name]
      ,[rev_location_id]
      ,[rev_location]
      ,[som_group_id]
      ,[som_group_name]
      ,[som_department_id]
      ,[som_department_name]
      ,[som_division_id]
      ,[som_division_name]
      ,[financial_division_id]
      ,[financial_division_name]
      ,[financial_sub_division_id]
      ,[financial_sub_division_name]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,[w_pod_id]
      ,[w_pod_name]
      ,[w_rev_location_id]
      ,[w_rev_location]
      ,[w_som_group_id]
      ,[w_som_group_name]
      ,[w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
      ,[w_financial_division_id]
      ,[w_financial_division_name]
      ,[w_financial_sub_division_id]
      ,[w_financial_sub_division_name]
      ,[report_date]
      ,[report_period]
      ,[prov_service_line_id]
      ,[prov_service_line]
      ,[hub_id]
      ,[hub_name]
      ,[practice_group_id]
      ,[practice_group_name]
      ,[upg_practice_region_id]
      ,[upg_practice_region_name]
      ,[upg_practice_id]
      ,[upg_practice_name]
      ,[upg_practice_flag]
      ,[som_hs_area_id]
      ,[som_hs_area_name]
      ,[WORKQUEUE_ID]
      ,[WORKQUEUE_NAME]
      ,[DESCRIPTION]
      ,[ITEM_ID]
      ,[PAT_ENC_CSN_ID]
      ,[PAT_ID]
      ,[DEPARTMENT_ID]
      ,[ITEM_STATUS]
      ,[ENTRY_DATE]
      ,[EXIT_DATE]
      ,[Elapsed_Time]
      ,[ENTRY_USER]
      ,[ENTRY_NAME]
      ,[Entry_UVA_Computing_ID]
      ,[Entry_wd_Supervisory_Organization_ID]
      ,[Entry_wd_Supervisory_Organization_Description]
      ,[Entry_workday_supervisory_org_name]
      ,[Entry_workday_supervisory_org_id]
      ,[Entry_access_team_id]
      ,[Entry_access_team_name]
      ,[EXIT_USER]
      ,[EXIT_NAME]
      ,[Exit_UVA_Computing_ID]
      ,[Exit_wd_Supervisory_Organization_ID]
      ,[Exit_wd_Supervisory_Organization_Description]
      ,[Exit_workday_supervisory_org_name]
      ,[Exit_workday_supervisory_org_id]
      ,[Exit_access_team_id]
      ,[Exit_access_team_name]
      ,[USER_MATCH_YN]
      ,[RULE_ID]
      ,[RULE_NAME]
      ,[Load_Dtm]
  INTO #wq
  FROM [DS_HSDM_APP].[TabRptg].[Dash_AccessManagement_MissingRegItem_Tiles]
  WHERE event_date BETWEEN '4/7/2025' AND '4/9/2025'

  SELECT
    WORKQUEUE_NAME,
    ITEM_ID,
    ITEM_STATUS,
    event_date,
    ENTRY_NAME,
    ENTRY_DATE,
    Entry_wd_Supervisory_Organization_Description,
    Entry_access_team_name,
    EXIT_NAME,
    EXIT_DATE,
    Exit_wd_Supervisory_Organization_Description,
    Exit_access_team_name,
	sk_Dash_AccessManagement_MissingRegItem_Tiles,
    PAT_ENC_CSN_ID,
    PAT_ID,
    event_type,
    event_count,
    event_id,
    event_category,
    epic_department_id,
    epic_department_name,
    epic_department_name_external,
    fmonth_num,
    fyear_num,
    fyear_name,
    peds,
    transplant,
    oncology,
    App_Flag,
    sk_Dim_Pt,
    sk_Fact_Pt_Acct,
    sk_Fact_Pt_Enc_Clrt,
    sk_dim_physcn,
    person_birth_date,
    person_gender,
    person_id,
    person_name,
    provider_id,
    provider_name,
    prov_typ,
    hs_area_id,
    hs_area_name,
    pod_id,
    pod_name,
    rev_location_id,
    rev_location,
    som_group_id,
    som_group_name,
    som_department_id,
    som_department_name,
    som_division_id,
    som_division_name,
    financial_division_id,
    financial_division_name,
    financial_sub_division_id,
    financial_sub_division_name,
    w_hs_area_id,
    w_hs_area_name,
    w_pod_id,
    w_pod_name,
    w_rev_location_id,
    w_rev_location,
    w_som_group_id,
    w_som_group_name,
    w_som_department_id,
    w_som_department_name,
    w_som_division_id,
    w_som_division_name,
    w_financial_division_id,
    w_financial_division_name,
    w_financial_sub_division_id,
    w_financial_sub_division_name,
    report_date,
    report_period,
    prov_service_line_id,
    prov_service_line,
    hub_id,
    hub_name,
    practice_group_id,
    practice_group_name,
    upg_practice_region_id,
    upg_practice_region_name,
    upg_practice_id,
    upg_practice_name,
    upg_practice_flag,
    som_hs_area_id,
    som_hs_area_name,
    WORKQUEUE_ID,
    DESCRIPTION,
    DEPARTMENT_ID,
    Elapsed_Time,
    ENTRY_USER,
    Entry_UVA_Computing_ID,
    Entry_wd_Supervisory_Organization_ID,
    Entry_workday_supervisory_org_name,
    Entry_workday_supervisory_org_id,
    Entry_access_team_id,
    EXIT_USER,
    Exit_UVA_Computing_ID,
    Exit_wd_Supervisory_Organization_ID,
    Exit_workday_supervisory_org_name,
    Exit_workday_supervisory_org_id,
    Exit_access_team_id,
    USER_MATCH_YN,
    RULE_ID,
    RULE_NAME,
    Load_Dtm
  FROM #wq
 -- ORDER BY
	--WORKQUEUE_NAME,
	--event_date,
	--ITEM_ID
 -- ORDER BY
	--WORKQUEUE_NAME,
	--Entry_wd_Supervisory_Organization_Description,
	--ENTRY_NAME,
	--ENTRY_DATE
  ORDER BY
	WORKQUEUE_NAME,
	ITEM_ID,
	event_date
/*
SELECT DISTINCT
	WORKQUEUE_NAME,
	Entry_wd_Supervisory_Organization_Description,
	ENTRY_NAME
INTO #util
FROM #wq

  -- Create index for temp table #util

  CREATE NONCLUSTERED INDEX IX_util ON #util (WORKQUEUE_NAME, Entry_wd_Supervisory_Organization_Description, ENTRY_NAME)

SELECT
	   util.WORKQUEUE_NAME
	 , util.Entry_wd_Supervisory_Organization_Description
	 , util.ENTRY_NAME
     , dt.day_date
	 , dt.fmonth_num
	 , dt.Fyear_num
	 , dt.FYear_name
INTO #utildatetable
FROM #util util
CROSS JOIN #datetable dt

  -- Create index for temp table #utildatetable
  
  CREATE NONCLUSTERED INDEX IX_utildatetable ON #utildatetable ([day_date], WORKQUEUE_NAME, Entry_wd_Supervisory_Organization_Description, ENTRY_NAME)

SELECT DISTINCT
	WORKQUEUE_NAME,
	Exit_wd_Supervisory_Organization_Description,
	EXIT_NAME
INTO #util2
FROM #wq

  -- Create index for temp table #util

  CREATE NONCLUSTERED INDEX IX_util2 ON #util2 (WORKQUEUE_NAME, Exit_wd_Supervisory_Organization_Description, EXIT_NAME)

SELECT
	   util.WORKQUEUE_NAME
	 , util.Exit_wd_Supervisory_Organization_Description
	 , util.EXIT_NAME
     , dt.day_date
	 , dt.fmonth_num
	 , dt.Fyear_num
	 , dt.FYear_name
INTO #utildatetable2
FROM #util2 util
CROSS JOIN #datetable dt

  -- Create index for temp table #utildatetable
  
  CREATE NONCLUSTERED INDEX IX_utildatetable2 ON #utildatetable2 ([day_date], WORKQUEUE_NAME, Exit_wd_Supervisory_Organization_Description, EXIT_NAME)

 -- SELECT
	--WORKQUEUE_NAME,
 --   Entry_wd_Supervisory_Organization_Description,
 --   ENTRY_NAME,
 --   day_date,
 --   fmonth_num,
 --   Fyear_num,
 --   FYear_name
 -- FROM #utildatetable
 -- ORDER BY
	--WORKQUEUE_NAME,
 --   Entry_wd_Supervisory_Organization_Description,
 --   ENTRY_NAME,
 --   day_date

 -- SELECT
	--WORKQUEUE_NAME,
 --   Exit_wd_Supervisory_Organization_Description,
 --   EXIT_NAME,
 --   day_date,
 --   fmonth_num,
 --   Fyear_num,
 --   FYear_name
 -- FROM #utildatetable2
 -- ORDER BY
	--WORKQUEUE_NAME,
 --   Exit_wd_Supervisory_Organization_Description,
 --   EXIT_NAME,
 --   day_date

	SELECT
		date_dim.WORKQUEUE_NAME,
        date_dim.Entry_wd_Supervisory_Organization_Description,
		date_dim.day_date AS ENTRY_DATE,
        date_dim.ENTRY_NAME,
        COUNT(*) AS Entry_WorkItem_Count
	FROM
		#utildatetable AS date_dim
LEFT OUTER JOIN
	(
	SELECT
		wq.WORKQUEUE_NAME,
		wq. Entry_wd_Supervisory_Organization_Description,
		wq.ENTRY_NAME,
		CAST(wq.ENTRY_DATE AS DATE) AS ENTRY_DATE
	FROM #wq wq

) util
ON  ((date_dim.day_date = CAST(util.ENTRY_DATE AS SMALLDATETIME))
     AND ((date_dim.WORKQUEUE_NAME = util.WORKQUEUE_NAME)
	 AND (date_dim.Entry_wd_Supervisory_Organization_Description = util.Entry_wd_Supervisory_Organization_Description)
	 AND (date_dim.ENTRY_NAME = util.ENTRY_NAME)))
	GROUP BY
		date_dim.WORKQUEUE_NAME,
		date_dim. Entry_wd_Supervisory_Organization_Description,
		date_dim.ENTRY_NAME,
		date_dim.day_date
	ORDER BY
		date_dim.WORKQUEUE_NAME,
		date_dim. Entry_wd_Supervisory_Organization_Description,
		date_dim.ENTRY_NAME,
		date_dim.day_date
*/
GO
