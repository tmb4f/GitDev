USE DS_HSDM_App

--SELECT [event_type]
--      ,[event_count]
--      ,[event_date]
--      ,[event_id]
--      ,[event_category]
--      ,[epic_department_id]
--      ,[epic_department_name]
--      ,[epic_department_name_external]
--      ,[fmonth_num]
--      ,[fyear_num]
--      ,[fyear_name]
--      ,[report_period]
--      ,[report_date]
--      ,[peds]
--      ,[transplant]
--      ,[sk_Dim_Pt]
--      ,[person_birth_date]
--      ,[person_gender]
--      ,[person_id]
--      ,[person_name]
--      ,[practice_group_id]
--      ,[practice_group_name]
--      ,[provider_id]
--      ,[provider_name]
--      ,[service_line_id]
--      ,[service_line]
--      ,[sub_service_line_id]
--      ,[sub_service_line]
--      ,[opnl_service_id]
--      ,[opnl_service_name]
--      ,[hs_area_id]
--      ,[hs_area_name]
--      ,[w_department_id]
--      ,[w_department_name]
--      ,[w_department_name_external]
--      ,[w_opnl_service_id]
--      ,[w_opnl_service_name]
--      ,[w_practice_group_id]
--      ,[w_practice_group_name]
--      ,[w_service_line_id]
--      ,[w_service_line_name]
--      ,[w_sub_service_line_id]
--      ,[w_sub_service_line_name]
--      ,[w_report_period]
--      ,[w_report_date]
--      ,[w_hs_area_id]
--      ,[w_hs_area_name]
--      ,[Load_Dtm]
--      ,[Reporting_Period_Enddate] = (SELECT MAX([Reporting_Period_Enddate]) FROM 
--       [DS_HSDM_App].etl.Data_Portal_Metrics_Master WHERE [sk_Data_Portal_Metrics_Master] = 8)

SELECT SUM([event_count]) AS event_count
      ,SUM(CASE WHEN ([event_category] = '10-Best possible' OR SUBSTRING([event_category],1,1) = '9') THEN 1 ELSE 0 END) AS top_box_event_count

  FROM 
      [DS_HSDM_App].[TabRptg].[Dash_BalancedScorecard_HCAHPS_Tiles] 
  
  WHERE
    [event_type] = 'Inpatient-HCAHPS'
	AND w_hs_area_id = 1
	AND event_date >= '7/1/2018'
	AND event_date < '7/1/2019'