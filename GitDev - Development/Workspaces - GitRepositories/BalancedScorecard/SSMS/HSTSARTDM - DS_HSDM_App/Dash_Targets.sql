USE DS_HSDM_APP

SELECT 
      [metric_id]
	  ,metric_name
      ,[portal_level]
      ,[portal_level_id]
      ,[portal_level_name]
      ,[comparator]
      ,[target_]
      ,[threshold]
      ,[lower_bound]
      ,[upper_bound]
      ,[stretch]
      ,[number_to_improve]
      ,[precision_]
      ,[format]
      ,[suffix]
      ,[NA]
      ,[TBD]
      ,[fyear]
      ,[current_]
      ,[Load_Dtm]
      ,Reporting_Period_Enddate = (SELECT MAX(Reporting_Period_Enddate) FROM 
      etl.Data_Portal_Metrics_Master WHERE metric_id = 20)
FROM [DS_HSDM_App].[TabRptg].[Dash_Targets]
WHERE metric_id = 20
AND fyear = 2025
--AND portal_level IN ('hs_area','organization','service name')
ORDER BY portal_level