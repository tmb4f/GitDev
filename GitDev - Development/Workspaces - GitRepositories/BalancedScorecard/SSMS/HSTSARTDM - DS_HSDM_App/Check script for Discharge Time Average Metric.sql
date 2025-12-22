USE DS_HSDM_APP

IF OBJECT_ID('tempdb..#csql ') IS NOT NULL
DROP TABLE #csql

SELECT 
       event_count
      ,event_date
      ,tile.epic_department_id
      ,tile.epic_department_name
      ,fmonth_num
      ,fyear_num
      ,fyear_name
      ,peds
      ,transplant
      ,person_birth_date
      ,person_gender
      ,person_id
      ,person_name
      ,provider_id
      ,provider_name
      ,PROV_TEAM
      ,[Discharge_Tm]
      ,[Discharge_DtTm]
	  ,DATEPART(HOUR,[Discharge_DtTm])*3600 AS Hour_Seconds
	  ,DATEPART(MINUTE,[Discharge_DtTm])*60 AS Minute_Seconds
	  ,(DATEPART(HOUR,[Discharge_DtTm])*3600+DATEPART(MINUTE,[Discharge_DtTm])*60) AS Time_Seconds
      ,hrs_from_dc_ord_to_dc_actual
      ,tile.Load_Dtm
      --provider-based mappings
      ,w_som_department_id
      ,w_som_department_name
      ,w_som_division_id
      ,w_som_division_name
      ,w_hs_area_id
      ,w_hs_area_name
      --location-based mappings
      ,o.[organization_id]
      ,s.[service_id]
      ,s.[service_src]
      ,c.[clinical_area_id]
      ,c.[clinical_area_src]
      ,coalesce(o.[organization_name], 'No Organization Assigned') organization_name
      ,coalesce(s.[service_name], 'No Service Assigned') service_name
      ,coalesce(c.[clinical_area_name], 'No Clinical Area Assigned') clinical_area_name
      ,g.[community_health_flag]
      ,g.[ambulatory_flag]
      ,g.[childrens_flag]
      ,g.[serviceline_division_flag]
      ,g.[mc_operation_flag]
      ,g.[inpatient_adult_flag]
      ,Reporting_Period_Enddate = (SELECT MAX(Reporting_Period_Enddate) FROM 
      etl.Data_Portal_Metrics_Master WHERE metric_id = 850)
INTO #csql
FROM
    [DS_HSDM_App].[TabRptg].Dash_PatientProgression_TimelyDischarge_Tiles tile
    LEFT JOIN [DS_HSDM_App].[Mapping].Epic_Dept_Groupers g ON tile.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id
WHERE
    event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE metric_id = 850)

SELECT
	AVG(Time_Seconds) AS Average_Time_Seconds,
	CONVERT(varchar, AVG(Time_Seconds) / 86400 ) + ':' + -- Days
    CONVERT(varchar, DATEADD(ms, (AVG(Time_Seconds) % 86400 ) * 1000, 0), 114)
as "Converted to D:HH:MM:SS.MS",
	CONVERT(varchar, AVG(Time_Seconds) / 86400 ) + ':' + -- Days
    CONVERT(varchar, DATEADD(ms, (AVG(Time_Seconds) % 86400 ) * 1000, 0), 108)
FROM #csql
WHERE 1 = 1
--AND event_date >= '11/1/2025' AND event_date <= '11/30/2025'
AND event_date >= '7/1/2025' AND event_date <= '11/30/2025'
AND w_hs_area_id = 1