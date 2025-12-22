USE DS_HSDM_APP

IF OBJECT_ID('tempdb..#csql ') IS NOT NULL
DROP TABLE #csql

SELECT 
       tile.event_count
      ,tile.event_date
	  ,tile.event_id
	  ,tile.event_category
      ,tile.epic_department_id
      ,tile.epic_department_name
	  ,tile.epic_department_name_external
      ,tile.fmonth_num
      ,tile.fyear_num
      ,tile.person_birth_date
      ,tile.person_gender
      ,tile.person_id
      ,tile.person_name
      ,tile.provider_id
      ,tile.provider_name
	  ,tile.prov_type
	  ,tile.peds_flag
      --provider-based mappings
	  ,tile.w_pod_id
      ,tile.w_pod_name
      ,tile.w_rev_location_id
      ,tile.w_rev_location
      ,tile.w_som_group_id
      ,tile.w_som_group_name
      ,tile.w_som_department_id
      ,tile.w_som_department_name
      ,tile.w_som_division_id
      ,tile.w_som_division_name
      ,tile.w_financial_division_id
      ,tile.w_financial_division_name
      ,tile.w_financial_sub_division_id
      ,tile.w_financial_sub_division_name
      ,tile.w_hs_area_id
      ,tile.w_hs_area_name

	  ,tile.discharge_destination
      ,tile.drg_code_name
      ,tile.LOS
      ,tile.sk_Dim_Clrt_Pt_Cls
      ,tile.patient_class
      ,tile.Ordr_Dtm
      ,tile.Ordr_Tm
      ,tile.Discharge_DtTm
      ,tile.Discharge_Tm
      ,tile.mins_from_dc_ord_to_dc_actual
      ,tile.dc_with_targeted_acuity_count
      ,tile.time_entered
      ,tile.acuity_score


      ,o.[organization_id]
      ,s.[service_id]
      ,s.[service_src]
      ,c.[clinical_area_id]
      ,c.[clinical_area_src]
      ,coalesce(o.[organization_name], 'No Organization Assigned') organization_name
      ,coalesce(s.[service_name], 'No Service Assigned') service_name
      ,coalesce(c.[clinical_area_name], 'No Clinical Area Assigned') clinical_area_name
      ,g.[community_health_flag]
      ,g.[community_health_mpg_flag]
      ,g.[ambulatory_flag]
      ,g.[upg_practice_flag]
      ,g.[childrens_flag]
      ,g.[serviceline_division_flag]
      ,g.[mc_operation_flag]
      ,g.[inpatient_adult_flag]
      ,tile.Load_Dtm
      ,ptdim.Sex
      ,ptdim.Gender_Identity_Name
      ,ptdim.Sexual_Orientation_Name_Current
      ,ptdim.Sex_Asgn_at_Birth_Name
      ,ptdim.FirstRace
      ,ptdim.Ethnicity
      ,ptdim.PreferredLanguage
      ,Reporting_Period_Enddate = (SELECT MAX(Reporting_Period_Enddate) FROM [DS_HSDM_App].etl.Data_Portal_Metrics_Master WHERE metric_id = 738)
INTO #csql
FROM
    [DS_HSDM_App].[TabRptg].Dash_PatientProgression_TimelyDischarge_Tiles tile
    LEFT JOIN [DS_HSDM_App].[TabRptgViz].Dim_Patient_Subset ptdim on tile.person_id = ptdim.MRN_int
    LEFT JOIN [DS_HSDM_App].[Mapping].Epic_Dept_Groupers g ON tile.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id
WHERE
    event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE [metric_id] = 738)
    AND
    fyear_num >= (SELECT MAX(fyear_num)-2 FROM DS_HSDW_Prod.dbo.Dim_Date 
                  WHERE day_date = (SELECT MAX([Reporting_Period_Enddate]) 
                  FROM [DS_HSDM_App].etl.Data_Portal_Metrics_Master WHERE [metric_id] = 738))

SELECT
	AVG(mins_from_dc_ord_to_dc_actual) AS Average_Minutes
FROM #csql
WHERE 1 = 1
AND event_date >= '6/1/2025' AND event_date <= '6/30/2025'
AND w_hs_area_id = 1