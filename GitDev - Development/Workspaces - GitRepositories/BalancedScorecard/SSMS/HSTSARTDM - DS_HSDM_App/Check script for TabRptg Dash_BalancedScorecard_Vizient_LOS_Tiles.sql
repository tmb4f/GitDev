USE DS_HSDM_App

SELECT 
       tile.event_count
      ,tile.event_date
      ,tile.epic_department_id
      ,tile.epic_department_name
      ,tile.fmonth_num
      ,tile.fyear_num
      ,tile.fyear_name
      ,tile.peds
      ,tile.transplant
      ,tile.person_birth_date
      ,tile.person_gender
      ,tile.person_id
      ,tile.person_name
      ,tile.provider_id
      ,tile.provider_name
      ,tile.[LOS]
      ,tile.[Year]
      ,tile.[Quarter]
      ,tile.Load_Dtm
      ,tile.w_som_department_id
      ,tile.w_som_department_name
      ,tile.w_som_division_id
      ,tile.w_som_division_name
      ,tile.w_hs_area_id
      ,tile.w_hs_area_name
      ,tile.w_serviceline_division_flag
      ,tile.w_serviceline_division_id
      ,tile.w_serviceline_division_name
      --location-based mappings
      ,o.[organization_id] as w_organization_id
      ,o.[organization_name] as w_organization_name 
      ,s.[service_id] as w_service_id
      ,s.[service_name] as w_service_name
      ,c.[clinical_area_id] as w_clinical_area_id
      ,c.[clinical_area_name] as w_clinical_area_name
      ,g.[childrens_flag] as w_childrens_flag
      ,g.[childrens_id] as w_childrens_id
      ,g.[childrens_name] as w_childrens_name
      ,iif(tile.sk_Fact_Pt_Enc_Clrt = spine.sk_Fact_Pt_Enc_Clrt, 1, NULL) as w_ambulatory_flag
      ,iif(tile.sk_Fact_Pt_Enc_Clrt = spine.sk_Fact_Pt_Enc_Clrt, 7, NULL) as w_mc_ambulatory_id
      ,iif(tile.sk_Fact_Pt_Enc_Clrt = spine.sk_Fact_Pt_Enc_Clrt, 'Spine Program', NULL) as w_mc_ambluatory_name
      ,g.[mc_operation_flag] as w_mc_operation_flag
      ,g.[mc_operation_id] as w_mc_operation_id
      ,g.[mc_operation_name] as w_mc_operation_name
      ,g.[inpatient_adult_flag] as w_inpatient_adult_flag
      ,g.[inpatient_adult_id] as w_inpatient_adult_id
      ,g.[inpatient_adult_name] as w_inpatient_adult_name
      --,target_som_department  = (SELECT max(target)
      --                           FROM [DS_HSDM_App].[TabRptg].[Dash_Targets_Redcap_FY2022]
      --                           WHERE [metric_key] = 439 AND dept_level = 'clinical_chair' AND dept_id = tile.w_som_department_id)
      --,target_hs_area         = (SELECT max(target)
      --                           FROM [DS_HSDM_App].[TabRptg].[Dash_Targets_Redcap_FY2022]
      --                           WHERE [metric_key] = 439 AND dept_level = 'hs_area' AND dept_id = tile.w_hs_area_id)
      --,target_childrens       = (SELECT max(target)
      --                           FROM [DS_HSDM_App].[TabRptg].[Dash_Targets_Redcap_FY2022]
      --                           WHERE [metric_key] = 439 AND dept_level = 'childrens' AND dept_id = g.childrens_id)
      --,target_serviceline_div = (SELECT max(target)
      --                           FROM [DS_HSDM_App].[TabRptg].[Dash_Targets_Redcap_FY2022]
      --                           WHERE [metric_key] = 439 AND dept_level = 'service_line_division' AND dept_id = tile.w_serviceline_division_id)
      --,target_mc_operation    = (SELECT max(target)
      --                           FROM [DS_HSDM_App].[TabRptg].[Dash_Targets_Redcap_FY2022]
      --                           WHERE [metric_key] = 439 AND dept_level = 'mc_operation' AND dept_id = g.mc_operation_id)
      --,target_inpatient_adult = (SELECT max(target)
      --                           FROM [DS_HSDM_App].[TabRptg].[Dash_Targets_Redcap_FY2022]
      --                           WHERE [metric_key] = 439 AND dept_level = 'inpatient_adult' AND dept_id = g.inpatient_adult_id)
      --,target_epic_department = (SELECT max(target)
      --                           FROM [DS_HSDM_App].[TabRptg].[Dash_Targets_Redcap_FY2022]
      --                           WHERE [metric_key] = 439 AND dept_level = 'department' AND dept_id = g.epic_department_id)
      --,Reporting_Period_Enddate = (SELECT MAX(Reporting_Period_Enddate) FROM 
      --etl.Data_Portal_Metrics_Master WHERE sk_Data_Portal_Metrics_Master = 317)
FROM
    [DS_HSDM_App].[TabRptg].[Dash_BalancedScorecard_Vizient_LOS_Tiles] tile
    LEFT JOIN [DS_HSDM_App].[Rptg].[Spine_DRG_Pt_Enc_Hsp_Clrt] spine on tile.sk_Fact_Pt_Enc_Clrt = spine.sk_Fact_Pt_Enc_Clrt
    LEFT JOIN [DS_HSDM_App].[Mapping].Epic_Dept_Groupers g ON tile.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id
WHERE
    tile.event_type = 'LOS' -- and
    --event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
    --       FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
    --       WHERE [sk_Data_Portal_Metrics_Master] = 317)

ORDER BY tile.event_date