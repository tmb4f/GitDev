USE DS_HSDM_APP
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
      ,tile.Load_Dtm
      ,tile.w_som_department_id
      ,tile.w_som_department_name
      ,tile.w_som_division_id
      ,tile.w_som_division_name
      ,tile.w_hs_area_id
      ,tile.w_hs_area_name
      ,o.[organization_id]
      ,s.[service_id]
      ,s.[service_src]
      ,c.[clinical_area_id]
      ,c.[clinical_area_src]
      ,coalesce(o.[organization_name], 'No Organization Assigned') organization_name
      ,coalesce(s.[service_name], 'No Service Assigned') service_name
      ,coalesce(c.[clinical_area_name], 'No Clinical Area Assigned') clinical_area_name
      ,g.[ambulatory_flag]
      ,g.[childrens_flag]
      ,g.[mc_operation_flag]
      ,g.[inpatient_adult_flag]
      ,g.[community_health_flag] 
      ,g.[serviceline_division_flag]
	  ,tile.incoming_transfer -- xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer') THEN 1 ELSE 0 
	  ,tile.accepted -- WHEN xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer') AND xt.AdmissionCSN IS NOT NULL THEN 1 ELSE 0
      ,Reporting_Period_Enddate = (SELECT MAX(Reporting_Period_Enddate) FROM 
      etl.Data_Portal_Metrics_Master WHERE metric_id = 794)
FROM
    [DS_HSDM_App].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles] tile
    LEFT JOIN [DS_HSDM_App].[Mapping].Epic_Dept_Groupers g ON tile.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id
WHERE
    tile.event_type = 'External Transfer Request' and
    event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE metric_id = 794)
