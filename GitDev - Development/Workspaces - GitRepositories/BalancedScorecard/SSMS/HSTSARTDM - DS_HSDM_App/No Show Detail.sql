/****** Script for SelectTopNRows command from SSMS  ******/
/*vwDim_PS_Location
DS_HSDM_VMOT Workday_FM_Buildings
[DS_HSDM_Prod].[CMS].[Dim_Facility]*/
SELECT
       ns.[epic_department_id] AS DEPARTMENT_ID
      ,[PAT_ENC_CSN_ID]
       --[sk_Dash_AmbOpt_ApptNoShowMetric_Tiles]
      ,[event_type]
      --,[event_count]
      ,[event_date]
      --,[event_id]
      --,[event_category]
      ,ns.[epic_department_name]
      ,[epic_department_name_external]
	  ,dep.Clrt_DEPt_Addr_Cty
	  ,dep.Clrt_DEPt_Addr_Zip
	  ,dep.Clrt_DEPt_Addr_St
	  --,mdm.FINANCE_COST_CODE
	  --,mdm.PEOPLESOFT_NAME
      --,[fmonth_num]
      --,[fyear_num]
      --,[fyear_name]
      --,[report_period]
      --,[report_date]
      --,[peds]
      --,[transplant]
      --,[oncology]
      --,ns.[sk_Dim_Pt]
      --,[sk_Fact_Pt_Acct]
      --,[sk_Fact_Pt_Enc_Clrt]
      ,[person_birth_date]
	  ,FLOOR((CAST(ns.APPT_DT AS INTEGER)
                                        - CAST(ns.person_birth_date AS INTEGER)
                                       ) / 365.25
                                      ) AS person_age
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      --,[practice_group_id]
      --,[practice_group_name]
      --,[provider_id]
      --,[provider_name]
      --,[sk_dim_physcn]
      --,[hs_area_id]
      --,[hs_area_name]
      --,[financial_division_id]
      --,[financial_division_name]
      --,[financial_sub_division_id]
      --,[financial_sub_division_name]
      --,[rev_location_id]
      --,[rev_location]
      --,[som_group_id]
      --,[som_group_name]
      --,[som_department_id]
      --,[som_department_name]
      --,[som_division_id]
      --,[som_division_name]
      --,[service_line_id]
      --,[service_line]
      --,[sub_service_line_id]
      --,[sub_service_line]
      --,[opnl_service_id]
      --,[opnl_service_name]
      --,[corp_service_line_id]
      --,[corp_service_line_name]
      --,[w_service_line_id]
      --,[w_service_line_name]
      --,[w_sub_service_line_id]
      --,[w_sub_service_line_name]
      --,[w_opnl_service_id]
      --,[w_opnl_service_name]
      --,[w_corp_service_line_id]
      --,[w_corp_service_line_name]
      --,[w_department_id]
      --,[w_department_name]
      --,[w_department_name_external]
      --,[w_practice_group_id]
      --,[w_practice_group_name]
      --,[w_report_period]
      --,[w_report_date]
      --,[w_hs_area_id]
      --,[w_hs_area_name]
      --,[w_financial_division_id]
      --,[w_financial_division_name]
      --,[w_financial_sub_division_id]
      --,[w_financial_sub_division_name]
      --,[w_rev_location_id]
      --,[w_rev_location]
      --,[w_som_group_id]
      --,[w_som_group_name]
      --,[w_som_department_id]
      --,[w_som_department_name]
      --,[w_som_division_id]
      --,[w_som_division_name]
      --,[pod_id]
      --,[pod_name]
      --,[hub_id]
      --,[hub_name]
      --,[w_pod_id]
      --,[w_pod_name]
      --,[w_hub_id]
      --,[w_hub_name]
      --,[w_som_hs_area_id]
      --,[w_som_hs_area_name]
      --,[w_upg_practice_flag]
      --,[w_upg_practice_region_id]
      --,[w_upg_practice_region_name]
      --,[w_upg_practice_id]
      --,[w_upg_practice_name]
      --,[w_serviceline_division_flag]
      --,[w_serviceline_division_id]
      --,[w_serviceline_division_name]
      --,[w_mc_operation_flag]
      --,[w_mc_operation_id]
      --,[w_mc_operation_name]
      --,[w_post_acute_flag]
      --,[w_ambulatory_operation_flag]
      --,[w_ambulatory_operation_id]
      --,[w_ambulatory_operation_name]
      --,[w_inpatient_adult_flag]
      --,[w_inpatient_adult_id]
      --,[w_inpatient_adult_name]
      --,[w_childrens_flag]
      --,[w_childrens_id]
      --,[w_childrens_name]
      --,[prov_service_line_id]
      --,[prov_service_line]
      --,[prov_hs_area_id]
      --,[prov_hs_area_name]
      ,[APPT_STATUS_FLAG]
      ,[CANCEL_REASON_C]
      ,[APPT_DT]
      --,[PRC_ID]
      ,[PRC_NAME]
      --,[UVaID]
      --,[VIS_NEW_TO_SYS_YN]
      --,[VIS_NEW_TO_DEP_YN]
      --,[VIS_NEW_TO_PROV_YN]
      ,[VIS_NEW_TO_SPEC_YN]
      --,[VIS_NEW_TO_SERV_AREA_YN]
      --,[VIS_NEW_TO_LOC_YN]
      --,[APPT_MADE_DATE]
      --,[ENTRY_DATE]
      ,[appt_event_No_Show]
      ,[appt_event_Canceled_Late]
      --,[appt_event_Canceled]
      --,[appt_event_Scheduled]
      --,[appt_event_Provider_Canceled]
      --,[appt_event_Completed]
      --,[appt_event_Arrived]
      ,[appt_event_New_to_Specialty]
      --,[DEPT_SPECIALTY_NAME]
      --,[PROV_SPECIALTY_NAME]
      ,[APPT_DTTM]
      ,[CANCEL_REASON_NAME]
      --,[financial_division]
      --,[financial_subdivision]
      ,[CANCEL_INITIATOR]
      ,[CANCEL_LEAD_HOURS]
      --,[APPT_CANC_DTTM]
      --,[Entry_UVaID]
      --,[Canc_UVaID]
      --,[PHONE_REM_STAT_NAME]
      --,[Cancel_Lead_Days]
      --,[APPT_MADE_DTTM]
      --,[Prov_Typ]
      --,[Staff_Resource]
      --,[APPT_SERIAL_NUM]
      --,[Appointment_Request_Date]
      --,[BILL_PROV_YN]
      ,[NoShow]
      ,[PatientCanceledLate]
      --,[Appointment]
      --,[F2F_Flag]
      --,[ENC_TYPE_C]
      ,[ENC_TYPE_TITLE],
      --,[Lip_Flag]
      --,[FINANCE_COST_CODE]
      --,[Prov_Based_Clinic]
      --,[Map_Type]
      --,[SUBLOC_ID]
      --,[SUBLOC_NAME]
      --,[Load_Dtm]
      --,[TELEHEALTH_MODE_NAME]
      --,[app_flag]
    pt.MRN_int,
    pt.MRN_display,
    --pt.PAT_ID,
    --Pt_Rec_Merged_Out,
    --Epic_Patient,
    Patient_Status,
    --Registration_Type,
    --Registration_Status,
    --LAST_ACCESS_DATE,
    --sk_PCP_SERsrc,
    --sk_PCP_Physcn,
    --Encounters,
    --BirthDate,
    --Name,
    pt.FirstName,
    pt.MiddleName,
    LastName,
    Deceased,
    DeathDate,
    pt.Sex,
	--gtab.Value AS gtab_Value,
	--gtab.Description AS gtab_Descritption,
    pt.MaritalStatus,
	--mstab.Value AS mstab_Value,
	--mstab.Description AS mstab_Description,
    Religion,
    pt.FirstRace,
    pt.MultiRacial,
    pt.Ethnicity,
	--rtab.Value AS rtab_Value,
	--rtab.Description AS rtab_Description,
    Ssn,
    pt.Address_Line1 AS pt_Address_Line1,
	geo.Address_Line1 AS geo_Address_Line1,
    Addres_Line2,
    pt.City AS pt_City,
	geo.City AS geo_City,
    pt.County AS pt_County,
	geo.County AS geo_County,
    pt.StateOrProvince,
	--stab.Value AS stab_Value,
    pt.Country AS pt_Country,
    geo.Country AS geo_Country,
    pt.PostalCode AS pt_PostalCode,
    geo.PostalCode AS geo_PostalCode,
	geo.lat,
	geo.long,
	geo.fips,
	geo.fips_lat,
	geo.fips_long--,
 --   HomePhoneNumber,
 --   WorkPhoneNumber,
	--pt.Email_Address,
 --   EnterpriseId,
 --   SmokingStatus,
 --   PrimaryFinancialClass,
 --   HighestLevelOfEducation,
 --   Restricted,
 --   Test,
 --   PreferredLanguage,
    --pt.PREV_MRN_1,
    --pt.PREV_MRN_2,
    --pt.PREV_MRN_3,
    --Preferred_Name,
    --Gender_Identity_Name,
    --Sexual_Orientation_Name_Current,
    --Sex_Asgn_at_Birth_Name,
    --IS_VALID_PAT_YN,
    --CompID
  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ApptNoShowMetric_Tiles] ns
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient pt
	ON pt.sk_Dim_Pt = ns.sk_Dim_Pt
  LEFT OUTER JOIN [DS_HSDW_Prod].[dbo].[Dim_Patient_Address_Geocode] geo
	ON geo.sk_Dim_Pt = ns.sk_Dim_Pt
  LEFT OUTER JOIN [DS_HSDW_Prod].[dbo].[Dim_Clrt_DEPt] dep
	ON dep.DEPARTMENT_ID = ns.epic_department_id
  LEFT OUTER JOIN
  (
  SELECT DISTINCT
	EPIC_DEPARTMENT_ID,
	FINANCE_COST_CODE,
	PEOPLESOFT_NAME
  FROM [DS_HSDW_Prod].[Rptg].[vwRef_MDM_Location_Master]
  ) mdm
	ON mdm.EPIC_DEPARTMENT_ID = ns.epic_department_id

LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = ns.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id

  WHERE ns.event_category = 'Detail'
  AND ns.event_date BETWEEN '1/1/2022 00:00' AND '2/28/2023 23:59'
  AND g.ambulatory_flag = 1
  ORDER BY ns.event_date