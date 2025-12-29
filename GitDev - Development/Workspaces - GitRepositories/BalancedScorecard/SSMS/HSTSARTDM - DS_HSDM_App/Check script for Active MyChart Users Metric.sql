USE DS_HSDM_APP

IF OBJECT_ID('tempdb..#main ') IS NOT NULL
DROP TABLE #main

IF OBJECT_ID('tempdb..#completed ') IS NOT NULL
DROP TABLE #completed

;WITH mdm AS (
		SELECT DISTINCT
				   rmlmh.EPIC_DEPARTMENT_ID
				  ,rmlmh.SERVICE_LINE_ID
				  ,rmlmh.SERVICE_LINE
				  ,rmlmh.SUB_SERVICE_LINE_ID
				  ,rmlmh.SUB_SERVICE_LINE
				  ,rmlmh.OPNL_SERVICE_ID
				  ,rmlmh.OPNL_SERVICE_NAME
				  ,rmlmh.CORP_SERVICE_LINE_ID
				  ,rmlmh.CORP_SERVICE_LINE
				  ,rmlmh.FINANCE_COST_CODE
				  ,rmlmh.HS_AREA_ID
				  ,rmlmh.HS_AREA_NAME
				  ,rmlmh.POD_ID
				  ,rmlmh.PFA_POD
				  ,rmlmh.HUB_ID
				  ,rmlmh.HUB
				  ,rmlmh.LOC_ID
				  ,rmlmh.REV_LOC_NAME
			FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS rmlmh
				INNER JOIN
				( --hx--most recent batch date per dep id
					SELECT mdmhx.EPIC_DEPARTMENT_ID
						  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
					FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS mdmhx
					GROUP BY mdmhx.EPIC_DEPARTMENT_ID
				)                                                 AS hx
					ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
					   AND rmlmh.BATCH_RUN_DT = hx.max_dt
		)
, MDM_DEP AS (			
	SELECT DISTINCT		
		mdm.EPIC_DEPARTMENT_ID	
		,mdm.HOSPITAL_CODE	
		,mdm.DE_HOSPITAL_CODE	
		,mdm.HOSPITAL_GROUP
	FROM	DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group_All AS mdm	--UL004 CHANGED TO MDM ALL VERSION
	WHERE	mdm.EPIC_DEPARTMENT_ID IS NOT NULL	
)

SELECT
	   MRN_int
	  ,event_date
	  ,ROW_NUMBER() OVER(PARTITION BY MRN_int ORDER BY event_date) AS rn
	  ,CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END AS APPT_STATUS_COMPLETED
	  ,CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END AS MYCHART_ACTIVATED
	  ,g.ambulatory_flag
	  ,g.community_health_flag
	  ,MDM_DEP.HOSPITAL_CODE
  INTO #main
  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] tabrptg
  LEFT OUTER JOIN mdm ON tabrptg.epic_department_id = mdm.EPIC_DEPARTMENT_ID
  LEFT JOIN MDM_DEP AS MDM_DEP ON tabrptg.epic_department_id = MDM_DEP.EPIC_DEPARTMENT_ID
  LEFT OUTER JOIN [DS_HSDM_App].Mapping.Epic_Dept_Groupers g ON g.epic_department_id = tabrptg.epic_department_id
  WHERE 1 = 1
  --AND Telehealth_Flag = 0
  AND MRN_int IS NOT NULL
  --AND g.ambulatory_flag = 1
  --AND g.community_health_flag = 0
  AND event_date >= '7/1/2024' AND event_date <= '6/30/2025'

 -- SELECT
	--MRN_int,
 --   event_date,
 --   rn,
 --   APPT_STATUS_COMPLETED,
 --   MYCHART_ACTIVATED,
	--HOSPITAL_CODE
 -- FROM #main
 -- ORDER BY MRN_int
	--			  , event_date

SELECT
    MRN_int,
	MAX(APPT_STATUS_COMPLETED) AS APPT_STATUS_COMPLETED
INTO #completed
FROM #main
GROUP BY
	MRN_int
ORDER BY
	MRN_int

SELECT
	*
FROM #completed
WHERE APPT_STATUS_COMPLETED = 1
ORDER BY
	MRN_int

/*
SELECT
	   MRN_int
      ,[epic_department_id]
      ,[fyear_num]
      ,[fmonth_num]
	  ,MAX([event_date]) AS Latest_Event_Date
	  ,CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END AS APPT_STATUS_COMPLETED
	  ,CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END AS MYCHART_ACTIVATED
	  --,[sk_Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
   --   ,[event_type]
   --   ,[event_count]
   --   ,[event_id]
   --   ,[event_category]
   --   ,[epic_department_name]
   --   ,[epic_department_name_external]
   --   ,[fyear_name]
   --   ,[report_period]
   --   ,[report_date]
   --   ,[peds]
   --   ,[transplant]
   --   ,[sk_Dim_Pt]
   --   ,[sk_Fact_Pt_Acct]
   --   ,[sk_Fact_Pt_Enc_Clrt]
   --   ,[person_birth_date]
   --   ,[person_gender]
   --   ,[person_id]
   --   ,[person_name]
   --   ,[practice_group_id]
   --   ,[practice_group_name]
   --   ,[provider_id]
   --   ,[provider_name]
   --   ,[service_line_id]
   --   ,[service_line]
   --   ,[sub_service_line_id]
   --   ,[sub_service_line]
   --   ,[opnl_service_id]
   --   ,[opnl_service_name]
   --   ,[corp_service_line_id]
   --   ,[corp_service_line_name]
   --   ,[hs_area_id]
   --   ,[hs_area_name]
   --   ,[pod_id]
   --   ,[pod_name]
   --   ,[hub_id]
   --   ,[hub_name]
   --   ,[w_department_id]
   --   ,[w_department_name]
   --   ,[w_department_name_external]
   --   ,[w_practice_group_id]
   --   ,[w_practice_group_name]
   --   ,[w_service_line_id]
   --   ,[w_service_line_name]
   --   ,[w_sub_service_line_id]
   --   ,[w_sub_service_line_name]
   --   ,[w_opnl_service_id]
   --   ,[w_opnl_service_name]
   --   ,[w_corp_service_line_id]
   --   ,[w_corp_service_line_name]
   --   ,[w_report_period]
   --   ,[w_report_date]
   --   ,[w_hs_area_id]
   --   ,[w_hs_area_name]
   --   ,[w_pod_id]
   --   ,[w_pod_name]
   --   ,[w_hub_id]
   --   ,[w_hub_name]
   --   ,[prov_service_line_id]
   --   ,[prov_service_line_name]
   --   ,[prov_hs_area_id]
   --   ,[prov_hs_area_name]
   --   ,[APPT_STATUS_FLAG]
   --   ,[APPT_STATUS_C]
   --   ,[CANCEL_REASON_C]
   --   ,[MRN_int]
   --   ,[CONTACT_DATE]
   --   ,[APPT_DT]
   --   ,[PAT_ENC_CSN_ID]
   --   ,[PRC_ID]
   --   ,[PRC_NAME]
   --   ,[sk_Dim_Physcn]
   --   ,[UVaID]
   --   ,[VIS_NEW_TO_SYS_YN]
   --   ,[VIS_NEW_TO_DEP_YN]
   --   ,[VIS_NEW_TO_PROV_YN]
   --   ,[VIS_NEW_TO_SPEC_YN]
   --   ,[VIS_NEW_TO_SERV_AREA_YN]
   --   ,[VIS_NEW_TO_LOC_YN]
   --   ,[APPT_MADE_DATE]
   --   ,[ENTRY_DATE]
   --   ,[CHECKIN_DTTM]
   --   ,[CHECKOUT_DTTM]
   --   ,[VISIT_END_DTTM]
   --   ,[CYCLE_TIME_MINUTES]
   --   ,[appt_event_No_Show]
   --   ,[appt_event_Canceled_Late]
   --   ,[appt_event_Canceled]
   --   ,[appt_event_Scheduled]
   --   ,[appt_event_Provider_Canceled]
   --   ,[appt_event_Completed]
   --   ,[appt_event_Arrived]
   --   ,[appt_event_New_to_Specialty]
   --   ,[Appointment_Lag_Days]
   --   ,[CYCLE_TIME_MINUTES_Adjusted]
   --   ,[Load_Dtm]
   --   ,[DEPT_SPECIALTY_NAME]
   --   ,[PROV_SPECIALTY_NAME]
   --   ,[APPT_DTTM]
   --   ,[ENC_TYPE_C]
   --   ,[ENC_TYPE_TITLE]
   --   ,[APPT_CONF_STAT_NAME]
   --   ,[ZIP]
   --   ,[APPT_CONF_DTTM]
   --   ,[SIGNIN_DTTM]
   --   ,[ARVL_LIST_REMOVE_DTTM]
   --   ,[ROOMED_DTTM]
   --   ,[NURSE_LEAVE_DTTM]
   --   ,[PHYS_ENTER_DTTM]
   --   ,[CANCEL_REASON_NAME]
   --   ,[financial_division]
   --   ,[financial_subdivision]
   --   ,[CANCEL_INITIATOR]
   --   ,[F2_Flag]
   --   ,[TIME_TO_ROOM_MINUTES]
   --   ,[TIME_IN_ROOM_MINUTES]
   --   ,[BEGIN_CHECKIN_DTTM]
   --   ,[PAGED_DTTM]
   --   ,[FIRST_ROOM_ASSIGN_DTTM]
   --   ,[CANCEL_LEAD_HOURS]
   --   ,[APPT_CANC_DTTM]
   --   ,[Entry_UVaID]
   --   ,[Canc_UVaID]
   --   ,[PHONE_REM_STAT_NAME]
   --   ,[CHANGE_DATE]
   --   ,[Cancel_Lead_Days]
   --   ,[financial_division_id]
   --   ,[financial_division_name]
   --   ,[financial_sub_division_id]
   --   ,[financial_sub_division_name]
   --   ,[rev_location_id]
   --   ,[rev_location]
   --   ,[som_group_id]
   --   ,[som_group_name]
   --   ,[som_department_id]
   --   ,[som_department_name]
   --   ,[som_division_id]
   --   ,[w_financial_division_id]
   --   ,[w_financial_division_name]
   --   ,[w_financial_sub_division_id]
   --   ,[w_financial_sub_division_name]
   --   ,[w_rev_location_id]
   --   ,[w_rev_location]
   --   ,[w_som_group_id]
   --   ,[w_som_group_name]
   --   ,[w_som_department_id]
   --   ,[w_som_department_name]
   --   ,[w_som_division_id]
   --   ,[som_division_name]
   --   ,[w_som_division_name]
   --   ,[APPT_MADE_DTTM]
   --   ,[BUSINESS_UNIT]
   --   ,[Prov_Typ]
   --   ,[Staff_Resource]
   --   ,[som_division_5]
   --   ,[w_som_hs_area_id]
   --   ,[w_som_hs_area_name]
   --   ,[APPT_SERIAL_NUM]
   --   ,[RESCHED_APPT_CSN_ID]
   --   ,[Appointment_Request_Date]
   --   ,[Appointment_Lag_Business_Days]
   --   ,[BILL_PROV_YN]
   --   ,[w_upg_practice_flag]
   --   ,[w_upg_practice_region_id]
   --   ,[w_upg_practice_region_name]
   --   ,[w_upg_practice_id]
   --   ,[w_upg_practice_name]
   --   ,[Lip_Flag]
   --   ,[FINANCE_COST_CODE]
   --   ,[Prov_Based_Clinic]
   --   ,[Map_Type]
   --   ,[w_serviceline_division_flag]
   --   ,[w_serviceline_division_id]
   --   ,[w_serviceline_division_name]
   --   ,[w_mc_operation_flag]
   --   ,[w_mc_operation_id]
   --   ,[w_mc_operation_name]
   --   ,[w_post_acute_flag]
   --   ,[w_ambulatory_operation_flag]
   --   ,[w_ambulatory_operation_id]
   --   ,[w_ambulatory_operation_name]
   --   ,[w_inpatient_adult_flag]
   --   ,[w_inpatient_adult_id]
   --   ,[w_inpatient_adult_name]
   --   ,[w_childrens_flag]
   --   ,[w_childrens_id]
   --   ,[w_childrens_name]
   --   ,[SUBLOC_ID]
   --   ,[SUBLOC_NAME]
   --   ,[MYCHART_STATUS_C]
   --   ,[MYCHART_STATUS_NAME]
   --   ,[PAT_SCHED_MYC_STAT_C]
   --   ,[PAT_SCHED_MYC_STAT_NAME]
   --   ,[Telehealth_Flag]
  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE Telehealth_Flag = 0
  GROUP BY fyear_num
                  , fmonth_num
				  , epic_department_id
				  , MRN_int
	              , CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END
				  , CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END
  --ORDER BY fyear_num
  --                , fmonth_num
		--		  , epic_department_id
		--		  , MRN_int
	 --             , CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END
		--		  , CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END
  ORDER BY MRN_int
				  , epic_department_id
                  , fyear_num
                  , fmonth_num
				  , MAX(event_date)
	              , CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END
				  , CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END

				  
SELECT
	   MRN_int
      ,[epic_department_id]
      ,[fyear_num]
      ,[fmonth_num]
	  ,MAX([event_date]) AS Latest_Event_Date
	  ,CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END AS APPT_STATUS_COMPLETED
	  ,CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END AS MYCHART_ACTIVATED
  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
  WHERE Telehealth_Flag = 0
  AND event_date >= '7/1/2024' AND event_date <= '6/30/2025'
  GROUP BY fyear_num
                  , fmonth_num
				  , epic_department_id
				  , MRN_int
	              , CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END
				  , CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END
  ORDER BY MRN_int
				  , epic_department_id
                  , fyear_num
                  , fmonth_num
				  , MAX(event_date)
	              , CASE WHEN APPT_STATUS_FLAG IN ('Completed','Arrived') THEN 1 ELSE 0 END
				  , CASE WHEN PAT_SCHED_MYC_STAT_C IS NOT NULL AND PAT_SCHED_MYC_STAT_C = 1 THEN 1 ELSE 0 END
*/
GO