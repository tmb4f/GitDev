USE DS_HSDM_APP

IF OBJECT_ID('tempdb..#appt ') IS NOT NULL
DROP TABLE #appt

SELECT
	  a.APPT_DTTM									AS 'AppointmentDate'
	, a.APPT_MADE_DATE
	, a.APPT_ENTRY_USER_ID
	, a.APPT_ENTRY_USER_NAME_WID
 	, a.DEPARTMENT_NAME								AS 'DepartmentName'
	, a.PROV_NAME_WID								AS 'Provider'
	, COALESCE(PRC_NAME,'')							AS 'VisitType'
	, a.LOC_NAME									AS 'Location'
	, FORMAT(CONVERT(DATETIME, a.APPT_DTTM),'tt')	AS 'AM/PM'	
	, COALESCE(t.Event_Count,0)						AS 'TelemedicineFlag'
	, a.CANCEL_REASON_NAME	
	, a.CANCEL_LEAD_HOURS
	, a.APPT_CANC_DATE
	, CASE WHEN a.APPT_STATUS_C = 3 -- Canceled
           AND a.CANCEL_INITIATOR = 'PATIENT'
           AND a.CANCEL_LEAD_HOURS < 24 THEN 'CANCELED, LATE'
           WHEN a.APPT_STATUS_C = 3 THEN 'CANCELED, NOT LATE'
      ELSE NULL END									AS 'LATE_CANCEL_YN'
	, a.SAME_DAY_CANC_YN
	, a.IDENTITY_ID									AS 'MRN'
	, a.PAT_ENC_CSN_ID								AS 'CSN'
	, a.APPT_STATUS_NAME
	, g.ambulatory_flag								
	, g.upg_practice_flag							
	, g.childrens_flag								
	, g.serviceline_division_flag					
	, g.mc_operation_flag							
	, g.inpatient_adult_flag	
	, c.clinical_area_id
	, c.clinical_area_name
	, s.service_id
	, s.service_name
	, o.organization_id
	, o.organization_name
INTO #appt

FROM
	[DS_HSDM_App].[Stage].[Scheduled_Appointment] a
	LEFT OUTER JOIN [DS_HSDM_App].[Stage].[Telemedicine_Encounters] t	ON	a.PAT_ENC_CSN_ID = t.Encounter_CSN
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat				ON pat.sk_Dim_Pt = a.sk_Dim_Pt
	LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g			ON a.department_id = g.epic_department_id
	LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c			on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
	LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s					on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
	LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o			on s.organization_id = o.organization_id

WHERE 1 = 1
AND a.APPT_ENTRY_USER_ID IN
('76280' -- Amanda Barnes
,'90358' -- Anita Baughan
,'91974' -- Amy Cameron
,'74402' -- Natalie Fox
,'92341' -- Catherine Hall
,'82075' -- Patricia Lidd
)
AND 	a.ENC_TYPE_C NOT IN ('2505','2506')
	AND pat.IS_VALID_PAT_YN = 'Y'
	AND a.APPT_MADE_DATE >= '2/1/2024'

			SELECT *
			FROM #appt
  --ORDER BY Provider, APPT_MADE_DATE
  ORDER BY APPT_ENTRY_USER_NAME_WID, APPT_MADE_DATE

			SELECT
				APPT_ENTRY_USER_NAME_WID
			   ,VisitType
			   ,DepartmentName
			   ,COUNT(*) AS ScheduledCount
			FROM #appt
			GROUP BY
				APPT_ENTRY_USER_NAME_WID
			   ,VisitType
			   ,DepartmentName
  --ORDER BY Provider, APPT_MADE_DATE
  --ORDER BY APPT_ENTRY_USER_NAME_WID, APPT_MADE_DATE
  ORDER BY APPT_ENTRY_USER_NAME_WID, VisitType, DepartmentName
  