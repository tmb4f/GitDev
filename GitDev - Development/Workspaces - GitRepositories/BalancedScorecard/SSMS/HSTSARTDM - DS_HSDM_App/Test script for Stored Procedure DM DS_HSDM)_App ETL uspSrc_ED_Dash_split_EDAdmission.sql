USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/********************************************************************
MODS: 06272022-DM2NB-Switch to use Patient_admitted for event_date 
			   rather than discharge_disposition_dt which can change 
			   as edits are made post-visit
*********************************************************************/

--ALTER PROC [ETL].[uspSrc_ED_Dash_split_EDAdmission]
--AS

SET NOCOUNT ON

SELECT sk_ED_Details
      ,person_id
      ,person_name
      ,person_birth_date
      ,person_gender
      ,sk_Dim_Pt

	  ,Patient_Admitted AS event_date
      --,Discharge_Disposition_Dt AS event_date
      ,epic_department_id
      ,epic_department_name
      ,epic_department_name_external
      ,service_line_id
      ,service_line
      ,sub_service_line_id
      ,sub_service_line
      ,opnl_service_id
      ,opnl_service_name
      ,practice_group_id
      ,practice_group_name
      ,hs_area_id
      ,hs_area_name
      ,ed_care_area
      ,Acuity_Level
      ,REASON_VISIT_NAME
      ,Clinical_Impression
      ,Means_of_Arrival
      ,longest_provider
      ,admitting_provider
      ,admitting_specialty
      ,admitting_service
      ,admitting_unit

      ,Discharge_Disposition_Dt
      ,Bed_Request_Dt
      ,Preadmit_Order_Dt
      ,Inpatient_Order_Dt

	  ,Patient_Admitted

      ,Departure_Dt
      ,Boarding_in_Hours
      ,LOS_in_Hours
      ,CARE_AREA_NAME

      ,Psych_Patient
      ,event_category
      ,event_count
      ,CAST('Admission' AS VARCHAR(50)) AS event_type
      ,Trauma_Level

      ,Load_Dtm
  FROM TabRptg.Dash_DailyHuddle_ED_Details
  WHERE Disposition = 'Admitted'

  --ORDER BY event_date
  ORDER BY person_name, event_date

  GO


