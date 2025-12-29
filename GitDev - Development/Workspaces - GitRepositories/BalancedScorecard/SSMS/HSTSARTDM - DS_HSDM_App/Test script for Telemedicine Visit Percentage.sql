USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
DECLARE @enddate SMALLDATETIME

--SET @startdate = '7/1/2020 00:00 AM'
--SET @enddate = '8/31/2020 11:59 PM'
--SET @startdate = '7/1/2021 00:00 AM'
SET @startdate = '3/1/2022 00:00 AM'
SET @enddate = '3/31/2022 11:59 PM'

    SET NOCOUNT ON; 

---------------------------------------------------
----get default Balanced Scorecard date range
IF  @startdate IS NULL
AND @enddate IS NULL
BEGIN 
    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT
                                                 ,@enddate OUTPUT;

    ---BDD 01/10/2019 for this proc, take it back another 6 months to the begin of the FY
	---  special (hopefully short term) reporting request
    SET @startdate = DATEADD(mm,-6,@startdate)

END 

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate
----------------------------------------------------

IF OBJECT_ID('tempdb..#Data_Source ') IS NOT NULL
DROP TABLE #Data_Source

--IF OBJECT_ID('tempdb..#F2F ') IS NOT NULL
--DROP TABLE #F2F

--IF OBJECT_ID('tempdb..#Telemed ') IS NOT NULL
--DROP TABLE #Telemed

SELECT	
        CASE WHEN event_category = 'Telehealth Encounter' THEN 1 ELSE 0 END AS x_is_telemed
	  , CASE WHEN event_category = 'All Encounters' AND Telehealth_Flag = 1 THEN event_count ELSE 0 END AS x_denominator
	  , Completed.*

INTO #Data_Source

FROM
(
SELECT
      'All Encounters' AS [event_category]
      ,[event_type]
      ,t.[epic_department_id]
      ,t.[epic_department_name]
      ,t.[Load_Dtm]
      ,[event_count]
      ,[event_date]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[provider_id]
      ,[provider_name]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,[w_pod_id]
      ,[w_pod_name]
      ,[w_hub_id]
      ,[w_hub_name]
      ,[w_som_group_id]
      ,[w_som_group_name]
      ,[w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
      ,[w_rev_location_id]
      ,[w_rev_location]
      ,[PAT_ENC_CSN_ID]
      ,[PRC_NAME] 
      ,[ENC_TYPE_TITLE]
      ,[APPT_STATUS_FLAG]
      ,NULL AS Communication_Type
      ,NULL AS [Smartphrase_Name]
      ,NULL AS [Smartdata_Element]
      ,NULL AS [Telehealth_Mode_Name]
      ,[Telehealth_Flag]
      ,o.[organization_id] AS w_organization_id
      ,o.[organization_name] AS w_organization_name 
      ,s.[service_id] AS w_service_id
      ,s.[service_name] AS w_service_name
      ,c.[clinical_area_id] AS w_clinical_area_id
      ,c.[clinical_area_name] AS w_clinical_area_name
      ,g.[ambulatory_flag] AS w_ambulatory_flag
      ,g.[upg_practice_flag] AS w_upg_practice_flag
      ,g.[childrens_flag] AS w_childrens_flag
      ,g.[serviceline_division_flag] AS w_serviceline_division_flag
      ,g.[mc_operation_flag] AS w_mc_operation_flag
      ,g.[inpatient_adult_flag] AS w_inpatient_adult_flag
      ,g.[childrens_ambulatory_id] AS w_childrens_ambulatory_id
      ,g.[childrens_ambulatory_name] AS w_childrens_ambulatory_name
      ,g.[mc_ambulatory_id] AS w_mc_ambulatory_id
      ,g.[mc_ambulatory_name] AS w_mc_ambulatory_name
      ,g.[ambulatory_operation_id] AS w_ambulatory_operation_id
      ,g.[ambulatory_operation_name] AS w_ambulatory_operation_name
      ,g.[childrens_id] AS w_childrens_id
      ,g.[childrens_name] AS w_childrens_name
      ,g.[serviceline_division_id] AS w_serviceline_division_id
      ,g.[serviceline_division_name] AS w_serviceline_division_name
      ,g.[mc_operation_id] AS w_mc_operation_id
      ,g.[mc_operation_name] AS w_mc_operation_name
      ,g.[inpatient_adult_id] AS w_inpatient_adult_id
      ,g.[inpatient_adult_name] AS w_inpatient_adult_name
      ,g.[upg_practice_region_id] AS w_upg_practice_region_id
      ,g.[upg_practice_region_name] AS w_upg_practice_region_name
      ,g.[upg_practice_id] AS w_upg_practice_id
      ,g.[upg_practice_name] AS w_upg_practice_name
      ,[Reporting_Period_Enddate] = 
          (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE [sk_Data_Portal_Metrics_Master] = 338)
FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] t
    LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON t.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id
WHERE 
    (appt_event_Completed = 1 OR appt_event_Arrived = 1)
    AND event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE [sk_Data_Portal_Metrics_Master] = 338)

UNION

SELECT
      [event_category]
      ,[event_type]
      ,t.[epic_department_id]
      ,t.[epic_department_name]
      ,t.[Load_Dtm]
      ,[event_count]
      ,[event_date]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[provider_id]
      ,[provider_name]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,[w_pod_id]
      ,[w_pod_name]
      ,[w_hub_id]
      ,[w_hub_name]
      ,[w_som_group_id]
      ,[w_som_group_name]
      ,[w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
      ,[w_rev_location_id]
      ,[w_rev_location]
      ,[Encounter_CSN] AS PAT_ENC_CSN_ID
      ,[Visit_Type] AS PRC_NAME
      ,[Encounter_Type] AS ENC_TYPE_TITLE
      ,[Encounter_Status] AS APPT_STATUS_FLAG
      , Communication_Type
      ,[Smartphrase_Name]
      ,[Smartdata_Element]
      ,[Telehealth_Mode_Name]
      ,1 AS [Telehealth_Flag]
      ,o.[organization_id] AS w_organization_id
      ,o.[organization_name] AS w_organization_name 
      ,s.[service_id] AS w_service_id
      ,s.[service_name] AS w_service_name
      ,c.[clinical_area_id] AS w_clinical_area_id
      ,c.[clinical_area_name] AS w_clinical_area_name
      ,g.[ambulatory_flag] AS w_ambulatory_flag
      ,g.[upg_practice_flag] AS w_upg_practice_flag
      ,g.[childrens_flag] AS w_childrens_flag
      ,g.[serviceline_division_flag] AS w_serviceline_division_flag
      ,g.[mc_operation_flag] AS w_mc_operation_flag
      ,g.[inpatient_adult_flag] AS w_inpatient_adult_flag
      ,g.[childrens_ambulatory_id] AS w_childrens_ambulatory_id
      ,g.[childrens_ambulatory_name] AS w_childrens_ambulatory_name
      ,g.[mc_ambulatory_id] AS w_mc_ambulatory_id
      ,g.[mc_ambulatory_name] AS w_mc_ambulatory_name
      ,g.[ambulatory_operation_id] AS w_ambulatory_operation_id
      ,g.[ambulatory_operation_name] AS w_ambulatory_operation_name
      ,g.[childrens_id] AS w_childrens_id
      ,g.[childrens_name] AS w_childrens_name
      ,g.[serviceline_division_id] AS w_serviceline_division_id
      ,g.[serviceline_division_name] AS w_serviceline_division_name
      ,g.[mc_operation_id] AS w_mc_operation_id
      ,g.[mc_operation_name] AS w_mc_operation_name
      ,g.[inpatient_adult_id] AS w_inpatient_adult_id
      ,g.[inpatient_adult_name] AS w_inpatient_adult_name
      ,g.[upg_practice_region_id] AS w_upg_practice_region_id
      ,g.[upg_practice_region_name] AS w_upg_practice_region_name
      ,g.[upg_practice_id] AS w_upg_practice_id
      ,g.[upg_practice_name] AS w_upg_practice_name
      ,[Reporting_Period_Enddate] = 
          (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE [sk_Data_Portal_Metrics_Master] = 338)
FROM [DS_HSDM_App].[TabRptg].[Dash_Telemedicine_Encounters_Tiles] t
    LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON t.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id
WHERE Encounter_Status = 'Complete'
    AND event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE [sk_Data_Portal_Metrics_Master] = 338)
) Completed
WHERE event_date >= @locstartdate AND event_date < @locenddate
AND provider_name LIKE '%SOLENSKI%'
--AND Completed.PAT_ENC_CSN_ID = 200047958469

SELECT
	x_is_telemed
   ,CASE WHEN [x_is_telemed] = 1 THEN [event_count] ELSE 0 END AS x_numerator
   ,x_denominator
   ,Telehealth_Flag
   ,event_category
   ,event_count
   ,event_date
   ,ENC_TYPE_TITLE
   ,PAT_ENC_CSN_ID
   ,epic_department_id
   ,epic_department_name
   ,provider_id
   ,provider_name
FROM #Data_Source
ORDER BY CASE WHEN [x_is_telemed] = 1 THEN [event_count] ELSE 0 END DESC, event_date
--ORDER BY PAT_ENC_CSN_ID, CASE WHEN [x_is_telemed] = 1 THEN [event_count] ELSE 0 END DESC
/*
    SELECT
	    epic_department_id
	  , epic_department_name
	  , provider_id
	  , provider_name
	  , PAT_ENC_CSN_ID
	  , event_date
      , CASE WHEN event_category = 'Telehealth Encounter' THEN 1 ELSE 0 END AS x_is_telemed
	  , event_count
	  , CASE WHEN event_category = 'All Encounters' AND Telehealth_Flag = 1 THEN event_count ELSE 0 END AS x_denominator
    FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
	WHERE event_date >= @locstartdate AND event_date < @locenddate
	AND provider_name LIKE '%SOLENSKI%'
	ORDER BY event_date
*/
/*
    SELECT COUNT(*) AS FaceToFace
	INTO #F2F
    FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles]
	WHERE F2_Flag = 1
	AND appt_event_Completed = 1
    AND event_date >= @locstartdate AND event_date < @locenddate

	SELECT COUNT(*) AS Telemedicine
	INTO #Telemed
    FROM [DS_HSDM_App].[TabRptg].[Dash_Telemedicine_Encounters_Tiles]
	WHERE Encounter_Status = 'Complete'
    AND event_date >= @locstartdate AND event_date < @locenddate

	SELECT CAST(CAST(Telemed.Telemedicine AS NUMERIC(9,2)) /
	       (CAST(F2F.FaceToFace AS NUMERIC(9,2)) + CAST(Telemed.Telemedicine AS NUMERIC(9,2))) AS NUMERIC(7,4)) AS Telemedicine_Visit_Percentage
	FROM #F2F F2F
	JOIN #Telemed Telemed
	ON 1 = 1
*/
GO


