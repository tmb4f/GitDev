USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--DECLARE @startdate SMALLDATETIME
--                 ,@enddate SMALLDATETIME

--SET @startdate = '1/1/2021 00:00 AM'
--SET @enddate = '6/30/2022 11:59 PM'

CREATE PROCEDURE [ETL].[uspSrc_BalancedScorecard_ClinicRanking_Details]
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
    )
AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_BalancedScorecard_ClinicRanking_Details
--WHO : Tom Burgan
--WHEN: 08/02/22
--WHY : Report clinic ranking based on an index of weighted, normalized scores for a set of metrics.
--					
/*-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
				DS_HSDW_Prod.Rptg.vwDim_Date
				DS_HSDM_App.TabRptg.Dash_AmbOpt_ApptNoShowMetric_Tiles
				DS_HSDM_App.Mapping.Epic_Dept_Groupers
				DS_HSDM_App.Mapping.Ref_Clinical_Area_Map
				DS_HSDM_App.Mapping.Ref_Service_Map
				DS_HSDM_App.Mapping.Ref_Organization_Map
				DS_HSDM_App.TabRptg.Dash_AmbOpt_ProvCancApptMetric_Tiles
				DS_HSDM_App.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles
				DS_HSDM_App.TabRptg.Dash_AmbOpt_MDStaffWorkedTogether_Tiles
				DS_HSDM_App.TabRptg.Dash_AmbOpt_BHStaffWorkedTogether_Tiles
				DS_HSDM_App.TabRptg.Dash_AmbOpt_DentistryStaffWorkedTogether_Tiles
				DS_HSDW_Prod.dbo.Fact_PressGaney_Responses
				DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt
				DS_HSDW_Prod.dbo.Dim_Clrt_DEPt
				DS_HSDM_App.TabRptg.Dash_AmbOpt_BudgetvsActualVisits_Tiles
				DS_HSDM_App.Mapping.Workday_Dept_Groupers
				DS_HSDM_App.Stage.Ambulatory_Clinic_Ranking_Map
				DS_HSDM_App.ETL.Data_Portal_Metrics_Master
				DS_HSDM_App.TabRptg.Dash_AmbOpt_EmpRetention_Tiles
--                
--      OUTPUTS:  [ETL].[.uspSrc_BalancedScorecard_ClinicRanking_Details]
--
--------------------------------------------------------------------------------------------------------------------------*/
--MODS: 	
--         08/02/2022 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

SELECT	
       'No Show' AS metric,		
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       NoShow AS numerator,
       Appointment AS denominator
INTO #NoShow
FROM
(
SELECT
			   fytd.Fyear_num
			  ,fytd.fmonth_num
			  ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
			  ,SUM(fytd.NoShow) AS NoShow
			  ,SUM(fytd.PatientCanceledLate) AS PatientCanceledLate
			  ,SUM(fytd.Appointment) AS Appointment
FROM
(
SELECT
			   date_dim.Fyear_num
			  ,date_dim.fmonth_num
			  ,date_dim.month_begin_date AS fmtd_month_begin_date
			  ,fymonth.month_begin_date
			  ,fymonth.organization_id
			  ,fymonth.organization_name
			  ,fymonth.service_id
			  ,fymonth.service_name
			  ,fymonth.clinical_area_id
			  ,fymonth.clinical_area_name
			  ,fymonth.NoShow
			  ,fymonth.PatientCanceledLate
			  ,fymonth.Appointment
FROM
(
SELECT DISTINCT
		ddte.Fyear_num
	   ,ddte.fmonth_num
	   ,ddte.month_begin_date
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
WHERE ddte.day_date >= @locstartdate
AND ddte.day_date <= @locenddate
) date_dim
LEFT OUTER JOIN
(
SELECT
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
              ,SUM(NoShow) AS NoShow
              ,SUM(PatientCanceledLate) AS PatientCanceledLate 
              ,SUM(Appointment) AS Appointment
FROM [TabRptg].[Dash_AmbOpt_ApptNoShowMetric_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
ON ddte.day_date = CAST(tabrptg.event_date AS SMALLDATETIME)
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id
WHERE event_category = 'Aggregate'
AND ambulatory_flag = 1 -- Ambulatory Operations Scorecard
AND event_date >= @locstartdate
AND event_date <= @locenddate
GROUP BY
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
)  fymonth
ON fymonth.Fyear_num = date_dim.Fyear_num
AND fymonth.fmonth_num = date_dim.fmonth_num
AND fymonth.month_begin_date <= date_dim.month_begin_date
) fytd
GROUP BY
               fytd.Fyear_num
			  ,fytd.fmonth_num
              ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
) noshow

SELECT
       'Bump' AS metric,	
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       Bump AS numerator,
       Appointment AS denominator
INTO #Bump
FROM
(
SELECT
			   fytd.Fyear_num
			  ,fytd.fmonth_num
			  ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
			  ,SUM(fytd.Bump) AS Bump
			  ,SUM(fytd.Appointment) AS Appointment
FROM
(
SELECT
			   date_dim.Fyear_num
			  ,date_dim.fmonth_num
			  ,date_dim.month_begin_date AS fmtd_month_begin_date
			  ,fymonth.month_begin_date
			  ,fymonth.organization_id
			  ,fymonth.organization_name
			  ,fymonth.service_id
			  ,fymonth.service_name
			  ,fymonth.clinical_area_id
			  ,fymonth.clinical_area_name
			  ,fymonth.Bump
			  ,fymonth.Appointment
FROM
(
SELECT DISTINCT
		ddte.Fyear_num
	   ,ddte.fmonth_num
	   ,ddte.month_begin_date
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
WHERE ddte.day_date >= @locstartdate
AND ddte.day_date <= @locenddate
) date_dim
LEFT OUTER JOIN
(
SELECT
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
              ,SUM(tabrptg.Bump) AS Bump
              ,SUM(Appointment) AS Appointment
FROM [TabRptg].[Dash_AmbOpt_ProvCancApptMetric_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
ON ddte.day_date = CAST(tabrptg.event_date AS SMALLDATETIME)
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id
WHERE event_category = 'Aggregate'
AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant')
AND ambulatory_flag = 1 -- Ambulatory Operations Scorecard
AND event_date >= @locstartdate
AND event_date <= @locenddate
GROUP BY
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
)  fymonth
ON fymonth.Fyear_num = date_dim.Fyear_num
AND fymonth.fmonth_num = date_dim.fmonth_num
AND fymonth.month_begin_date <= date_dim.month_begin_date
) fytd
GROUP BY
               fytd.Fyear_num
			  ,fytd.fmonth_num
              ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
)  bump

SELECT	
       'Completed' AS metric,		
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       Completed AS numerator,
       1 AS denominator
INTO #Completed
FROM
(
SELECT
			   fytd.Fyear_num
			  ,fytd.fmonth_num
			  ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
			  ,SUM(fytd.Completed) AS Completed
FROM
(
SELECT
			   date_dim.Fyear_num
			  ,date_dim.fmonth_num
			  ,date_dim.month_begin_date AS fmtd_month_begin_date
			  ,fymonth.month_begin_date
			  ,fymonth.organization_id
			  ,fymonth.organization_name
			  ,fymonth.service_id
			  ,fymonth.service_name
			  ,fymonth.clinical_area_id
			  ,fymonth.clinical_area_name
			  ,fymonth.Completed
FROM
(
SELECT DISTINCT
		ddte.Fyear_num
	   ,ddte.fmonth_num
	   ,ddte.month_begin_date
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
WHERE ddte.day_date >= @locstartdate
AND ddte.day_date <= @locenddate
) date_dim
LEFT OUTER JOIN
(
SELECT
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
              ,SUM(tabrptg.event_count) AS Completed
FROM [TabRptg].[Dash_AmbOpt_ScheduledAppointmentMetric_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
ON ddte.day_date = CAST(tabrptg.event_date AS SMALLDATETIME)
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id
WHERE (tabrptg.appt_event_Arrived = 1 OR tabrptg.appt_event_Completed = 1)
AND ambulatory_flag = 1 -- Ambulatory Operations Scorecard
AND event_date >= @locstartdate
AND event_date <= @locenddate
GROUP BY
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
)  fymonth
ON fymonth.Fyear_num = date_dim.Fyear_num
AND fymonth.fmonth_num = date_dim.fmonth_num
AND fymonth.month_begin_date <= date_dim.month_begin_date
) fytd
GROUP BY
               fytd.Fyear_num
			  ,fytd.fmonth_num
              ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
) completed

SELECT
       'Staff Worked Together' AS metric,	
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       weighted_score AS numerator,
       Responses AS denominator
INTO #MDStaffWorkedTogether
FROM
(
SELECT
			   fytd.Fyear_num
			  ,fytd.fmonth_num
			  ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
			  ,CAST(SUM(fytd.weighted_score) AS INTEGER) AS weighted_score
			  ,SUM(fytd.Responses) AS Responses
FROM
(
SELECT
			   date_dim.Fyear_num
			  ,date_dim.fmonth_num
			  ,date_dim.month_begin_date AS fmtd_month_begin_date
			  ,fymonth.month_begin_date
			  ,fymonth.organization_id
			  ,fymonth.organization_name
			  ,fymonth.service_id
			  ,fymonth.service_name
			  ,fymonth.clinical_area_id
			  ,fymonth.clinical_area_name
			  ,fymonth.weighted_score
			  ,fymonth.Responses
FROM
(
SELECT DISTINCT
		ddte.Fyear_num
	   ,ddte.fmonth_num
	   ,ddte.month_begin_date
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
WHERE ddte.day_date >= @locstartdate
AND ddte.day_date <= @locenddate
) date_dim
LEFT OUTER JOIN
(
SELECT
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
              ,SUM(tabrptg.weighted_score) AS weighted_score
              ,SUM(tabrptg.event_count) AS Responses
FROM [TabRptg].[Dash_AmbOpt_MDStaffWorkedTogether_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
ON ddte.day_date = CAST(tabrptg.event_date AS SMALLDATETIME)
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id
WHERE tabrptg.event_count = 1
AND tabrptg.ambulatory_flag = 1 -- Ambulatory Operations Scorecard
AND SUBSTRING(Survey_Designator,1,2) = 'MD'
AND event_date >= @locstartdate
AND event_date <= @locenddate
GROUP BY
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
)  fymonth
ON fymonth.Fyear_num = date_dim.Fyear_num
AND fymonth.fmonth_num = date_dim.fmonth_num
AND fymonth.month_begin_date <= date_dim.month_begin_date
) fytd
GROUP BY
               fytd.Fyear_num
			  ,fytd.fmonth_num
              ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
) mdstaffworkedtogether

SELECT
       'Staff Worked Together' AS metric,	
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       weighted_score AS numerator,
       Responses AS denominator
INTO #OYStaffWorkedTogether
FROM
(
SELECT
			   fytd.Fyear_num
			  ,fytd.fmonth_num
			  ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
			  ,CAST(SUM(fytd.weighted_score) AS INTEGER) AS weighted_score
			  ,SUM(fytd.Responses) AS Responses
FROM
(
SELECT
			   date_dim.Fyear_num
			  ,date_dim.fmonth_num
			  ,date_dim.month_begin_date AS fmtd_month_begin_date
			  ,fymonth.month_begin_date
			  ,fymonth.organization_id
			  ,fymonth.organization_name
			  ,fymonth.service_id
			  ,fymonth.service_name
			  ,fymonth.clinical_area_id
			  ,fymonth.clinical_area_name
			  ,fymonth.weighted_score
			  ,fymonth.Responses
FROM
(
SELECT DISTINCT
		ddte.Fyear_num
	   ,ddte.fmonth_num
	   ,ddte.month_begin_date
FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
WHERE ddte.day_date >= @locstartdate
AND ddte.day_date <= @locenddate
) date_dim
LEFT OUTER JOIN
(
SELECT
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
              ,SUM(tabrptg.weighted_score) AS weighted_score
              ,SUM(tabrptg.event_count) AS Responses
FROM [TabRptg].[Dash_AmbOpt_BHStaffWorkedTogether_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
ON ddte.day_date = CAST(tabrptg.event_date AS SMALLDATETIME)
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id
WHERE tabrptg.event_count = 1
AND g.ambulatory_flag = 1 -- Ambulatory Operations Scorecard
AND event_date >= @locstartdate
AND event_date <= @locenddate
GROUP BY
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
)  fymonth
ON fymonth.Fyear_num = date_dim.Fyear_num
AND fymonth.fmonth_num = date_dim.fmonth_num
AND fymonth.month_begin_date <= date_dim.month_begin_date
) fytd
GROUP BY
               fytd.Fyear_num
			  ,fytd.fmonth_num
              ,fytd.fmtd_month_begin_date
			  ,fytd.organization_id
			  ,fytd.organization_name
			  ,fytd.service_id
			  ,fytd.service_name
			  ,fytd.clinical_area_id
			  ,fytd.clinical_area_name
) oystaffworkedtogether

SELECT
       'Staff Worked Together' AS metric,	
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       weighted_score AS numerator,
       Responses AS denominator
INTO #DSStaffWorkedTogether
FROM
(
SELECT
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date AS fmtd_month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
			  ,SUM(tabrptg.weighted_score) AS weighted_score
			  ,SUM(tabrptg.event_count) AS Responses
FROM [TabRptg].[Dash_AmbOpt_DentistryStaffWorkedTogether_Tiles] tabrptg
INNER JOIN
(
SELECT DISTINCT
	SURVEY_ID
  , Pat_Enc_CSN_Id
  , sk_Fact_Pt_Acct
FROM DS_HSDW_Prod.dbo.Fact_PressGaney_Responses 
WHERE Svc_Cde = 'DS'
) resp
ON tabrptg.event_id = resp.SURVEY_ID
LEFT OUTER JOIN
(
SELECT DISTINCT
	PAT_ENC_CSN_ID
  , sk_Fact_Pt_Acct
  , sk_Dim_Clrt_DEPt
FROM DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt
) enc
ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
LEFT OUTER JOIN
(
SELECT DISTINCT
	sk_Dim_Clrt_DEPt
  , DEPARTMENT_ID
FROM DS_HSDW_Prod.dbo.Dim_Clrt_DEPt
) dep
ON dep.sk_Dim_Clrt_DEPt = enc.sk_Dim_Clrt_DEPt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
ON ddte.day_date = CAST(tabrptg.event_date AS SMALLDATETIME)
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = dep.DEPARTMENT_ID
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id
WHERE tabrptg.event_count = 1
AND event_date >= @locstartdate
AND event_date <= @locenddate
GROUP BY
               ddte.Fyear_num
			  ,ddte.fmonth_num
              ,ddte.month_begin_date
			  ,o.organization_id
			  ,o.organization_name
			  ,s.service_id
			  ,s.service_name
			  ,c.clinical_area_id
			  ,c.clinical_area_name
) dsstaffworkedtogether

select 
'Budgeted UOS' as metric
,actuals.fyear_num
,actuals.fmonth_num
,actuals.event_date
,actuals.organization_id
,actuals.organization_name
,actuals.service_id
,actuals.service_name
,actuals.clinical_area_id
,actuals.clinical_area_name
, numerator
, denominator
INTO #BudgetedUOS
FROM
--"Actuals" rows
(
SELECT o.organization_id
      ,o.organization_name
      ,s.service_id
      ,s.service_name
	  ,c.clinical_area_id
	  ,c.clinical_area_name
	  ,c.sk_Ref_Clinical_Area_Map
      ,t.[event_date]
      ,t.[fyear_num]
	  ,t.fmonth_num
      ,sum(t.[Amount]) as numerator
FROM
    [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_BudgetvsActualVisits_Tiles] t
    JOIN DS_HSDM_App.[Mapping].[Workday_Dept_Groupers] g on g.[medical_center_ps_deptid] = t.[peoplesoft_dept_id]
	JOIN Stage.Ambulatory_Clinic_Ranking_Map acr_map on g.workday_department_id = acr_map.workday_department_id
	JOIN Mapping.Ref_Clinical_Area_Map c on acr_map.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    JOIN DS_HSDM_App.Mapping.Ref_Service_Map s on g.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o on s.organization_id = o.organization_id
WHERE
    t.event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE [sk_Data_Portal_Metrics_Master] = 187)
	and event_type = 'Actuals'
	and g.ambulatory_flag = 1
GROUP BY o.organization_id
      ,o.organization_name
      ,s.service_id
      ,s.service_name
	  ,c.clinical_area_id
	  ,c.clinical_area_name
	  ,c.sk_Ref_Clinical_Area_Map
      ,t.[event_date]
      ,t.[fyear_num]
	  ,t.fmonth_num
) as actuals

JOIN

-- "Budget" rows
(
SELECT o.organization_id
      ,o.organization_name
      ,s.service_id
      ,s.service_name
	  ,c.clinical_area_id
	  ,c.clinical_area_name
	  ,c.sk_Ref_Clinical_Area_Map
      ,t.[event_date]
      ,t.[fyear_num]
	  ,t.fmonth_num
      ,sum(t.[Amount]) as denominator
FROM
    [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_BudgetvsActualVisits_Tiles] t
    JOIN DS_HSDM_App.[Mapping].[Workday_Dept_Groupers] g on g.[medical_center_ps_deptid] = t.[peoplesoft_dept_id]
	JOIN Stage.Ambulatory_Clinic_Ranking_Map acr_map on g.workday_department_id = acr_map.workday_department_id
	JOIN Mapping.Ref_Clinical_Area_Map c on acr_map.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    JOIN DS_HSDM_App.Mapping.Ref_Service_Map s on g.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o on s.organization_id = o.organization_id
WHERE
    t.event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE [sk_Data_Portal_Metrics_Master] = 187)
	and event_type = 'Budget'
	and g.ambulatory_flag = 1
GROUP BY o.organization_id
      ,o.organization_name
      ,s.service_id
      ,s.service_name
	  ,c.clinical_area_id
	  ,c.clinical_area_name
	  ,c.sk_Ref_Clinical_Area_Map
      ,t.[event_date]
      ,t.[fyear_num]
	  ,t.fmonth_num
) as budget

on actuals.sk_Ref_Clinical_Area_Map = budget.sk_Ref_Clinical_Area_Map
and actuals.event_date = budget.event_date

SELECT 
'First Year Retention' as metric
,t.fyear_num
,t.fmonth_num
,DATEADD(DAY, 1, EOMONTH(event_date, -1)) as event_date
,o.organization_id
,o.organization_name
,s.service_id
,s.service_name
,c.clinical_area_id
,c.clinical_area_name
,sum(event_count) as numerator
,sum(case when event_category is NULL and cast(LengthOfService as DECIMAL(10, 2)) <= 1 then 1 else 0 end) as denominator
INTO #FirstYearRetention
FROM
  TabRptg.Dash_AmbOpt_EmpRetention_Tiles t
  JOIN Stage.Ambulatory_Clinic_Ranking_Map acr_map on t.workday_department_id = acr_map.workday_department_id
  JOIN Mapping.Workday_Dept_Groupers g on t.workday_department_id =  g.workday_department_id
  JOIN Mapping.Ref_Clinical_Area_Map c on acr_map.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
  JOIN Mapping.Ref_Service_Map s on g.sk_Ref_Service_Map = s.sk_Ref_Service_Map
  JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o on s.organization_id = o.organization_id
WHERE g.ambulatory_flag = 1
AND event_date >= @locstartdate
AND event_date <= @locenddate
GROUP BY
	fyear_num, fmonth_num, DATEADD(DAY, 1, EOMONTH(event_date, -1)), o.organization_id, organization_name, service_id, service_name, clinical_area_id, clinical_area_name

SELECT *
FROM #NoShow
UNION ALL
SELECT *
FROM #Bump
UNION ALL
SELECT *
FROM #Completed
UNION ALL
SELECT *
FROM #MDStaffWorkedTogether
WHERE organization_id IS NOT NULL
UNION ALL
SELECT *
FROM #OYStaffWorkedTogether
WHERE organization_id IS NOT NULL
UNION ALL
SELECT *
FROM #DSStaffWorkedTogether
WHERE organization_id IS NOT NULL
UNION ALL
SELECT *
FROM #BudgetedUOS
UNION ALL
SELECT *
FROM #FirstYearRetention
ORDER BY metric, fyear_num, fmonth_num, event_date, organization_id, service_id, clinical_area_id

GO


