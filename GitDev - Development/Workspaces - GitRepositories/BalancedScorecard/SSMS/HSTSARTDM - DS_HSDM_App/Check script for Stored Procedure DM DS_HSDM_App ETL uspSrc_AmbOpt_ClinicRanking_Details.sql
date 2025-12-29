USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_ClinicRanking_Details]

--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_ClinicRanking_Details
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
				DS_HSDM_App.TabRptg.Dash_AmbOpt_BudgetvsActualVisitsUPG_Tiles
				DS_HSDM_App.TabRptg.Dash_AmbOpt_EmpFunctionalVacancyRate_Tiles
				DS_HSDM_App.TabRptg.Dash_AmbOpt_UnavailRsnSlotUtilization_Tiles
--                
--      OUTPUTS:  [ETL].[.uspSrc_AmbOpt_ClinicRanking_Details]
--
--------------------------------------------------------------------------------------------------------------------------*/
--MODS: 	
--         08/02/2022 - TMB - create stored procedure
--         08/10/2022 - TMB - add Functional Vacancy metric summary; update Budgeted UOS script
--         08/11/2022 - TMB - add Budgeted UOS UPG script as a separate extract (i.e., has its own TabRptg table)
--         08/15/2022 - TMB - edit First Year Retention script
--			05/26/2023 - TMB - add Slot Utilization script
--************************************************************************************************************************

    SET NOCOUNT ON;
 
 DECLARE @startdate SMALLDATETIME,
         @enddate SMALLDATETIME

	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#SlotUtilization ') IS NOT NULL
DROP TABLE #SlotUtilization

SELECT	
       'Slot Utilization' AS metric,		
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       slotutil.OpeningsBooked AS numerator,
       slotutil.RegularOpeningsAvailable + slotutil.RegularOpeningsUnavailable AS denominator
INTO #SlotUtilization
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
			  ,SUM(fytd.OpeningsBooked) AS OpeningsBooked
			  ,SUM(fytd.RegularOpeningsAvailable) AS RegularOpeningsAvailable
			  ,SUM(fytd.RegularOpeningsUnavailable) AS RegularOpeningsUnavailable
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
			  ,fymonth.OpeningsBooked
			  ,fymonth.RegularOpeningsAvailable
			  ,fymonth.RegularOpeningsUnavailable
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
              ,SUM(tabrptg.Openings_Booked) AS OpeningsBooked
              ,SUM(tabrptg.Regular_Openings_Available) AS RegularOpeningsAvailable 
              ,SUM(CASE WHEN tabrptg.AMB_Scorecard_Flag = 1 THEN tabrptg.Regular_Openings_Unavailable ELSE 0 END) AS RegularOpeningsUnavailable
FROM [TabRptg].[Dash_AmbOpt_UnavailRsnSlotUtilization_Tiles] tabrptg
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
WHERE tabrptg.STAFF_RESOURCE_C = 1 -- Person
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
) slotutil

SELECT 
       organization_id,
       organization_name,
       service_id,
       service_name,
       clinical_area_id,
       clinical_area_name,
	   --metric,
       fyear_num,
       fmonth_num,
       event_date,
       numerator,
       denominator,
	   CASE WHEN denominator > 0 THEN CAST(CAST(numerator AS NUMERIC(7,2)) / CAST(denominator AS NUMERIC(7,2)) AS NUMERIC(10,6)) ELSE 0.0 END AS Rate
FROM #SlotUtilization
--ORDER BY metric, fyear_num, fmonth_num, event_date, organization_id, service_id, clinical_area_id
ORDER BY organization_id, service_id, clinical_area_id,  fyear_num, fmonth_num, event_date

GO


