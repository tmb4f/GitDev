USE [DS_HSDM_APP]
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
				DS_HSDM_App.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles
				DS_HSDM_App.TabRptg.Dash_AmbOpt_SessionAdherence_Tiles
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
--			07/02/2024 - TMB - add Third Next Available and 4 Hour Session Adherence, drop Bump Rate and
--												Functional Vacancy
--			07/16/2024 - TMB - add Scheduled Slot Adherence
--************************************************************************************************************************

    SET NOCOUNT ON;
 
 DECLARE @startdate SMALLDATETIME,
         @enddate SMALLDATETIME

SET @startdate = '5/1/2024'
SET @enddate = '5/31/2024'

	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#NoShow ') IS NOT NULL
DROP TABLE #NoShow

--IF OBJECT_ID('tempdb..#Bump ') IS NOT NULL
--DROP TABLE #Bump

IF OBJECT_ID('tempdb..#Completed ') IS NOT NULL
DROP TABLE #Completed

IF OBJECT_ID('tempdb..#MDStaffWorkedTogether ') IS NOT NULL
DROP TABLE #MDStaffWorkedTogether

IF OBJECT_ID('tempdb..#OYStaffWorkedTogether ') IS NOT NULL
DROP TABLE #OYStaffWorkedTogether

IF OBJECT_ID('tempdb..#DSStaffWorkedTogether ') IS NOT NULL
DROP TABLE #DSStaffWorkedTogether

IF OBJECT_ID('tempdb..#BudgetedUOS ') IS NOT NULL
DROP TABLE #BudgetedUOS

IF OBJECT_ID('tempdb..#FirstYearRetention ') IS NOT NULL
DROP TABLE #FirstYearRetention

IF OBJECT_ID('tempdb..#BudgetedUOSUPG ') IS NOT NULL
DROP TABLE #BudgetedUOSUPG

--IF OBJECT_ID('tempdb..#FunctionalVacancy ') IS NOT NULL
--DROP TABLE #FunctionalVacancy

IF OBJECT_ID('tempdb..#SlotUtilization ') IS NOT NULL
DROP TABLE #SlotUtilization

IF OBJECT_ID('tempdb..#ApptAvailability ') IS NOT NULL
DROP TABLE #ApptAvailability

IF OBJECT_ID('tempdb..#SessionAdherence ') IS NOT NULL
DROP TABLE #SessionAdherence

IF OBJECT_ID('tempdb..#SlotAdherence ') IS NOT NULL
DROP TABLE #SlotAdherence


SELECT	
       'Third Next Available' AS metric,		
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
       apptavail.days_wait AS numerator,
       apptavail.count_days_wait AS denominator
INTO #ApptAvailability
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
			  ,SUM(fytd.days_wait) AS days_wait
			  ,SUM(fytd.count_days_wait) AS count_days_wait
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
			  ,fymonth.days_wait
			  ,fymonth.count_days_wait
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
              ,SUM(tabrptg.days_wait) AS days_wait
              ,COUNT(tabrptg.days_wait) AS count_days_wait
FROM [TabRptg].[Dash_AmbOpt_AppointmentAvailability_Tiles] tabrptg
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
WHERE tabrptg.access_type = 'PROV'
AND provider_type_ot_name <> 'Resource'
AND ddte.weekday_ind = 1 -- M-F
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
) apptavail

--TRUNCATE TABLE TabRptg.Dash_AmbOpt_ClinicRanking_Details

--INSERT TabRptg.Dash_AmbOpt_ClinicRanking_Details
--           (metric
--           ,fyear_num
--           ,fmonth_num
--           ,event_date
--           ,organization_id
--           ,organization_name
--           ,service_id
--           ,service_name
--           ,clinical_area_id
--           ,clinical_area_name
--           ,numerator
--           ,denominator
--		   )
SELECT *
FROM #ApptAvailability
ORDER BY metric, fyear_num, fmonth_num, event_date, organization_id, service_id, clinical_area_id

GO


