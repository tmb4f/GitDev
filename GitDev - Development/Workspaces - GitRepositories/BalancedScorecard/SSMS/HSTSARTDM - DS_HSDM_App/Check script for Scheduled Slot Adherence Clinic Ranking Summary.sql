USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

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

IF OBJECT_ID('tempdb..#SlotAdherence ') IS NOT NULL
DROP TABLE #SlotAdherence

SELECT	
       'Scheduled Slot Adherence' AS metric,		
       Fyear_num AS fyear_num,
       fmonth_num,
       fmtd_month_begin_date AS event_date,
	   organization_id,
	   organization_name,
	   service_id,
	   service_name,
       clinical_area_id,
       clinical_area_name,
	   epic_department_id,
	   epic_department_name,
       slotadher.Approved_Slots AS numerator,
       slotadher.Total_Slots AS denominator
INTO #SlotAdherence
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
			  ,fytd.epic_department_id
			  ,fytd.epic_department_name
			  ,SUM(fytd.Approved_Slots) AS Approved_Slots
			  ,SUM(fytd.Total_Slots) AS Total_Slots
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
			  ,fymonth.epic_department_id
			  ,fymonth.epic_department_name
			  ,fymonth.Approved_Slots
			  ,fymonth.Total_Slots
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
			  ,tabrptg.epic_department_id
			  ,tabrptg.epic_department_name
              ,SUM(tabrptg.Approved_Slots) AS Approved_Slots
              ,SUM(tabrptg.Total_Slots) AS Total_Slots
FROM [TabRptg].[Dash_AmbOpt_SessionAdherence_Tiles] tabrptg
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
WHERE ambulatory_flag = 1 -- Ambulatory Operations Scorecard
AND tabrptg.AMB_Scorecard_Flag = 1
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
			  ,tabrptg.epic_department_id
			  ,tabrptg.epic_department_name
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
			  ,fytd.epic_department_id
			  ,fytd.epic_department_name
) slotadher

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
FROM #SlotAdherence
--ORDER BY metric, fyear_num, fmonth_num, event_date, organization_id, service_id, clinical_area_id, epic_department_id
ORDER BY metric, fyear_num, fmonth_num, event_date, epic_department_name, organization_id, service_id, clinical_area_id

GO


