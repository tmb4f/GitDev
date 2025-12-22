USE DS_HSDM_App
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
                 ,@enddate SMALLDATETIME

--SET @startdate = '7/1/2020 00:00 AM'
--SET @startdate = '7/1/2021 00:00 AM'
--SET @startdate = '10/1/2021 00:00 AM'
--SET @startdate = '7/1/2022 00:00 AM'
--SET @startdate = '12/1/2022 00:00 AM'
--SET @startdate = '12/1/2024 00:00 AM'
SET @startdate = '11/1/2025 00:00 AM'
--SET @enddate = '6/30/2021 11:59 PM'
--SET @enddate = '7/14/2021 11:59 PM'
--SET @enddate = '7/31/2022 11:59 PM'
--SET @enddate = '12/31/2022 11:59 PM'
--SET @enddate = '12/31/2024 11:59 PM'
SET @enddate = '11/30/2025 11:59 PM'

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

/*
Appointment Slot Utilization

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Validation extract for Table DM DS_HSDM_App TabRptg Dash_AmbOpt_UnavailRsnSlotUtilization_Tiles.sql
*/

IF OBJECT_ID('tempdb..#TabRptg4') IS NOT NULL
DROP TABLE #TabRptg4

SELECT
	   o.organization_id
	  ,o.organization_name
	  ,s.service_id
	  ,s.service_name
	  ,c.clinical_area_id
	  ,c.clinical_area_name
	  ,tabrptg.som_department_id
	  ,tabrptg.som_department_name
      ,tabrptg.[epic_department_id]
      ,tabrptg.[epic_department_name]
      ,tabrptg.epic_department_name_external
      ,[provider_id]
      ,[provider_name]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[event_date]
      ,[UNAVAILABLE_RSN_C]
      ,[UNAVAILABLE_RSN_NAME]
      ,[AMB_Scorecard_Flag]
      ,[Openings_Booked]
      ,[Regular_Openings_Available]
      ,[Regular_Openings_Unavailable]
      ,[Openings_Booked] AS Numerator
	  ,[Regular_Openings_Available] +
	   CASE WHEN tabrptg.AMB_Scorecard_Flag = 1 THEN tabrptg.Regular_Openings_Unavailable ELSE 0 END AS Denominator

  INTO #TabRptg4

  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_UnavailRsnSlotUtilization_Tiles] tabrptg

	LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON tabrptg.epic_department_id = g.epic_department_id
	LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
	LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
	LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id
  WHERE
  tabrptg.STAFF_RESOURCE_C = 1
  --AND g.ambulatory_flag = 1
  --AND tabrptg.event_count = 1
  AND tabrptg.hs_area_id = 1

  --AND tabrptg.AMB_Scorecard_Flag = 1

  AND CAST(event_date AS SMALLDATETIME) >= @locstartdate
      AND CAST(event_date  AS SMALLDATETIME) <= @locenddate

  AND tabrptg.PROVIDER_TYPE_C IN (
	'4', -- Anesthesiologist
	'108', -- Dentist
	'2506', -- Doctor of Philosophy
	'9', -- Nurse Practitioner
	'105', -- Optometrist
	'1', -- Physician
	'6', -- Physician Assistant
	'10' --Psychologist
	)

SELECT
    'Appointment Slot Utilization' AS Metric
   ,'Tab Table' AS Source
   ,'MTD' AS [Time Period]
   ,SUM(Numerator) AS Openings_Booked
   ,SUM(Regular_Openings_Available) AS Regular_Openings
FROM #TabRptg4

GO