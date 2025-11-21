USE [CLARITY_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =====================================================================================
-- Alter procedure uspSrc_AmbOptHuddle_Unavail_Rsn_Slot_Utilization
-- =====================================================================================

ALTER PROCEDURE [ETL].[uspSrc_AmbOptHuddle_Unavail_Rsn_Slot_Utilization]
AS

DECLARE  @startdate SMALLDATETIME
DECLARE  @enddate SMALLDATETIME

DECLARE @yesterday DATETIME
DECLARE @today DATETIME

SET @yesterday = DATEADD(dd,-1,(CAST(CAST(GETDATE() AS DATE) AS DATETIME)))
SET @today = CAST(CAST(GETDATE() AS DATE) AS DATETIME)

SET @startdate = CAST(DATEADD(dd,-16,@today) AS SMALLDATETIME)
SET @enddate = CAST(@today AS SMALLDATETIME)

--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOptHuddle_Unavail_Rsn_Slot_Utilization
--WHO : Tom Burgan
--WHEN: 11/06/25
--WHY : Report daily slot utilization percentages by department and provider.
--		Percent Booked Overall = Regular + Irregular + Available + Unavailable Slots Booked /
--                                                Regular Openings Available
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
--              CLARITY_App.Rptg.vwDim_Date
--				CLARITY.dbo.V_AVAILABILITY
--				CLARITY_App.Stage.AmbOpt_Excluded_Department
--				CLARITY.dbo.CLARITY_SER
--				CLARITY_App.Rptg.vwRef_MDM_Location_Master
--				CLARITY_App.Rptg.vwRef_Physcn_Combined
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOptHuddle_Unavail_Rsn_Slot_Utilization]
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--       10/14/2025 - TMB - create stored procedure
--       11/06/2025 - TMB - Edited stored procedure,  added logic to update current Stage table
--		 11/14/2025 - TMB - Edited stored procedure to comply with new Stage table layout
--       11/15/2025 - TMB - Changed location of BUSINESS_UNIT in SELECT statement
--       11/16/2025 - TMB - Edited IF EXISTS statement
--       11/20/2025 - TMB - Edited logic for extracting provider division attributes; TRY_CAST ids having 'na' values
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    --IF @startdate IS NULL
    --    AND @enddate IS NULL
    --    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
DECLARE @slotstartdate DATETIME,
        @slotenddate DATETIME
SET @slotstartdate = CAST(@startdate AS DATETIME)
SET @slotenddate   = CAST(@enddate AS DATETIME)
-------------------------------------------------------------------------------
--/*
--/		We are storing and maintaining in the TabRptg table two snapshots:  Today's and Yesterday's extracts
--/
--/			Update the existing event_category = 'Today' rows to event_category = 'Yesterday'
--/
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Stage' AND TABLE_NAME = 'AmbOptHuddle_Dash_Unavail_Rsn_Slot_Utilization')
BEGIN
    UPDATE Stage.AmbOptHuddle_Dash_Unavail_Rsn_Slot_Utilization
    SET event_category = 'Yesterday'
    WHERE event_category = 'Today';
END
--/
--/			Delete the extract rows for the previous event_category = 'Yesterday' snapshot
--/
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Stage' AND TABLE_NAME = 'AmbOptHuddle_Dash_Unavail_Rsn_Slot_Utilization')
BEGIN
DELETE FROM Stage.AmbOptHuddle_Dash_Unavail_Rsn_Slot_Utilization
WHERE SnapShot_Dt < @yesterday;
END
--*/

SELECT date_dim.day_date
      ,date_dim.fmonth_num
      ,date_dim.Fyear_num
      ,date_dim.FYear_name
INTO #datetable
FROM CLARITY_App.Rptg.vwDim_Date AS date_dim
WHERE date_dim.day_date >= @slotstartdate
AND date_dim.day_date < @slotenddate

  -- Create index for temp table #datetable

  CREATE UNIQUE CLUSTERED INDEX IX_datetable ON #datetable ([day_date])

SELECT
       util.DEPARTMENT_ID
	 , util.PROV_ID
	 , util.UNAVAILABLE_RSN_C
	 , util.UNAVAILABLE_RSN_NAME
	 , emp.LAST_ACCS_DATE
	 , ser.PROV_NAME
	 , ser.STAFF_RESOURCE_C
	 , ser.STAFF_RESOURCE
	 , ser.PROVIDER_TYPE_C
	 , ser.PROV_TYPE
	 , CAST(util.SLOT_BEGIN_TIME AS DATE) AS SLOT_BEGIN_DATE
	 , SUM(util.[Regular Openings]) AS [Regular Openings]
	 , SUM(util.[Overbook Openings]) AS [Overbook Openings]
	 , SUM(util.NUM_APTS_SCHEDULED) AS NUM_APTS_SCHEDULED
	 , SUM(util.[Openings Booked]) AS [Openings Booked]
	 , SUM(util.[Regular Openings Available]) AS [Regular Openings Available]
	 , SUM(util.[Regular Openings Unavailable]) AS [Regular Openings Unavailable]
	 , SUM(util.[Overbook Openings Available]) AS [Overbook Openings Available]
	 , SUM(util.[Overbook Openings Unavailable]) AS [Overbook Openings Unavailable]
	 , SUM(util.[Regular Openings Booked]) AS [Regular Openings Booked]
	 , SUM(util.[Overbook Openings Booked]) AS [Overbook Openings Booked]
	 , SUM(util.[Regular Outside Template Booked]) AS [Regular Outside Template Booked]
	 , SUM(util.[Overbook Outside Template Booked]) AS [Overbook Outside Template Booked]
	 , SUM(util.[Regular Openings Available Booked]) AS [Regular Openings Available Booked]
	 , SUM(util.[Overbook Openings Available Booked]) AS [Overbook Openings Available Booked]
	 , SUM(util.[Regular Openings Unavailable Booked]) AS [Regular Openings Unavailable Booked]
	 , SUM(util.[Overbook Openings Unavailable Booked]) AS [Overbook Openings Unavailable Booked]
     , SUM(util.[Regular Outside Template Available Booked]) AS [Regular Outside Template Available Booked]
	 , SUM(util.[Overbook Outside Template Available Booked]) AS [Overbook Outside Template Available Booked]
     , SUM(util.[Regular Outside Template Unavailable Booked]) AS [Regular Outside Template Unavailable Booked]
	 , SUM(util.[Overbook Outside Template Unavailable Booked]) AS [Overbook Outside Template Unavailable Booked]
INTO #utilsum
FROM
(
SELECT slot.DEPARTMENT_ID
	  ,slot.PROV_ID
	  ,slot.SLOT_BEGIN_TIME
	  ,slot.UNAVAILABLE_RSN_C
	  ,slot.UNAVAILABLE_RSN_NAME
	  ,slot.[Regular Openings]
	  ,slot.[Overbook Openings]
	  ,slot.NUM_APTS_SCHEDULED
	  ,slot.[Openings Booked]
	  ,slot.[Regular Openings Available]
	  ,slot.[Regular Openings Unavailable]
	  ,slot.[Overbook Openings Available]
	  ,slot.[Overbook Openings Unavailable]
	  ,COALESCE(appt.[Regular Openings Booked],0) AS [Regular Openings Booked]
	  ,COALESCE(appt.[Overbook Openings Booked],0) AS [Overbook Openings Booked]
	  ,COALESCE(appt.[Regular Outside Template Booked],0) AS [Regular Outside Template Booked]
	  ,COALESCE(appt.[Overbook Outside Template Booked],0) AS [Overbook Outside Template Booked]
	  ,COALESCE(appt.[Regular Openings Available Booked],0) AS [Regular Openings Available Booked]
	  ,COALESCE(appt.[Overbook Openings Available Booked],0) AS [Overbook Openings Available Booked]
	  ,COALESCE(appt.[Regular Openings Unavailable Booked],0) AS [Regular Openings Unavailable Booked]
	  ,COALESCE(appt.[Overbook Openings Unavailable Booked],0) AS [Overbook Openings Unavailable Booked]
      ,COALESCE(appt.[Regular Outside Template Available Booked],0) AS [Regular Outside Template Available Booked]
	  ,COALESCE(appt.[Overbook Outside Template Available Booked],0) AS [Overbook Outside Template Available Booked]
      ,COALESCE(appt.[Regular Outside Template Unavailable Booked],0) AS [Regular Outside Template Unavailable Booked]
	  ,COALESCE(appt.[Overbook Outside Template Unavailable Booked],0) AS [Overbook Outside Template Unavailable Booked]
FROM
(
	SELECT
	       [AVAILABILITY].DEPARTMENT_ID
	      ,[AVAILABILITY].PROV_ID
	      ,[AVAILABILITY].SLOT_BEGIN_TIME
		  ,[AVAILABILITY].UNAVAILABLE_RSN_C
		  ,[AVAILABILITY].UNAVAILABLE_RSN_NAME
	      ,[AVAILABILITY].ORG_REG_OPENINGS AS [Regular Openings]
		  ,[AVAILABILITY].ORG_OVBK_OPENINGS AS [Overbook Openings]
	      ,[AVAILABILITY].NUM_APTS_SCHEDULED
	      ,[AVAILABILITY].NUM_APTS_SCHEDULED AS [Openings Booked]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL THEN [AVAILABILITY].ORG_REG_OPENINGS ELSE 0 END AS [Regular Openings Available]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL THEN [AVAILABILITY].ORG_OVBK_OPENINGS ELSE 0 END AS [Overbook Openings Available]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL THEN [AVAILABILITY].ORG_REG_OPENINGS ELSE 0 END AS [Regular Openings Unavailable]
	      ,CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL THEN [AVAILABILITY].ORG_OVBK_OPENINGS ELSE 0 END AS [Overbook Openings Unavailable]

    FROM  CLARITY.dbo.V_AVAILABILITY [AVAILABILITY]
		
    WHERE  [AVAILABILITY].APPT_NUMBER = 0
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) >= @slotstartdate
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) <  @slotenddate

    ORDER BY [AVAILABILITY].DEPARTMENT_ID
           , [AVAILABILITY].PROV_ID
	       , [AVAILABILITY].SLOT_BEGIN_TIME
		   , [AVAILABILITY].UNAVAILABLE_RSN_C
		   , [AVAILABILITY].UNAVAILABLE_RSN_NAME
		     OFFSET 0 ROWS

) slot
LEFT OUTER JOIN
(
    SELECT
	       [AVAILABILITY].DEPARTMENT_ID
		  ,[AVAILABILITY].PROV_ID
		  ,[AVAILABILITY].SLOT_BEGIN_TIME
		  ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Regular Openings Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Overbook Openings Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Regular Outside Template Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Overbook Outside Template Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Regular Openings Available Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Overbook Openings Available Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Regular Openings Unavailable Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'N' THEN 1 ELSE 0 END) AS [Overbook Openings Unavailable Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Regular Outside Template Available Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Overbook Outside Template Available Booked]
		  ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'N' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Regular Outside Template Unavailable Booked]
	      ,SUM(CASE WHEN [AVAILABILITY].UNAVAILABLE_RSN_C IS NOT NULL AND [AVAILABILITY].APPT_OVERBOOK_YN = 'Y' AND [AVAILABILITY].OUTSIDE_TEMPLATE_YN = 'Y' THEN 1 ELSE 0 END) AS [Overbook Outside Template Unavailable Booked]

    FROM  CLARITY.dbo.V_AVAILABILITY [AVAILABILITY]
		
    WHERE  [AVAILABILITY].APPT_NUMBER > 0
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) >= @slotstartdate
    AND CAST(CAST([AVAILABILITY].SLOT_BEGIN_TIME AS DATE) AS DATETIME) <  @slotenddate

    GROUP BY [AVAILABILITY].DEPARTMENT_ID
           , [AVAILABILITY].PROV_ID
	       , [AVAILABILITY].SLOT_BEGIN_TIME

    ORDER BY [AVAILABILITY].DEPARTMENT_ID
           , [AVAILABILITY].PROV_ID
	       , [AVAILABILITY].SLOT_BEGIN_TIME
		     OFFSET 0 ROWS

) appt
ON ((appt.DEPARTMENT_ID = slot.DEPARTMENT_ID)
    AND (appt.PROV_ID = slot.PROV_ID)
	AND (appt.SLOT_BEGIN_TIME = slot.SLOT_BEGIN_TIME)
	  )
) util

LEFT OUTER JOIN
(
SELECT
	PROV_ID,
	CAST(LAST_ACCS_DATETIME AS DATE) AS LAST_ACCS_DATE
FROM CLARITY.dbo.CLARITY_EMP
) emp
ON emp.PROV_ID = util.PROV_ID

LEFT OUTER JOIN
(
SELECT
	PROV_ID,
	PROV_NAME,
	STAFF_RESOURCE_C,
	STAFF_RESOURCE,
	PROVIDER_TYPE_C,
	PROV_TYPE
FROM CLARITY.dbo.CLARITY_SER
) ser
ON ser.PROV_ID = util.PROV_ID

WHERE ser.STAFF_RESOURCE = 'Resource' -- Include non-Person resource types; non-Person providers will not have a LAST_ACCS_DATE value
OR CAST(util.SLOT_BEGIN_TIME AS DATE) <= emp.LAST_ACCS_DATE -- Exclude templates with slot dates after a provider's last Epic access

GROUP BY util.DEPARTMENT_ID
       , util.PROV_ID
	   , emp.LAST_ACCS_DATE
	   , ser.PROV_NAME
	   , ser.STAFF_RESOURCE_C
	   , ser.STAFF_RESOURCE
	   , ser.PROVIDER_TYPE_C
	   , ser.PROV_TYPE
	   , CAST(util.SLOT_BEGIN_TIME AS DATE)
	   , util.UNAVAILABLE_RSN_C
	   , util.UNAVAILABLE_RSN_NAME

ORDER BY CAST(util.SLOT_BEGIN_TIME AS DATE)
       , util.DEPARTMENT_ID
       , util.PROV_ID
	   , util.UNAVAILABLE_RSN_C

  -- Create index for temp table #utilsum

CREATE UNIQUE CLUSTERED INDEX IX_utilsum ON #utilsum ([SLOT_BEGIN_DATE], [DEPARTMENT_ID], [PROV_ID], [UNAVAILABLE_RSN_C])

SELECT DISTINCT
       DEPARTMENT_ID
	 , PROV_ID
INTO #util
FROM #utilsum

  -- Create index for temp table #util

  CREATE NONCLUSTERED INDEX IX_util ON #util ([DEPARTMENT_ID], [PROV_ID])

SELECT util.DEPARTMENT_ID
     , util.PROV_ID
     , dt.day_date
	 , dt.fmonth_num
	 , dt.Fyear_num
	 , dt.FYear_name
INTO #utildatetable
FROM #util util
CROSS JOIN #datetable dt

  -- Create index for temp table #utildatetable
  
  CREATE NONCLUSTERED INDEX IX_utildatetable ON #utildatetable ([day_date], [DEPARTMENT_ID], [PROV_ID])

---------------------------------------------------------------------------------------
INSERT Stage.AmbOptHuddle_Dash_Unavail_Rsn_Slot_Utilization
           (event_type
           ,event_count
           ,event_date
           ,fmonth_num
           ,Fyear_num
           ,FYear_name
           ,report_period
           ,report_date
           ,event_category
           ,pod_name
           ,hub_id
           ,hub_name
           ,epic_department_id
           ,epic_department_name
           ,epic_department_name_external
           ,provider_id
           ,provider_name
           ,service_line_id
           ,service_line
           ,prov_service_line_id
           ,prov_service_line
           ,sub_service_line_id
           ,sub_service_line
           ,opnl_service_id
           ,opnl_service_name
           ,corp_service_line_id
           ,corp_service_line
           ,hs_area_id
           ,hs_area_name
           ,prov_hs_area_id
           ,prov_hs_area_name
           ,Regular_Openings
           ,Overbook_Openings
           ,Openings_Booked
           ,Regular_Openings_Available
           ,Regular_Openings_Unavailable
           ,Overbook_Openings_Available
           ,Overbook_Openings_Unavailable
           ,Regular_Openings_Booked
           ,Overbook_Openings_Booked
           ,Regular_Outside_Template_Booked
           ,Overbook_Outside_Template_Booked
           ,Regular_Openings_Available_Booked
           ,Overbook_Openings_Available_Booked
           ,Regular_Openings_Unavailable_Booked
           ,Overbook_Openings_Unavailable_Booked
           ,Regular_Outside_Template_Available_Booked
           ,Overbook_Outside_Template_Available_Booked
           ,Regular_Outside_Template_Unavailable_Booked
           ,Overbook_Outside_Template_Unavailable_Booked
           ,STAFF_RESOURCE_C
           ,STAFF_RESOURCE
           ,PROVIDER_TYPE_C
           ,PROV_TYPE
           ,rev_location_id
           ,rev_location
           ,financial_division_id
           ,financial_division_name
           ,financial_sub_division_id
           ,financial_sub_division_name
           ,som_group_id
           ,som_group_name
           ,som_department_id
           ,som_department_name
           ,som_division_id
           ,som_division_name
           ,BUSINESS_UNIT
		   ,som_hs_area_id
		   ,som_hs_area_name
           ,SUBLOC_ID
           ,SUBLOC_NAME
		   ,app_flag
		   ,UNAVAILABLE_RSN_C
		   ,UNAVAILABLE_RSN_NAME
		   ,AMB_Scorecard_Flag
		   ,organization_id
		   ,organization_name
		   ,service_id
		   ,service_name
		   ,clinical_area_id
		   ,clinical_area_name
		   ,ambulatory_flag
		   ,community_health_flag
           ,SnapShot_Dt
		   )
SELECT
	tabrptg.event_type,
    tabrptg.event_count,
    tabrptg.event_date,
    tabrptg.fmonth_num,
    tabrptg.Fyear_num,
    tabrptg.FYear_name,
    tabrptg.report_period,
    tabrptg.report_date,
	tabrptg.event_category AS event_category,
    tabrptg.pod_name,
    tabrptg.hub_id,
    tabrptg.hub_name,
    tabrptg.epic_department_id,
    tabrptg.epic_department_name,
    tabrptg.epic_department_name_external,
    tabrptg.provider_id,
    tabrptg.provider_name,
    tabrptg.SERVICE_LINE_ID,
    tabrptg.SERVICE_LINE,
    tabrptg.prov_service_line_id,
    tabrptg.prov_service_line,
    tabrptg.SUB_SERVICE_LINE_ID,
    tabrptg.SUB_SERVICE_LINE,
    tabrptg.OPNL_SERVICE_ID,
    tabrptg.OPNL_SERVICE_NAME,
    tabrptg.CORP_SERVICE_LINE_ID,
    tabrptg.CORP_SERVICE_LINE,
    tabrptg.HS_AREA_ID,
    tabrptg.HS_AREA_NAME,
    tabrptg.prov_hs_area_id,
    tabrptg.prov_hs_area_name,
    tabrptg.[Regular Openings],
    tabrptg.[Overbook Openings],
    tabrptg.[Openings Booked],
    tabrptg.[Regular Openings Available],
    tabrptg.[Regular Openings Unavailable],
    tabrptg.[Overbook Openings Available],
    tabrptg.[Overbook Openings Unavailable],
    tabrptg.[Regular Openings Booked],
    tabrptg.[Overbook Openings Booked],
    tabrptg.[Regular Outside Template Booked],
    tabrptg.[Overbook Outside Template Booked],
    tabrptg.[Regular Openings Available Booked],
    tabrptg.[Overbook Openings Available Booked],
    tabrptg.[Regular Openings Unavailable Booked],
    tabrptg.[Overbook Openings Unavailable Booked],
    tabrptg.[Regular Outside Template Available Booked],
    tabrptg.[Overbook Outside Template Available Booked],
    tabrptg.[Regular Outside Template Unavailable Booked],
    tabrptg.[Overbook Outside Template Unavailable Booked],
    tabrptg.STAFF_RESOURCE_C,
    tabrptg.STAFF_RESOURCE,
    tabrptg.PROVIDER_TYPE_C,
    tabrptg.PROV_TYPE,
    tabrptg.financial_division_id,
    tabrptg.financial_division_name,
    tabrptg.financial_sub_division_id,
    tabrptg.financial_sub_division_name,
    tabrptg.rev_location_id,
    tabrptg.rev_location,
    tabrptg.som_group_id,
    tabrptg.som_group_name,
    tabrptg.som_department_id,
    tabrptg.som_department_name,
    tabrptg.som_division_id,
    tabrptg.som_division_name,
    tabrptg.BUSINESS_UNIT,
    tabrptg.som_hs_area_id,
    tabrptg.som_hs_area_name,
    tabrptg.SUBLOC_ID,
    tabrptg.SUBLOC_NAME,
	CASE WHEN tabrptg.PROV_TYPE IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST','LICENSED CLINICAL SOCIAL WORKER') THEN 1 ELSE 0 END AS app_flag,
	tabrptg.UNAVAILABLE_RSN_C,
	tabrptg.UNAVAILABLE_RSN_NAME,
	CASE WHEN UNAVAILABLE_RSN_C IN
(
 1 -- PTO/Sick/Vacation
,4 --	CTT ONLY - Holiday
,9 --	CME/MOC
,14 -- CTT ONLY - Attending/Precepting
,24 -- Facility Issue
,131 --	CTT ONLY - Template Optimization  
, 21 -- CTT ONLY - Grand Rounds
, 23 -- Protected Leave
, 135 -- CTT ONLY - Remote Outreach Clinic
, 136 -- CTT ONLY - Add-On Session
, 138 -- CTT ONLY - Approved Meetings
, 139 -- Scheduled Inpatient Shifts
)
			   THEN 0 -- Not included in denominator calculation
			   ELSE 1 -- Included in denominator calculation
	END AS AMB_Scorecard_Flag,
	tabrptg.organization_id, -- INTEGER
	tabrptg.organization_name, -- VARCHAR(50)
	tabrptg.service_id, -- INTEGER
	tabrptg.service_name, -- VARCHAR(150)
	tabrptg.clinical_area_id, -- INTEGER
	tabrptg.clinical_area_name, -- VARCHAR(150)
	tabrptg.ambulatory_flag, -- INTEGER
	tabrptg.community_health_flag, -- INTEGER
	CAST(GETDATE() AS DATETIME) AS SnapShot_Dt -- DATETIME

FROM
(
SELECT 
       CAST('Slot Utilization' AS VARCHAR(50)) AS event_type
      ,CASE WHEN util.SLOT_BEGIN_DATE IS NOT NULL THEN 1
            ELSE 0
       END AS event_count
      ,date_dim.day_date AS event_date
      ,date_dim.fmonth_num
      ,date_dim.Fyear_num
      ,date_dim.FYear_name
      ,CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
      ,CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date
	  ,CAST('Today' AS VARCHAR(150)) AS event_category
      ,mdm.PFA_POD AS pod_name
	  ,mdm.HUB_ID AS hub_id
	  ,mdm.HUB AS hub_name
      ,date_dim.DEPARTMENT_ID AS epic_department_id
      ,mdm.EPIC_DEPT_NAME AS epic_department_name
      ,mdm.EPIC_EXT_NAME AS epic_department_name_external
	  ,date_dim.PROV_ID AS provider_id
      ,sersrc.Prov_Nme AS provider_name
      ,mdm.service_line_id
      ,mdm.service_line
	  ,physcn.Service_Line_ID AS prov_service_line_id
	  ,physcn.Service_Line AS prov_service_line
      ,mdm.sub_service_line_id
      ,mdm.sub_service_line
      ,mdm.opnl_service_id
      ,mdm.opnl_service_name
      ,mdm.corp_service_line_id
      ,mdm.corp_service_line
      ,mdm.hs_area_id
      ,mdm.hs_area_name
	  ,physcn.hs_area_id AS prov_hs_area_id
	  ,physcn.hs_area_name AS prov_hs_area_name
	  ,util.[Regular Openings]
	  ,util.[Overbook Openings]
	  ,util.NUM_APTS_SCHEDULED
	  ,util.[Openings Booked]
	  ,util.[Regular Openings Available]
	  ,util.[Regular Openings Unavailable]
	  ,util.[Overbook Openings Available]
	  ,util.[Overbook Openings Unavailable]
	  ,util.[Regular Openings Booked]
	  ,util.[Overbook Openings Booked]
	  ,util.[Regular Outside Template Booked]
	  ,util.[Overbook Outside Template Booked]
	  ,util.[Regular Openings Available Booked]
	  ,util.[Overbook Openings Available Booked]
	  ,util.[Regular Openings Unavailable Booked]
	  ,util.[Overbook Openings Unavailable Booked]
      ,util.[Regular Outside Template Available Booked]
	  ,util.[Overbook Outside Template Available Booked]
      ,util.[Regular Outside Template Unavailable Booked]
	  ,util.[Overbook Outside Template Unavailable Booked]
	  ,util.STAFF_RESOURCE_C
	  ,util.STAFF_RESOURCE
	  ,COALESCE(ptot.PROV_TYPE_OT_C, util.PROVIDER_TYPE_C, NULL) AS PROVIDER_TYPE_C
	  ,COALESCE(ptot.PROV_TYPE_OT_NAME, util.PROV_TYPE, NULL) AS PROV_TYPE
	  ,mdm.LOC_ID AS rev_location_id
	  ,mdm.REV_LOC_NAME AS rev_location
	  --,TRY_CAST(ser.RPT_GRP_SIX AS INT) AS financial_division_id
	  --,CAST(dvsn.Epic_Financial_Division AS VARCHAR(150)) AS financial_division_name
	  --,TRY_CAST(ser.RPT_GRP_EIGHT AS INT) AS financial_sub_division_id
	  --,CAST(dvsn.Epic_Financial_SubDivision AS VARCHAR(150)) AS financial_sub_division_name
	  --,sersrc.Financial_Division AS financial_division_id
	  ,TRY_CAST(sersrc.Financial_Division AS INT) AS financial_division_id
	  ,sersrc.Financial_Division_Name AS financial_division_name
	  --,sersrc.Financial_SubDivision AS financial_sub_division_id
	  ,TRY_CAST(sersrc.Financial_SubDivision AS INT) AS financial_sub_division_id
	  ,sersrc.Financial_SubDivision_Name AS financial_sub_division_name
	  ,dvsn.som_group_id
	  ,dvsn.som_group_name
	  ,dvsn.Department_ID AS som_department_id
  	  ,CAST(dvsn.Department AS VARCHAR(150)) AS som_department_name
	  ,CAST(dvsn.Org_Number AS INT) AS som_division_id
	  ,CAST(dvsn.Organization AS VARCHAR(150)) AS som_division_name
	  ,dvsn.som_hs_area_id
	  ,dvsn.som_hs_area_name
	  ,mdm.BUSINESS_UNIT
	  ,supp.SUBLOC_ID
	  ,supp.SUBLOC_NAME
	  ,util.UNAVAILABLE_RSN_C
	  ,util.UNAVAILABLE_RSN_NAME 
	  ,o.organization_id
	  ,COALESCE(o.[organization_name], 'No Organization Assigned') organization_name
	  ,s.service_id
	  ,COALESCE(s.[service_name], 'No Service Assigned') service_name
	  ,c.clinical_area_id
	  ,COALESCE(c.[clinical_area_name], 'No Clinical Area Assigned') clinical_area_name
	  ,g.ambulatory_flag
	  ,g.community_health_flag

FROM
    #utildatetable AS date_dim
LEFT OUTER JOIN
(
    SELECT DISTINCT
           main.DEPARTMENT_ID AS epic_department_id
          ,main.PROV_ID AS provider_id
		  ,main.PROV_NAME
		  ,main.STAFF_RESOURCE_C
		  ,main.STAFF_RESOURCE
		  ,main.PROVIDER_TYPE_C
		  ,main.PROV_TYPE
--Select
          ,main.SLOT_BEGIN_DATE
		  ,main.UNAVAILABLE_RSN_C
		  ,main.UNAVAILABLE_RSN_NAME
	      ,main.[Regular Openings]
	      ,main.[Overbook Openings]
		  ,main.NUM_APTS_SCHEDULED
	      ,main.[Openings Booked]
	      ,main.[Regular Openings Available]
	      ,main.[Regular Openings Unavailable]
	      ,main.[Overbook Openings Available]
	      ,main.[Overbook Openings Unavailable]
	      ,main.[Regular Openings Booked]
	      ,main.[Overbook Openings Booked]
	      ,main.[Regular Outside Template Booked]
	      ,main.[Overbook Outside Template Booked]
	      ,main.[Regular Openings Available Booked]
	      ,main.[Overbook Openings Available Booked]
	      ,main.[Regular Openings Unavailable Booked]
	      ,main.[Overbook Openings Unavailable Booked]
          ,main.[Regular Outside Template Available Booked]
	      ,main.[Overbook Outside Template Available Booked]
          ,main.[Regular Outside Template Unavailable Booked]
	      ,main.[Overbook Outside Template Unavailable Booked]

    FROM
        #utilsum AS main -- main

) util
ON  ((date_dim.day_date = CAST(util.SLOT_BEGIN_DATE AS SMALLDATETIME))
     AND ((date_dim.DEPARTMENT_ID = util.epic_department_id)
	      AND (date_dim.PROV_ID = util.provider_id)))

LEFT OUTER JOIN CLARITY_App.Stage.AmbOpt_Excluded_Department excl
ON excl.DEPARTMENT_ID = date_dim.DEPARTMENT_ID
LEFT OUTER JOIN CLARITY_App.Rptg.vwCLARITY_SER_OT_PROV_TYPE ptot
ON date_dim.PROV_ID = ptot.PROV_ID AND date_dim.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
LEFT OUTER JOIN
(
    SELECT ROW_NUMBER() OVER (PARTITION BY EPIC_DEPARTMENT_ID ORDER BY mdm_LM.MDM_BATCH_ID DESC) AS Seq
	      ,PFA_POD
	      ,HUB_ID
	      ,HUB
          ,[EPIC_DEPARTMENT_ID]
          ,[EPIC_DEPT_NAME]
          ,[EPIC_EXT_NAME]
          ,[LOC_ID]
          ,[REV_LOC_NAME]
          ,service_line_id
          ,service_line
          ,sub_service_line_id
          ,sub_service_line
          ,opnl_service_id
          ,opnl_service_name
          ,corp_service_line_id
          ,corp_service_line
          ,hs_area_id
          ,hs_area_name
		  ,BUSINESS_UNIT
	FROM
    (
        SELECT DISTINCT
	           PFA_POD
	          ,HUB_ID
	          ,HUB
              ,[EPIC_DEPARTMENT_ID]
              ,[EPIC_DEPT_NAME]
              ,[EPIC_EXT_NAME]
              ,[LOC_ID]
              ,[REV_LOC_NAME]
              ,service_line_id
              ,service_line
              ,sub_service_line_id
              ,sub_service_line
              ,opnl_service_id
              ,opnl_service_name
              ,corp_service_line_id
              ,corp_service_line
              ,hs_area_id
              ,hs_area_name
			  ,BUSINESS_UNIT
			  ,MDM_BATCH_ID
	    FROM CLARITY_App.Rptg.vwRef_MDM_Location_Master_History) mdm_LM
) AS mdm
ON (mdm.EPIC_DEPARTMENT_ID = date_dim.DEPARTMENT_ID)
AND mdm.Seq = 1

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN CLARITY_App.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.PROV_ID = date_dim.PROV_ID

                -- --------------------------------------
                -- Financial Division               --
                -- --------------------------------------
				LEFT OUTER JOIN
				(
				    SELECT
					    PROV_ID,
                        Prov_Nme,
                        Financial_Division,
                        Financial_Division_Name,
                        Financial_SubDivision,
                        Financial_SubDivision_Name
					FROM CLARITY_App.Rptg.vwDim_Clrt_SERsrc) sersrc
                    ON (sersrc.PROV_ID = date_dim.PROV_ID)

                -- -------------------------------------
                -- SOM Financial Division Subdivision--
                -- -------------------------------------
				LEFT OUTER JOIN
				(
				    SELECT
					    Epic_Financial_Division_Code,
						Epic_Financial_Division,
                        Epic_Financial_Subdivision_Code,
						Epic_Financial_Subdivision,
                        Department,
                        Department_ID,
                        Organization,
                        Org_Number,
                        som_group_id,
                        som_group_name,
						som_hs_area_id,
						som_hs_area_name
					FROM CLARITY_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
						ON dvsn.Epic_Financial_Subdivision_Code = sersrc.Financial_SubDivision

                -- --------------------------------------
                -- Supplemental Dept Subloc --
                -- --------------------------------------
				LEFT OUTER JOIN
				(
				    SELECT
					    Department_Name,
                        Department_ID,
                        SUBLOC_ID,
						SUBLOC_NAME
					FROM CLARITY_App.Rptg.vwRef_MDM_Supplemental_Dept_Subloc) supp
                    ON (supp.Department_ID = date_dim.DEPARTMENT_ID)

				LEFT JOIN CLARITY_App.[Mapping].Epic_Dept_Groupers				g			ON g.epic_department_id =	date_dim.DEPARTMENT_ID
				LEFT JOIN CLARITY_App.[Mapping].Ref_Clinical_Area_Map				c			ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
				LEFT JOIN CLARITY_App.[Mapping].Ref_Service_Map					s			ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
				LEFT JOIN CLARITY_App.[Mapping].Ref_Organization_Map				o			ON o.organization_id = s.organization_id  

WHERE
      ((date_dim.day_date >= @slotstartdate) AND (date_dim.day_date < @slotenddate))
      AND excl.DEPARTMENT_ID IS NULL
) tabrptg

WHERE
	tabrptg.event_count = 1 -- event_dates with SLOT_BEGIN_DATEs

ORDER BY
	tabrptg.epic_department_name,
	tabrptg.provider_name,
	tabrptg.event_date

GO


