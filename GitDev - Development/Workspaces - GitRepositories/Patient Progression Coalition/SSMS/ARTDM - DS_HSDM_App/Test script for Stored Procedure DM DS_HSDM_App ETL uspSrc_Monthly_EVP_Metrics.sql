USE [DS_HSDM_APP]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--CREATE PROCEDURE [ETL].[uspSrc_Monthly_EVP_Metrics]
--AS
/***************************************************************************************************************************************************************************************************************************
WHAT: Monthly EVP Reporting
WHO : CA/OA
WHEN: 9/1/2025
WHY : Aggregate metrics into a single table for selected data portal metrics to source Tableau workbook used for monthly email to the EVP
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
INFO:                  
      INPUTS:   [Metric list below]
                
      OUTPUTS:  DS_HSDM_APP.TabRptg.Dash_Monthly_EVP_Metrics
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Metric_ID	Data_Portal					Metric_Name					TabRptg_Table_Name
15			Balanced Scorecard			Voluntary Turnover			Dash_AmbOpt_EmpTurnover_Tiles
21			Balanced Scorecard			Operating Margin			Dash_BalancedScorecard_FinMetrics_Tiles
81			Balanced Scorecard			30-Day Readmission Rate		Dash_BalancedScorecard_Readmissions_Tiles
192			Balanced Scorecard			Mortality Rate				Dash_BalancedScorecard_MortalityRate_Tiles
439			Balanced Scorecard			Length of Stay (LOS)		Dash_BalancedScorecard_Vizient_LOS_Tiles
458			Ambulatory					New Patient Access in 14	Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles
496			Daily Huddle				OR TAT						Dash_Periop_Scorecard_ORTAT_Tiles 
526			Ambulatory Optimization		Unique Patients Served		Dash_AmbOpt_ScheduledAppointmentMetric_Tiles
650			Balanced Scorecard			Mortality Index				Dash_BalancedScorecard_MortalityIndex_CH_Tiles
721			Balanced Scorecard			Mortality					Dash_BalancedScorecard_MortalityRateVizient_Tiles
755			Balanced Scorecard			CMI - Epic					Dash_BalancedScorecard_CMI_CDI_Tiles
818			Balanced Scorecard			Mortality Index Vizient		Dash_BalancedScorecard_MortalityIndexVizient_Tiles
943			Periop Scorecard			OR Utilization				Dash_Periop_Scorecard_OR_Utilization_Tiles
794			Patient Progression				Transfer Acceptance		Dash_PatientProgression_ExternalTransferStatus_Tiles
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
MODS: 	9/1/2025 - DM2NB - Addition of Readmission metric
        9/2/2025 - DM2NB - Addition of Two month prior start and end dates-needed when metric is a daily metric-prior value will use two month dates, current value will use prev dates
        9/3/2025 - DM2NB - Removed two month prior logic since the summary won't be sent until after data portal refresh so standard current and previous ranges will suffice
        9/4/2025 - NKK3U - Addition of CMI metric
        9/4/2025 - NKK3U - Addition of Operating Margin metric
		9/5/2025 - YSM2KX - Addition of New Patient Access in 14 (id 458) and Ambulatory Patients New to UVA Health (id 526)
        9/8/2025 - YSM2KX - Addition of Third Next Available (id 461). Also converted 458,526, and 461 to use organization instead of hs area. Corrected formatting and labels for 458
****************************************************************************************************************************************************************************************************************************/

SET NOCOUNT ON

IF OBJECT_ID('tempdb..#ADT ') IS NOT NULL
DROP TABLE #ADT

IF OBJECT_ID('tempdb..#mi794 ') IS NOT NULL
DROP TABLE #mi794

SELECT 
	  CAST('Incoming Transfer Request' AS VARCHAR(50))	AS event_type
	, CAST('Intake Request Completed' AS VARCHAR(150))	AS event_category
	, CASE WHEN xt.Disposition = 'Completed' THEN 1 ELSE 0 END AS event_count
	, dd.day_date AS event_date
	, dd.fmonth_num
	, dd.FYear_num
	, dd.FYear_name
	, CAST(LEFT(DATENAME(MM, dd.day_date), 3) + ' ' + CAST(DAY(dd.day_date) AS VARCHAR(2)) AS VARCHAR(10))			AS report_period
	, CAST(CAST(dd.day_date AS DATE) AS SMALLDATETIME)																AS report_date
	, [TransferID] AS event_id
	, xt.DestinationUnitID AS epic_department_id
	, mdm.epic_department_name AS epic_department_name
	, mdm.epic_department_name_external AS epic_department_external
	, CAST(CASE WHEN FLOOR((CAST(dd.day_date AS INTEGER) 
							- CAST(xt.PatientDOB AS INTEGER)
							) / 365.25
							) < 18 THEN
					1
				ELSE
					0
				END AS SMALLINT)																					AS peds
	, CAST(xt.PatientDOB AS DATE) AS person_birth_date
	, CAST(xt.PatientMR AS INT) AS person_id
	, pat.Name AS person_name
	, xt.AcceptingMD_ID AS provider_id
	, xt.Accepting_MD AS provider_name
      ,[AdmissionCSN] AS PAT_ENC_CSN_ID
      ,[EntryTime]
      ,[AcctNbrint]
      ,[TierLevel]
      ,[Isolation]
      ,[referringProviderName]
      ,[Referring_Facility]
      ,[TransferReason]
      ,[TransferMode]
      ,[Diagnosis]
      ,[ServiceNme]
      ,[LevelOfCare]
	  ,xt.TransferTypeHx
      ,[PlacementStatusName]
      ,[XTPlacementStatusName]
      ,[XTPlacementStatusDateTime]
      ,[ETA]
      ,[PatientReferredTo]
      ,[AdtPatientFacilityID]
      ,[AdtPatientFacility]
      ,[BedAssigned]
      ,[BedType]
      ,[DispositionReason]
      ,[Disposition] AS Transfer_Center_Request_Status
      ,[Accepting_Timestamp]
      ,[Accepting_MD]
      ,[AcceptingMD_ServiceLine]
      ,[CloseTime]
      ,[PatientType]
      ,[ProtocolNme]
      ,xt.[Load_Dtm]
	  ,o.organization_name
	  ,s.service_name
	  ,c.clinical_area_name
	  ,pat.sk_Dim_Pt
	  ,pat.Sex AS person_gender
	  ,mdm.hs_area_id
	  ,mdm.hs_area_name
	  ,mdm.LOC_ID AS rev_location_id
	  ,mdm.REV_LOC_NAME AS rev_location

 INTO #ADT

  FROM [DS_HSDM_Prod].[Rptg].[ADT_TransferCenter_ExternalTransfers] xt
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date dd
  ON dd.day_date = CAST(xt.EntryTime AS DATE)
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient pat							ON	pat.MRN_display = xt.PatientMR
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc mdm		ON	xt.DestinationUnitID = mdm.epic_department_id
  LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON xt.DestinationUnitID = g.epic_department_id
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id

  WHERE 1=1
    AND TransferTypeID = 1 -- REQUEST_TYPE_MAPPING_C = 1,	REQUEST_TYPE_MAPPING_NAME = Transfer, 	REQUEST_TYPE_C = 2026,	REQUEST_TYPE_NAME = Incoming Transfer
	AND xt.TransferTypeHx = 'Incoming Transfer' -- 'Medical Intrafacility Transfer'
	
	AND CAST(EntryTime AS DATE) >= '7/1/2025'
	AND CAST(EntryTime AS DATE) <=  '7/31/2025'

SELECT
	*
FROM #ADT
ORDER BY
	event_count DESC,
	event_date
/*
SELECT 
       tile.event_count
      ,tile.event_date
      ,tile.epic_department_id
      ,tile.epic_department_name
      ,tile.fmonth_num
      ,tile.fyear_num
      ,tile.fyear_name
      ,tile.peds
      ,tile.transplant
      ,tile.person_birth_date
      ,tile.person_gender
      ,tile.person_id
      ,tile.person_name
      ,tile.provider_id
      ,tile.provider_name
      ,tile.Load_Dtm
      ,tile.w_som_department_id
      ,tile.w_som_department_name
      ,tile.w_som_division_id
      ,tile.w_som_division_name
      ,tile.w_hs_area_id
      ,tile.w_hs_area_name
      ,o.[organization_id]
      ,s.[service_id]
      ,s.[service_src]
      ,c.[clinical_area_id]
      ,c.[clinical_area_src]
      ,COALESCE(o.[organization_name], 'No Organization Assigned') organization_name
      ,COALESCE(s.[service_name], 'No Service Assigned') service_name
      ,COALESCE(c.[clinical_area_name], 'No Clinical Area Assigned') clinical_area_name
      ,g.[ambulatory_flag]
      ,g.[childrens_flag]
      ,g.[mc_operation_flag]
      ,g.[inpatient_adult_flag]
      ,g.[community_health_flag] 
      ,g.[serviceline_division_flag]
      ,Reporting_Period_Enddate = (SELECT MAX(Reporting_Period_Enddate) FROM 
      etl.Data_Portal_Metrics_Master WHERE metric_id = 794)

INTO #mi794

FROM
    [DS_HSDM_App].[TabRptg].[Dash_PatientProgression_ExternalTransfers_Tiles] tile
    LEFT JOIN [DS_HSDM_App].[Mapping].Epic_Dept_Groupers g ON tile.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id
WHERE
    tile.event_type = 'Incoming Transfer Request' AND
    event_date <= (SELECT MAX([Reporting_Period_Enddate]) 
           FROM [DS_HSDM_App].[ETL].Data_Portal_Metrics_Master 
           WHERE metric_id = 794);
*/
/*
WITH metrics --pull relevant items from the data lineage tables per metric
AS (SELECT dol.Metric_ID
          ,dol.Metric_Name
          ,dol.Metric_Sub_Name
          ,vdd.month_begin_date                             AS Reporting_Period_Startdate      --EVP metrics will be for the current month--provide appropriate start and end dates for data viz
          ,dol.Reporting_Period_Enddate
          ,DATEADD(MONTH, -1, vdd.month_begin_date)         AS Prev_Reporting_Period_Startdate --need prior month for comparison of current performance against previous
          ,vdde.month_end_date                              AS Prev_Reporting_Period_Enddate
          ,dpol.TabRptg_Table_Name
    FROM ETL.Data_Portal_Object_Lineage                AS dpol
        LEFT OUTER JOIN ETL.Data_Portal_Metrics_Master AS dol
            ON dol.sk_Data_Portal_Object_Lineage = dpol.sk_Data_Portal_Object_Lineage
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date   AS vdd
            ON vdd.day_date = dol.Reporting_Period_Enddate
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date   AS vdde
            ON vdde.day_date = DATEADD(MONTH, -1, dol.Reporting_Period_Enddate)
        CROSS APPLY
    ( --fy--find starting date of fiscal year
        SELECT TOP (1)
               vdd.day_date
        FROM DS_HSDW_Prod.Rptg.vwDim_Date AS vdd
        WHERE vdd.Fyear_num =
        (   --find FY of reporting_period_enddate
            SELECT TOP (1)
                   ddte.Fyear_num
            FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
            WHERE CAST(reporting_period_enddate AS DATE) = ddte.day_date
            ORDER BY ddte.Fyear_num
        )
        ORDER BY vdd.day_date
    )                                                  AS fy
    WHERE dol.Metric_ID IN ( 21, 818, 192, 439, 81, 943, 496, 755, 526, 15, 650, 721, 458, 461 ))


    ,calc_type --pull relevant items from the metadata table per metric
AS (SELECT metric_id
          ,multiplier
          ,name     AS Metric_Name
          ,sub_name AS Metric_Sub_Name
    FROM DS_HSDM_APP.DataPortal.Descriptive_Metadata
    WHERE metric_id IN ( 21, 818, 192, 439, 81, 943, 496, 755, 526, 15, 650, 721, 458, 461 )
          AND active_flag = 1)

    ,targets --pull relevant items from the targets table per metric
AS (SELECT Dash_Targets.metric_id
          ,Dash_Targets.portal_level
          ,Dash_Targets.portal_level_name
          ,Dash_Targets.portal_level_id
          ,Dash_Targets.comparator
          ,COALESCE(Dash_Targets.DAR_target, Dash_Targets.target_)      AS target_
          ,COALESCE(Dash_Targets.threshold, Dash_Targets.DAR_threshold) AS threshold
          ,Dash_Targets.stretch
          ,Dash_Targets.precision_
          ,Dash_Targets.format
          ,Dash_Targets.suffix
          ,Dash_Targets.NA
          ,Dash_Targets.TBD
          ,Dash_Targets.fyear
          ,Dash_Targets.current_
    FROM DS_HSDM_APP.TabRptg.Dash_Targets              AS Dash_Targets
        INNER JOIN metrics                             AS m
            ON m.Metric_ID = Dash_Targets.metric_id
        LEFT OUTER JOIN ETL.Data_Portal_Metrics_Master AS dol
            ON dol.Metric_ID = Dash_Targets.metric_id
    WHERE Dash_Targets.metric_id IN ( 21, 818, 192, 439, 81, 943, 496, 755, 526, 15, 650, 721, 458, 461 )
          AND Dash_Targets.fyear =
          (
              SELECT TOP (1)
                     ddte.Fyear_num
              FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
              WHERE CAST(m.Reporting_Period_Enddate AS DATE) = CAST(ddte.day_date AS DATE)
              ORDER BY ddte.Fyear_num
          ))
*/
/*
--metric 818 uva mc mortality index
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mi.Mort_Viz), 0)          AS numerator       --in case of null return zero
      ,COALESCE(SUM(mi.EXPECTEDMORTALITY), 0) AS denominator
      ,'deaths'                               AS numerator_title --match to front tile
      ,'expected deaths'                      AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mi.Mort_Viz), 0) = 0
                AND COALESCE(SUM(mi.EXPECTEDMORTALITY), 0) = 0
           THEN 0
           ELSE CAST((COALESCE(SUM(mi.Mort_Viz), 0) / (COALESCE(SUM(mi.EXPECTEDMORTALITY), 0) * 1.)) AS NUMERIC(18, 2))
       END                                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                   AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Vizient'                              AS benchmark_agency
      ,'Inpatient Quality'                    AS metric_topic
      ,mi.w_hs_area_id
      ,mi.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                           AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_MortalityIndexVizient_Tiles AS mi
    CROSS APPLY metrics                                         AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mi.w_hs_area_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_mi.w_hs_area_id
              ,prev_mi.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mi.Mort_Viz), 0) = 0
                        AND COALESCE(SUM(prev_mi.EXPECTEDMORTALITY), 0) = 0
                   THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mi.Mort_Viz), 0) / (COALESCE(SUM(prev_mi.EXPECTEDMORTALITY), 0) * 1.)) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_MortalityIndexVizient_Tiles AS prev_mi
            CROSS APPLY metrics                                         AS pm
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mi.w_hs_area_id
        WHERE prev_mi.RISKTYPECODE = 18 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 818
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_mi.w_hs_area_id
                ,prev_mi.w_hs_area_name
                ,pt.target_
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mi.w_hs_area_id
WHERE mi.RISKTYPECODE = 18 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 818
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
      AND mi.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,mi.w_hs_area_id
        ,mi.w_hs_area_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
UNION ALL
--metric 650 ch individual aggregates
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(ch_mi.event_count), 0)   AS numerator       --in case of null return zero
      ,COALESCE(SUM(ch_mi.mort_expected), 0) AS denominator
      ,'deaths'                              AS numerator_title --match to front tile
      ,'expected deaths'                     AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(ch_mi.event_count), 0) = 0
                AND COALESCE(SUM(ch_mi.mort_expected), 0) = 0
           THEN 0
           ELSE CAST((COALESCE(SUM(ch_mi.event_count), 0) / (COALESCE(SUM(ch_mi.mort_expected), 0) * 1.)) AS NUMERIC(18, 2))
       END                                   AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                  AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Premier'                             AS benchmark_agency
      ,'Inpatient Quality'                   AS metric_topic
      ,ch_mi.w_hs_area_id
      ,ch_mi.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                          AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_MortalityIndex_CH_Tiles AS ch_mi
    CROSS APPLY metrics                                     AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_name = ch_mi.w_hs_area_name
           AND t.portal_level <> 'uva_health'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_mi.w_hs_area_id
              ,prev_mi.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mi.event_count), 0) = 0
                        AND COALESCE(SUM(prev_mi.mort_expected), 0) = 0
                   THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mi.event_count), 0) / (COALESCE(SUM(prev_mi.mort_expected), 0) * 1.)) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_MortalityIndex_CH_Tiles AS prev_mi
            CROSS APPLY metrics                                     AS pm
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_name = prev_mi.w_hs_area_name
                   AND pt.portal_level <> 'uva_health'
        WHERE pm.Metric_ID = 650
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_mi.w_hs_area_id
                ,prev_mi.w_hs_area_name
                ,pt.target_
    )                         AS prior_value
        ON prior_value.w_hs_area_id = ch_mi.w_hs_area_id
WHERE m.Metric_ID = 650
      AND ch_mi.event_date >= m.Reporting_Period_Startdate
      AND ch_mi.event_date <= m.Reporting_Period_Enddate
      AND ch_mi.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,ch_mi.w_hs_area_id
        ,ch_mi.w_hs_area_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
UNION ALL
--metric 650 ch overall aggregate
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(ch_mi_overall.event_count), 0)   AS numerator       --in case of null return zero
      ,COALESCE(SUM(ch_mi_overall.mort_expected), 0) AS denominator
      ,'deaths'                                      AS numerator_title --match to front tile
      ,'expected deaths'                             AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(ch_mi_overall.event_count), 0) = 0
                AND COALESCE(SUM(ch_mi_overall.mort_expected), 0) = 0
           THEN 0
           ELSE CAST((COALESCE(SUM(ch_mi_overall.event_count), 0) / (COALESCE(SUM(ch_mi_overall.mort_expected), 0) * 1.)) AS NUMERIC(18, 2))
       END                                           AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                          AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Premier'                                     AS benchmark_agency
      ,'Inpatient Quality'                           AS metric_topic
      ,NULL
      ,'Community Health'
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                                  AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_MortalityIndex_CH_Tiles AS ch_mi_overall
    CROSS APPLY metrics                                     AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_name = 'Community Health'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mi.event_count), 0) = 0
                        AND COALESCE(SUM(prev_mi.mort_expected), 0) = 0
                   THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mi.event_count), 0) / (COALESCE(SUM(prev_mi.mort_expected), 0) * 1.)) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
              ,pm.Metric_ID
        FROM TabRptg.Dash_BalancedScorecard_MortalityIndex_CH_Tiles AS prev_mi
            CROSS APPLY metrics                                     AS pm
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_name = 'Community Health'
        WHERE pm.Metric_ID = 650
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
              AND prev_mi.w_hs_area_id IS NOT NULL
        GROUP BY pt.target_
                ,pm.Metric_ID
    )                         AS prior_value
        ON ct.metric_id = prior_value.Metric_ID
WHERE m.Metric_ID = 650
      AND ch_mi_overall.event_date >= m.Reporting_Period_Startdate
      AND ch_mi_overall.event_date <= m.Reporting_Period_Enddate
      AND ch_mi_overall.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
        ,prior_value.prior_target
UNION ALL
--metric 721 mortality rate
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mr.event_count), 0)         AS numerator       --in case of null return zero
      ,COALESCE(COUNT(DISTINCT mr.event_id), 0) AS denominator
      ,'deaths'                                 AS numerator_title --match to front tile
      ,'expected deaths'                        AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mr.event_count), 0) = 0
                AND COALESCE(COUNT(DISTINCT mr.event_id), 0) = 0
           THEN 0
           ELSE CAST(((COALESCE(SUM(mr.event_count), 0) * 1.) / COALESCE(COUNT(DISTINCT mr.event_id), 0)) * ct.multiplier AS NUMERIC(18, 2))
       END                                      AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                      AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                               AS benchmark_agency
      ,'Inpatient Quality'                      AS metric_topic
      ,mr.w_hs_area_id
      ,mr.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                             AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_MortalityRateVizient_Tiles AS mr
    CROSS APPLY metrics                                        AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mr.w_hs_area_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_mr.w_hs_area_id
              ,prev_mr.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mr.event_count), 0) = 0
                        AND COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) = 0
                   THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mr.event_count), 0) / (COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_MortalityRateVizient_Tiles AS prev_mr
            CROSS APPLY metrics                                        AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mr.w_hs_area_id
        WHERE pm.Metric_ID = 721
              AND prev_mr.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mr.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_mr.w_hs_area_id
                ,prev_mr.w_hs_area_name
                ,pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mr.w_hs_area_id
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 721
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,mr.w_hs_area_id
        ,mr.w_hs_area_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
        ,ct.multiplier
UNION ALL
--metric 192 ch mortality rate
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mr.event_count), 0)                    AS numerator       --in case of null return zero
      ,COALESCE(COUNT(DISTINCT mr.sk_Fact_Pt_Enc_Clrt), 0) AS denominator
      ,'deaths'                                            AS numerator_title --match to front tile
      ,'expected deaths'                                   AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mr.event_count), 0) = 0
                AND COALESCE(COUNT(DISTINCT mr.sk_Fact_Pt_Enc_Clrt), 0) = 0
           THEN 0
           ELSE CAST(((COALESCE(SUM(mr.event_count), 0) * 1.) / COALESCE(COUNT(DISTINCT mr.sk_Fact_Pt_Enc_Clrt), 0)) * ct.multiplier AS NUMERIC(18, 2))
       END                                                 AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                                 AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Epic'                                              AS benchmark_agency
      ,'Inpatient Quality'                                 AS metric_topic
      ,mr.w_hs_area_id
      ,mr.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                                        AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_MortalityRate_Tiles AS mr
    CROSS APPLY metrics                                 AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mr.w_hs_area_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_mr.w_hs_area_id
              ,prev_mr.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mr.event_count), 0) = 0
                        AND COALESCE(COUNT(DISTINCT prev_mr.sk_Fact_Pt_Enc_Clrt), 0) = 0
                   THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mr.event_count), 0) / (COALESCE(COUNT(DISTINCT prev_mr.sk_Fact_Pt_Enc_Clrt), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_MortalityRate_Tiles AS prev_mr
            CROSS APPLY metrics                                 AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mr.w_hs_area_id
        WHERE pm.Metric_ID = 192
              AND prev_mr.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mr.event_date <= pm.Prev_Reporting_Period_Enddate
              AND prev_mr.w_hs_area_id > 1 --UVAMC uses metric id 721
        GROUP BY prev_mr.w_hs_area_id
                ,prev_mr.w_hs_area_name
                ,pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mr.w_hs_area_id
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 192
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
    AND mr.w_hs_area_id > 1 --UVAMC uses metric id 721
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,mr.w_hs_area_id
        ,mr.w_hs_area_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
        ,ct.multiplier
UNION ALL
--metric 192 overall ch mortality rate
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mr.event_count), 0)             AS numerator       --in case of null return zero
      ,COALESCE(COUNT(DISTINCT mr.AcctNbr_Clrt), 0) AS denominator
      ,'deaths'                                     AS numerator_title --match to front tile
      ,'expected deaths'                            AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mr.event_count), 0) = 0
                AND COALESCE(COUNT(DISTINCT mr.sk_Fact_Pt_Enc_Clrt), 0) = 0
           THEN 0
           ELSE CAST(((COALESCE(SUM(mr.event_count), 0) * 1.) / COALESCE(COUNT(DISTINCT mr.sk_Fact_Pt_Enc_Clrt), 0)) * ct.multiplier AS NUMERIC(18, 2))
       END                                          AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                          AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,''                                           AS benchmark_agency
      ,'Inpatient Quality'                          AS metric_topic
      ,NULL
      ,'Community Health'
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                                 AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_MortalityRate_Tiles AS mr
    CROSS APPLY metrics                                 AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mr.w_hs_area_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mr.event_count), 0) = 0
                        AND COALESCE(COUNT(DISTINCT prev_mr.sk_Fact_Pt_Enc_Clrt), 0) = 0
                   THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mr.event_count), 0) / (COALESCE(COUNT(DISTINCT prev_mr.sk_Fact_Pt_Enc_Clrt), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
              ,pct.metric_id
        FROM TabRptg.Dash_BalancedScorecard_MortalityRate_Tiles AS prev_mr
            CROSS APPLY metrics                                 AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_name = 'Community Health'
        WHERE pm.Metric_ID = 192
              AND prev_mr.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mr.event_date <= pm.Prev_Reporting_Period_Enddate
              AND prev_mr.w_hs_area_id > 1 --UVAMC uses metric id 721
        GROUP BY pt.target_
                ,pct.multiplier
                ,pct.metric_id
    )                         AS prior_value
        ON ct.metric_id = prior_value.metric_id
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 192
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
    AND mr.w_hs_area_id > 1 --UVAMC uses metric id 721
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
        ,ct.multiplier
UNION ALL
--metric 81 readmission rate
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,readm.numerator
      ,readm.denominator
      ,'readmissions'         AS numerator_title --match to front tile
      ,'inpatient discharges' AS denominator_title
      ,CASE
           WHEN readm.denominator = 0
           THEN 0
           ELSE CAST((readm.numerator / (readm.denominator * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                    AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,CASE WHEN prior_value.w_hs_area_id>1 THEN 'Epic' ELSE 'Vizient' END              AS benchmark_agency
      ,'Patient Progression'  AS metric_topic
      ,readm.w_hs_area_id
      ,readm.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator           AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM
( --readm
    SELECT MAX(COALESCE(rsq.numerator, 0))   AS numerator
          ,MAX(COALESCE(rsq.denominator, 0)) AS denominator
          ,rsq.w_hs_area_id
          ,rsq.w_hs_area_name
          ,rsq.Metric_ID
    FROM
    ( --rsq--sub-query to sum per event_type and hs_area
        SELECT CASE
                   WHEN r.event_type = 'readmission'
                   THEN SUM(r.event_count)
                   ELSE 0
               END AS numerator
              ,CASE
                   WHEN r.event_type = 'discharge'
                   THEN SUM(r.event_count)
                   ELSE 0
               END AS denominator
              ,r.w_hs_area_id
              ,r.w_hs_area_name
              ,sm.Metric_ID
        FROM TabRptg.Dash_BalancedScorecard_Readmissions_Tiles AS r
            CROSS APPLY metrics                                AS sm
        WHERE sm.Metric_ID = 81
              AND r.event_date >= sm.Reporting_Period_Startdate
              AND r.event_date <= sm.Reporting_Period_Enddate
        GROUP BY r.w_hs_area_id
                ,r.w_hs_area_name
                ,r.event_type
                ,sm.Metric_ID
    ) AS rsq
    GROUP BY rsq.w_hs_area_id
            ,rsq.w_hs_area_name
            ,rsq.Metric_ID
)                       AS readm
    CROSS APPLY metrics AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = readm.w_hs_area_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_readm.w_hs_area_id
              ,prev_readm.w_hs_area_name
              ,CASE
                   WHEN prev_readm.denominator = 0
                   THEN 0
                   ELSE CAST((prev_readm.numerator / (prev_readm.denominator * 1.)) * ct.multiplier AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM
        ( --prev_readm
            SELECT MAX(COALESCE(sq.numerator, 0))   AS numerator
                  ,MAX(COALESCE(sq.denominator, 0)) AS denominator
                  ,sq.w_hs_area_id
                  ,sq.w_hs_area_name
                  ,sq.Metric_ID
            FROM
            ( --sq--sub-query to sum per event_type and hs_area
                SELECT CASE
                           WHEN r.event_type = 'readmission'
                           THEN SUM(r.event_count)
                           ELSE 0
                       END AS numerator
                      ,CASE
                           WHEN r.event_type = 'discharge'
                           THEN SUM(r.event_count)
                           ELSE 0
                       END AS denominator
                      ,r.w_hs_area_id
                      ,r.w_hs_area_name
                      ,sm.Metric_ID
                FROM TabRptg.Dash_BalancedScorecard_Readmissions_Tiles AS r
                    CROSS APPLY metrics                                AS sm
                WHERE sm.Metric_ID = 81
                      AND r.event_date >= sm.Prev_Reporting_Period_Startdate
                      AND r.event_date <= sm.Prev_Reporting_Period_Enddate
                GROUP BY r.w_hs_area_id
                        ,r.w_hs_area_name
                        ,r.event_type
                        ,sm.Metric_ID
            ) AS sq
            GROUP BY sq.w_hs_area_id
                    ,sq.w_hs_area_name
                    ,sq.Metric_ID
        )                             AS prev_readm
            LEFT OUTER JOIN targets   AS pt
                ON prev_readm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_readm.w_hs_area_id
            LEFT OUTER JOIN calc_type AS ct
                ON pt.metric_id = ct.metric_id
        WHERE prev_readm.Metric_ID = 81
    )                         AS prior_value
        ON prior_value.w_hs_area_id = readm.w_hs_area_id
WHERE m.Metric_ID = 81
      AND readm.w_hs_area_id IS NOT NULL
      AND t.portal_level NOT IN ( 'uva_health', 'organization' ) --extra entries in the target tool causing duplication of rows
UNION ALL
--metric 81 overall CH readmission rate
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,readm.numerator
      ,readm.denominator
      ,'readmissions'         AS numerator_title --match to front tile
      ,'inpatient discharges' AS denominator_title
      ,CASE
           WHEN readm.denominator = 0
           THEN 0
           ELSE CAST((readm.numerator / (readm.denominator * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                    AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Epic'              AS benchmark_agency
      ,'Patient Progression'  AS metric_topic
      ,NULL
      ,'Community Health'
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator           AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM
( --readm
    SELECT MAX(COALESCE(rsq.numerator, 0))   AS numerator
          ,MAX(COALESCE(rsq.denominator, 0)) AS denominator
          ,rsq.Metric_ID
    FROM
    ( --rsq--sub-query to sum per event_type and hs_area
        SELECT CASE
                   WHEN r.event_type = 'readmission'
                   THEN SUM(r.event_count)
                   ELSE 0
               END AS numerator
              ,CASE
                   WHEN r.event_type = 'discharge'
                   THEN SUM(r.event_count)
                   ELSE 0
               END AS denominator
              ,sm.Metric_ID
        FROM TabRptg.Dash_BalancedScorecard_Readmissions_Tiles AS r
            CROSS APPLY metrics                                AS sm
        WHERE sm.Metric_ID = 81
              AND r.event_date >= sm.Reporting_Period_Startdate
              AND r.event_date <= sm.Reporting_Period_Enddate
              AND r.w_hs_area_id > 1
        GROUP BY r.event_type
                ,sm.Metric_ID
    ) AS rsq
    GROUP BY rsq.Metric_ID
)                       AS readm
    CROSS APPLY metrics AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_name = 'Community Health'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT CASE
                   WHEN prev_readm.denominator = 0
                   THEN 0
                   ELSE CAST((prev_readm.numerator / (prev_readm.denominator * 1.)) * ct.multiplier AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
              ,prev_readm.Metric_ID
        FROM
        ( --prev_readm
            SELECT MAX(COALESCE(sq.numerator, 0))   AS numerator
                  ,MAX(COALESCE(sq.denominator, 0)) AS denominator
                  ,sq.Metric_ID
            FROM
            ( --sq--sub-query to sum per event_type and hs_area
                SELECT CASE
                           WHEN r.event_type = 'readmission'
                           THEN SUM(r.event_count)
                           ELSE 0
                       END AS numerator
                      ,CASE
                           WHEN r.event_type = 'discharge'
                           THEN SUM(r.event_count)
                           ELSE 0
                       END AS denominator
                      ,sm.Metric_ID
                FROM TabRptg.Dash_BalancedScorecard_Readmissions_Tiles AS r
                    CROSS APPLY metrics                                AS sm
                WHERE sm.Metric_ID = 81
                      AND r.event_date >= sm.Prev_Reporting_Period_Startdate
                      AND r.event_date <= sm.Prev_Reporting_Period_Enddate
                      AND r.w_hs_area_id > 1
                GROUP BY r.event_type
                        ,sm.Metric_ID
            ) AS sq
            GROUP BY sq.Metric_ID
        )                             AS prev_readm
            LEFT OUTER JOIN targets   AS pt
                ON prev_readm.Metric_ID = pt.metric_id
                   AND pt.portal_level_name = 'Community Health'
            LEFT OUTER JOIN calc_type AS ct
                ON pt.metric_id = ct.metric_id
        WHERE prev_readm.Metric_ID = 81
    )                         AS prior_value
        ON prior_value.Metric_ID = m.Metric_ID
WHERE m.Metric_ID = 81
UNION ALL
--metric 755 CMI
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mr.CMI), 0)                 AS numerator       --in case of null return zero
      ,COALESCE(COUNT(DISTINCT mr.event_id), 0) AS denominator
      ,'DRG Weights         '                   AS numerator_title --match to front tile
      ,'Inpatient Discharges'                   AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mr.CMI), 0) = 0
                AND COALESCE(COUNT(DISTINCT mr.event_id), 0) = 0
           THEN 0
           ELSE CAST(((COALESCE(SUM(mr.CMI), 0) * 1.0) / COALESCE(COUNT(DISTINCT mr.event_id), 0))  AS NUMERIC(18, 2))
       END                                      AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                     AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                               AS benchmark_agency
      ,'Inpatient Quality'                      AS metric_topic
      ,mr.w_hs_area_id
      ,mr.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                             AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_CMI_CDI_Tiles AS mr
    CROSS APPLY metrics                           AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mr.w_hs_area_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_mr.w_hs_area_id
              ,prev_mr.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mr.CMI), 0) = 0
                        AND COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) = 0
                   THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mr.CMI), 0) / (COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) * 1.))  AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_CMI_CDI_Tiles AS prev_mr
            CROSS APPLY metrics                           AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mr.w_hs_area_id
        WHERE pm.Metric_ID = 755
              AND prev_mr.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mr.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_mr.w_hs_area_id
                ,prev_mr.w_hs_area_name
                ,pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mr.w_hs_area_id
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 755
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
	AND mr.hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,mr.w_hs_area_id
        ,mr.w_hs_area_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
        ,ct.multiplier
UNION ALL
--metric 21 Operating Margin
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(mr.OM_P, 0)                 AS numerator       --in case of null return zero
      ,1 AS denominator
      ,'Operating Margin'                   AS numerator_title --match to front tile
      ,''                             AS denominator_title
      , CAST(COALESCE(mr.OM_P, 0) * 100 AS NUMERIC(18, 2)) AS  Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                   AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                            AS benchmark_agency
      ,'Finance'                             AS metric_topic
      ,mr.w_hs_area_id
      ,mr.w_hs_area_name
      ,CAST(COALESCE(mr.OM_T, 0) * 100 AS NUMERIC(18, 2)) AS target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                             AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_FinMetrics_Tiles AS mr
    CROSS APPLY metrics                           AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mr.w_hs_area_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_mr.w_hs_area_id
              ,prev_mr.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(prev_mr.OM_P, 0) = 0                        
                   THEN 0
                   ELSE CAST((COALESCE(prev_mr.OM_P, 0) * 100) AS NUMERIC(18, 2))
               END        AS prior_value
              ,CAST(COALESCE(prev_mr.OM_T, 0) * 100 AS NUMERIC(18, 2)) AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_FinMetrics_Tiles AS prev_mr
            CROSS APPLY metrics                           AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mr.w_hs_area_id
        WHERE pm.Metric_ID = 21
              AND prev_mr.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mr.event_date <= pm.Prev_Reporting_Period_Enddate
			  AND prev_mr.event_type = 'Operating Margin'
			  AND prev_mr.hs_area_id = 1
			  AND prev_mr.OM_P <> 0
			  AND prev_mr.OM_P IS NOT NULL
        GROUP BY prev_mr.w_hs_area_id
                ,prev_mr.w_hs_area_name
                ,prev_mr.OM_P
				,prev_mr.OM_T
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mr.w_hs_area_id
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 21
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
	AND mr.event_type = 'Operating Margin'
	AND mr.hs_area_id = 1
	AND mr.OM_P <> 0
    AND mr.OM_P IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
		,mr.OM_P
		,mr.OM_T
        ,prior_value.prior_value
        ,mr.w_hs_area_id
        ,mr.w_hs_area_name
        ,prior_value.prior_target
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
        ,ct.multiplier        
UNION ALL


--metric 458 New Patient Access within 14 Days
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mi.event_count), 0)          AS numerator       --in case of null return zero
      ,COALESCE(COUNT(*), 0) AS denominator
      ,'Access in 14 Days'                               AS numerator_title --match to front tile
      ,'Appointments'                      AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mi.event_count), 0) = 0
                AND COALESCE(COUNT(*), 0) = 0
           THEN 0
           ELSE CAST((COALESCE(SUM(mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                  AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Vizient'                              AS benchmark_agency
      ,'Ambulatory Access'                    AS metric_topic
      ,o.organization_id AS w_hs_area_id
      ,o.organization_name AS w_hs_area_name -- using organization instead of hs area for ambulatory
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                           AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles mi 
	INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers g ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
	INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                         AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = o.organization_id
		   AND t.portal_level = 'organization' -- using organization instead of hs area for ambulatory
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_o.organization_id  -- using organization instead of hs area for ambulatory
              ,prev_o.organization_name 
			  ,CASE --calc specific to producing front tile #
				   WHEN COALESCE(SUM(prev_mi.event_count), 0) = 0
						AND COALESCE(COUNT(*), 0) = 0
				   THEN 0
				   ELSE CAST((COALESCE(SUM(prev_mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
				END      AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles AS prev_mi 
			INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers prev_g ON prev_mi.epic_department_id = prev_g.epic_department_id
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map prev_c on prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map prev_s on prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map prev_o on prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                         AS pm
			LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_s.organization_id
				    AND pt.portal_level = 'organization' -- using organization instead of hs area for ambulatory
        WHERE prev_g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 458
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_o.organization_id  -- using organization instead of hs area for ambulatory
				,prev_o.organization_name
                ,pt.target_
				,pct.multiplier
    )                         AS prior_value
        ON prior_value.organization_id = o.organization_id
WHERE g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 458
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
      AND mi.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,o.organization_id
        ,o.organization_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
		,ct.multiplier

UNION ALL

--metric 526 New Unique patients to UVA
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(COUNT(DISTINCT mi.person_id), 0)          AS numerator       --in case of null return zero
      , 0 AS denominator
      ,'New Patients'                               AS numerator_title --match to front tile
      ,'N/A'                      AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(COUNT(DISTINCT mi.person_id), 0) = 0
           THEN 0
           ELSE CAST(COALESCE(COUNT(DISTINCT mi.person_id), 0) AS NUMERIC(18, 2))
       END                                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                   AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                              AS benchmark_agency
      ,'Ambulatory Access'                    AS metric_topic
      ,o.organization_id AS w_hs_area_id
      ,o.organization_name AS w_hs_area_name -- using organization instead of hs area for ambulatory
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                           AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles mi 
	INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers g ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
	INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                         AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = o.organization_id
		   AND t.portal_level = 'organization' -- using organization instead of hs area for ambulatory
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_o.organization_id  -- using organization instead of hs area for ambulatory
              ,prev_o.organization_name 
			  ,CASE --calc specific to producing front tile #
				   WHEN COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) = 0
				   THEN 0
				   ELSE CAST(COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) AS NUMERIC(18, 2))
				END      AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles AS prev_mi 
			INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers prev_g ON prev_mi.epic_department_id = prev_g.epic_department_id
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map prev_c on prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map prev_s on prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map prev_o on prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                         AS pm
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_s.organization_id
				    AND pt.portal_level = 'organization' -- using organization instead of hs area for ambulatory
        WHERE prev_g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
			  AND prev_mi.VIS_NEW_TO_SYS_YN = 1 --any necessary filters applied to match those present in the workbook
			  AND (prev_mi.appt_event_completed = 1 or prev_mi.appt_event_arrived = 1) --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 526
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_o.organization_id
                ,prev_o.organization_name
                ,pt.target_
    )                         AS prior_value
        ON prior_value.organization_id = o.organization_id
WHERE g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
	  AND mi.VIS_NEW_TO_SYS_YN = 1 --any necessary filters applied to match those present in the workbook
	  AND (mi.appt_event_completed = 1 or mi.appt_event_arrived = 1) --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 526
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
      AND mi.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,o.organization_id
        ,o.organization_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD

UNION ALL

--metric 461 Third Next Available
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(AVG(mi.days_wait*1.0), 0)          AS numerator       --in case of null return zero
      , 0 AS denominator
      ,'3rd Next Available Business Days'                               AS numerator_title --match to front tile
      ,'N/A'                      AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(mi.days_wait*1.0), 0)  = 0
           THEN 0
           ELSE CAST(COALESCE(AVG(mi.days_wait*1.0), 0)  AS NUMERIC(18, 2))
       END                                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                   AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'MGMA'                              AS benchmark_agency
      ,'Ambulatory Access'                    AS metric_topic
      ,o.organization_id AS w_hs_area_id
      ,o.organization_name AS w_hs_area_name -- using organization instead of hs area for ambulatory
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                           AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles mi 
	INNER JOIN DS_HSDW_Prod.dbo.Dim_Date dd ON dd.day_date = mi.event_date --need to filter out weekends
	INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers g ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
	INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                         AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = o.organization_id
		   AND t.portal_level = 'organization' -- using organization instead of hs area for ambulatory
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_o.organization_id  -- using organization instead of hs area for ambulatory
              ,prev_o.organization_name 
			  ,CASE --calc specific to producing front tile #
				   WHEN COALESCE(AVG(prev_mi.days_wait*1.0), 0)  = 0
				   THEN 0
				   ELSE CAST(COALESCE(AVG(prev_mi.days_wait*1.0), 0)  AS NUMERIC(18, 2))
				END      AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles AS prev_mi 
			INNER JOIN DS_HSDW_Prod.dbo.Dim_Date prev_dd ON prev_dd.day_date = prev_mi.event_date --need to filter out weekends
			INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers prev_g ON prev_mi.epic_department_id = prev_g.epic_department_id
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map prev_c on prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map prev_s on prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map prev_o on prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                         AS pm
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_s.organization_id
				    AND pt.portal_level = 'organization' -- using organization instead of hs area for ambulatory
        WHERE prev_g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
			  AND prev_mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
			  AND prev_mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
			  AND prev_dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 461
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_o.organization_id
                ,prev_o.organization_name
                ,pt.target_
    )                         AS prior_value
        ON prior_value.organization_id = o.organization_id
WHERE g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
	  AND mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
	  AND mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
	  AND dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 461
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
      AND mi.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,o.organization_id
        ,o.organization_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD

UNION ALL
*/
/*
--metric 794 Transfer Acceptance
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(AVG(mi.days_wait*1.0), 0)          AS numerator       --in case of null return zero
      , 0 AS denominator
      ,'External Transfers Not Accepted'                               AS numerator_title --match to front tile
      ,'N/A'                      AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(mi.days_wait*1.0), 0)  = 0
           THEN 0
           ELSE CAST(COALESCE(AVG(mi.days_wait*1.0), 0)  AS NUMERIC(18, 2))
       END                                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                   AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                             AS benchmark_agency
      ,'Patient Progression'                    AS metric_topic
      ,o.organization_id AS w_hs_area_id
      ,o.organization_name AS w_hs_area_name -- using organization instead of hs area for ambulatory
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                           AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_App.TabRptg.Dash_PatientProgression_ExternalTransferStatus_Tiles mi 
	INNER JOIN DS_HSDW_Prod.dbo.Dim_Date dd ON dd.day_date = mi.event_date --need to filter out weekends
	INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers g ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
	INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                         AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = o.organization_id
		   AND t.portal_level = 'organization' -- using organization instead of hs area for ambulatory
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_o.organization_id  -- using organization instead of hs area for ambulatory
              ,prev_o.organization_name 
			  ,CASE --calc specific to producing front tile #
				   WHEN COALESCE(AVG(prev_mi.days_wait*1.0), 0)  = 0
				   THEN 0
				   ELSE CAST(COALESCE(AVG(prev_mi.days_wait*1.0), 0)  AS NUMERIC(18, 2))
				END      AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles AS prev_mi 
			INNER JOIN DS_HSDW_Prod.dbo.Dim_Date prev_dd ON prev_dd.day_date = prev_mi.event_date --need to filter out weekends
			INNER JOIN	DS_HSDM_App.Mapping.Epic_Dept_Groupers prev_g ON prev_mi.epic_department_id = prev_g.epic_department_id
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map prev_c on prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map prev_s on prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
			INNER JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map prev_o on prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                         AS pm
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_s.organization_id
				    AND pt.portal_level = 'organization' -- using organization instead of hs area for ambulatory
        WHERE prev_g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
			  AND prev_mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
			  AND prev_mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
			  AND prev_dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 461
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_o.organization_id
                ,prev_o.organization_name
                ,pt.target_
    )                         AS prior_value
        ON prior_value.organization_id = o.organization_id
WHERE g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
	  AND mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
	  AND mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
	  AND dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 461
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
      AND mi.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,o.organization_id
        ,o.organization_name
        ,prior_value.prior_target
        ,t.target_
        ,t.threshold
        ,t.portal_level
        ,t.portal_level_name
        ,t.portal_level_id
        ,t.comparator
        ,t.precision_
        ,t.NA
        ,t.TBD
*/
GO


