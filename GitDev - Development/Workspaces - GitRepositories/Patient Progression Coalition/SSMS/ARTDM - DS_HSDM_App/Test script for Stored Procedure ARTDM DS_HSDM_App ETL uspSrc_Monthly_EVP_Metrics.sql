USE [DS_HSDM_APP];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

--ALTER PROCEDURE [ETL].[uspSrc_Monthly_EVP_Metrics]
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
Metric_ID	Data_Portal					Metric_Name						TabRptg_Table_Name
21			Balanced Scorecard			Operating Margin				Dash_BalancedScorecard_FinMetrics_Tiles
81			Balanced Scorecard			30-Day Readmission Rate			Dash_BalancedScorecard_Readmissions_Tiles
192			Balanced Scorecard			Mortality Rate					Dash_BalancedScorecard_MortalityRate_Tiles
439			Balanced Scorecard			Length of Stay (LOS)			Dash_BalancedScorecard_Vizient_LOS_Tiles
458			Ambulatory Optimization		New Patients Appts				Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles
461			Ambulatory Optimization		Established Patient Access		Dash_AmbOpt_AppointmentAvailability_Tiles
526			Ambulatory Optimization		Unique Patients Served			Dash_AmbOpt_ScheduledAppointmentMetric_Tiles
528			Ambulatory Optimization		Overall Turnover				Dash_AmbOpt_EmpTurnover_Tiles
595			Balanced Scorecard			Length of Stay (LOS)			Dash_BalancedScorecard_LOS_CH_Tiles
650			Balanced Scorecard			Mortality Index					Dash_BalancedScorecard_MortalityIndex_CH_Tiles
721			Balanced Scorecard			Mortality						Dash_BalancedScorecard_MortalityRateVizient_Tiles
755			Balanced Scorecard			CMI - Epic						Dash_BalancedScorecard_CMI_CDI_Tiles
800			Periop Scorecard			Turnaround Time					Dash_Periop_Scorecard_ORTAT_Tiles 
818			Balanced Scorecard			Mortality Index					Vizient	Dash_BalancedScorecard_MortalityIndexVizient_Tiles
870			Ambulatory Optimization		Overall Turnover (CH)			Dash_AmbOpt_EmpTurnover_CH_Tiles
943			Periop Scorecard			OR Utilization					Dash_Periop_Scorecard_OR_Utilization_Tiles
1097		Patient Progression				Transfers Not Accepted		Dash_PatientProgression_ExternalTransferStatus_Tiles
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
MODS: 	9/1/2025 - DM2NB - Addition of Readmission metric
        9/2/2025 - DM2NB - Addition of Two month prior start and end dates-needed when metric is a daily metric-prior value will use two month dates, current value will use prev dates
        9/3/2025 - DM2NB - Removed two month prior logic since the summary won't be sent until after data portal refresh so standard current and previous ranges will suffice
        9/4/2025 - NKK3U - Addition of CMI metric
        9/4/2025 - NKK3U - Addition of Operating Margin metric
        9/5/2025 - YSM2KX - Addition of New Patient Access in 14 (id 458) and Ambulatory Patients New to UVA Health (id 526)
        9/8/2025 - YSM2KX - Addition of Third Next Available (id 461). Also converted 458,526, and 461 to use organization instead of hs area. Corrected formatting and labels for 458
        9/17/2025 - KLS9AC - Addition of Length of Stay (LOS) (id 439)
        9/17/2025 - KLS9AC - Addition of Length of Stay CH (LOS) (id 595)
        9/24/2025 - DM2NB - Addition of calculations for more levels: UVA Health, UVAMC, Community Health, CP, HM, PW; add OR util rate
        9/25/2025 - DM2NB - Cleaned up metric id lists; addition of overall turnover
        9/25/2025 - DM2NB - Addition of overall turnover for CH; include metric 595 in CTEs
 		9/29/2025 - NKK3U - Addition of Operating Margin Dollars metric; left Metric 21 in the code in case they also want OM percentage   
		10/13/2025 - TMB4F - Addition of External Transfer Requests Not Accepted for UVA-MC (id 1097)
****************************************************************************************************************************************************************************************************************************/

SET NOCOUNT ON;

WITH metrics --pull relevant items from the data lineage tables per metric
AS (SELECT dol.Metric_ID
          ,dol.Metric_Name
          ,dol.Metric_Sub_Name
          ,vdd.month_begin_date                     AS Reporting_Period_Startdate      --EVP metrics will be for the current month--provide appropriate start and end dates for data viz
          ,dol.Reporting_Period_Enddate
          ,DATEADD(MONTH, -1, vdd.month_begin_date) AS Prev_Reporting_Period_Startdate --need prior month for comparison of current performance against previous
          ,vdde.month_end_date                      AS Prev_Reporting_Period_Enddate
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
    WHERE dol.Metric_ID IN ( 21, 81, 192, 439, 458, 461, 526, 528, 595, 650, 721, 755, 800, 818, 870, 943, 1082, 794 ))
    ,calc_type --pull relevant items from the metadata table per metric
AS (SELECT metric_id
          ,multiplier
          ,name     AS Metric_Name
          ,sub_name AS Metric_Sub_Name
    FROM DS_HSDM_APP.DataPortal.Descriptive_Metadata
    WHERE metric_id IN ( 21, 81, 192, 439, 458, 461, 526, 528, 595, 650, 721, 755, 800, 818, 870, 943, 1082, 794 )
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
    WHERE Dash_Targets.metric_id IN ( 21, 81, 192, 439, 458, 461, 526, 528, 595, 650, 721, 755, 800, 818, 870, 943, 1082, 794 )
          AND Dash_Targets.fyear =
          (
              SELECT TOP (1)
                     ddte.Fyear_num
              FROM DS_HSDW_Prod.Rptg.vwDim_Date AS ddte
              WHERE CAST(m.Reporting_Period_Enddate AS DATE) = CAST(ddte.day_date AS DATE)
              ORDER BY ddte.Fyear_num
          ))
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
                AND COALESCE(SUM(mi.EXPECTEDMORTALITY), 0) = 0 THEN 0
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
                        AND COALESCE(SUM(prev_mi.EXPECTEDMORTALITY), 0) = 0 THEN 0
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
--metric 650 ch mortality index individual aggregates
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
                AND COALESCE(SUM(ch_mi.mort_expected), 0) = 0 THEN 0
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
                        AND COALESCE(SUM(prev_mi.mort_expected), 0) = 0 THEN 0
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
--metric 650 ch mortality index overall aggregate
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
                AND COALESCE(SUM(ch_mi_overall.mort_expected), 0) = 0 THEN 0
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
                        AND COALESCE(SUM(prev_mi.mort_expected), 0) = 0 THEN 0
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
                AND COALESCE(COUNT(DISTINCT mr.event_id), 0) = 0 THEN 0
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
                        AND COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) = 0 THEN 0
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
                AND COALESCE(COUNT(DISTINCT mr.sk_Fact_Pt_Enc_Clrt), 0) = 0 THEN 0
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
                        AND COALESCE(COUNT(DISTINCT prev_mr.sk_Fact_Pt_Enc_Clrt), 0) = 0 THEN 0
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
                AND COALESCE(COUNT(DISTINCT mr.sk_Fact_Pt_Enc_Clrt), 0) = 0 THEN 0
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
                        AND COALESCE(COUNT(DISTINCT prev_mr.sk_Fact_Pt_Enc_Clrt), 0) = 0 THEN 0
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
           WHEN readm.denominator = 0 THEN 0
           ELSE CAST((readm.numerator / (readm.denominator * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                    AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,CASE
           WHEN prior_value.w_hs_area_id > 1 THEN 'Epic'
           ELSE 'Vizient'
       END                    AS benchmark_agency
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
                   WHEN r.event_type = 'readmission' THEN SUM(r.event_count)
                   ELSE 0
               END AS numerator
              ,CASE
                   WHEN r.event_type = 'discharge' THEN SUM(r.event_count)
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
                   WHEN prev_readm.denominator = 0 THEN 0
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
                           WHEN r.event_type = 'readmission' THEN SUM(r.event_count)
                           ELSE 0
                       END AS numerator
                      ,CASE
                           WHEN r.event_type = 'discharge' THEN SUM(r.event_count)
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
                      AND r.w_hs_area_id <> 4 --LTACH
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
      AND readm.w_hs_area_id <> 4 --LTACH
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
           WHEN readm.denominator = 0 THEN 0
           ELSE CAST((readm.numerator / (readm.denominator * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                    AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Epic'                 AS benchmark_agency
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
                   WHEN r.event_type = 'readmission' THEN SUM(r.event_count)
                   ELSE 0
               END AS numerator
              ,CASE
                   WHEN r.event_type = 'discharge' THEN SUM(r.event_count)
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
                   WHEN prev_readm.denominator = 0 THEN 0
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
                           WHEN r.event_type = 'readmission' THEN SUM(r.event_count)
                           ELSE 0
                       END AS numerator
                      ,CASE
                           WHEN r.event_type = 'discharge' THEN SUM(r.event_count)
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
--metric 81 overall UVA Health readmission rate
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
           WHEN readm.denominator = 0 THEN 0
           ELSE CAST((readm.numerator / (readm.denominator * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                    AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                    AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Epic'                 AS benchmark_agency
      ,'Patient Progression'  AS metric_topic
      ,NULL
      ,'UVA Health'
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
                   WHEN r.event_type = 'readmission' THEN SUM(r.event_count)
                   ELSE 0
               END AS numerator
              ,CASE
                   WHEN r.event_type = 'discharge' THEN SUM(r.event_count)
                   ELSE 0
               END AS denominator
              ,sm.Metric_ID
        FROM TabRptg.Dash_BalancedScorecard_Readmissions_Tiles AS r
            CROSS APPLY metrics                                AS sm
        WHERE sm.Metric_ID = 81
              AND r.event_date >= sm.Reporting_Period_Startdate
              AND r.event_date <= sm.Reporting_Period_Enddate
              AND r.w_hs_area_id <> 1 --LTACH
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
           AND t.portal_level_name = 'UVA Health'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT CASE
                   WHEN prev_readm.denominator = 0 THEN 0
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
                           WHEN r.event_type = 'readmission' THEN SUM(r.event_count)
                           ELSE 0
                       END AS numerator
                      ,CASE
                           WHEN r.event_type = 'discharge' THEN SUM(r.event_count)
                           ELSE 0
                       END AS denominator
                      ,sm.Metric_ID
                FROM TabRptg.Dash_BalancedScorecard_Readmissions_Tiles AS r
                    CROSS APPLY metrics                                AS sm
                WHERE sm.Metric_ID = 81
                      AND r.event_date >= sm.Prev_Reporting_Period_Startdate
                      AND r.event_date <= sm.Prev_Reporting_Period_Enddate
                      AND r.w_hs_area_id <> 4 --LTACH
                GROUP BY r.event_type
                        ,sm.Metric_ID
            ) AS sq
            GROUP BY sq.Metric_ID
        )                             AS prev_readm
            LEFT OUTER JOIN targets   AS pt
                ON prev_readm.Metric_ID = pt.metric_id
                   AND pt.portal_level_name = 'UVA Health'
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
                AND COALESCE(COUNT(DISTINCT mr.event_id), 0) = 0 THEN 0
           ELSE CAST(((COALESCE(SUM(mr.CMI), 0) * 1.0) / COALESCE(COUNT(DISTINCT mr.event_id), 0)) AS NUMERIC(18, 2))
       END                                      AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                     AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                               AS benchmark_agency
      ,'Resource Management'                    AS metric_topic
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
                        AND COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) = 0 THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mr.CMI), 0) / (COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) * 1.)) AS NUMERIC(18, 2))
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
--metric 755 CMI--overall CH
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
                AND COALESCE(COUNT(DISTINCT mr.event_id), 0) = 0 THEN 0
           ELSE CAST(((COALESCE(SUM(mr.CMI), 0) * 1.0) / COALESCE(COUNT(DISTINCT mr.event_id), 0)) AS NUMERIC(18, 2))
       END                                      AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                     AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                               AS benchmark_agency
      ,'Resource Management'                    AS metric_topic
      ,NULL
      ,'Community Health'
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
           AND t.portal_level_name = 'Community Health'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mr.CMI), 0) = 0
                        AND COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) = 0 THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mr.CMI), 0) / (COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) * 1.)) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
              ,pct.metric_id
        FROM TabRptg.Dash_BalancedScorecard_CMI_CDI_Tiles AS prev_mr
            CROSS APPLY metrics                           AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_name = 'Community Health'
        WHERE pm.Metric_ID = 755
              AND prev_mr.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mr.event_date <= pm.Prev_Reporting_Period_Enddate
              AND prev_mr.w_hs_area_id > 1
        GROUP BY pt.target_
                ,pct.multiplier
                ,pct.metric_id
    )                         AS prior_value
        ON prior_value.metric_id = m.Metric_ID
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 755
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
    AND mr.hs_area_id > 1
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
--metric 755 CMI--overall UVA Health
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
                AND COALESCE(COUNT(DISTINCT mr.event_id), 0) = 0 THEN 0
           ELSE CAST(((COALESCE(SUM(mr.CMI), 0) * 1.0) / COALESCE(COUNT(DISTINCT mr.event_id), 0)) AS NUMERIC(18, 2))
       END                                      AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                     AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                               AS benchmark_agency
      ,'Resource Management'                    AS metric_topic
      ,NULL
      ,'UVA Health'
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
           AND t.portal_level_name = 'UVA Health'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mr.CMI), 0) = 0
                        AND COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) = 0 THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mr.CMI), 0) / (COALESCE(COUNT(DISTINCT prev_mr.event_id), 0) * 1.)) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
              ,pct.metric_id
        FROM TabRptg.Dash_BalancedScorecard_CMI_CDI_Tiles AS prev_mr
            CROSS APPLY metrics                           AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_name = 'UVA Health'
        WHERE pm.Metric_ID = 755
              AND prev_mr.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mr.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
                ,pct.metric_id
    )                         AS prior_value
        ON prior_value.metric_id = m.Metric_ID
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 755
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
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
--metric 21 Operating Margin Percent
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(mr.OM_P, 0)                               AS numerator       --in case of null return zero
      ,0                                                  AS denominator
      ,'Operating Margin'                                 AS numerator_title --match to front tile
      ,'N/A'                                              AS denominator_title
      ,CAST(COALESCE(mr.OM_P, 0) * 100 AS NUMERIC(18, 2)) AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                                AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                                         AS benchmark_agency
      ,'Resource Management'                              AS metric_topic
      ,mr.w_hs_area_id
      ,mr.w_hs_area_name
      ,CAST(COALESCE(mr.OM_T, 0) * 100 AS NUMERIC(18, 2)) AS target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                                       AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_FinMetrics_Tiles AS mr
    CROSS APPLY metrics                              AS m
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
                   WHEN COALESCE(prev_mr.OM_P, 0) = 0 THEN 0
                   ELSE CAST((COALESCE(prev_mr.OM_P, 0) * 100) AS NUMERIC(18, 2))
               END                                                     AS prior_value
              ,CAST(COALESCE(prev_mr.OM_T, 0) * 100 AS NUMERIC(18, 2)) AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_FinMetrics_Tiles AS prev_mr
            CROSS APPLY metrics                              AS pm
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
--metric 1082 Operating Margin Dollars
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(mr.OP_Margin_Dollars_Actual, 0)                 AS numerator       --in case of null return zero
      ,0                                                        AS denominator
      ,'Operating Margin'             AS numerator_title --match to front tile
      ,'N/A'                          AS denominator_title
      , CAST(COALESCE(mr.OP_Margin_Dollars_Actual, 0) AS NUMERIC(18, 2)) AS  Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                  AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                            AS benchmark_agency
      ,'Finance'                             AS metric_topic
      ,mr.w_hs_area_id
      ,mr.w_hs_area_name
      ,CAST(COALESCE(mr.OP_Margin_Dollars_Budget, 0) AS NUMERIC(18, 2)) AS target_
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
                   WHEN COALESCE(prev_mr.OP_Margin_Dollars_Actual, 0) = 0                        
                   THEN 0
                   ELSE CAST((COALESCE(prev_mr.OP_Margin_Dollars_Actual, 0)) AS NUMERIC(18, 2))
               END        AS prior_value
              ,CAST(COALESCE(prev_mr.OP_Margin_Dollars_Budget, 0) AS NUMERIC(18, 2)) AS prior_target
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
			  AND prev_mr.OP_Margin_Dollars_Actual <> 0
			  AND prev_mr.OP_Margin_Dollars_Actual IS NOT NULL
        GROUP BY prev_mr.w_hs_area_id
                ,prev_mr.w_hs_area_name
                ,prev_mr.OP_Margin_Dollars_Actual
				,prev_mr.OP_Margin_Dollars_Budget
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mr.w_hs_area_id
WHERE --any necessary filters applied to match those present in the workbook
    m.Metric_ID = 1082
    AND mr.event_date >= m.Reporting_Period_Startdate
    AND mr.event_date <= m.Reporting_Period_Enddate
	AND mr.event_type = 'Operating Margin'
	AND mr.hs_area_id = 1
	AND mr.OP_Margin_Dollars_Actual <> 0
    AND mr.OP_Margin_Dollars_Actual IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
		,mr.OP_Margin_Dollars_Actual
		,mr.OP_Margin_Dollars_Budget
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
--metric 458 New Patient Access within 14 Days--UVA Health overall
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mi.event_count), 0) AS numerator       --in case of null return zero
      ,COALESCE(COUNT(*), 0)            AS denominator
      ,'Access in 14 Days'              AS numerator_title --match to front tile
      ,'Appointments'                   AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mi.event_count), 0) = 0
                AND COALESCE(COUNT(*), 0) = 0 THEN 0
           ELSE CAST((COALESCE(SUM(mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                              AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                              AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Vizient'                        AS benchmark_agency
      ,'Ambulatory'                     AS metric_topic
      ,NULL                             AS w_hs_area_id
      ,'UVA Health'                     AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                     AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                                  AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 1
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               'UVA Health' AS w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mi.event_count), 0) = 0
                        AND COALESCE(COUNT(*), 0) = 0 THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
               END          AS prior_value
              ,pt.target_   AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                                  AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 1
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.ambulatory_flag = 1
              AND pm.Metric_ID = 458
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_name = 'UVA Health' --overall number includes CH
WHERE g.ambulatory_flag = 1
      AND m.Metric_ID = 458
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        --,mi.w_hs_area_id
        --,mi.w_hs_area_name
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
--metric 458 New Patient Access within 14 Days--UVA MC
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mi.event_count), 0) AS numerator       --in case of null return zero
      ,COALESCE(COUNT(*), 0)            AS denominator
      ,'Access in 14 Days'              AS numerator_title --match to front tile
      ,'Appointments'                   AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mi.event_count), 0) = 0
                AND COALESCE(COUNT(*), 0) = 0 THEN 0
           ELSE CAST((COALESCE(SUM(mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                              AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                              AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Vizient'                        AS benchmark_agency
      ,'Ambulatory'                     AS metric_topic
      ,1                                AS w_hs_area_id
      ,'Medical Center'                 AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                     AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                                  AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 1
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               1          AS w_hs_area_id
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mi.event_count), 0) = 0
                        AND COALESCE(COUNT(*), 0) = 0 THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                                  AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 1
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.mc_operation_flag = 1
              AND pm.Metric_ID = 458
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = 1
WHERE g.mc_operation_flag = 1
      AND m.Metric_ID = 458
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
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
--metric 458 New Patient Access within 14 Days--CH overall
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mi.event_count), 0) AS numerator       --in case of null return zero
      ,COALESCE(COUNT(*), 0)            AS denominator
      ,'Access in 14 Days'              AS numerator_title --match to front tile
      ,'Appointments'                   AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mi.event_count), 0) = 0
                AND COALESCE(COUNT(*), 0) = 0 THEN 0
           ELSE CAST((COALESCE(SUM(mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                              AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                              AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Vizient'                        AS benchmark_agency
      ,'Ambulatory'                     AS metric_topic
      ,NULL                             AS w_hs_area_id
      ,'Community Health'               AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                     AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                                  AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 8
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               'Community Health' AS w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mi.event_count), 0) = 0
                        AND COALESCE(COUNT(*), 0) = 0 THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
               END                AS prior_value
              ,pt.target_         AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                                  AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 8
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.community_health_mpg_flag = 1
              AND pm.Metric_ID = 458
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_name = 'Community Health' --overall number includes CH
WHERE g.community_health_mpg_flag = 1
      AND m.Metric_ID = 458
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        --,mi.w_hs_area_id
        --,mi.w_hs_area_name
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
--metric 458 New Patient Access within 14 Days--CH individual sites
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(SUM(mi.event_count), 0) AS numerator       --in case of null return zero
      ,COALESCE(COUNT(*), 0)            AS denominator
      ,'Access in 14 Days'              AS numerator_title --match to front tile
      ,'Appointments'                   AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(mi.event_count), 0) = 0
                AND COALESCE(COUNT(*), 0) = 0 THEN 0
           ELSE CAST((COALESCE(SUM(mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * ct.multiplier AS NUMERIC(18, 2))
       END                              AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                              AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Vizient'                        AS benchmark_agency
      ,'Ambulatory'                     AS metric_topic
      ,mi.w_hs_area_id
      ,mi.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                     AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                                  AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mi.w_hs_area_id
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               prev_mi.w_hs_area_id
              ,prev_mi.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_mi.event_count), 0) = 0
                        AND COALESCE(COUNT(*), 0) = 0 THEN 0
                   ELSE CAST((COALESCE(SUM(prev_mi.event_count), 0) / (COALESCE(COUNT(*), 0) * 1.)) * pct.multiplier AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                    prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map             prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                   prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map              prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                                  AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mi.w_hs_area_id
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.community_health_mpg_flag = 1
              AND pm.Metric_ID = 458
              AND prev_mi.w_hs_area_id > 1
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
                ,prev_mi.w_hs_area_id
                ,prev_mi.w_hs_area_name
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mi.w_hs_area_id
WHERE g.community_health_mpg_flag = 1
      AND m.Metric_ID = 458
      AND mi.w_hs_area_id > 1
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
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
        ,ct.multiplier
UNION ALL
--metric 526 New Unique patients to UVA--UVA Health overall
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(COUNT(DISTINCT mi.person_id), 0) AS numerator       --in case of null return zero
      ,0                                         AS denominator
      ,'New Patients'                            AS numerator_title --match to front tile
      ,'N/A'                                     AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(COUNT(DISTINCT mi.person_id), 0) = 0 THEN 0
           ELSE CAST(COALESCE(COUNT(DISTINCT mi.person_id), 0) AS NUMERIC(18, 2))
       END                                       AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                      AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                                AS benchmark_agency
      ,'Ambulatory'                              AS metric_topic
      ,NULL                                      AS w_hs_area_id
      ,'UVA Health'                              AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                              AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                               AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 1
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               'UVA Health' AS w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) AS NUMERIC(18, 2))
               END          AS prior_value
              ,pt.target_   AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                               AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 1
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.ambulatory_flag = 1
              AND prev_mi.VIS_NEW_TO_SYS_YN = 1
              AND
              (
                  prev_mi.appt_event_Completed = 1
                  OR prev_mi.appt_event_Arrived = 1
              )
              AND pm.Metric_ID = 526
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_name = 'UVA Health' --overall number includes CH
WHERE g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
      AND mi.VIS_NEW_TO_SYS_YN = 1
      AND
      (
          mi.appt_event_Completed = 1
          OR mi.appt_event_Arrived = 1
      )
      AND m.Metric_ID = 526
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        --,mi.w_hs_area_id
        --,mi.w_hs_area_name
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
--metric 526 New Unique patients to UVA--UVA MC
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(COUNT(DISTINCT mi.person_id), 0) AS numerator       --in case of null return zero
      ,0                                         AS denominator
      ,'New Patients'                            AS numerator_title --match to front tile
      ,'N/A'                                     AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(COUNT(DISTINCT mi.person_id), 0) = 0 THEN 0
           ELSE CAST(COALESCE(COUNT(DISTINCT mi.person_id), 0) AS NUMERIC(18, 2))
       END                                       AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                      AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                                AS benchmark_agency
      ,'Ambulatory'                              AS metric_topic
      ,1                                         AS w_hs_area_id
      ,'Medical Center'                          AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                              AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                               AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 1
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               1          AS w_hs_area_id
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                               AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 1
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.mc_operation_flag = 1
              AND prev_mi.VIS_NEW_TO_SYS_YN = 1
              AND
              (
                  prev_mi.appt_event_Completed = 1
                  OR prev_mi.appt_event_Arrived = 1
              )
              AND pm.Metric_ID = 526
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = 1
WHERE g.mc_operation_flag = 1
      AND mi.VIS_NEW_TO_SYS_YN = 1
      AND
      (
          mi.appt_event_Completed = 1
          OR mi.appt_event_Arrived = 1
      )
      AND m.Metric_ID = 526
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
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
--metric 526 New Unique patients to UVA--CH overall
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(COUNT(DISTINCT mi.person_id), 0) AS numerator       --in case of null return zero
      ,0                                         AS denominator
      ,'New Patients'                            AS numerator_title --match to front tile
      ,'N/A'                                     AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(COUNT(DISTINCT mi.person_id), 0) = 0 THEN 0
           ELSE CAST(COALESCE(COUNT(DISTINCT mi.person_id), 0) AS NUMERIC(18, 2))
       END                                       AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                      AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                                AS benchmark_agency
      ,'Ambulatory'                              AS metric_topic
      ,NULL                                      AS w_hs_area_id
      ,'Community Health'                        AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                              AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                               AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 8
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               'Community Health' AS w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) AS NUMERIC(18, 2))
               END                AS prior_value
              ,pt.target_         AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                               AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 8
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.community_health_mpg_flag = 1
              AND prev_mi.VIS_NEW_TO_SYS_YN = 1
              AND
              (
                  prev_mi.appt_event_Completed = 1
                  OR prev_mi.appt_event_Arrived = 1
              )
              AND pm.Metric_ID = 526
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_name = 'Community Health' --overall number includes CH
WHERE g.community_health_mpg_flag = 1
      AND mi.VIS_NEW_TO_SYS_YN = 1
      AND
      (
          mi.appt_event_Completed = 1
          OR mi.appt_event_Arrived = 1
      )
      AND m.Metric_ID = 526
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        --,mi.w_hs_area_id
        --,mi.w_hs_area_name
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
--metric 526 New Unique patients to UVA--CH individual sites
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(COUNT(DISTINCT mi.person_id), 0) AS numerator       --in case of null return zero
      ,0                                         AS denominator
      ,'New Patients'                            AS numerator_title --match to front tile
      ,'N/A'                                     AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(COUNT(DISTINCT mi.person_id), 0) = 0 THEN 0
           ELSE CAST(COALESCE(COUNT(DISTINCT mi.person_id), 0) AS NUMERIC(18, 2))
       END                                       AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                      AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                                AS benchmark_agency
      ,'Ambulatory'                              AS metric_topic
      ,mi.w_hs_area_id
      ,mi.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                              AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles mi
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                               AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mi.w_hs_area_id
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               prev_mi.w_hs_area_id
              ,prev_mi.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(COUNT(DISTINCT prev_mi.person_id), 0) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_ScheduledAppointmentMetric_Tiles AS prev_mi
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers                 prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map          prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map                prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map           prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                               AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mi.w_hs_area_id
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.community_health_mpg_flag = 1
              AND prev_mi.VIS_NEW_TO_SYS_YN = 1
              AND
              (
                  prev_mi.appt_event_Completed = 1
                  OR prev_mi.appt_event_Arrived = 1
              )
              AND pm.Metric_ID = 526
              AND prev_mi.w_hs_area_id > 1
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
                ,prev_mi.w_hs_area_id
                ,prev_mi.w_hs_area_name
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mi.w_hs_area_id
WHERE g.community_health_mpg_flag = 1
      AND mi.VIS_NEW_TO_SYS_YN = 1
      AND
      (
          mi.appt_event_Completed = 1
          OR mi.appt_event_Arrived = 1
      )
      AND m.Metric_ID = 526
      AND mi.w_hs_area_id > 1
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
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
        ,ct.multiplier
UNION ALL
--metric 461 Third Next Available--UVA Health overall
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(AVG(mi.days_wait * 1.0), 0) AS numerator       --in case of null return zero
      ,0                                    AS denominator
      ,'3rd Next Available Business Days'   AS numerator_title --match to front tile
      ,'N/A'                                AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(mi.days_wait * 1.0), 0) = 0 THEN 0
           ELSE CAST(COALESCE(AVG(mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
       END                                  AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                 AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'MGMA'                               AS benchmark_agency
      ,'Ambulatory'                         AS metric_topic
      ,NULL                                 AS w_hs_area_id
      ,'UVA Health'                         AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                         AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles mi
    INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           dd
        ON dd.day_date = mi.event_date --need to filter out weekends
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                            AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 1
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               'UVA Health' AS w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(AVG(prev_mi.days_wait * 1.0), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(AVG(prev_mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
               END          AS prior_value
              ,pt.target_   AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles AS prev_mi
            INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           prev_dd
                ON prev_dd.day_date = prev_mi.event_date --need to filter out weekends
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                            AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 1
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.ambulatory_flag = 1
              AND prev_mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
              AND prev_mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
              AND prev_dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 461
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_name = 'UVA Health' --overall number includes CH
WHERE g.ambulatory_flag = 1 --any necessary filters applied to match those present in the workbook
      AND mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
      AND mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
      AND dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 461
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        --,mi.w_hs_area_id
        --,mi.w_hs_area_name
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
--metric 461 Third Next Available--UVA MC
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(AVG(mi.days_wait * 1.0), 0) AS numerator       --in case of null return zero
      ,0                                    AS denominator
      ,'3rd Next Available Business Days'   AS numerator_title --match to front tile
      ,'N/A'                                AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(mi.days_wait * 1.0), 0) = 0 THEN 0
           ELSE CAST(COALESCE(AVG(mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
       END                                  AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                 AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'MGMA'                               AS benchmark_agency
      ,'Ambulatory'                         AS metric_topic
      ,1                                    AS w_hs_area_id
      ,'Medical Center'                     AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                         AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles mi
    INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           dd
        ON dd.day_date = mi.event_date --need to filter out weekends
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                            AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 1
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               1          AS w_hs_area_id
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(AVG(prev_mi.days_wait * 1.0), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(AVG(prev_mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles AS prev_mi
            INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           prev_dd
                ON prev_dd.day_date = prev_mi.event_date --need to filter out weekends
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                            AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 1
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.mc_operation_flag = 1
              AND prev_mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
              AND prev_mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
              AND prev_dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 461
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = 1
WHERE g.mc_operation_flag = 1
      AND mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
      AND mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
      AND dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 461
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
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
--metric 461 Third Next Available--CH overall
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(AVG(mi.days_wait * 1.0), 0) AS numerator       --in case of null return zero
      ,0                                    AS denominator
      ,'3rd Next Available Business Days'   AS numerator_title --match to front tile
      ,'N/A'                                AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(mi.days_wait * 1.0), 0) = 0 THEN 0
           ELSE CAST(COALESCE(AVG(mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
       END                                  AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                 AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'MGMA'                               AS benchmark_agency
      ,'Ambulatory'                         AS metric_topic
      ,NULL                                 AS w_hs_area_id
      ,'Community Health'                   AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                         AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles mi
    INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           dd
        ON dd.day_date = mi.event_date --need to filter out weekends
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                            AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 8
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               'Community Health' AS w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(AVG(prev_mi.days_wait * 1.0), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(AVG(prev_mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
               END                AS prior_value
              ,pt.target_         AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles AS prev_mi
            INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           prev_dd
                ON prev_dd.day_date = prev_mi.event_date --need to filter out weekends
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                            AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 8
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.community_health_mpg_flag = 1
              AND prev_mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
              AND prev_mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
              AND prev_dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 461
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_name = 'Community Health' --overall number includes CH
WHERE g.community_health_mpg_flag = 1
      AND mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
      AND mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
      AND dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 461
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        --,mi.w_hs_area_id
        --,mi.w_hs_area_name
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
--metric 461 Third Next Available--CH individual sites
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(AVG(mi.days_wait * 1.0), 0) AS numerator       --in case of null return zero
      ,0                                    AS denominator
      ,'3rd Next Available Business Days'   AS numerator_title --match to front tile
      ,'N/A'                                AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(mi.days_wait * 1.0), 0) = 0 THEN 0
           ELSE CAST(COALESCE(AVG(mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
       END                                  AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                 AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'MGMA'                               AS benchmark_agency
      ,'Ambulatory'                         AS metric_topic
      ,mi.w_hs_area_id
      ,mi.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                         AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles mi
    INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           dd
        ON dd.day_date = mi.event_date --need to filter out weekends
    INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              g
        ON mi.epic_department_id = g.epic_department_id -- needed for Ambulatory Flag Check
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        o
        ON s.organization_id = o.organization_id --this and 2 joins above to get organization
    CROSS APPLY metrics                                            AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = mi.w_hs_area_id
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               prev_mi.w_hs_area_id
              ,prev_mi.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(AVG(prev_mi.days_wait * 1.0), 0) = 0 THEN 0
                   ELSE CAST(COALESCE(AVG(prev_mi.days_wait * 1.0), 0) AS NUMERIC(18, 2))
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_AppointmentAvailability_Tiles AS prev_mi
            INNER JOIN DS_HSDW_Prod.dbo.Dim_Date                           prev_dd
                ON prev_dd.day_date = prev_mi.event_date --need to filter out weekends
            INNER JOIN DS_HSDM_APP.Mapping.Epic_Dept_Groupers              prev_g
                ON prev_mi.epic_department_id = prev_g.epic_department_id
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map       prev_c
                ON prev_g.sk_Ref_Clinical_Area_Map = prev_c.sk_Ref_Clinical_Area_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map             prev_s
                ON prev_c.sk_Ref_Service_Map = prev_s.sk_Ref_Service_Map
            INNER JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map        prev_o
                ON prev_s.organization_id = prev_o.organization_id --this and 2 joins above to get organization
            CROSS APPLY metrics                                            AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_mi.w_hs_area_id
                   AND pt.portal_level = 'hs_area'
        WHERE prev_g.community_health_mpg_flag = 1
              AND prev_mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
              AND prev_mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
              AND prev_dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 461
              AND prev_mi.w_hs_area_id > 1
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
                ,prev_mi.w_hs_area_id
                ,prev_mi.w_hs_area_name
    )                         AS prior_value
        ON prior_value.w_hs_area_id = mi.w_hs_area_id
WHERE g.community_health_mpg_flag = 1
      AND mi.access_type = 'PROV' --any necessary filters applied to match those present in the workbook
      AND mi.provider_type_ot_name <> 'Resource' --any necessary filters applied to match those present in the workbook
      AND dd.weekday_ind = 1 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 461
      AND mi.w_hs_area_id > 1
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
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
        ,ct.multiplier
UNION ALL
-- Metric 439 LOS
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,COALESCE(AVG(CAST(los.LOS AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los.event_count AS NUMERIC(18, 2))), 0) AS numerator
      ,COALESCE(SUM(CAST(los.event_count AS NUMERIC(18, 2))), 0)                                                 AS denominator
      ,'inpatient days'                                                                                      AS numerator_title --match to front tile
      ,'inpatient stays'                                                                                     AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(CAST(los.LOS AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los.event_count AS NUMERIC(18, 2))), 0) = 0
                AND COALESCE(SUM(CAST(los.event_count AS NUMERIC(18, 2))), 0) = 0 THEN 0
           ELSE COALESCE(AVG(CAST(los.LOS AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los.event_count AS NUMERIC(18, 2))), 0) / COALESCE(SUM(CAST(los.event_count AS NUMERIC(18, 2))), 0)
       END                                                                                                   AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                                                                                  AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Vizient'                                                                                             AS benchmark_agency
      ,'Patient Progression'                                                                                 AS metric_topic
      ,los.w_hs_area_id
      ,los.w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                                                                                          AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_BalancedScorecard_Vizient_LOS_Tiles AS los
    CROSS APPLY metrics                               AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = los.w_hs_area_id
           AND t.portal_level_name = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_los.w_hs_area_id
              ,prev_los.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(AVG(CAST(prev_los.LOS AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(prev_los.event_count AS NUMERIC(18, 2))), 0) = 0
                        AND COALESCE(SUM(CAST(prev_los.event_count AS NUMERIC(18, 2))), 0) = 0 THEN 0
                   ELSE
                   COALESCE(AVG(CAST(prev_los.LOS AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(prev_los.event_count AS NUMERIC(18, 2))), 0)
                   / COALESCE(SUM(CAST(prev_los.event_count AS NUMERIC(18, 2))), 0)
               END        AS prior_value
              ,pt.target_ AS prior_target
        FROM TabRptg.Dash_BalancedScorecard_Vizient_LOS_Tiles AS prev_los
            CROSS APPLY metrics                               AS pm
            LEFT OUTER JOIN targets AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = prev_los.w_hs_area_id
                   AND pt.portal_level_name = 'hs_area'
        WHERE prev_los.RiskTypeCode = 18 --any necessary filters applied to match those present in the workbook
              AND pm.Metric_ID = 439
              AND prev_los.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_los.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY prev_los.w_hs_area_id
                ,prev_los.w_hs_area_name
                ,pt.target_
    )                         AS prior_value
        ON prior_value.w_hs_area_id = los.w_hs_area_id
WHERE los.RiskTypeCode = 18 --any necessary filters applied to match those present in the workbook
      AND m.Metric_ID = 439
      AND los.event_date >= m.Reporting_Period_Startdate
      AND los.event_date <= m.Reporting_Period_Enddate
      AND los.w_hs_area_id IS NOT NULL
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,los.w_hs_area_id
        ,los.w_hs_area_name
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
-- metric 595 CH LOS
SELECT m_ch.Metric_ID
      ,ct_ch.Metric_Name
      ,ct_ch.Metric_Sub_Name
      ,m_ch.Reporting_Period_Startdate
      ,m_ch.Reporting_Period_Enddate
      ,COALESCE(AVG(CAST(los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) AS numerator
      ,COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0)                                                               AS denominator
      ,'inpatient days'                                                                                                           AS numerator_title --match to front tile
      ,'inpatient stays'                                                                                                          AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(CAST(los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) = 0
                AND COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) = 0 THEN 0
           ELSE
           COALESCE(AVG(CAST(los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) / COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0)
       END                                                                                                                        AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                                                                                                       AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Premier'                                                                                                                  AS benchmark_agency
      ,'Patient Progression'                                                                                                      AS metric_topic
      ,los_ch.w_hs_area_id
      ,los_ch.w_hs_area_name
      ,t_ch.target_
      ,t_ch.threshold
      ,t_ch.portal_level
      ,t_ch.portal_level_name
      ,t_ch.portal_level_id
      ,t_ch.comparator                                                                                                            AS target_logic
      ,t_ch.precision_
      ,t_ch.NA
      ,t_ch.TBD
FROM DS_HSDM_APP.TabRptg.Dash_BalancedScorecard_LOS_CH_Tiles los_ch
    CROSS APPLY metrics                                      AS m_ch
    LEFT OUTER JOIN calc_type                        AS ct_ch
        ON m_ch.Metric_ID = ct_ch.metric_id
    LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS t_ch
        ON m_ch.Metric_ID = t_ch.metric_id
           AND t_ch.portal_level_id = los_ch.w_hs_area_id
           AND t_ch.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT prev_los_ch.w_hs_area_id
              ,prev_los_ch.w_hs_area_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(AVG(CAST(prev_los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0) = 0
                        AND COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0) = 0 THEN 0
                   ELSE
                   COALESCE(AVG(CAST(prev_los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0)
                   / COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0)
               END           AS prior_value
              ,pt_ch.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_BalancedScorecard_LOS_CH_Tiles AS prev_los_ch
            CROSS APPLY metrics                                      AS pm_ch
            LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS pt_ch
                ON pm_ch.Metric_ID = pt_ch.metric_id
                   AND pt_ch.portal_level_id = prev_los_ch.w_hs_area_id
                   AND pt_ch.portal_level = 'hs_area'
        WHERE pm_ch.Metric_ID = 595
              AND prev_los_ch.w_hs_area_id > 1 --this is a CH only metric
              AND prev_los_ch.event_date >= pm_ch.Prev_Reporting_Period_Startdate
              AND prev_los_ch.event_date <= pm_ch.Prev_Reporting_Period_Enddate
        GROUP BY prev_los_ch.w_hs_area_id
                ,prev_los_ch.w_hs_area_name
                ,pt_ch.target_
    )                                                AS prior_value
        ON prior_value.w_hs_area_id = los_ch.w_hs_area_id
WHERE m_ch.Metric_ID = 595
      AND los_ch.w_hs_area_id > 1
      AND los_ch.event_date >= m_ch.Reporting_Period_Startdate
      AND los_ch.event_date <= m_ch.Reporting_Period_Enddate
      AND los_ch.w_hs_area_id IS NOT NULL
GROUP BY m_ch.Metric_ID
        ,ct_ch.Metric_Name
        ,ct_ch.Metric_Sub_Name
        ,m_ch.Reporting_Period_Startdate
        ,m_ch.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,los_ch.w_hs_area_id
        ,los_ch.w_hs_area_name
        ,prior_value.prior_target
        ,t_ch.target_
        ,t_ch.threshold
        ,t_ch.portal_level
        ,t_ch.portal_level_name
        ,t_ch.portal_level_id
        ,t_ch.comparator
        ,t_ch.precision_
        ,t_ch.NA
        ,t_ch.TBD
UNION ALL
-- metric 595 CH LOS -OVERALL
SELECT m_ch.Metric_ID
      ,ct_ch.Metric_Name
      ,ct_ch.Metric_Sub_Name
      ,m_ch.Reporting_Period_Startdate
      ,m_ch.Reporting_Period_Enddate
      ,COALESCE(AVG(CAST(los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) AS numerator
      ,COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0)                                                               AS denominator
      ,'inpatient days'                                                                                                           AS numerator_title --match to front tile
      ,'inpatient stays'                                                                                                          AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(AVG(CAST(los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) = 0
                AND COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) = 0 THEN 0
           ELSE
           COALESCE(AVG(CAST(los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0) / COALESCE(SUM(CAST(los_ch.event_count AS NUMERIC(18, 2))), 0)
       END                                                                                                                        AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                                                                                                       AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Premier'                                                                                                                  AS benchmark_agency
      ,'Patient Progression'                                                                                                      AS metric_topic
      ,NULL                                                                                                                       AS w_hs_area_id
      ,'Community Health'                                                                                                         AS w_hs_area_name
      ,t_ch.target_
      ,t_ch.threshold
      ,t_ch.portal_level
      ,t_ch.portal_level_name
      ,t_ch.portal_level_id
      ,t_ch.comparator                                                                                                            AS target_logic
      ,t_ch.precision_
      ,t_ch.NA
      ,t_ch.TBD
FROM DS_HSDM_APP.TabRptg.Dash_BalancedScorecard_LOS_CH_Tiles los_ch
    CROSS APPLY metrics                                      AS m_ch
    LEFT OUTER JOIN calc_type                        AS ct_ch
        ON m_ch.Metric_ID = ct_ch.metric_id
    LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS t_ch
        ON m_ch.Metric_ID = t_ch.metric_id
           AND t_ch.portal_level_name = 'Community Health'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT pm_ch.Metric_ID
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(AVG(CAST(prev_los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0) = 0
                        AND COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0) = 0 THEN 0
                   ELSE
                   COALESCE(AVG(CAST(prev_los_ch.LOS_ACTUAL AS NUMERIC(18, 2))), 0) * COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0)
                   / COALESCE(SUM(CAST(prev_los_ch.event_count AS NUMERIC(18, 2))), 0)
               END           AS prior_value
              ,pt_ch.target_ AS prior_target
        FROM DS_HSDM_APP.TabRptg.Dash_BalancedScorecard_LOS_CH_Tiles AS prev_los_ch
            CROSS APPLY metrics                                      AS pm_ch
            LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS pt_ch
                ON pm_ch.Metric_ID = pt_ch.metric_id
                   AND pt_ch.portal_level_name = 'Community Health'
        WHERE pm_ch.Metric_ID = 595
              AND prev_los_ch.w_hs_area_id > 1 --this is a CH only metric
              AND prev_los_ch.event_date >= pm_ch.Prev_Reporting_Period_Startdate
              AND prev_los_ch.event_date <= pm_ch.Prev_Reporting_Period_Enddate
        GROUP BY pm_ch.Metric_ID
                ,pt_ch.target_
    )                                                AS prior_value
        ON prior_value.Metric_ID = ct_ch.metric_id
WHERE m_ch.Metric_ID = 595
      AND los_ch.w_hs_area_id > 1
      AND los_ch.event_date >= m_ch.Reporting_Period_Startdate
      AND los_ch.event_date <= m_ch.Reporting_Period_Enddate
GROUP BY m_ch.Metric_ID
        ,ct_ch.Metric_Name
        ,ct_ch.Metric_Sub_Name
        ,m_ch.Reporting_Period_Startdate
        ,m_ch.Reporting_Period_Enddate
        ,prior_value.prior_value
        ,prior_value.prior_target
        ,t_ch.target_
        ,t_ch.threshold
        ,t_ch.portal_level
        ,t_ch.portal_level_name
        ,t_ch.portal_level_id
        ,t_ch.comparator
        ,t_ch.precision_
        ,t_ch.NA
        ,t_ch.TBD
UNION ALL
--metric id 943 --OR Utilization rate
SELECT orm.Metric_ID
      ,orct.Metric_Name
      ,orct.Metric_Sub_Name
      ,orm.Reporting_Period_Startdate
      ,orm.Reporting_Period_Enddate
      ,COALESCE(SUM(or_util.USED), 0)      AS numerator
      ,COALESCE(SUM(or_util.AVAILABLE), 0) AS denominator
      ,'used'                              AS numerator_title --match to front tile
      ,'available'                         AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(or_util.USED), 0) = 0
                AND COALESCE(SUM(or_util.AVAILABLE), 0) = 0 THEN 0
           ELSE ROUND((COALESCE(SUM(or_util.USED) * 1., 0) / COALESCE(SUM(or_util.AVAILABLE), 0)) * 100, 2)
       END                                 AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                 AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,NULL                                AS benchmark_agency
      ,'Operating Rooms'                   AS metric_topic
      ,1                                   AS w_hs_area_id
      ,'Medical Center'                    AS w_hs_area_name
      ,ort.target_
      ,ort.threshold
      ,ort.portal_level
      ,ort.portal_level_name
      ,ort.portal_level_id
      ,ort.comparator                      AS target_logic
      ,ort.precision_
      ,ort.NA
      ,ort.TBD
FROM TabRptg.Dash_Periop_Scorecard_OR_Utilization_Tiles     AS or_util
    LEFT JOIN [DS_HSDM_APP].[Mapping].Epic_Dept_Groupers    g
        ON or_util.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map       s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map  o
        ON s.organization_id = o.organization_id
    CROSS APPLY metrics                                     AS orm
    LEFT OUTER JOIN calc_type                        AS orct
        ON orm.Metric_ID = orct.metric_id
    LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS ort
        ON orm.Metric_ID = ort.metric_id
           AND ort.portal_level_id = s.service_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT ps.service_id
              ,ps.service_name
              ,CASE --calc specific to producing front tile #
                   WHEN COALESCE(SUM(prev_or_util.USED), 0) = 0
                        AND COALESCE(SUM(prev_or_util.AVAILABLE), 0) = 0 THEN 0
                   ELSE ROUND((COALESCE(SUM(prev_or_util.USED), 0) / COALESCE(SUM(prev_or_util.AVAILABLE), 0)) * 100, 2)
               END         AS prior_value
              ,ppt.target_ AS prior_target
        FROM TabRptg.Dash_Periop_Scorecard_OR_Utilization_Tiles     AS prev_or_util
            LEFT JOIN [DS_HSDM_APP].[Mapping].Epic_Dept_Groupers    pg
                ON prev_or_util.epic_department_id = pg.epic_department_id
            LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map pc
                ON pg.sk_Ref_Clinical_Area_Map = pc.sk_Ref_Clinical_Area_Map
            LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map       ps
                ON pc.sk_Ref_Service_Map = ps.sk_Ref_Service_Map
            LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map  po
                ON ps.organization_id = po.organization_id
            CROSS APPLY metrics                                     AS ppm
            LEFT OUTER JOIN calc_type                        AS pct
                ON ppm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS ppt
                ON ppm.Metric_ID = ppt.metric_id
                   AND ppt.portal_level_id = ps.service_id
        WHERE ppm.Metric_ID = 943
              AND ps.service_id = 4 --level to match periop scorecard
              AND prev_or_util.event_date >= ppm.Prev_Reporting_Period_Startdate
              AND prev_or_util.event_date <= ppm.Prev_Reporting_Period_Enddate
        GROUP BY ps.service_id
                ,ps.service_name
                ,ppt.target_
                ,pct.multiplier
    )                                                AS prior_value
        ON prior_value.service_id = s.service_id
WHERE orm.Metric_ID = 943
      AND or_util.event_date >= orm.Reporting_Period_Startdate
      AND or_util.event_date <= orm.Reporting_Period_Enddate
      AND s.service_id = 4 --level to match periop scorecard
GROUP BY orm.Metric_ID
        ,orct.Metric_Name
        ,orct.Metric_Sub_Name
        ,orm.Reporting_Period_Startdate
        ,orm.Reporting_Period_Enddate
        ,s.service_id
        ,s.service_name
        ,ort.target_
        ,ort.threshold
        ,ort.portal_level
        ,ort.portal_level_name
        ,ort.portal_level_id
        ,ort.comparator
        ,ort.precision_
        ,ort.NA
        ,ort.TBD
        ,prior_value.prior_value
        ,prior_value.prior_target
        ,orct.multiplier
UNION ALL
--metric id 800 --OR Turnaround Time
SELECT orm.Metric_ID
      ,orct.Metric_Name
      ,orct.Metric_Sub_Name
      ,orm.Reporting_Period_Startdate
      ,orm.Reporting_Period_Enddate
      ,COALESCE(SUM(or_util.TAT), 0)         AS numerator
      ,COALESCE(SUM(or_util.event_count), 0) AS denominator
      ,'tat minutes'                         AS numerator_title --match to front tile
      ,'cases w/TAT'                         AS denominator_title
      ,CASE --calc specific to producing front tile #
           WHEN COALESCE(SUM(or_util.TAT), 0) = 0
                AND COALESCE(SUM(or_util.event_count), 0) = 0 THEN 0
           ELSE ROUND(COALESCE(SUM(or_util.TAT) * 1., 0) / COALESCE(SUM(or_util.event_count), 0), 2)
       END                                   AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,NULL                                  AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,NULL                                  AS benchmark_agency
      ,'Operating Rooms'                     AS metric_topic
      ,1                                     AS w_hs_area_id
      ,'Medical Center'                      AS w_hs_area_name
      ,ort.target_
      ,ort.threshold
      ,ort.portal_level
      ,ort.portal_level_name
      ,ort.portal_level_id
      ,ort.comparator                        AS target_logic
      ,ort.precision_
      ,ort.NA
      ,ort.TBD
FROM TabRptg.Dash_Periop_Scorecard_ORTAT_Tiles              AS or_util
    LEFT JOIN [DS_HSDM_APP].[Mapping].Epic_Dept_Groupers    g
        ON or_util.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map c
        ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map       s
        ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map  o
        ON s.organization_id = o.organization_id
    CROSS APPLY metrics                                     AS orm
    LEFT OUTER JOIN calc_type                        AS orct
        ON orm.Metric_ID = orct.metric_id
    LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS ort
        ON orm.Metric_ID = ort.metric_id
           AND ort.portal_level_id = s.service_id
    LEFT OUTER JOIN
    ( --prior_value
        SELECT
            --ps.service_id
            --            ,ps.service_name
            CASE --calc specific to producing front tile #
                WHEN COALESCE(SUM(prev_or_util.TAT), 0) = 0
                     AND COALESCE(SUM(prev_or_util.event_count), 0) = 0 THEN 0
                ELSE ROUND(COALESCE(SUM(prev_or_util.TAT) * 1., 0) / COALESCE(SUM(prev_or_util.event_count), 0), 2)
            END         AS prior_value
           ,ppt.target_ AS prior_target
           ,ppm.Metric_ID
        FROM TabRptg.Dash_Periop_Scorecard_ORTAT_Tiles              AS prev_or_util
            LEFT JOIN [DS_HSDM_APP].[Mapping].Epic_Dept_Groupers    pg
                ON prev_or_util.epic_department_id = pg.epic_department_id
            LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Clinical_Area_Map pc
                ON pg.sk_Ref_Clinical_Area_Map = pc.sk_Ref_Clinical_Area_Map
            LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Service_Map       ps
                ON pc.sk_Ref_Service_Map = ps.sk_Ref_Service_Map
            LEFT JOIN [DS_HSDM_APP].[Mapping].Ref_Organization_Map  po
                ON ps.organization_id = po.organization_id
            CROSS APPLY metrics                                     AS ppm
            LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS ppt
                ON ppm.Metric_ID = ppt.metric_id
                   AND ppt.portal_level_id = ps.service_id
        WHERE ppm.Metric_ID = 800
              AND prev_or_util.TAT IS NOT NULL
              AND prev_or_util.LOC_NAME IS NOT NULL
              AND prev_or_util.EFFECTIVELY_CANCELED IS NULL
              AND prev_or_util.event_category = 'aggregate'
              AND ps.service_id = 4
              AND prev_or_util.event_date >= ppm.Prev_Reporting_Period_Startdate
              AND prev_or_util.event_date <= ppm.Prev_Reporting_Period_Enddate
        GROUP BY ppt.target_
                ,ppm.Metric_ID
    )                                                AS prior_value
        ON prior_value.Metric_ID = orm.Metric_ID
WHERE orm.Metric_ID = 800
      AND or_util.TAT IS NOT NULL
      AND or_util.LOC_NAME IS NOT NULL
      AND or_util.EFFECTIVELY_CANCELED IS NULL
      AND or_util.event_category = 'aggregate'
      AND or_util.event_date >= orm.Reporting_Period_Startdate
      AND or_util.event_date <= orm.Reporting_Period_Enddate
      AND s.service_id = 4
GROUP BY orm.Metric_ID
        ,orct.Metric_Name
        ,orct.Metric_Sub_Name
        ,orm.Reporting_Period_Startdate
        ,orm.Reporting_Period_Enddate
        ,ort.target_
        ,ort.threshold
        ,ort.portal_level
        ,ort.portal_level_name
        ,ort.portal_level_id
        ,ort.comparator
        ,ort.precision_
        ,ort.NA
        ,ort.TBD
        ,prior_value.prior_value
        ,prior_value.prior_target
UNION ALL
--metric 528--Overall Turnover -- UVAMC
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,MAX(   CASE
                  WHEN turnover.rn = 1 THEN turnover.rolling12monthst
                  ELSE NULL
              END
          )                  AS numerator
      ,MAX(   CASE
                  WHEN turnover.rn = 1 THEN turnover.rolling12avg
                  ELSE NULL
              END
          )                  AS denominator
      ,'total terminations'  AS numerator_title --match to front tile
      ,'avg. head count/mon' AS denominator_title
      ,MAX(   CASE
                  WHEN turnover.rn = 1 THEN turnover.vol_turn
                  ELSE NULL
              END
          )                  AS value
      ,MAX(   CASE
                  WHEN turnover.rn = 2 THEN turnover.vol_turn
                  ELSE NULL
              END
          )                  AS prior_value
      ,NULL                  AS prior_target
      ,'%'                   AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,NULL                  AS benchmark_agency
      ,'Resource Management' AS metric_topic
      ,1                     AS w_hs_area_id
      ,'Medical Center'      AS w_hs_area_name
      ,rt.target_
      ,rt.threshold
      ,rt.portal_level
      ,rt.portal_level_name
      ,rt.portal_level_id
      ,rt.comparator         AS target_logic
      ,rt.precision_
      ,rt.NA
      ,rt.TBD
FROM
( --turnover
    SELECT main.month_begin_date
          ,main.rolling12monthst
          ,main.rolling12avg
          ,(CAST(main.rolling12monthst AS DECIMAL(10, 2)) / CAST(main.rolling12avg AS DECIMAL(10, 2))) * 100 AS vol_turn
          ,ROW_NUMBER() OVER (ORDER BY main.month_begin_date DESC)                                           AS rn
    FROM
    ( --main
        SELECT tt.month_begin_date
              ,CASE
                   WHEN ROW_NUMBER() OVER (ORDER BY tt.month_begin_date) > 11 THEN SUM(tt.total_t) OVER (ORDER BY tt.month_begin_date
                                                                                                         ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                                                                                                        )
               END AS rolling12monthst
              ,CASE
                   WHEN ROW_NUMBER() OVER (ORDER BY te.month_begin_date) > 11 THEN AVG(te.total_e) OVER (ORDER BY te.month_begin_date
                                                                                                         ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                                                                                                        )
               END AS rolling12avg
        FROM
        (
            SELECT DISTINCT
                   vdd.month_begin_date
                  ,SUM(e.event_count) OVER (PARTITION BY vdd.month_begin_date ORDER BY vdd.month_begin_date) AS total_t
            FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_EmpTurnover_Tiles AS e
                INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date            AS vdd
                    ON e.event_date = vdd.day_date
                CROSS APPLY metrics
            WHERE e.JobType NOT IN ( 'Temporary', 'wage' )
                  AND e.event_category IS NOT NULL
                  AND
                  (
                      (
                          e.BusinessTitle NOT LIKE 'hcp%'
                          AND e.BusinessTitle <> 'house staff'
                      )
                      OR e.BusinessTitle IS NULL
                  )
                  AND e.WorkDayCompany = 'UVA Medical Center'
                  AND e.JobProfile NOT LIKE '%hcp%'
                  AND metrics.Metric_ID = 528
                  AND e.event_date <= metrics.Reporting_Period_Enddate
        )     AS tt
            INNER JOIN
            (
                SELECT DISTINCT
                       vdd.month_begin_date
                      ,COUNT(et.EmplID) OVER (PARTITION BY vdd.month_begin_date ORDER BY vdd.month_begin_date) AS total_e
                FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_EmpTurnover_Tiles AS et
                    INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date            AS vdd
                        ON et.event_date = vdd.day_date
                    CROSS APPLY metrics
                WHERE et.JobType NOT IN ( 'Temporary', 'wage' )
                      AND
                      (
                          (
                              et.BusinessTitle NOT LIKE 'hcp%'
                              AND et.BusinessTitle <> 'house staff'
                          )
                          OR et.BusinessTitle IS NULL
                      )
                      AND et.JobProfile NOT LIKE '%hcp%'
                      AND et.WorkDayCompany = 'UVA Medical Center'
                      AND metrics.Metric_ID = 528
                      AND et.event_date <= metrics.Reporting_Period_Enddate
            ) AS te
                ON tt.month_begin_date = te.month_begin_date
    ) AS main
)                       AS turnover
    CROSS APPLY metrics AS m
    LEFT OUTER JOIN calc_type                        AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS rt
        ON m.Metric_ID = rt.metric_id
           AND rt.portal_level_id = 1
           AND rt.portal_level = 'hs_area'
WHERE m.Metric_ID = 528
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,rt.target_
        ,rt.threshold
        ,rt.portal_level
        ,rt.portal_level_name
        ,rt.portal_level_id
        ,rt.comparator
        ,rt.precision_
        ,rt.NA
        ,rt.TBD
UNION ALL
--metric 870--Overall Turnover -- Community Health
SELECT m.Metric_ID
      ,ct.Metric_Name
      ,ct.Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      ,MAX(   CASE
                  WHEN turnover.rn = 1 THEN turnover.rolling12monthst
                  ELSE NULL
              END
          )                  AS numerator
      ,MAX(   CASE
                  WHEN turnover.rn = 1 THEN turnover.rolling12avg
                  ELSE NULL
              END
          )                  AS denominator
      ,'total terminations'  AS numerator_title --match to front tile
      ,'avg. head count' AS denominator_title
      ,MAX(   CASE
                  WHEN turnover.rn = 1 THEN turnover.vol_turn
                  ELSE NULL
              END
          )                  AS value
      ,MAX(   CASE
                  WHEN turnover.rn = 2 THEN turnover.vol_turn
                  ELSE NULL
              END
          )                  AS prior_value
      ,NULL                  AS prior_target
      ,'%'                   AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,NULL                  AS benchmark_agency
      ,'Resource Management' AS metric_topic
      ,NULL                  AS w_hs_area_id
      ,'Community Health'    AS w_hs_area_name
      ,rt.target_
      ,rt.threshold
      ,rt.portal_level
      ,rt.portal_level_name
      ,rt.portal_level_id
      ,rt.comparator         AS target_logic
      ,rt.precision_
      ,rt.NA
      ,rt.TBD
FROM
( --turnover
    SELECT main.month_begin_date
          ,main.rolling12monthst
          ,main.rolling12avg
          ,(CAST(main.rolling12monthst AS DECIMAL(10, 2)) / CAST(main.rolling12avg AS DECIMAL(10, 2))) * 100 AS vol_turn
          ,ROW_NUMBER() OVER (ORDER BY main.month_begin_date DESC)                                           AS rn
    FROM
    ( --main
        SELECT tt.month_begin_date
              ,CASE
                   WHEN ROW_NUMBER() OVER (ORDER BY tt.month_begin_date) > 11 THEN SUM(tt.total_t) OVER (ORDER BY tt.month_begin_date
                                                                                                         ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                                                                                                        )
               END AS rolling12monthst
              ,CASE
                   WHEN ROW_NUMBER() OVER (ORDER BY te.month_begin_date) > 11 THEN AVG(te.total_e) OVER (ORDER BY te.month_begin_date
                                                                                                         ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                                                                                                        )
               END AS rolling12avg
        FROM
        (
            SELECT DISTINCT
                   vdd.month_begin_date
                  ,SUM(e.event_count) OVER (PARTITION BY vdd.month_begin_date ORDER BY vdd.month_begin_date) AS total_t
            FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_EmpTurnover_CH_Tiles AS e
                INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date               AS vdd
                    ON e.event_date = vdd.day_date
                CROSS APPLY metrics
            WHERE e.Job_PRN IN ( 'N', '' )
                  AND e.Is_Contingent_Worker = 'N'
                  AND metrics.Metric_ID = 870
                  AND e.CH_Radiology_Flag = 0
                  AND e.event_date <= metrics.Reporting_Period_Enddate
        )     AS tt
            INNER JOIN
            (
                SELECT DISTINCT
                       vdd.month_begin_date
                      ,COUNT(et.EmplID) OVER (PARTITION BY vdd.month_begin_date ORDER BY vdd.month_begin_date) AS total_e
                FROM DS_HSDM_APP.TabRptg.Dash_AmbOpt_EmpTurnover_CH_Tiles AS et
                    INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date               AS vdd
                        ON et.event_date = vdd.day_date
                    CROSS APPLY metrics
                WHERE et.Job_PRN IN ( 'N', '' )
                      AND et.Is_Contingent_Worker = 'N'
                      AND metrics.Metric_ID = 870
                      AND et.CH_Radiology_Flag = 0
                      AND et.event_date <= metrics.Reporting_Period_Enddate
            ) AS te
                ON tt.month_begin_date = te.month_begin_date
    ) AS main
)                       AS turnover
    CROSS APPLY metrics AS m
    LEFT OUTER JOIN calc_type                        AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN DS_HSDM_APP.TabRptg.Dash_Targets AS rt
        ON m.Metric_ID = rt.metric_id
           AND rt.portal_level_id = 8
           AND rt.portal_level = 'uva_health'
WHERE m.Metric_ID = 870
GROUP BY m.Metric_ID
        ,ct.Metric_Name
        ,ct.Metric_Sub_Name
        ,m.Reporting_Period_Startdate
        ,m.Reporting_Period_Enddate
        ,rt.target_
        ,rt.threshold
        ,rt.portal_level
        ,rt.portal_level_name
        ,rt.portal_level_id
        ,rt.comparator
        ,rt.precision_
        ,rt.NA
        ,rt.TBD
ORDER BY metric_topic
        ,metric_id;
*/
--UNION ALL
--metric 1097 Not Accepted Transfers Rate --UVA MC
SELECT DISTINCT
       m.Metric_ID
      ,ct.Metric_Name
      --,ct.Metric_Sub_Name
      ,'Not Accepted Rate' AS Metric_Sub_Name
      ,m.Reporting_Period_Startdate
      ,m.Reporting_Period_Enddate
      --,COALESCE(COUNT(mi.accepted), 0) AS numerator       --in case of null return zero
      ,COALESCE(SUM(CASE WHEN mi.accepted = 0 THEN 1 ELSE 0 END), 0) AS numerator       --in case of null return zero
      ,COALESCE(COUNT(mi.event_count), 0) AS denominator
      ,'Not Accepted Transfers'                            AS numerator_title --match to front tile
      ,'External Transfer Requests'                                     AS denominator_title
      ,CASE --calc specific to producing front tile #
           --WHEN COALESCE(COUNT(mi.accepted), 0) = 0 THEN 0
           WHEN COALESCE(SUM(CASE WHEN mi.accepted = 0 THEN 1 ELSE 0 END), 0) = 0 THEN 0
           ELSE CAST(
						--CAST(COALESCE(COUNT(mi.accepted), 0) AS NUMERIC(18, 2)) /
						CAST(COALESCE(SUM(CASE WHEN mi.accepted = 0 THEN 1 ELSE 0 END), 0) AS NUMERIC(18, 2)) /
						CAST(COALESCE(COUNT(mi.event_count), 0) AS NUMERIC(18, 2))
					 AS NUMERIC(18, 2))
       END                                       AS Value
      ,prior_value.prior_value
      ,prior_value.prior_target
      ,'%'                                      AS value_symbol    --symbol following value as in: %, days, min, etc.; NULL if metric has no symbol suffix
      ,'Internal'                                AS benchmark_agency
      ,'Patient Progression'                              AS metric_topic
      ,1                                         AS w_hs_area_id
      ,'Medical Center'                          AS w_hs_area_name
      ,t.target_
      ,t.threshold
      ,t.portal_level
      ,t.portal_level_name
      ,t.portal_level_id
      ,t.comparator                              AS target_logic
      ,t.precision_
      ,t.NA
      ,t.TBD
FROM TabRptg.Dash_PatientProgression_ExternalTransferStatus_Tiles mi
    CROSS APPLY metrics                                               AS m
    LEFT OUTER JOIN calc_type AS ct
        ON m.Metric_ID = ct.metric_id
    LEFT OUTER JOIN targets   AS t
        ON m.Metric_ID = t.metric_id
           AND t.portal_level_id = 1
           AND t.portal_level = 'hs_area'
    LEFT OUTER JOIN
    ( --prior_value
        SELECT DISTINCT
               1          AS w_hs_area_id
			  ,CASE --calc specific to producing front tile #
				   WHEN COALESCE(COUNT(prev_mi.accepted), 0) = 0 THEN 0
				   ELSE CAST(
								CAST(COALESCE(COUNT(prev_mi.accepted), 0) AS NUMERIC(18, 2)) /
								CAST(COALESCE(COUNT(prev_mi.event_count), 0) AS NUMERIC(18, 2))
							 AS NUMERIC(18, 2))
			    END                                       AS prior_value
              ,pt.target_ AS prior_target
        FROM TabRptg.Dash_PatientProgression_ExternalTransferStatus_Tiles AS prev_mi
            CROSS APPLY metrics                                               AS pm
            LEFT OUTER JOIN calc_type AS pct
                ON pm.Metric_ID = pct.metric_id
            LEFT OUTER JOIN targets   AS pt
                ON pm.Metric_ID = pt.metric_id
                   AND pt.portal_level_id = 1
                   AND pt.portal_level = 'hs_area'
        WHERE 1 = 1
		      AND prev_mi.incoming_transfer = 1
              --AND pm.Metric_ID = 1097
              AND pm.Metric_ID = 794
              AND prev_mi.event_date >= pm.Prev_Reporting_Period_Startdate
              AND prev_mi.event_date <= pm.Prev_Reporting_Period_Enddate
        GROUP BY pt.target_
                ,pct.multiplier
    )                         AS prior_value
        ON prior_value.w_hs_area_id = 1
WHERE 1 =1
	  AND mi.incoming_transfer = 1 -- TransferTypeHx IN ('Incoming Transfer','Medical Intrafacility Transfer')
      --AND m.Metric_ID = 1097
      AND m.Metric_ID = 794
      AND mi.event_date >= m.Reporting_Period_Startdate
      AND mi.event_date <= m.Reporting_Period_Enddate
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



