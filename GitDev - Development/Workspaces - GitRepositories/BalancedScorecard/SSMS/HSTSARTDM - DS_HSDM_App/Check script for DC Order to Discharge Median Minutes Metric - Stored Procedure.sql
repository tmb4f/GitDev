USE [DS_HSDW_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [ETL].[uspSrc_Patient_Progression_Timely_Discharges]
--AS
    /*******************************************************************************************************************
      WHAT  : Timely Discharges from inpatient units, all dispositions except for "Expired" or "Left Against Medical Advice"
      WHO   : Original author unknown - existing query for Data Portal Timely Discharges
      WHEN  : Original date unknown - first documented 10/25/23
      WHY   : Data Portal Timely Discharges dashboard - governed by Patient Progression (Feature #40443)
    *******************************************************************************************************************
    INFO: Conversion of existing Timely Discharges dashboard to TabRptg/Data Portal standards
    
         INPUTS  :
                    DS_HSDM_App.ETL.usp_Get_Dash_Dates_BalancedScorecard
                    DS_HSDM_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv
                    DS_HSDW_Prod.Rptg.vwClrt_ADT_Ordr
                    DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt
                    DS_HSDW_Prod.Rptg.vwDim_Clrt_Flo_GrpRow
                    DS_HSDW_Prod.Rptg.vwDim_Clrt_Pt
                    DS_HSDW_Prod.Rptg.vwDim_Clrt_Pt_Cls
                    DS_HSDW_Prod.Rptg.vwDim_Clrt_SERSrc
                    DS_HSDW_Prod.Rptg.vwDim_Date
                    DS_HSDW_Prod.Rptg.vwDim_DRG
                    DS_HSDW_Prod.Rptg.vwDim_Patient
                    DS_HSDW_Prod.Rptg.vwFact_Clrt_ADT
                    DS_HSDW_Prod.Rptg.vwFact_Clrt_Flo_Msr
                    DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
                    DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt
                    DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Disch_Disp_All
                    DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt
                    DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
    
         OUTPUTS :
            1)  Required columns for TabRptg Table.
    
            2)  Metric specific columns:
                discharge_destination                       - Discharge Destination
                drg_code_name                               - DRG Code and Name for the encounter (is this per Billing?)
                LOS                                         - Length of stay for the encounter
                sk_Dim_Clrt_Pt_Cls                          - Patient class key for the encounter
                patient_class                               - Patient class value for the encounter
                Ordr_Dtm                                    - Datetime of the final discharge order for the given encounter
                Order_Tm                                    - Time of the final discharge order for the given encounter
                dc_order_by_10am_count                      - Event flag column if given encounter's discharge order was ordered on or before target of 10AM
                mins_from_10am_to_dc_order                  - Time in minutes from discharge order to 10AM, will be negative if before 10AM and positive if after 10AM
                Discharge_DtTm                              - Datetime of the given encounter's actual discharge
                Discharge_Tm                                - Time of the given encounter's actual discharge
                dc_actual_by_noon_count                     - Event flag column if given encounter's actual discharge was completed by 12pm
                mins_from_noon_to_dc_actual                 - Time in minutes from actual discharge to 12PM, will be negative if before 12PM and positive if after 12PM
                mins_from_dc_ord_to_dc_actual               - Time in minutes from discharge order to actual discharge
                dc_with_targeted_acuity_count               - Event flag column if given encounter was discharged with one of the targeted {C, D, E, H} acutities/patient progression levels
                time_entered                                - Time the discharge acuity level/patient progression level was recorded on the flowsheet for the encounter
                acuity_score                                - Actual acuity level/patient progressions level that was recorded on the flowsheet
                prov_team									- Provider team at discharge
                hrs_from_dc_ord_to_dc_actual				- For Tableau, Time in hours from discharge order to actual discharge
    
         NOTES:
    **********************************************************************************************************************************************************************************************
    TAGS:
    
    MODS:	06/25/2024-DM2NB-Add provider team at discharge for use on patient progression dashbaord; comment out DROP TABLE statements
            07/04/2024-DM2NB-Add hrs_from_dc_ord_to_dc_actual column to provide hours for Tableau; correct filter that was dropping some discharge orders; switch to use the MDM history table
            08/13/2024-DM2NB-Change start date to include beginning of the FY
            02/03/2025-DM2NB-Add expected discharge data elements and discharge date/time entry timestamp
            02/19/2025-NRM3V-[User Stories: #55659, #55581]Remove all department level exclusions, add new column for department type in order to allow workbook level filtering
            02/24/2025-NRM3V-[User Story: #55868] Add back in department level exclusions allowing DataViz to only update `Discharge Order by 10AM` workbooks, leaving others untouched for now
            04/30/2025-DM2NB-[User Story: #58014] Add filter for patients with a death date on or before discharge date time.
            07/03/2025-DM2NB-[User Story: #60257] Remove LOS and DRG_Code_name (these will be added later in the day after fact_pt_acct processing); adjust provider team logic to ensure no dupes
            07/11/2025-USR5NV -Add app_flag and ordering provider to the output table for the project of Advanced Practice Provider (APP) dashboard. Attribution to APP is based on ordering provider.
            09/09/2025-USR5NV -Add Atn_LINE column to ROW_NUMBER() to find the last entered discharge attending when start and end time are the same
			09/26/2025-TMB4F -[User Story: #62975] Change setting of LOS value to a calculation
			10/07/2025-TMB4F -[User Story: #63690] Remove the exclusion for Newborn encounters
    **********************************************************************************************************************************************************************************************/

    SET NOCOUNT ON;
    /* SET STATISTICS IO,TIME ON; */
    DECLARE
        @startdate     SMALLDATETIME = NULL,
        @enddate       SMALLDATETIME = NULL,
        @startdate_key INT           = NULL,
        @enddate_key   INT           = NULL;

    ---------------------------------------------------
    --get default Balanced Scorecard date range
    IF @startdate IS NULL
       AND @enddate IS NULL
        BEGIN
            EXEC DS_HSDM_APP.ETL.usp_Get_Dash_Dates_BalancedScorecard
                @startdate OUTPUT,
                @enddate OUTPUT;

            ---For this proc, take it back another 6 months to the begin of the FY
            SET @startdate = DATEADD(mm, -6, @startdate);

        END;
    ----------------------------------------------------

    /* convert date to datekeys as allows for much more performant filtering of vwFact_Pt_Enc_Clrt */
    SET @startdate_key = TRY_CONVERT(INT, TRY_CONVERT(CHAR(8), @startdate, 112));
    SET @enddate_key = TRY_CONVERT(INT, TRY_CONVERT(CHAR(8), @enddate, 112));

IF OBJECT_ID('tempdb..#targeted_discharges ') IS NOT NULL
DROP TABLE #targeted_discharges

IF OBJECT_ID('tempdb..#Expected_discharges ') IS NOT NULL
DROP TABLE #Expected_discharges

IF OBJECT_ID('tempdb..#dc_entry ') IS NOT NULL
DROP TABLE #dc_entry

IF OBJECT_ID('tempdb..#prior_adt_event_department ') IS NOT NULL
DROP TABLE #prior_adt_event_department

IF OBJECT_ID('tempdb..#attending_providers_per_encounter ') IS NOT NULL
DROP TABLE #attending_providers_per_encounter

IF OBJECT_ID('tempdb..#denominator_discharges ') IS NOT NULL
DROP TABLE #denominator_discharges

IF OBJECT_ID('tempdb..#discharge_orders ') IS NOT NULL
DROP TABLE #discharge_orders

IF OBJECT_ID('tempdb..#patient_acuity_scores_per_encounter ') IS NOT NULL
DROP TABLE #patient_acuity_scores_per_encounter

IF OBJECT_ID('tempdb..#RptgTbl ') IS NOT NULL
DROP TABLE #RptgTbl

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

IF OBJECT_ID('tempdb..#summary2 ') IS NOT NULL
DROP TABLE #summary2

    /* get all discharges that occurred during desired date range */
    --DROP TABLE IF EXISTS #targeted_discharges;
    SELECT
         sk_Fact_Pt_Enc_Clrt,
         PAT_ENC_CSN_ID,
         AcctNbr_Clrt
    INTO #targeted_discharges
    FROM
         DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt
    WHERE
         sk_Disch_Dte >= @startdate_key
         AND sk_Disch_Dte < @enddate_key
    OPTION (RECOMPILE);

    CREATE CLUSTERED INDEX skFactPtEnc
        ON #targeted_discharges (sk_Fact_Pt_Enc_Clrt);
    CREATE NONCLUSTERED INDEX Csn
        ON #targeted_discharges (PAT_ENC_CSN_ID);
    CREATE NONCLUSTERED INDEX AcctNbr_Clrt
        ON #targeted_discharges (AcctNbr_Clrt);

    --pull expected discharge values
    SELECT
            edd.PAT_ENC_CSN_ID,
            edd.sk_Fact_Pt_Enc_Clrt,
            edd.LINE,
            edd.Expected_Discharge_Approx_Hx,
            edd.EXPECTED_DISCH_DTTM_HX_TIME,
            edd.Expected_Discharge_Comment_HX,
            CONVERT(DATETIME, edd.EXPECTED_DISCH_UPD_HX_UTC_DTTM AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') AS expected_disch_upd_hx_local_dttm,
            edd.EXPECTED_DISCHARGE_USER_HX_ID,
            vdcep.EMPlye_Nme,
            edd.Expected_Discharge_Upd_Src
    INTO    #Expected_discharges
    FROM
            DS_HSDW_Prod.dbo.Fact_Clrt_Expected_Discharge AS edd
        INNER JOIN
            #targeted_discharges                          AS td
                ON td.sk_Fact_Pt_Enc_Clrt = edd.sk_Fact_Pt_Enc_Clrt
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt          AS vfpec
                ON vfpec.sk_Fact_Pt_Enc_Clrt = td.sk_Fact_Pt_Enc_Clrt
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye           AS vdcep
                ON edd.sk_Dim_Clrt_EMPlye = vdcep.sk_Dim_Clrt_EMPlye
    WHERE
            edd.EXPECTED_DISCHARGE_APPROX_HX_C = 10 --morning
            AND edd.EXPECTED_DISCHARGE_UPD_SRC_C = 1 --user entered
            AND CAST(CONVERT(
                                DATETIME,
                                edd.EXPECTED_DISCH_UPD_HX_UTC_DTTM AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'
                            ) AS DATE) < CAST(vfpec.Dsch_Dtm AS DATE); --entry of EDD must be before day of discharge

    --find timestamps for discharge date entry--for calculating lag between dc and data entry
    SELECT
            adt.sk_Fact_Pt_Enc_Clrt,
            adt.EVENT_ID,
            adt.ORIG_EVENT_TIME
    INTO    #dc_entry
    FROM
            DS_HSDW_Prod.dbo.Fact_Clrt_ADT_Census AS adt
        INNER JOIN
            #targeted_discharges                  AS td
                ON td.sk_Fact_Pt_Enc_Clrt = adt.sk_Fact_Pt_Enc_Clrt
    WHERE
            adt.Discharge = 1
            AND adt.sk_Dim_Clrt_ADT_Evnt = 2
            AND adt.Cancelled = 0;

    /* parse adt event table to get the from-department (i.e. prior dept) for each event */
    --DROP TABLE IF EXISTS #prior_adt_event_department;

    SELECT
            adt.sk_Fact_Pt_Enc_Clrt,
            adt.sk_Dim_Clrt_ADT_Evnt,
            LEAD(adt.sk_Dim_Clrt_DEPt) OVER (PARTITION BY
                                                 adt.sk_Fact_Pt_Enc_Clrt
                                             ORDER BY
                                                 adt.seq DESC
                                            ) AS sk_Dim_Clrt_DEPt_prev,
            /* seems to be a tie-breaker for multiple discharges per encounter? but, doesn't appear to ever be used in original query */
            ROW_NUMBER() OVER (PARTITION BY
                                   adt.sk_Fact_Pt_Enc_Clrt
                               ORDER BY
                                   adt.IN_DTTM DESC,
                                   adt.OUT_DTTM DESC
                              )               AS DschSeq,
            adt.EVENT_ID
    INTO    #prior_adt_event_department
    FROM
            DS_HSDW_Prod.Rptg.vwFact_Clrt_ADT AS adt
        INNER JOIN
            #targeted_discharges              AS td
                ON adt.sk_Fact_Pt_Enc_Clrt = td.sk_Fact_Pt_Enc_Clrt;

    CREATE CLUSTERED INDEX sk_PtEncClrt
        ON #prior_adt_event_department (sk_Fact_Pt_Enc_Clrt);
    CREATE NONCLUSTERED INDEX EvntType_Seq_Incl_Prev_Dept
        ON #prior_adt_event_department (sk_Dim_Clrt_ADT_Evnt, DschSeq)
        INCLUDE (sk_Dim_Clrt_DEPt_prev);

    /*
    get last recorded attending provider for each encounter
    used to attribute the discharge physician
    */
    -- DROP TABLE IF EXISTS #attending_providers_per_encounter;
    SELECT
            atn.PAT_ENC_CSN_ID,
            atn.Prov_Id,
            ROW_NUMBER() OVER (PARTITION BY
                                   atn.sk_Fact_Pt_Enc_Clrt
                               ORDER BY
                                   atn.Atn_Beg_Dtm DESC,
                                   (CASE
                                        WHEN atn.Atn_End_Dtm = '1900-01-01'
                                            THEN
                                            GETDATE()
                                        ELSE
                                            atn.Atn_End_Dtm
                                    END
                                   ) DESC,
                                   atn.Atn_LINE DESC
                              ) AS Atn_Seq
    INTO    #attending_providers_per_encounter
    FROM
            DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All AS atn
        INNER JOIN
            #targeted_discharges                         AS td
                ON atn.PAT_ENC_CSN_ID = td.PAT_ENC_CSN_ID;

    CREATE NONCLUSTERED INDEX CSN_Incl_AtnSeq
        ON #attending_providers_per_encounter (Atn_Seq, PAT_ENC_CSN_ID)
        INCLUDE (Prov_Id);

    /*
    narrow the list of total discharges that fit just the desired date range
    using additional criteria (i.e. discharge dispos, patient class )
    this subset of total discharges becomes the actual denominator
    used for all metrics related to timely discharges
    */
    --DROP TABLE IF EXISTS #denominator_discharges;
    SELECT
            pt.AcctNbr_int             AS Account_Num,
            pt.sk_Fact_Pt_Acct,
            pt.sk_Dim_Clrt_Pt,
            dis.sk_Dim_Pt,
            pt.Dsch_Dtm                AS Discharge_DtTm,
            CONVERT(DATE, pt.Dsch_Dtm) AS Discharge_Dt,
            pt.Dsch_Tm                 AS Discharge_Tm,
            adt.sk_Dim_Clrt_ADT_Evnt   AS [Event],
            adt.sk_Dim_Clrt_DEPt_prev  AS sk_Dim_Clrt_DEPt,
            dept.Clrt_DEPt_Typ         AS department_type,
            atn.Prov_Id,
            dis.sk_Dim_Clrt_Disch_Disp,
            dis.Disch_Disp_Descr,
            NULL                       AS drg_code_name,
            DATEDIFF(DAY, CAST(pt.Adm_Dtm AS DATE), CAST(pt.Dsch_Dtm AS DATE)) AS LOS,
            pt.sk_Fact_Pt_Enc_Clrt,
            pt.sk_Dim_Clrt_Pt_Cls,
            ed.Expected_Discharge_Approx_Hx,
            ed.EXPECTED_DISCH_DTTM_HX_TIME,
            ed.Expected_Discharge_Comment_HX,
            ed.expected_disch_upd_hx_local_dttm,
            ed.EMPlye_Nme,
            ed.Expected_Discharge_Upd_Src,
            de.ORIG_EVENT_TIME
    INTO    #denominator_discharges
    FROM
            #prior_adt_event_department                    AS adt
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt           AS pt
                ON adt.sk_Fact_Pt_Enc_Clrt = pt.sk_Fact_Pt_Enc_Clrt
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwDim_Patient                AS patient
                ON patient.sk_Dim_Pt = pt.sk_Dim_Pt
        LEFT OUTER JOIN
            #attending_providers_per_encounter             AS atn
                ON (
                       atn.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID
                       AND atn.Atn_Seq = 1 /* Find the last attending provider */
                   )
        LEFT OUTER JOIN
            DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Disch_Disp_All AS dis
                ON dis.PAT_ENC_CSN_ID = pt.PAT_ENC_CSN_ID
        LEFT OUTER JOIN
            DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt              AS dept
                ON adt.sk_Dim_Clrt_DEPt_prev = dept.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN
            ( --ed
                SELECT
                        e.PAT_ENC_CSN_ID,
                        e.sk_Fact_Pt_Enc_Clrt,
                        e.LINE,
                        e.Expected_Discharge_Approx_Hx,
                        e.EXPECTED_DISCH_DTTM_HX_TIME,
                        e.Expected_Discharge_Comment_HX,
                        e.expected_disch_upd_hx_local_dttm,
                        e.EXPECTED_DISCHARGE_USER_HX_ID,
                        e.EMPlye_Nme,
                        e.Expected_Discharge_Upd_Src
                FROM
                        #Expected_discharges AS e
                    INNER JOIN
                        ( --m--last entered line
                            SELECT
                                d.sk_Fact_Pt_Enc_Clrt,
                                MAX(LINE) AS line
                            FROM
                                #Expected_discharges AS d
                            GROUP BY
                                d.sk_Fact_Pt_Enc_Clrt
                        )                    AS m
                            ON m.sk_Fact_Pt_Enc_Clrt = e.sk_Fact_Pt_Enc_Clrt
                               AND m.line = e.LINE
            )                                              AS ed
                ON ed.sk_Fact_Pt_Enc_Clrt = adt.sk_Fact_Pt_Enc_Clrt
        LEFT OUTER JOIN
            #dc_entry                                      AS de
                ON de.sk_Fact_Pt_Enc_Clrt = adt.sk_Fact_Pt_Enc_Clrt
                   AND de.EVENT_ID = adt.EVENT_ID
    WHERE
            adt.sk_Dim_Clrt_ADT_Evnt = 2 /* Discharge */
            AND adt.DschSeq = 1 /* get latest/last discharge event */
            /* dept exclusion criteria - use Clrt_Dept_Ext_Nme to exclude ED & Main OR & Periop & Unknown & EP/Cath Lab */
            AND dept.Clrt_DEPt_Ext_Nme NOT IN (
                                                  'Main Operating Room-Periop', 'UVA Emergency Department', 'UVHE ADMIT',
                                                  'TCIR TC2A', 'TCIR TC2B', 'TCIR TC3A', 'Post Anesthesia Care Unit',
                                                  'UVA Health System Cardiac Transition Unit', 'PERIOP', 'GPERIOP', 'Null',
                                                  'Unknown', 'Cardiac Cath Lab', 'Outpatient Surgery at Battle',
                                                  'Endoscopy/Bronchoscopy Procedure Suite'
                                              )
            AND NOT EXISTS
        (/* essentially, if patient actually left the hospital alive (and not AMA) they should be captured in the denominator */
            SELECT
                dis.sk_Dim_Clrt_Disch_Disp
            INTERSECT
            (SELECT
                 dispo
             FROM
                 (
                     VALUES
                         (
                             4
                         ), /* Expired */
                         (
                             26
                         ), /* Expired at Home */
                         (
                             27
                         ), /* Expired in Medical Facility */
                         (
                             28
                         ), /* Expired - Place Unknown */
                         (
                             40
                         )  /* Left Against Medical Advice */
                 ) AS excluded_discharge_dispos (dispo) )
        )
            AND
                (
                    patient.DeathDate = '1900-01-01'
                    OR patient.DeathDate > CONVERT(DATE, pt.Dsch_Dtm)
                ) --some discharge dispositions are incorrect for deceased patients at the time the SP runs
            AND pt.sk_Dim_Clrt_Pt_Cls NOT IN (
                                                 2, /* Outpatient */ 3 /* Emergency */
                                             );

    CREATE CLUSTERED INDEX DischDenom
        ON #denominator_discharges (sk_Fact_Pt_Enc_Clrt);
    CREATE NONCLUSTERED INDEX DischDt
        ON #denominator_discharges (Discharge_Dt)
        INCLUDE
        (sk_Dim_Clrt_Pt,
         Discharge_DtTm,
         Discharge_Tm,
         [Event],
         sk_Dim_Clrt_DEPt,
         department_type,
         Prov_Id,
         sk_Dim_Clrt_Disch_Disp,
         Disch_Disp_Descr,
         drg_code_name,
         LOS,
         sk_Dim_Clrt_Pt_Cls
        );

    /* get last discharge order per patient encounter */
    --DROP TABLE IF EXISTS #discharge_orders;

    SELECT
            adt_orders.Ordr_Dtm,
            adt_orders.sk_Fact_Pt_Enc_Clrt,
            ser.PROV_ID,
            ser.Prov_Nme,
            ser.Prov_Typ,
            ser.Financial_Division,
            ser.Financial_Division_Name,
            ser.Financial_SubDivision,
            ser.Financial_SubDivision_Name,
            ROW_NUMBER() OVER (PARTITION BY
                                   adt_orders.sk_Fact_Pt_Enc_Clrt
                               ORDER BY
                                   adt_orders.Ordr_Dtm DESC
                              ) AS Order_Seq
    INTO    #discharge_orders
    FROM
            DS_HSDW_Prod.Rptg.vwFact_Ordr_Prcdr       AS adt_orders
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwDim_Clrt_EAPrcdr      AS eap
                ON adt_orders.sk_Dim_Clrt_EAPrcdr = eap.sk_Dim_Clrt_EAPrcdr
        INNER JOIN
            #targeted_discharges                      AS td
                ON adt_orders.sk_Fact_Pt_Enc_Clrt = td.sk_Fact_Pt_Enc_Clrt
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwDim_Clrt_Ordr_Chrcstc AS chc
                ON adt_orders.sk_Dim_Clrt_Ordr_Chrcstc = chc.sk_Dim_Clrt_Ordr_Chrcstc
        LEFT JOIN
            DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye       AS emp
                ON adt_orders.sk_Creatr_EMPlye = emp.sk_Dim_Clrt_EMPlye
        LEFT JOIN
            DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc       AS ser
                ON emp.sk_Dim_Clrt_SERsrc = ser.sk_Dim_Clrt_SERsrc
    WHERE
            chc.Ordr_Typ_Nme = 'Discharge'
            AND eap.PROC_CODE = 'ADT8' /* Discharge Patient */

    CREATE CLUSTERED INDEX PtEncClrt
        ON #discharge_orders (sk_Fact_Pt_Enc_Clrt);

    /* get the patient progression/acuity scores per encounter include order/index to allow finding the last per encounter */
    --DROP TABLE IF EXISTS #patient_acuity_scores_per_encounter;

    SELECT
            fsm.sk_Fact_Pt_Enc_Clrt,
            fsm.Msr_Ent_Dtm               AS time_entered,
            CONVERT(CHAR(1), fsm.Msr_Val) AS acuity_score, /* only care about the first character for our targeted progession levels */
            peh.sk_Dim_Clrt_DEPt,
            ROW_NUMBER() OVER (PARTITION BY
                                   peh.PAT_ENC_CSN_ID
                               ORDER BY
                                   fsm.Msr_Rec_Dtm DESC
                              )           AS disp_seq
    INTO    #patient_acuity_scores_per_encounter
    FROM
            DS_HSDW_Prod.Rptg.vwFact_Clrt_Flo_Msr    AS fsm
        INNER JOIN
            #targeted_discharges                     AS td
                ON fsm.sk_Fact_Pt_Enc_Clrt = td.sk_Fact_Pt_Enc_Clrt
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt AS peh
                ON fsm.sk_Fact_Pt_Enc_Clrt = peh.sk_Fact_Pt_Enc_Clrt
        INNER JOIN
            DS_HSDW_Prod.Rptg.vwDim_Clrt_Flo_GrpRow  AS grr
                ON grr.sk_Dim_Clrt_Flo_GrpRow = fsm.sk_Dim_Clrt_Flo_GrpRow
    WHERE
            grr.FLO_MEAS_ID = '305100299' /* R UVA PATIENT PROGRESSION LEVEL */
            AND NOT EXISTS
        (
            SELECT
                peh.ADT_PATIENT_STAT
            INTERSECT
            SELECT
                'Preadmission'
        )
            AND NOT EXISTS
        (
            SELECT
                peh.ADMIT_CONF_STAT
            INTERSECT
            (SELECT
                 'Canceled'
             UNION ALL
             SELECT
                 'Pending')
        );

    CREATE CLUSTERED INDEX Pt_Enc_Clrt
        ON #patient_acuity_scores_per_encounter (sk_Fact_Pt_Enc_Clrt);
    CREATE NONCLUSTERED INDEX DispSeq_ProgLevel
        ON #patient_acuity_scores_per_encounter (disp_seq, acuity_score);

        ;
    WITH final_acuity_score_per_encounter
        AS (/* last targeted acuity score per encounter */
               SELECT
                   acu.sk_Fact_Pt_Enc_Clrt,
                   acu.time_entered,
                   acu.acuity_score,
                   acu.sk_Dim_Clrt_DEPt
               FROM
                   #patient_acuity_scores_per_encounter AS acu
               WHERE
                   acu.disp_seq = 1 /* get the last/latest patient progression disposition level */
                   AND acu.acuity_score IN (
                                               'D', /* Transfer or Discharge Today */
                                               'C', /* Transfer or Discharge Tomorrow */
                                               'E', /* Medically ready for DC, pending resolution of non-medical issue */
                                               'H'  /* Hospice Conversion (pilot) */
                                           )),
         FINAL
        AS (   SELECT
                       dd.sk_Dim_Clrt_Pt,
                       td.PAT_ENC_CSN_ID,
                       dd.Discharge_DtTm,
                       dd.Discharge_Dt,
                       dd.Discharge_Tm,
                       dd.Event,
                       dd.sk_Dim_Clrt_DEPt,
                       dd.Prov_Id,
                       dd.sk_Dim_Clrt_Disch_Disp,
                       dd.Disch_Disp_Descr,
                       dd.drg_code_name,
                       dd.LOS,
                       dd.sk_Dim_Clrt_Pt_Cls,
                       do.Ordr_Dtm,
                       CONVERT(TIME, do.Ordr_Dtm)    AS Ordr_Tm,
                       acuity_score.time_entered,
                       acuity_score.acuity_score,
                       acuity_score.sk_Dim_Clrt_DEPt AS ac_sk_Dim_Clrt_DEPt,
                       --five new columns 02032025
                       dd.Expected_Discharge_Approx_Hx,
                       dd.EXPECTED_DISCH_DTTM_HX_TIME,
                       dd.expected_disch_upd_hx_local_dttm,
                       dd.Expected_Discharge_Comment_HX,
                       dd.EMPlye_Nme,
                       dd.ORIG_EVENT_TIME,
                       dd.department_type,
                       dd.sk_Dim_Pt,
                       dd.sk_Fact_Pt_Enc_Clrt,
                       do.PROV_ID                    AS Ordering_PROV_ID,
                       do.Prov_Nme                   AS Ordering_Prov_Nme,
                       do.Prov_Typ                   AS ORDERING_Prov_Typ,
                       do.Financial_Division,
                       do.Financial_Division_Name,
                       do.Financial_SubDivision,
                       do.Financial_SubDivision_Name,
                       CASE
                           WHEN do.Prov_Typ IN (
                                                   'NURSE PRACTITIONER', 'PHYSICIAN ASSISTANT', 'NURSE ANESTHETIST',
                                                   'CLINICAL NURSE SPECIALIST', 'GENETIC COUNSELOR', 'AUDIOLOGIST'
                                               )
                               THEN
                               1
                           ELSE
                               0
                       END                           AS app_flag
               FROM
                       #denominator_discharges          AS dd
                   LEFT OUTER JOIN
                       #discharge_orders                AS do
                           ON dd.sk_Fact_Pt_Enc_Clrt = do.sk_Fact_Pt_Enc_Clrt
                              AND do.Order_Seq = 1 --Last discharge order
                   LEFT OUTER JOIN
                       final_acuity_score_per_encounter AS acuity_score
                           ON dd.sk_Fact_Pt_Enc_Clrt = acuity_score.sk_Fact_Pt_Enc_Clrt
                   LEFT OUTER JOIN
                       #targeted_discharges             AS td
                           ON dd.sk_Fact_Pt_Enc_Clrt = td.sk_Fact_Pt_Enc_Clrt)

    ----BDD 11/6/2023 insert directly to stage table. Assumes Truncate is handled by the SSIS package

    --INSERT INTO Stage.Dash_PatientProgression_TimelyDischarge
    --    (
    --        event_type,
    --        event_count,
    --        event_date,
    --        event_id,
    --        event_category,
    --        hs_area_id,
    --        hs_area_name,
    --        epic_department_id,
    --        epic_department_name,
    --        epic_department_name_external,
    --        fmonth_num,
    --        fyear_num,
    --        fyear_name,
    --        person_birth_date,
    --        person_gender,
    --        person_id,
    --        person_name,
    --        provider_id,
    --        provider_name,
    --        prov_type,
    --        financial_division_id,
    --        financial_division_name,
    --        financial_sub_division_id,
    --        financial_sub_division_name,
    --        som_hs_area_id,
    --        som_hs_area_name,
    --        som_group_id,
    --        som_group_name,
    --        som_department_id,
    --        som_department_name,
    --        som_division_id,
    --        som_division_name,
    --        ordering_provider_id,
    --        ordering_provider_name,
    --        ordering_prov_type,
    --        ordering_financial_division_id,
    --        ordering_financial_division_name,
    --        ordering_financial_sub_division_id,
    --        ordering_financial_sub_division_name,
    --        ordering_som_hs_area_id,
    --        ordering_som_hs_area_name,
    --        ordering_som_group_id,
    --        ordering_som_group_name,
    --        ordering_som_department_id,
    --        ordering_som_department_name,
    --        ordering_som_division_id,
    --        ordering_som_division_name,
    --        app_flag,
    --        peds_flag,
    --        discharge_destination,
    --        drg_code_name,
    --        LOS,
    --        sk_Dim_Clrt_Pt_Cls,
    --        patient_class,
    --        Ordr_Dtm,
    --        Ordr_Tm,
    --        dc_order_by_10am_count,
    --        mins_from_10am_to_dc_order,
    --        Discharge_DtTm,
    --        Discharge_Tm,
    --        dc_actual_by_noon_count,
    --        mins_from_noon_to_dc_actual,
    --        mins_from_dc_ord_to_dc_actual,
    --        dc_with_targeted_acuity_count,
    --        time_entered,
    --        acuity_score,
    --        PROV_TEAM,
    --        hrs_from_dc_ord_to_dc_actual,
    --        expected_and_actual_morning_dc,  --int
    --        expected_morning_dc,             --int
    --        expected_entry_emplye_nme,       --varchar(160)
    --        dc_entry_timestamp,              --datetime
    --        mins_from_dc_entry_to_dc_actual, --int
    --        department_type,                 /* varchar(50) */
    --        sk_Dim_Pt,
    --        sk_Fact_Pt_Enc_Clrt
    --    )
                SELECT
                    /* REQUIRED FIELDS FOR TAB_TABLE */

                    /* EVENT FIELDS */
                        CAST('DISCHARGE' AS VARCHAR(50))                                                      AS event_type,
                        CAST(CASE
                                 WHEN fin.PAT_ENC_CSN_ID IS NULL
                                     THEN
                                     1
                                 ELSE
                                     0
                             END AS INT)                                                                      AS event_count,
                        CAST(cal.day_date AS SMALLDATETIME)                                                   AS event_date,
                        CAST(fin.PAT_ENC_CSN_ID AS NUMERIC(18))                                               AS event_id,
                        CAST('PATIENT PROGRESSION' AS VARCHAR(50))                                            AS event_category,
                                      /* GROUPING FIELDS */
                        TRY_CAST(mdm.HS_AREA_ID AS INT)                                                       AS hs_area_id,
                        CAST(mdm.HS_AREA_NAME AS VARCHAR(150))                                                AS hs_area_name,
                                      /* DISCHARGE DEPARTMENT FIELDS */
                        CAST(mdm.EPIC_DEPARTMENT_ID AS NUMERIC(18, 0))                                        AS epic_department_id,
                        CAST(mdm.EPIC_DEPT_NAME AS VARCHAR(255))                                              AS epic_department_name,
                        CAST(mdm.EPIC_EXT_NAME AS VARCHAR(255))                                               AS epic_department_name_external,
                                      /* DATE COLUMNS */
                        CAST(cal.fmonth_num AS SMALLINT)                                                      AS fmonth_num,
                        CAST(cal.Fyear_num AS SMALLINT)                                                       AS fyear_num,
                        CAST(cal.FYear_name AS VARCHAR(10))                                                   AS fyear_name,
                                      /* PATIENT FIELDS */
                        CAST(pat.BirthDate AS DATETIME)                                                       AS person_birth_date,
                        CAST(pat.Sex AS VARCHAR(255))                                                         AS person_gender,
                        TRY_CAST(pat.MRN_int AS INT)                                                          AS person_id,
                        CAST(pat.Name AS VARCHAR(200))                                                        AS person_name,
                                      /* LAST ATTENDING PROVIDER FIELDS */
                        TRY_CAST(ser.PROV_ID AS INT)                                                          AS provider_id,
                        CAST(ser.Prov_Nme AS VARCHAR(50))                                                     AS provider_name,
                        CAST(ser.Prov_Typ AS VARCHAR(66))                                                     AS prov_type,
                                      /* FINANCIAL DIVISION FIELDS */
                        TRY_CAST(ser.Financial_Division AS INT)                                               AS financial_division_id,
                        NULLIF(TRY_CAST(ser.Financial_Division_Name AS VARCHAR(150)), 'na')                   AS financial_division_name,
                        TRY_CAST(ser.Financial_SubDivision AS INT)                                            AS financial_sub_division_id,
                        NULLIF(TRY_CAST(ser.Financial_SubDivision_Name AS VARCHAR(150)), 'na')                AS financial_sub_division_name,
                                      /* SOM FIELDS */
                        TRY_CAST(org.som_hs_area_id AS INT)                                                   AS som_hs_area_id,
                        CAST(org.som_hs_area_name AS VARCHAR(150))                                            AS som_hs_area_name,
                        TRY_CAST(org.som_group_id AS INT)                                                     AS som_group_id,
                        CAST(org.som_group_name AS VARCHAR(150))                                              AS som_group_name,
                        TRY_CAST(org.Department_ID AS INT)                                                    AS som_department_id,
                        CAST(org.Department AS VARCHAR(150))                                                  AS som_department_name,
                        TRY_CAST(org.Org_Number AS INT)                                                       AS som_division_id,
                        CAST(org.Organization AS VARCHAR(150))                                                AS som_division_name,
                                      /*Ordering provider*/
                        TRY_CAST(fin.Ordering_PROV_ID AS INT)                                                 AS ordering_provider_id,
                        CAST(fin.Ordering_Prov_Nme AS VARCHAR(50))                                            AS ordering_provider_name,
                        CAST(fin.ORDERING_Prov_Typ AS VARCHAR(66))                                            AS ordering_prov_type,

                                      /* FINANCIAL DIVISION FIELDS FOR ORDERING PROVIDER*/
                        TRY_CAST(fin.Financial_Division AS INT)                                               AS ordering_financial_division_id,
                        NULLIF(TRY_CAST(fin.Financial_Division_Name AS VARCHAR(150)), 'na')                   AS ordering_financial_division_name,
                        TRY_CAST(fin.Financial_SubDivision AS INT)                                            AS ordering_financial_sub_division_id,
                        NULLIF(TRY_CAST(fin.Financial_SubDivision_Name AS VARCHAR(150)), 'na')                AS ordering_financial_sub_division_name,
                                      /* ORDERING PROVIDER SOM FIELDS */
                        TRY_CAST(ord_org.som_hs_area_id AS INT)                                               AS ordering_som_hs_area_id,
                        CAST(ord_org.som_hs_area_name AS VARCHAR(150))                                        AS ordering_som_hs_area_name,
                        TRY_CAST(ord_org.som_group_id AS INT)                                                 AS ordering_som_group_id,
                        CAST(ord_org.som_group_name AS VARCHAR(150))                                          AS ordering_som_group_name,
                        TRY_CAST(ord_org.Department_ID AS INT)                                                AS ordering_som_department_id,
                        CAST(ord_org.Department AS VARCHAR(150))                                              AS ordering_som_department_name,
                        TRY_CAST(ord_org.Org_Number AS INT)                                                   AS ordering_som_division_id,
                        CAST(ord_org.Organization AS VARCHAR(150))                                            AS ordering_som_division_name,
                        fin.app_flag, /*Advanced practice provider flag*/

                                      /* OPTIONAL FLAGS */
                        CAST(CASE
                                 WHEN age.age_at_encounter < 18
                                     THEN
                                     1
                                 ELSE
                                     0
                             END AS SMALLINT)                                                                 AS peds_flag,
                                      /* CUSTOM FIELDS SPECIFIC TO METRIC */
                        fin.Disch_Disp_Descr                                                                  AS discharge_destination,
                        fin.drg_code_name,
                        fin.LOS,
                        fin.sk_Dim_Clrt_Pt_Cls,
                        pat_cls.Pt_Cls_Nme                                                                    AS patient_class,
                                      /* DISCHARGE ORDER TIME METRICS */
                        fin.Ordr_Dtm,
                        fin.Ordr_Tm,
                        CASE
                            WHEN fin.Ordr_Tm <= '10:00:00'
                                THEN
                                1
                            ELSE
                                0
                        END                                                                                   AS dc_order_by_10am_count,
                        DATEDIFF(MINUTE, '10:00:00', fin.Ordr_Tm)                                             AS mins_from_10am_to_dc_order,
                                      /* DISCHARGE TIME METRICS */
                        fin.Discharge_DtTm,
                        fin.Discharge_Tm,
                        CASE
                            WHEN fin.Discharge_Tm <= '12:00:00'
                                THEN
                                1
                            ELSE
                                0
                        END                                                                                   AS dc_actual_by_noon_count,
                        DATEDIFF(MINUTE, '12:00:00', fin.Discharge_Tm)                                        AS mins_from_noon_to_dc_actual,
                        DATEDIFF(MINUTE, fin.[Ordr_Dtm], fin.Discharge_DtTm)                                  AS mins_from_dc_ord_to_dc_actual,
                                      /* TARGETED ACUITY PROGRESSION LEVEL METRICS */
                        CASE
                            WHEN fin.acuity_score IS NULL
                                THEN
                                1
                            ELSE
                                0
                        END                                                                                   AS dc_with_targeted_acuity_count,
                        fin.time_entered,
                        fin.acuity_score,
                                      /* ADDITION OF PROV TEAM AND ORD TO ACTUAL IN HOURS FOR USE ON PATIENT PROGRESSION DASH */
                        prov_team_dc.RECORD_NAME                                                              AS PROV_TEAM,
                        CAST((DATEDIFF(MINUTE, fin.[Ordr_Dtm], fin.Discharge_DtTm)) / 60.0 AS NUMERIC(18, 2)) AS hrs_from_dc_ord_to_dc_actual,
                                      /* ADDITION OF EXPECTED DISCHARGE METRICS*/
                        CASE
                            WHEN CAST(fin.EXPECTED_DISCH_DTTM_HX_TIME AS DATE) = CAST(fin.Discharge_DtTm AS DATE)
                                 AND DATEPART(HOUR, fin.Discharge_DtTm) < 12
                                THEN
                                1
                            ELSE
                                0
                        END                                                                                   AS expected_and_actual_morning_dc,
                        CASE
                            WHEN fin.EMPlye_Nme IS NOT NULL
                                THEN
                                1
                            ELSE
                                0
                        END                                                                                   AS expected_morning_dc,
                        fin.EMPlye_Nme                                                                        AS expected_entry_emplye_nme,
                        fin.ORIG_EVENT_TIME                                                                   AS dc_entry_timestamp,
                        DATEDIFF(MINUTE, fin.Discharge_DtTm, fin.ORIG_EVENT_TIME)                             AS mins_from_dc_entry_to_dc_actual,
                                      /* ADDITION OF DEPARTMENT_TYPE */
                        fin.department_type,
                        fin.sk_Dim_Pt,
                        fin.sk_Fact_Pt_Enc_Clrt
				INTO #RptgTbl
                FROM
                        DS_HSDW_Prod.Rptg.vwDim_Date AS cal
                    LEFT OUTER JOIN
                        ((FINAL                         AS fin
                    INNER JOIN
                        DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
                            ON fin.sk_Dim_Clrt_Pt = pat.sk_Dim_Clrt_Pt
                               AND pat.IS_VALID_PAT_YN = 'Y')

                    /* last attending provider */
                    LEFT OUTER JOIN
                        DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc AS ser
                            ON fin.Prov_Id = ser.PROV_ID

                    /* peds calculation */
                    OUTER APPLY
                        (
                            SELECT
                                DATEDIFF(YEAR, pat.BirthDate, fin.Discharge_DtTm)/* find difference in years between the birthdate and event date */
                                - CASE
                                      WHEN DATEADD(YEAR, DATEDIFF(YEAR, pat.BirthDate, fin.Discharge_DtTm), pat.BirthDate)/* add numbers of years difference to birthdate */
                                > fin.Discharge_DtTm
                                          THEN
                                          1 /* if this date is after the event date, subtract a year */
                                      ELSE
                                          0
                                  END AS age_at_encounter
                        )                                   AS age
                    LEFT OUTER JOIN
                        DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt                       AS dept
                            ON fin.sk_Dim_Clrt_DEPt = dept.sk_Dim_Clrt_DEPt
                    LEFT OUTER JOIN
                        (
                            --snippet to query mdm history for hs area values
                            SELECT  DISTINCT
                                    rmlmh.EPIC_DEPARTMENT_ID,
                                    hx.max_dt,
                                    rmlmh.EPIC_DEPT_NAME,
                                    rmlmh.EPIC_EXT_NAME,
                                    rmlmh.EPIC_DEPT_TYPE,
                                    rmlmh.HS_AREA_ID,
                                    rmlmh.HS_AREA_NAME
                            FROM
                                    DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History AS rmlmh
                                INNER JOIN
                                    ( --hx--most recent batch date per dep id
                                        SELECT
                                            mdmhx.EPIC_DEPARTMENT_ID,
                                            MAX(mdmhx.BATCH_RUN_DT) AS max_dt
                                        FROM
                                            DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History AS mdmhx
                                        GROUP BY
                                            mdmhx.EPIC_DEPARTMENT_ID
                                    )                                                   AS hx
                                        ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
                                           AND rmlmh.BATCH_RUN_DT = hx.max_dt
                        )                                                       AS mdm
                            ON dept.DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID
                    LEFT OUTER JOIN
                        DS_HSDW_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv AS org
                            ON ser.Financial_SubDivision = org.Epic_Financial_Subdivision_Code
                    LEFT OUTER JOIN
                        DS_HSDW_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv AS ord_org /*To get som divisions for ordering provider*/
                            ON fin.Financial_SubDivision = ord_org.Epic_Financial_Subdivision_Code
                    LEFT OUTER JOIN
                        DS_HSDW_Prod.Rptg.vwDim_Clrt_Pt_Cls                     AS pat_cls
                            ON fin.sk_Dim_Clrt_Pt_Cls = pat_cls.sk_Dim_Clrt_Pt_Cls)
                            ON cal.day_date = fin.Discharge_Dt
                    OUTER APPLY
                        ( --prov_team_dc
                            SELECT
                                ppt_dc.PAT_ENC_CSN_ID,
                                ppt_dc.LINE,
                                ppt_dc.ID,
                                ppt_dc.RECORD_NAME,
                                ppt_dc.TEAM_AUDIT_ID,
                                ppt_dc.Team_Action,
                                ppt_dc.PRIMARYTEAM_AUDI_YN,
                                ppt_dc.TEAMAUDIT_USER_ID,
                                ppt_dc.TEAM_AUDIT_INSTANT,
                                ppt_dc.NEXT_AUDIT_INSTANT
                            FROM
                                ( --ppt_dc
                                    SELECT
                                            prteam.PAT_ENC_CSN_ID,
                                            prteam.Team_LINE                       AS LINE,
                                            team.ID,
                                            team.RECORD_NAME,
                                            prteam.ID                              AS TEAM_AUDIT_ID,
                                            prteam.Team_Action,
                                            prteam.PRIMARYTEAM_AUDI_YN,
                                            prov_emp.EMPlye_Usr_ID                 AS TEAMAUDIT_USER_ID,
                                            prteam.TEAM_AUDIT_INSTANT,
                                            LEAD(prteam.TEAM_AUDIT_INSTANT) OVER (PARTITION BY
                                                                                      prteam.PAT_ENC_CSN_ID
                                                                                  ORDER BY
                                                                                      prteam.TEAM_AUDIT_INSTANT
                                                                                 ) AS NEXT_AUDIT_INSTANT
                                    FROM
                                            DS_HSDW_Prod.dbo.Fact_Clrt_EPT_TEAM_AUDIT  AS prteam
                                        INNER JOIN
                                            DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye        AS prov_emp
                                                ON prov_emp.sk_Dim_Clrt_EMPlye = prteam.sk_Dim_Clrt_EMPlye
                                        INNER JOIN
                                            DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc        AS prov_ser
                                                ON prov_ser.sk_Dim_Clrt_SERsrc = prov_emp.sk_Dim_Clrt_SERsrc
                                        INNER JOIN
                                            DS_HSDW_Prod.dbo.Dim_Clrt_Prov_Team_Record AS team
                                                ON team.ID = prteam.ID
                                    WHERE
                                            prteam.PRIMARYTEAM_AUDI_YN = 'Y' --primary prov team
                                            AND prteam.PAT_ENC_CSN_ID = fin.PAT_ENC_CSN_ID
                                ) AS ppt_dc
                            WHERE
                                fin.Discharge_DtTm >= TEAM_AUDIT_INSTANT
                                AND fin.Discharge_DtTm < COALESCE(NEXT_AUDIT_INSTANT, GETDATE())
                        )                            AS prov_team_dc
                WHERE
                        1 = 1
                        AND cal.day_date >= @startdate
                        AND cal.day_date < @enddate

SELECT
	*
INTO #summary
FROM #RptgTbl
WHERE 1 = 1
AND event_count = 0
AND hs_area_id = 1
AND event_date >= '11/1/2025'
AND event_date <= '11/30/2025'
--AND event_date >= '7/1/2025'
--AND event_date <= '9/30/2025'
ORDER BY
	event_date

SELECT
	AVG(mins_from_dc_ord_to_dc_actual) AS Average_Minutes,
	MIN(mins_from_dc_ord_to_dc_actual) AS Min_Minutes,
	MAX(mins_from_dc_ord_to_dc_actual) AS Max_Minutes
FROM #summary
WHERE 1 = 1
AND event_date >= '11/1/2025' AND event_date <= '11/30/2025'
--AND event_date >= '7/1/2025' AND event_date <= '9/30/2025'
AND hs_area_id = 1

SELECT DISTINCT
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [mins_from_dc_ord_to_dc_actual]) OVER () AS Median_Minutes
FROM #summary
WHERE 1 = 1
AND event_date >= '11/1/2025' AND event_date <= '11/30/2025'
--AND event_date >= '7/1/2025' AND event_date <= '9/30/2025'
AND hs_area_id = 1

SELECT
	*
INTO #summary2
FROM #RptgTbl
WHERE 1 = 1
AND event_count = 0
AND hs_area_id = 1
--AND event_date >= '9/1/2025'
--AND event_date <= '9/30/2025'
AND event_date >= '7/1/2025'
AND event_date <= '11/30/2025'
ORDER BY
	event_date

SELECT
	AVG(mins_from_dc_ord_to_dc_actual) AS Average_Minutes,
	MIN(mins_from_dc_ord_to_dc_actual) AS Min_Minutes,
	MAX(mins_from_dc_ord_to_dc_actual) AS Max_Minutes
FROM #summary2
WHERE 1 = 1
--AND event_date >= '9/1/2025' AND event_date <= '9/30/2025'
AND event_date >= '7/1/2025' AND event_date <= '11/30/2025'
AND hs_area_id = 1

SELECT DISTINCT
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [mins_from_dc_ord_to_dc_actual]) OVER () AS Median_Minutes
FROM #summary2
WHERE 1 = 1
--AND event_date >= '9/1/2025' AND event_date <= '9/30/2025'
AND event_date >= '7/1/2025' AND event_date <= '11/30/2025'
AND hs_area_id = 1

GO


