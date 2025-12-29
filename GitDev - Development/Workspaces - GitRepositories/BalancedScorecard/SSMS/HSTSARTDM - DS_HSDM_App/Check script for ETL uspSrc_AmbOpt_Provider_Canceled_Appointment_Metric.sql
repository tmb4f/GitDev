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
SET @startdate = '4/27/2025 00:00 AM'
--SET @enddate = '6/30/2021 11:59 PM'
--SET @enddate = '7/14/2021 11:59 PM'
--SET @enddate = '7/31/2022 11:59 PM'
--SET @enddate = '12/31/2022 11:59 PM'
--SET @enddate = '12/31/2024 11:59 PM'
SET @enddate = '5/6/2025 11:59 PM'

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
Provider-initiated Cancellation Rate

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure DM DS_HSDM_App ETL uspSrc_AmbOpt_Provider_Canceled_Appointment_Metric.sql
*/

IF OBJECT_ID('tempdb..#main2 ') IS NOT NULL
DROP TABLE #main2

IF OBJECT_ID('tempdb..#RptgTmp2 ') IS NOT NULL
DROP TABLE #RptgTmp2

IF OBJECT_ID('tempdb..#aggregate ') IS NOT NULL
DROP TABLE #aggregate


SELECT evnts2.*
     , CASE WHEN evnts2.Bump = 1 AND evnts2.CANCEL_LEAD_HOURS <= 24 THEN 1 ELSE 0 END AS Bump_WIn_24_Hrs
     , SUM(evnts2.Bump) OVER (PARTITION BY evnts2.APPT_SERIAL_NUM ORDER BY evnts2.APPT_SERIAL_NUM) AS ASN_Bumps
     , SUM(evnts2.Bump_WIn_45_Days) OVER (PARTITION BY evnts2.APPT_SERIAL_NUM ORDER BY evnts2.APPT_SERIAL_NUM) AS ASN_Bumps_WIn_45_Days
     , SUM(evnts2.Bump_WIn_30_Days) OVER (PARTITION BY evnts2.APPT_SERIAL_NUM ORDER BY evnts2.APPT_SERIAL_NUM) AS ASN_Bumps_WIn_30_Days
     , SUM(evnts2.appt_event_Provider_Canceled) OVER (PARTITION BY evnts2.APPT_SERIAL_NUM ORDER BY evnts2.APPT_SERIAL_NUM) AS ASN_appt_event_Provider_Canceled
	 , DATEDIFF(dd, evnts2.APPT_DT, evnts2.Next_APPT_DT) AS Rescheduled_Lag_Days

	INTO #main2

FROM
(
	SELECT evnts.*
		 , CASE WHEN evnts.appt_event_Provider_Canceled = 1 AND CANCEL_REASON_NAME = 'Provider Unavailable' AND Cancel_Lead_Days <= 30 THEN 1 ELSE 0 END AS Bump
		 , CASE WHEN evnts.appt_event_Provider_Canceled = 1 AND CANCEL_REASON_NAME = 'Provider Unavailable' AND Cancel_Lead_Days <= 45 THEN 1 ELSE 0 END AS Bump_WIn_45_Days
		 , CASE WHEN evnts.appt_event_Provider_Canceled = 1 AND CANCEL_REASON_NAME = 'Provider Unavailable' AND Cancel_Lead_Days <= 30 THEN 1 ELSE 0 END AS Bump_WIn_30_Days
		 , CASE WHEN COALESCE(evnts.appt_event_Canceled,0) = 0 OR evnts.appt_event_Canceled_Late = 1 OR (evnts.appt_event_Provider_Canceled = 1 AND CANCEL_REASON_NAME = 'Provider Unavailable' AND evnts.Cancel_Lead_Days <= 30) THEN 1 ELSE 0 END AS Appointment
		 , ROW_NUMBER() OVER (PARTITION BY evnts.APPT_SERIAL_NUM ORDER BY evnts.APPT_MADE_DTTM) AS Seq -- sequence number for identifying and ordering linked appointments
		 , LEAD(evnts.APPT_DT) OVER (PARTITION BY evnts.APPT_SERIAL_NUM ORDER BY evnts.APPT_MADE_DTTM) AS Next_APPT_DT

	FROM
	(
		SELECT DISTINCT
			main.epic_department_id,
			main.peds,
			main.transplant,
			main.sk_Dim_Pt,
			main.sk_Fact_Pt_Acct,
			main.sk_Fact_Pt_Enc_Clrt,
			main.person_birth_date,
			main.person_gender,
			main.person_id,
			main.person_name,
			main.provider_id,
			main.provider_name,
			main.APPT_STATUS_FLAG,
			main.APPT_STATUS_C,
			main.CANCEL_INITIATOR,
			main.CANCEL_REASON_C,
			main.APPT_DT,
			main.PAT_ENC_CSN_ID,
			main.PRC_ID,
			main.PRC_NAME,
			main.VIS_NEW_TO_SYS_YN,
			main.VIS_NEW_TO_DEP_YN,
			main.VIS_NEW_TO_PROV_YN,
			main.VIS_NEW_TO_SPEC_YN,
			main.VIS_NEW_TO_SERV_AREA_YN,
			main.VIS_NEW_TO_LOC_YN,
			main.APPT_MADE_DATE,
			main.ENTRY_DATE,
													-- Appt Status Flags
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_FLAG IN ( 'No Show' ))
				) THEN
					1
				ELSE
					0
			END AS appt_event_No_Show,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_FLAG IN ( 'Canceled Late' ))
				) THEN
					1
				ELSE
					0
			END AS appt_event_Canceled_Late,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_FLAG IN ( 'Canceled' ))
				) THEN
					1
				ELSE
					0
			END AS appt_event_Canceled,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_FLAG IN ( 'Scheduled' ))
				) THEN
					1
				ELSE
					0
			END AS appt_event_Scheduled,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_C = 3)
					AND (main.CANCEL_INITIATOR = 'PROVIDER')
				) THEN
					1
				ELSE
					0
			END AS appt_event_Provider_Canceled,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_C IN ( 2 ))
				) THEN
					1
				ELSE
					0
			END AS appt_event_Completed,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_C IN ( 6 ))
				) THEN
					1
				ELSE
					0
			END AS appt_event_Arrived,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.VIS_NEW_TO_SPEC_YN = 'Y')
				) THEN
					1
				ELSE
					0
			END AS appt_event_New_to_Specialty,
													-- Calculated columns
		-- Assumes that there is always a referral creation date (CHANGE_DATE) documented when a referral entry date (ENTRY_DATE) is documented
			CASE
				WHEN main.ENTRY_DATE IS NULL THEN
					main.APPT_MADE_DATE
				WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
					main.APPT_MADE_DATE
				WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
					main.ENTRY_DATE
				ELSE
					main.CHANGE_DATE
			END AS Appointment_Request_Date,

			main.DEPT_SPECIALTY_NAME,
			main.PROV_SPECIALTY_NAME,
			main.APPT_DTTM,
			main.CANCEL_REASON_NAME,
			main.SER_RPT_GRP_SIX,
			main.SER_RPT_GRP_EIGHT,
			main.CANCEL_LEAD_HOURS,
			main.APPT_CANC_DTTM,
			main.PHONE_REM_STAT_NAME,
			main.CHANGE_DATE,
			CASE
				WHEN
				(
					(main.APPT_STATUS_FLAG IS NOT NULL)
					AND (main.APPT_STATUS_FLAG IN ( 'Canceled','Canceled Late' ))
				) THEN
					DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT)
				ELSE
					CAST(NULL AS INT)
			END AS Cancel_Lead_Days,
			main.APPT_MADE_DTTM,
			main.APPT_SERIAL_NUM,
			main.BILL_PROV_YN,
			main.APPT_ENTRY_USER_ID,
			main.APPT_CANC_USER_ID,
			main.Load_Dtm,
		    main.PROV_TYPE_OT_NAME,
			main.F2F_Flag,
		    main.ENC_TYPE_C,
		    main.ENC_TYPE_TITLE

		FROM
		( --main
			SELECT
					appts.DEPARTMENT_ID AS epic_department_id,
					CAST(CASE
							WHEN FLOOR((CAST(appts.APPT_DT AS INTEGER)
										- CAST(CAST(pat.BirthDate AS DATETIME) AS INTEGER)
										) / 365.25
										) < 18 THEN
								1
							ELSE
								0
						END AS SMALLINT) AS peds,
					CAST(CASE
							WHEN tx.pat_enc_csn_id IS NOT NULL THEN
								1
							ELSE
								0
						END AS SMALLINT) AS transplant,
					appts.sk_Dim_Pt,
					appts.sk_Fact_Pt_Acct,
					appts.sk_Fact_Pt_Enc_Clrt,
					pat.BirthDate AS person_birth_date,
					pat.Sex AS person_gender,
					CAST(appts.IDENTITY_ID AS INT) AS person_id,
					pat.Name AS person_name,
					appts.PROV_ID AS provider_id,
					appts.PROV_NAME AS provider_name,
					--Select
					appts.APPT_STATUS_FLAG,
					appts.APPT_STATUS_C,		
					appts.CANCEL_INITIATOR,
		            appts.CANCEL_REASON_C,
					appts.APPT_DT,
					appts.PAT_ENC_CSN_ID,
					appts.PRC_ID,
					appts.PRC_NAME,
					COALESCE(appts.VIS_NEW_TO_SYS_YN,'N') AS VIS_NEW_TO_SYS_YN,
					COALESCE(appts.VIS_NEW_TO_DEP_YN,'N') AS VIS_NEW_TO_DEP_YN,
					COALESCE(appts.VIS_NEW_TO_PROV_YN,'N') AS VIS_NEW_TO_PROV_YN,
					COALESCE(appts.VIS_NEW_TO_SPEC_YN,'N') AS VIS_NEW_TO_SPEC_YN,
					COALESCE(appts.VIS_NEW_TO_SERV_AREA_YN,'N') AS VIS_NEW_TO_SERV_AREA_YN,
					COALESCE(appts.VIS_NEW_TO_LOC_YN,'N') AS VIS_NEW_TO_LOC_YN,
		            appts.APPT_MADE_DATE,
		            appts.ENTRY_DATE,
					appts.DEPT_SPECIALTY_NAME,
					appts.PROV_SPECIALTY_NAME,
					appts.APPT_DTTM,
					appts.CANCEL_REASON_NAME,
					appts.SER_RPT_GRP_SIX,
					appts.SER_RPT_GRP_EIGHT,
					appts.CANCEL_LEAD_HOURS,
					appts.APPT_CANC_DTTM,
					appts.PHONE_REM_STAT_NAME,
					appts.CHANGE_DATE,
					appts.APPT_MADE_DTTM,
					appts.APPT_SERIAL_NUM,
					appts.BILL_PROV_YN,
					appts.APPT_ENTRY_USER_ID,
					appts.APPT_CANC_USER_ID,
					appts.Load_Dtm,
					appts.PROV_TYPE_OT_NAME,
					appts.F2F_Flag,
				    appts.ENC_TYPE_C,
				    appts.ENC_TYPE_TITLE

			FROM Stage.Scheduled_Appointment AS appts
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
					ON pat.sk_Dim_Pt = appts.sk_Dim_Pt

				-- -------------------------------------
				-- Identify transplant encounter--
				-- -------------------------------------
				LEFT OUTER JOIN
				(
					SELECT DISTINCT
						btd.pat_enc_csn_id,
						btd.Event_Transplanted AS transplant_surgery_dt,
						btd.hosp_admsn_time AS Adm_Dtm
					FROM DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart AS btd
					WHERE (
								btd.TX_Episode_Phase = 'transplanted'
								AND btd.TX_Stat_Dt >= @locstartdate 
								AND btd.TX_Stat_Dt <  @locenddate
							)
							AND btd.TX_GroupedPhaseStatus = 'TX-ADMIT'
				) AS tx
					ON appts.PAT_ENC_CSN_ID = tx.pat_enc_csn_id

				-- -------------------------------------
				-- Excluded departments--
				-- -------------------------------------
				LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
					ON excl.DEPARTMENT_ID = appts.DEPARTMENT_ID

			WHERE (appts.APPT_DT >= @locstartdate
				AND appts.APPT_DT < @locenddate)
			AND excl.DEPARTMENT_ID IS NULL
		    AND pat.IS_VALID_PAT_YN = 'Y'
			AND appts.ENC_TYPE_C NOT IN ('2505','2506')

		) AS main
	) AS evnts
) AS evnts2
ORDER BY
	APPT_DT,
	epic_department_id,
	PROV_TYPE_OT_NAME,
	provider_id,
	provider_name,
	BILL_PROV_YN,
	DEPT_SPECIALTY_NAME,
	PROV_SPECIALTY_NAME,
	SER_RPT_GRP_SIX,
	SER_RPT_GRP_EIGHT

  -- Create index for temp table #main2

CREATE NONCLUSTERED INDEX IX_NC_BUMP ON #main2 (APPT_DT, epic_department_id, PROV_TYPE_OT_NAME, provider_id)

SELECT CAST('CanceledByProvider' AS VARCHAR(50)) AS event_type,
       rpt.event_count,
	   rpt.event_date,
       rpt.fmonth_num,
       rpt.Fyear_num,
       rpt.FYear_name,
       rpt.report_period,
       rpt.report_date,
	   rpt.event_category,
	   mdmloc.pod_id,
       mdmloc.pod_name,
	   mdmloc.hub_id,
	   mdmloc.hub_name,
       rpt.epic_department_id,
       mdm.epic_department_name AS epic_department_name,
       mdm.epic_department_name_external AS epic_department_name_external,
       rpt.peds,
       rpt.transplant,
       rpt.sk_Dim_Pt,
       rpt.sk_Fact_Pt_Acct,
       rpt.sk_Fact_Pt_Enc_Clrt,
	   rpt.person_birth_date,
	   rpt.person_gender,
	   rpt.person_id,
	   rpt.person_name,
       CAST(NULL AS INT) AS practice_group_id,
       CAST(NULL AS VARCHAR(150)) AS practice_group_name,
       rpt.provider_id,
	   rpt.provider_name,
	   mdm.service_line_id,
	   mdm.service_line,
       physsvc.Service_Line_ID AS prov_service_line_id,
       physsvc.Service_Line AS prov_service_line,
	   mdm.sub_service_line_id,
	   mdm.sub_service_line,
	   mdm.opnl_service_id,
	   mdm.opnl_service_name,
	   mdm.corp_service_line_id,
	   mdm.corp_service_line,
	   mdm.hs_area_id,
	   mdm.hs_area_name,
       physsvc.hs_area_id AS prov_hs_area_id,
       physsvc.hs_area_name AS prov_hs_area_name,
	   rpt.APPT_STATUS_FLAG,
	   rpt.CANCEL_REASON_C,
       rpt.APPT_DT,
	   rpt.Next_APPT_DT,
	   rpt.Rescheduled_Lag_Days,
	   rpt.PAT_ENC_CSN_ID,
	   rpt.PRC_ID,
	   rpt.PRC_NAME,
	   ser.sk_Dim_Physcn,
	   doc.UVaID,
	   rpt.VIS_NEW_TO_SYS_YN,
	   rpt.VIS_NEW_TO_DEP_YN,
	   rpt.VIS_NEW_TO_PROV_YN,
	   rpt.VIS_NEW_TO_SPEC_YN,
	   rpt.VIS_NEW_TO_SERV_AREA_YN,
	   rpt.VIS_NEW_TO_LOC_YN,
       rpt.APPT_MADE_DATE,
       rpt.ENTRY_DATE,
       rpt.appt_event_No_Show,
       rpt.appt_event_Canceled_Late,
       rpt.appt_event_Canceled,
       rpt.appt_event_Scheduled,
       rpt.appt_event_Provider_Canceled,
       rpt.appt_event_Completed,
       rpt.appt_event_Arrived,
       rpt.appt_event_New_to_Specialty,
	   rpt.Appointment_Lag_Days,
	   rpt.DEPT_SPECIALTY_NAME,
	   rpt.PROV_SPECIALTY_NAME,
	   rpt.APPT_DTTM,
	   rpt.CANCEL_REASON_NAME,
	   rpt.SER_RPT_GRP_SIX AS financial_division,
	   rpt.SER_RPT_GRP_EIGHT AS financial_subdivision,
	   rpt.CANCEL_INITIATOR,
	   rpt.CANCEL_LEAD_HOURS,
	   rpt.APPT_CANC_DTTM,
	   rpt.Entry_UVaID,
	   rpt.Canc_UVaID,
	   rpt.PHONE_REM_STAT_NAME,
	   rpt.Cancel_Lead_Days,
	   rpt.APPT_MADE_DTTM,
	   COALESCE(rpt.PROV_TYPE_OT_NAME, ser.Prov_Typ, NULL) AS Prov_Typ,
	   ser.Staff_Resource,				   
    -- SOM
	   dvsn.som_group_id,
	   dvsn.som_group_name,
	   mdmloc.LOC_ID AS rev_location_id,
	   mdmloc.REV_LOC_NAME AS rev_location,
	   TRY_CAST(ser.Financial_Division AS INT) AS financial_division_id,
	   CASE WHEN ser.Financial_Division_Name <> 'na' THEN CAST(ser.Financial_Division_Name AS VARCHAR(150)) ELSE NULL END AS financial_division_name,
	   TRY_CAST(ser.Financial_SubDivision AS INT) AS financial_sub_division_id,
	   CASE WHEN ser.Financial_SubDivision_Name <> 'na' THEN CAST(ser.Financial_SubDivision_Name AS VARCHAR(150)) ELSE NULL END AS financial_sub_division_name,
	   dvsn.Department_ID AS som_department_id,

				   CAST(dvsn.Department AS VARCHAR(150)) AS som_department_name,
				   CAST(dvsn.Org_Number AS INT) AS som_division_id,
				   CAST(dvsn.Organization AS VARCHAR(150)) AS som_division_name,

	   dvsn.som_hs_area_id,
	   dvsn.som_hs_area_name,
	   rpt.APPT_SERIAL_NUM,
	   rpt.Appointment_Request_Date,
	   rpt.BILL_PROV_YN,
       rpt.Bump,
       rpt.Appointment,
	   mdmloc.UPG_PRACTICE_FLAG AS upg_practice_flag,
	   CAST(mdmloc.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id,
	   CAST(mdmloc.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name,
	   CAST(mdmloc.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id,
	   CAST(mdmloc.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name,
	   rpt.F2F_Flag,
	   rpt.ENC_TYPE_C,
	   rpt.ENC_TYPE_TITLE,
	   CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag,
	   mdmloc.FINANCE_COST_CODE,
	   dep.Prov_Based_Clinic,
	   map.Map_Type,
	   rpt.Bump_WIn_24_Hrs,
	   rpt.Bump_WIn_45_Days,
	   rpt.Bump_WIn_30_Days,
	   o.organization_id,
	   o.organization_name,
	   s.service_id,
	   s.service_name,
	   c.clinical_area_id,
	   c.clinical_area_name,
	   g.ambulatory_flag,
	   g.community_health_flag,
		   CASE WHEN COALESCE(rpt.PROV_TYPE_OT_NAME, ser.Prov_Typ, NULL) IN (
					'Anesthesiologist',
					--'Audiologist',
					--'Clinical Social Worker',
					--'Counselor',
					'Dentist',
					'Doctor of Philosophy',
					--'Fellow',  -- 01/13/2025
					--'Financial Counselor',
					--'Genetic Counselor',
					--'Hygienist',
					--'Licensed Clinical Social Worker',
					--'Licensed Nurse',
					--'LPC Resident',
					--'Medical Assistant',
					'Nurse Practitioner',
					--'Nutritionist',
					--'Occupational Therapist',
					'Optometrist',
					--'P&O Practitioner',
					--'Pharmacist',
					--'Physical Therapist',
					'Physician',
					'Physician Assistant',
					'Psychologist'--,
					--'Registered Dietitian',
					--'Registered Nurse',
					--'Resident',
					--'Resource',
					--'Social Worker',
					--'Speech and Language Pathologist',
					--'Technician',
					--'Unknown'
		   ) THEN 1 ELSE 0 END AS AMB_Scorecard_Flag

INTO #RptgTmp2

FROM
(
SELECT aggr.APPT_DT,
       aggr.epic_department_id,
	   aggr.PROV_TYPE_OT_NAME,
       aggr.provider_id,
       aggr.provider_name,
	   aggr.BILL_PROV_YN,
	   aggr.DEPT_SPECIALTY_NAME,
	   aggr.PROV_SPECIALTY_NAME,
	   aggr.SER_RPT_GRP_SIX,
	   aggr.SER_RPT_GRP_EIGHT,
       CASE
           WHEN aggr.APPT_DT IS NOT NULL THEN
               1
           ELSE
               0
       END AS event_count,
       date_dim.day_date AS event_date,
       date_dim.fmonth_num,
       date_dim.Fyear_num,
       date_dim.FYear_name,
       CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
       CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date,
       CAST('Aggregate' AS VARCHAR(150)) AS event_category,
	   aggr.peds,
	   aggr.transplant,
       CAST(NULL AS INT) AS sk_Dim_Pt,
       CAST(NULL AS INT) AS sk_Fact_Pt_Acct,
       CAST(NULL AS INT) AS sk_Fact_Pt_Enc_Clrt,
       CAST(NULL AS DATE) AS person_birth_date,
       CAST(NULL AS VARCHAR(254)) AS person_gender,
       CAST(NULL AS INT) AS person_id,
       CAST(NULL AS VARCHAR(200)) AS person_name,
       CAST(NULL AS VARCHAR(254)) AS APPT_STATUS_FLAG,
       CAST(NULL AS INT) AS CANCEL_REASON_C,
       CAST(NULL AS NUMERIC(18,0)) AS PAT_ENC_CSN_ID,
       CAST(NULL AS VARCHAR(18)) AS PRC_ID,
       CAST(NULL AS VARCHAR(200)) AS PRC_NAME,

       CAST(NULL AS INT) AS VIS_NEW_TO_SYS_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_DEP_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_PROV_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_SPEC_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_SERV_AREA_YN,
       CAST(NULL AS INT) AS VIS_NEW_TO_LOC_YN,

       CAST(NULL AS DATETIME) AS APPT_MADE_DATE,
       CAST(NULL AS DATETIME) AS ENTRY_DATE,

--For tableau calc purposes null = 0 per Sue.
       ISNULL(aggr.appt_event_No_Show,CAST(0 AS INT)) AS appt_event_No_Show,
       ISNULL(aggr.appt_event_Canceled_Late,CAST(0 AS INT)) AS appt_event_Canceled_Late,
       ISNULL(aggr.appt_event_Canceled,CAST(0 AS INT)) AS appt_event_Canceled,
       ISNULL(aggr.appt_event_Scheduled,CAST(0 AS INT)) AS appt_event_Scheduled,
       ISNULL(aggr.appt_event_Provider_Canceled,CAST(0 AS INT)) AS appt_event_Provider_Canceled,
       ISNULL(aggr.appt_event_Completed,CAST(0 AS INT)) AS appt_event_Completed,
       ISNULL(aggr.appt_event_Arrived,CAST(0 AS INT)) AS appt_event_Arrived,
       ISNULL(aggr.appt_event_New_to_Specialty,CAST(0 AS INT)) AS appt_event_New_to_Specialty,
	   	   
       ISNULL(aggr.Bump,CAST(0 AS INT)) AS Bump,
       ISNULL(aggr.Appointment,CAST(0 AS INT)) AS Appointment,
       ISNULL(aggr.Bump_WIn_24_Hrs,CAST(0 AS INT)) AS Bump_WIn_24_Hrs,
       ISNULL(aggr.Bump_WIn_45_Days,CAST(0 AS INT)) AS Bump_WIn_45_Days,
       ISNULL(aggr.Bump_WIn_30_Days,CAST(0 AS INT)) AS Bump_WIn_30_Days,

       CAST(NULL AS INT) AS Appointment_Lag_Days,
	   CAST(NULL AS DATETIME) AS APPT_DTTM,
       CAST(NULL AS VARCHAR(254)) AS CANCEL_REASON_NAME,
       CAST(NULL AS VARCHAR(55)) AS CANCEL_INITIATOR,
       CAST(NULL AS INT) AS CANCEL_LEAD_HOURS,
       CAST(NULL AS DATETIME) AS APPT_CANC_DTTM,
       CAST(NULL AS VARCHAR(254)) AS Entry_UVaID,
       CAST(NULL AS VARCHAR(254)) AS Canc_UVaID,
       CAST(NULL AS VARCHAR(254)) AS PHONE_REM_STAT_NAME,
       CAST(NULL AS INT) AS Cancel_Lead_Days,
       CAST(NULL AS DATETIME) AS APPT_MADE_DTTM,
       CAST(NULL AS NUMERIC(18,0)) AS APPT_SERIAL_NUM,
       CAST(NULL AS DATETIME) AS Appointment_Request_Date,
       CAST(NULL AS SMALLDATETIME) AS Load_Dtm,
       CAST(NULL AS DATETIME) AS Next_APPT_DT,
       CAST(NULL AS INT) AS Rescheduled_Lag_Days,
       CAST(NULL AS INT) AS F2F_Flag,
       CAST(NULL AS VARCHAR(66)) AS ENC_TYPE_C,
       CAST(NULL AS VARCHAR(254)) AS ENC_TYPE_TITLE

FROM
(
    SELECT day_date,
           fmonth_num,
           Fyear_num,
           FYear_name
    FROM DS_HSDW_Prod.Rptg.vwDim_Date

) date_dim
    LEFT OUTER JOIN
    (
		SELECT
			   evnts.APPT_DT,
			   evnts.epic_department_id,
			   evnts.PROV_TYPE_OT_NAME,
			   evnts.provider_id,
			   evnts.provider_name,
			   evnts.BILL_PROV_YN,
			   evnts.DEPT_SPECIALTY_NAME,
			   evnts.PROV_SPECIALTY_NAME,
			   evnts.SER_RPT_GRP_SIX,
			   evnts.SER_RPT_GRP_EIGHT,
			   evnts.peds,
			   evnts.transplant,
			   SUM(evnts.appt_event_No_Show) AS appt_event_No_Show,
			   SUM(evnts.appt_event_Canceled_Late) AS appt_event_Canceled_Late,
			   SUM(evnts.appt_event_Canceled) AS appt_event_Canceled,
			   SUM(evnts.appt_event_Scheduled) AS appt_event_Scheduled,
			   SUM(evnts.appt_event_Provider_Canceled) AS appt_event_Provider_Canceled,
			   SUM(evnts.appt_event_Completed) AS appt_event_Completed,
			   SUM(evnts.appt_event_Arrived) AS appt_event_Arrived,
			   SUM(evnts.appt_event_New_to_Specialty) AS appt_event_New_to_Specialty,
			   SUM(evnts.Bump) AS Bump,
			   SUM(evnts.Appointment) AS Appointment,
			   SUM(evnts.Bump_WIn_24_Hrs) AS Bump_WIn_24_Hrs,
			   SUM(evnts.Bump_WIn_45_Days) AS Bump_WIn_45_Days,
			   SUM(evnts.Bump_WIn_30_Days) AS Bump_WIn_30_Days

		FROM #main2 evnts
		GROUP BY
			evnts.APPT_DT,
			evnts.epic_department_id,
			evnts.PROV_TYPE_OT_NAME,
			evnts.provider_id,
			evnts.provider_name,
			evnts.BILL_PROV_YN,
			evnts.DEPT_SPECIALTY_NAME,
			evnts.PROV_SPECIALTY_NAME,
			evnts.SER_RPT_GRP_SIX,
			evnts.SER_RPT_GRP_EIGHT,
			evnts.peds,
			evnts.transplant
	) aggr
		
        ON (date_dim.day_date = CAST(aggr.APPT_DT AS SMALLDATETIME))

WHERE date_dim.day_date >= @locstartdate
      AND date_dim.day_date < @locenddate
UNION ALL
SELECT main.APPT_DT,
       main.epic_department_id,
	   main.PROV_TYPE_OT_NAME,
       main.provider_id,
       main.provider_name,
	   main.BILL_PROV_YN,
	   main.DEPT_SPECIALTY_NAME,
	   main.PROV_SPECIALTY_NAME,
	   main.SER_RPT_GRP_SIX,
	   main.SER_RPT_GRP_EIGHT,
	   --main.appt_event_Provider_Canceled AS event_count,
	   CASE WHEN main.appt_event_Provider_Canceled = 1 AND main.Cancel_Lead_Days <= 30 AND main.CANCEL_REASON_NAME = 'Provider Unavailable' THEN 1 ELSE 0 END AS event_count,
       date_dim.day_date AS event_date,
       date_dim.fmonth_num,
       date_dim.Fyear_num,
       date_dim.FYear_name,
       CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
       CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date,
       CAST('Detail' AS VARCHAR(150)) AS event_category,
       main.peds,
       main.transplant,
       main.sk_Dim_Pt,
       main.sk_Fact_Pt_Acct,
       main.sk_Fact_Pt_Enc_Clrt,
       main.person_birth_date,
       main.person_gender,
       main.person_id,
       main.person_name,
       main.APPT_STATUS_FLAG,
       main.CANCEL_REASON_C,
       main.PAT_ENC_CSN_ID,
       main.PRC_ID,
       main.PRC_NAME,

---BDD 5/9/2018 per Sue, change these from Y/N varchar(1) to 1/0 ints. Null = 0 per Tom and Sue
       CASE WHEN main.VIS_NEW_TO_SYS_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SYS_YN,
       CASE WHEN main.VIS_NEW_TO_DEP_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_DEP_YN,
       CASE WHEN main.VIS_NEW_TO_PROV_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_PROV_YN,
       CASE WHEN main.VIS_NEW_TO_SPEC_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SPEC_YN,
       CASE WHEN main.VIS_NEW_TO_SERV_AREA_YN = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SERV_AREA_YN,
       CASE WHEN main.VIS_NEW_TO_LOC_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_LOC_YN,

       main.APPT_MADE_DATE,
       main.ENTRY_DATE,

	   main.appt_event_No_Show,
       main.appt_event_Canceled_Late,
       main.appt_event_Canceled,
       main.appt_event_Scheduled,
       main.appt_event_Provider_Canceled,
       main.appt_event_Completed,
       main.appt_event_Arrived,
       main.appt_event_New_to_Specialty,
	   
       main.Bump,
	   main.Appointment,
	   main.Bump_WIn_24_Hrs,
	   main.Bump_WIn_45_Days,
	   main.Bump_WIn_30_Days,
	   
       CASE
           WHEN (main.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, main.Appointment_Request_Date, main.APPT_DT)
           ELSE CAST(NULL AS INT)
       END AS Appointment_Lag_Days,
       main.APPT_DTTM,
	   main.CANCEL_REASON_NAME,
	   main.CANCEL_INITIATOR,
	   main.CANCEL_LEAD_HOURS,
	   main.APPT_CANC_DTTM,
	   entryemp.EMPlye_Systm_Login AS Entry_UVaID,
	   cancemp.EMPlye_Systm_Login AS Canc_UVaID,
	   main.PHONE_REM_STAT_NAME,
	   main.Cancel_Lead_Days,
	   main.APPT_MADE_DTTM,
	   main.APPT_SERIAL_NUM,
	   main.Appointment_Request_Date,
	   main.Load_Dtm,
	   main.Next_APPT_DT,
	   main.Rescheduled_Lag_Days,
	   main.F2F_Flag,
	   main.ENC_TYPE_C,
	   main.ENC_TYPE_TITLE
FROM #main2 main
INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date date_dim
ON main.APPT_DT = date_dim.day_date
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp
	ON entryemp.EMPlye_Usr_ID = main.APPT_ENTRY_USER_ID
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye cancemp
	ON cancemp.EMPlye_Usr_ID = main.APPT_CANC_USER_ID
WHERE main.ASN_appt_event_Provider_Canceled > 0
) rpt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
ON ser.PROV_ID = rpt.provider_id
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
ON dep.DEPARTMENT_ID = rpt.epic_department_id
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
ON mdm.epic_department_id = rpt.epic_department_id
LEFT OUTER JOIN
(
    SELECT ROW_NUMBER() OVER (PARTITION BY EPIC_DEPARTMENT_ID ORDER BY mdm_LM.MDM_BATCH_ID DESC) AS Seq
	      ,CAST(NULL AS VARCHAR(66)) AS POD_ID
	      ,PFA_POD AS pod_name
	      ,HUB_ID
	      ,HUB AS hub_name
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
		  ,CAST(NULL AS INT) AS upg_practice_flag
		  ,CAST(NULL AS VARCHAR(254)) AS upg_practice_region_id
		  ,CAST(NULL AS VARCHAR(254)) AS upg_practice_region_name
		  ,CAST(NULL AS VARCHAR(66)) AS upg_practice_id
		  ,CAST(NULL AS VARCHAR(150)) AS upg_practice_name
		  ,mdm_LM.FINANCE_COST_CODE
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
			  ,FINANCE_COST_CODE
	    FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History) mdm_LM
) AS mdmloc
ON (mdmloc.EPIC_DEPARTMENT_ID = rpt.epic_department_id)
AND mdmloc.Seq = 1
LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
ON map.Deptid = CAST(mdmloc.FINANCE_COST_CODE AS INTEGER)
LEFT JOIN
(
    SELECT sk_Dim_Physcn,
            UVaID,
            Service_Line,
			ProviderGroup
    FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
    WHERE current_flag = 1
) AS doc
    ON doc.sk_Dim_Physcn = ser.sk_Dim_Physcn
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
    ON physsvc.Physician_Roster_Name = CASE
                                            WHEN (ser.sk_Dim_Physcn > 0) THEN
                                                doc.Service_Line
                                            ELSE
                                                'No Value Specified'
                                        END
-- -------------------------------------
-- SOM Financial Division Subdivision --
-- -------------------------------------
LEFT OUTER JOIN
(
	SELECT
		Epic_Financial_Division_Code,
        Epic_Financial_Subdivision_Code,
        Department,
        Department_ID,
        Organization,
        Org_Number,
        som_group_id,
        som_group_name,
		som_hs_area_id,
		som_hs_area_name
	FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
	ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.Financial_Division AS INT)
		AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.Financial_SubDivision AS INT))
-- -------------------------------------
-- Supplemental Org Group Names--
-- -------------------------------------
LEFT OUTER JOIN
(
	SELECT 
		EPIC_DEPARTMENT_ID,
		EPIC_DEPT_NAME,
		EPIC_EXT_NAME,
		serviceline_division_flag,
		serviceline_division_id,
		serviceline_division_name,
		mc_operation_flag,
		mc_operation_id,
		mc_operation_name,
		post_acute_flag,
		ambulatory_operation_flag,
		ambulatory_operation_id,
		ambulatory_operation_name,
		inpatient_adult_flag,
		inpatient_adult_id,
		inpatient_adult_name,
		childrens_flag,
		childrens_id,
		childrens_name,
		Load_Dtm
	FROM Rptg.vwRef_MDM_Supplemental) org
	ON (org.EPIC_DEPARTMENT_ID = rpt.epic_department_id)

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
	FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Supplemental_Dept_Subloc) supp
    ON (supp.Department_ID = rpt.epic_department_id)

LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = rpt.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id

ORDER BY rpt.event_category, rpt.event_date, mdmloc.pod_id, mdmloc.hub_id, rpt.epic_department_id, rpt.provider_id;

SELECT
    event_category,
    APPT_DT,
    APPT_DTTM,
    APPT_CANC_DTTM,
    Cancel_Lead_Days,
	event_type,
    event_count,
    event_date,
    fmonth_num,
    Fyear_num,
    FYear_name,
    report_period,
    report_date,
    POD_ID,
    pod_name,
    HUB_ID,
    hub_name,
    epic_department_id,
    epic_department_name,
    epic_department_name_external,
    peds,
    transplant,
    sk_Dim_Pt,
    sk_Fact_Pt_Acct,
    sk_Fact_Pt_Enc_Clrt,
    person_birth_date,
    person_gender,
    person_id,
    person_name,
    practice_group_id,
    practice_group_name,
    provider_id,
    provider_name,
    service_line_id,
    service_line,
    prov_service_line_id,
    prov_service_line,
    sub_service_line_id,
    sub_service_line,
    opnl_service_id,
    opnl_service_name,
    corp_service_line_id,
    corp_service_line,
    hs_area_id,
    hs_area_name,
    prov_hs_area_id,
    prov_hs_area_name,
    APPT_STATUS_FLAG,
    CANCEL_REASON_C,
    Next_APPT_DT,
    Rescheduled_Lag_Days,
    PAT_ENC_CSN_ID,
    PRC_ID,
    PRC_NAME,
    sk_Dim_Physcn,
    UVaID,
    VIS_NEW_TO_SYS_YN,
    VIS_NEW_TO_DEP_YN,
    VIS_NEW_TO_PROV_YN,
    VIS_NEW_TO_SPEC_YN,
    VIS_NEW_TO_SERV_AREA_YN,
    VIS_NEW_TO_LOC_YN,
    APPT_MADE_DATE,
    ENTRY_DATE,
    appt_event_No_Show,
    appt_event_Canceled_Late,
    appt_event_Canceled,
    appt_event_Scheduled,
    appt_event_Provider_Canceled,
    appt_event_Completed,
    appt_event_Arrived,
    appt_event_New_to_Specialty,
    Appointment_Lag_Days,
    DEPT_SPECIALTY_NAME,
    PROV_SPECIALTY_NAME,
    CANCEL_REASON_NAME,
    financial_division,
    financial_subdivision,
    CANCEL_INITIATOR,
    CANCEL_LEAD_HOURS,
    Entry_UVaID,
    Canc_UVaID,
    PHONE_REM_STAT_NAME,
    APPT_MADE_DTTM,
    Prov_Typ,
    Staff_Resource,
    som_group_id,
    som_group_name,
    rev_location_id,
    rev_location,
    financial_division_id,
    financial_division_name,
    financial_sub_division_id,
    financial_sub_division_name,
    som_department_id,
    som_department_name,
    som_division_id,
    som_division_name,
    som_hs_area_id,
    som_hs_area_name,
    APPT_SERIAL_NUM,
    Appointment_Request_Date,
    BILL_PROV_YN,
    Bump,
    Appointment,
    upg_practice_flag,
    upg_practice_region_id,
    upg_practice_region_name,
    upg_practice_id,
    upg_practice_name,
    F2F_Flag,
    ENC_TYPE_C,
    ENC_TYPE_TITLE,
    Lip_Flag,
    FINANCE_COST_CODE,
    Prov_Based_Clinic,
    Map_Type,
    Bump_WIn_24_Hrs,
    Bump_WIn_45_Days,
    Bump_WIn_30_Days,
    organization_id,
    organization_name,
    service_id,
    service_name,
    clinical_area_id,
    clinical_area_name,
    ambulatory_flag,
    community_health_flag,
    AMB_Scorecard_Flag
FROM #RptgTmp2
ORDER BY
	event_category,
	APPT_DT,
	APPT_DTTM,
	APPT_CANC_DTTM

SELECT
    'Provider-initiated Cancellation Rate' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(Appointment) AS [Appointment],
	SUM(Bump) AS [Bump]
FROM #RptgTmp2 rpt
WHERE event_category = 'Aggregate'
--AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant')
AND rpt.AMB_Scorecard_Flag = 1
AND rpt.ambulatory_flag = 1
--AND (rpt.ambulatory_flag = 1 OR rpt.community_health_flag = 1)

SELECT
    'Provider-initiated Cancellation Rate' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(Appointment) AS [Appointment],
	SUM(Bump) AS [Bump]
FROM TabRptg.Dash_AmbOpt_ProvCancApptMetric_Tiles tabrptg

LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
ON o.organization_id = s.organization_id
WHERE event_category = 'Aggregate'
--AND (Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant')
AND (tabrptg.AMB_Scorecard_Flag = 1 AND Prov_Typ <> 'Fellow')
AND ambulatory_flag = 1
--AND (ambulatory_flag = 1 OR community_health_flag = 1)
AND tabrptg.event_date >= @locstartdate
      AND tabrptg.event_date < @locenddate

GO