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
No Show + Late Cancel

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Check extract for Stored Procedure DM DS_HSDM_App ETL uspSrc_AmbOpt_Scheduled_Appointment_NoShow_Metric.sql
*/

IF OBJECT_ID('tempdb..#main ') IS NOT NULL
DROP TABLE #main

IF OBJECT_ID('tempdb..#RptgTmp ') IS NOT NULL
DROP TABLE #RptgTmp

IF OBJECT_ID('tempdb..#TabRptg ') IS NOT NULL
DROP TABLE #TabRptg

SELECT evnts2.epic_department_id,
       evnts2.peds,
       evnts2.transplant,
       evnts2.sk_Dim_Pt,
       evnts2.sk_Fact_Pt_Acct,
       evnts2.sk_Fact_Pt_Enc_Clrt,
       evnts2.person_birth_date,
       evnts2.person_gender,
       evnts2.person_id,
       evnts2.person_name,
       evnts2.provider_id,
       evnts2.provider_name,
       evnts2.APPT_STATUS_FLAG,
       evnts2.APPT_STATUS_C,
       evnts2.CANCEL_INITIATOR,
       evnts2.CANCEL_REASON_C,
       evnts2.APPT_DT,
       evnts2.PAT_ENC_CSN_ID,
       evnts2.PRC_ID,
       evnts2.PRC_NAME,
       evnts2.VIS_NEW_TO_SYS_YN,
       evnts2.VIS_NEW_TO_DEP_YN,
       evnts2.VIS_NEW_TO_PROV_YN,
       evnts2.VIS_NEW_TO_SPEC_YN,
       evnts2.VIS_NEW_TO_SERV_AREA_YN,
       evnts2.VIS_NEW_TO_LOC_YN,
       evnts2.APPT_MADE_DATE,
       evnts2.ENTRY_DATE,
       evnts2.appt_event_No_Show,
       evnts2.appt_event_Canceled_Late,
       evnts2.appt_event_Canceled,
       evnts2.appt_event_Scheduled,
       evnts2.appt_event_Provider_Canceled,
       evnts2.appt_event_Completed,
       evnts2.appt_event_Arrived,
       evnts2.appt_event_New_to_Specialty,
       evnts2.Appointment_Request_Date,
       evnts2.DEPT_SPECIALTY_NAME,
       evnts2.PROV_SPECIALTY_NAME,
       evnts2.APPT_DTTM,
       evnts2.CANCEL_REASON_NAME,
       evnts2.SER_RPT_GRP_SIX,
       evnts2.SER_RPT_GRP_EIGHT,
       evnts2.CANCEL_LEAD_HOURS,
       evnts2.APPT_CANC_DTTM,
       evnts2.PHONE_REM_STAT_NAME,
       evnts2.CHANGE_DATE,
       evnts2.Cancel_Lead_Days,
       evnts2.APPT_MADE_DTTM,
       evnts2.APPT_SERIAL_NUM,
       evnts2.BILL_PROV_YN,
       evnts2.APPT_ENTRY_USER_ID,
       evnts2.APPT_CANC_USER_ID,
       evnts2.Load_Dtm,
       evnts2.F2F_Flag,
       evnts2.ENC_TYPE_C,
       evnts2.ENC_TYPE_TITLE,
       evnts2.Prov_Typ,
       evnts2.NoShow,
       evnts2.PatientCanceledLate,
       evnts2.Appointment

	INTO #main

FROM
(
	SELECT evnts.epic_department_id,
           evnts.peds,
           evnts.transplant,
           evnts.sk_Dim_Pt,
           evnts.sk_Fact_Pt_Acct,
           evnts.sk_Fact_Pt_Enc_Clrt,
           evnts.person_birth_date,
           evnts.person_gender,
           evnts.person_id,
           evnts.person_name,
           evnts.provider_id,
           evnts.provider_name,
           evnts.APPT_STATUS_FLAG,
           evnts.APPT_STATUS_C,
           evnts.CANCEL_INITIATOR,
           evnts.CANCEL_REASON_C,
           evnts.APPT_DT,
           evnts.PAT_ENC_CSN_ID,
           evnts.PRC_ID,
           evnts.PRC_NAME,
           evnts.VIS_NEW_TO_SYS_YN,
           evnts.VIS_NEW_TO_DEP_YN,
           evnts.VIS_NEW_TO_PROV_YN,
           evnts.VIS_NEW_TO_SPEC_YN,
           evnts.VIS_NEW_TO_SERV_AREA_YN,
           evnts.VIS_NEW_TO_LOC_YN,
           evnts.APPT_MADE_DATE,
           evnts.ENTRY_DATE,
           evnts.appt_event_No_Show,
           evnts.appt_event_Canceled_Late,
           evnts.appt_event_Canceled,
           evnts.appt_event_Scheduled,
           evnts.appt_event_Provider_Canceled,
           evnts.appt_event_Completed,
           evnts.appt_event_Arrived,
           evnts.appt_event_New_to_Specialty,
           evnts.Appointment_Request_Date,
           evnts.DEPT_SPECIALTY_NAME,
           evnts.PROV_SPECIALTY_NAME,
           evnts.APPT_DTTM,
           evnts.CANCEL_REASON_NAME,
           evnts.SER_RPT_GRP_SIX,
           evnts.SER_RPT_GRP_EIGHT,
           evnts.CANCEL_LEAD_HOURS,
           evnts.APPT_CANC_DTTM,
           evnts.PHONE_REM_STAT_NAME,
           evnts.CHANGE_DATE,
           evnts.Cancel_Lead_Days,
           evnts.APPT_MADE_DTTM,
           evnts.APPT_SERIAL_NUM,
           evnts.BILL_PROV_YN,
           evnts.APPT_ENTRY_USER_ID,
           evnts.APPT_CANC_USER_ID,
           evnts.Load_Dtm,
           evnts.F2F_Flag,
           evnts.ENC_TYPE_C,
           evnts.ENC_TYPE_TITLE,
           COALESCE(evnts.PROV_TYPE_OT_NAME, evnts.Prov_Typ, NULL) AS Prov_Typ,
           CASE WHEN evnts.appt_event_No_Show = 1 OR evnts.appt_event_Canceled_Late = 1 THEN 1 ELSE 0 END AS NoShow,
           CASE WHEN evnts.appt_event_Canceled_Late = 1 THEN 1 ELSE 0 END AS PatientCanceledLate,
           CASE WHEN COALESCE(evnts.appt_event_Canceled,0) = 0 OR evnts.appt_event_Canceled_Late = 1 OR (evnts.appt_event_Provider_Canceled = 1 AND evnts.Cancel_Lead_Days <= 30) THEN 1 ELSE 0 END AS Appointment

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
			ser.Prov_Typ,
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
		    --AND ((excl.DEPARTMENT_ID IS NULL) OR (appts.DEPARTMENT_ID IN (10242001,10243126))) -- 10242001	UVPC TELEMEDICINE, 10243126	UVHE URGENT VIDEO CL
			AND pat.IS_VALID_PAT_YN = 'Y'
			AND appts.ENC_TYPE_C NOT IN ('2505','2506')

		) AS main
		LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
		ON ser.PROV_ID = main.provider_id
	) AS evnts
) AS evnts2
ORDER BY
	APPT_DT,
	epic_department_id,
	Prov_Typ,
	provider_id,
	provider_name,
	BILL_PROV_YN,
	DEPT_SPECIALTY_NAME,
	PROV_SPECIALTY_NAME,
	SER_RPT_GRP_SIX,
	SER_RPT_GRP_EIGHT

  -- Create index for temp table #main

CREATE NONCLUSTERED INDEX IX_NC_NOSHOW ON #main (APPT_DT, epic_department_id, Prov_Typ, provider_id)

SELECT CAST('NoShow' AS VARCHAR(50)) AS event_type,
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
	   mdmloc.[EPIC_DEPT_NAME] AS epic_department_name,
	   mdmloc.[EPIC_EXT_NAME] AS epic_department_name_external,
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
	   mdmloc.service_line_id,
	   mdmloc.service_line,
       physsvc.Service_Line_ID AS prov_service_line_id,
       physsvc.Service_Line AS prov_service_line,
	   mdmloc.sub_service_line_id,
	   mdmloc.sub_service_line,
	   mdmloc.opnl_service_id,
	   mdmloc.opnl_service_name,
	   mdmloc.corp_service_line_id,
	   mdmloc.corp_service_line,
	   mdmloc.hs_area_id,
	   mdmloc.hs_area_name,
       physsvc.hs_area_id AS prov_hs_area_id,
       physsvc.hs_area_name AS prov_hs_area_name,
	   rpt.APPT_STATUS_FLAG,
	   rpt.CANCEL_REASON_C,
       rpt.APPT_DT,
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
	   rpt.Prov_Typ,
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
       rpt.NoShow,
	   rpt.PatientCanceledLate,
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
	   org.serviceline_division_flag,
	   org.serviceline_division_id,
	   org.serviceline_division_name,
	   org.mc_operation_flag,
	   org.mc_operation_id,
	   org.mc_operation_name,
	   org.post_acute_flag,
	   org.ambulatory_operation_flag,
	   org.ambulatory_operation_id,
	   org.ambulatory_operation_name,
	   org.inpatient_adult_flag,
	   org.inpatient_adult_id,
	   org.inpatient_adult_name,
	   org.childrens_flag,
	   org.childrens_id,
	   org.childrens_name,
	   supp.SUBLOC_ID,
	   supp.SUBLOC_NAME,

	   o.organization_id,
       o.organization_name,
       s.service_id,
       s.service_name,
       c.clinical_area_id,
       c.clinical_area_name,
       g.ambulatory_flag,
	   g.community_health_flag

INTO #RptgTmp

FROM
(
SELECT aggr.APPT_DT,
       aggr.epic_department_id,
	   aggr.Prov_Typ,
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
       date_dim.month_num,
	   date_dim.month_name,
       date_dim.year_num,
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
 
       ISNULL(aggr.NoShow,CAST(0 AS INT)) AS NoShow,	   	   
       ISNULL(aggr.PatientCanceledLate,CAST(0 AS INT)) AS PatientCanceledLate,
       ISNULL(aggr.Appointment,CAST(0 AS INT)) AS Appointment,

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
       CAST(NULL AS INT) AS F2F_Flag,
       CAST(NULL AS VARCHAR(66)) AS ENC_TYPE_C,
       CAST(NULL AS VARCHAR(254)) AS ENC_TYPE_TITLE

FROM
(
    SELECT day_date,
           month_num,
		   month_name,
           year_num,
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
			   evnts.Prov_Typ,
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
			   SUM(evnts.NoShow) AS NoShow,
			   SUM(evnts.PatientCanceledLate) AS PatientCanceledLate,
			   SUM(evnts.Appointment) AS Appointment

		FROM #main evnts
		GROUP BY
			evnts.APPT_DT,
			evnts.epic_department_id,
			evnts.Prov_Typ,
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
	   main.Prov_Typ,
       main.provider_id,
       main.provider_name,
	   main.BILL_PROV_YN,
	   main.DEPT_SPECIALTY_NAME,
	   main.PROV_SPECIALTY_NAME,
	   main.SER_RPT_GRP_SIX,
	   main.SER_RPT_GRP_EIGHT,
	   1 AS event_count,
       date_dim.day_date AS event_date,
       date_dim.month_num,
	   date_dim.month_name,
       date_dim.year_num,
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
	   
       main.NoShow,	   
       main.PatientCanceledLate,
	   main.Appointment,
	   
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
	   main.F2F_Flag,
	   main.ENC_TYPE_C,
	   main.ENC_TYPE_TITLE
FROM #main main
INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date date_dim
ON main.APPT_DT = date_dim.day_date
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp
	ON entryemp.EMPlye_Usr_ID = main.APPT_ENTRY_USER_ID
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye cancemp
	ON cancemp.EMPlye_Usr_ID = main.APPT_CANC_USER_ID
WHERE main.appt_event_No_Show = 1 OR main.appt_event_Canceled_Late = 1
) rpt
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
ON ser.PROV_ID = rpt.provider_id
LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
ON dep.DEPARTMENT_ID = rpt.epic_department_id
--LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
--ON mdm.epic_department_id = rpt.epic_department_id
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

SELECT
    'No Show + Late Cancel' AS Metric
   ,'Stored Procedure' AS Source
   ,'MTD' AS [Time Period]
   ,SUM(CASE WHEN (event_category = 'Aggregate' AND (appt_event_No_Show > 0 OR appt_event_Canceled_Late > 0)) THEN NoShow ELSE 0 END) AS NoShow
   ,SUM(CASE WHEN (event_category = 'Aggregate' AND appt_event_Canceled_Late > 0) THEN PatientCanceledLate ELSE 0 END) AS PatientCanceledLate
   ,SUM(CASE WHEN (event_category = 'Aggregate') THEN Appointment ELSE 0 END) AS Appointment
FROM #RptgTmp
WHERE event_category = 'Aggregate'
AND ambulatory_flag = 1
--AND (ambulatory_flag = 1 OR community_health_flag = 1)

SELECT
    'No Show + Late Cancel' AS Metric
   ,'Stored Procedure' AS Source
   ,'MTD' AS [Time Period]
   ,SUM(CASE WHEN (event_category = 'Detail' AND (appt_event_No_Show > 0 OR appt_event_Canceled_Late > 0)) THEN NoShow ELSE 0 END) AS NoShow
   ,SUM(CASE WHEN (event_category = 'Detail' AND appt_event_Canceled_Late > 0) THEN PatientCanceledLate ELSE 0 END) AS PatientCanceledLate

FROM #RptgTmp
WHERE event_category = 'Detail'
AND ambulatory_flag = 1
--AND (ambulatory_flag = 1 OR community_health_flag = 1)

SELECT tabrptg.[sk_Dash_AmbOpt_ApptNoShowMetric_Tiles]
      ,tabrptg.[event_type]
      ,tabrptg.[event_count]
      ,tabrptg.[event_date]
      ,tabrptg.[event_id]
      ,tabrptg.[event_category]
      ,tabrptg.[epic_department_id]
      ,tabrptg.[epic_department_name]
      ,tabrptg.[epic_department_name_external]
      ,tabrptg.[fmonth_num]
      ,tabrptg.[fyear_num]
      ,tabrptg.[fyear_name]
      ,tabrptg.[report_period]
      ,tabrptg.[report_date]
      ,tabrptg.[peds]
      ,tabrptg.[transplant]
      ,tabrptg.[oncology]
      ,tabrptg.[sk_Dim_Pt]
      ,tabrptg.[sk_Fact_Pt_Acct]
      ,tabrptg.[sk_Fact_Pt_Enc_Clrt]
      ,tabrptg.[person_birth_date]
      ,tabrptg.[person_gender]
      ,tabrptg.[person_id]
      ,tabrptg.[person_name]
      ,tabrptg.[practice_group_id]
      ,tabrptg.[practice_group_name]
      ,tabrptg.[provider_id]
      ,tabrptg.[provider_name]
      ,tabrptg.[sk_dim_physcn]
      ,tabrptg.[hs_area_id]
      ,tabrptg.[hs_area_name]
      ,tabrptg.[financial_division_id]
      ,tabrptg.[financial_division_name]
      ,tabrptg.[financial_sub_division_id]
      ,tabrptg.[financial_sub_division_name]
      ,tabrptg.[rev_location_id]
      ,tabrptg.[rev_location]
      ,tabrptg.[som_group_id]
      ,tabrptg.[som_group_name]
      ,tabrptg.[som_department_id]
      ,tabrptg.[som_department_name]
      ,tabrptg.[som_division_id]
      ,tabrptg.[som_division_name]
      ,tabrptg.[service_line_id]
      ,tabrptg.[service_line]
      ,tabrptg.[sub_service_line_id]
      ,tabrptg.[sub_service_line]
      ,tabrptg.[opnl_service_id]
      ,tabrptg.[opnl_service_name]
      ,tabrptg.[corp_service_line_id]
      ,tabrptg.[corp_service_line_name]
      ,tabrptg.[w_service_line_id]
      ,tabrptg.[w_service_line_name]
      ,tabrptg.[w_sub_service_line_id]
      ,tabrptg.[w_sub_service_line_name]
      ,tabrptg.[w_opnl_service_id]
      ,tabrptg.[w_opnl_service_name]
      ,tabrptg.[w_corp_service_line_id]
      ,tabrptg.[w_corp_service_line_name]
      ,tabrptg.[w_department_id]
      ,tabrptg.[w_department_name]
      ,tabrptg.[w_department_name_external]
      ,tabrptg.[w_practice_group_id]
      ,tabrptg.[w_practice_group_name]
      ,tabrptg.[w_report_period]
      ,tabrptg.[w_report_date]
      ,tabrptg.[w_hs_area_id]
      ,tabrptg.[w_hs_area_name]
      ,tabrptg.[w_financial_division_id]
      ,tabrptg.[w_financial_division_name]
      ,tabrptg.[w_financial_sub_division_id]
      ,tabrptg.[w_financial_sub_division_name]
      ,tabrptg.[w_rev_location_id]
      ,tabrptg.[w_rev_location]
      ,tabrptg.[w_som_group_id]
      ,tabrptg.[w_som_group_name]
      ,tabrptg.[w_som_department_id]
      ,tabrptg.[w_som_department_name]
      ,tabrptg.[w_som_division_id]
      ,tabrptg.[w_som_division_name]
      ,tabrptg.[pod_id]
      ,tabrptg.[pod_name]
      ,tabrptg.[hub_id]
      ,tabrptg.[hub_name]
      ,tabrptg.[w_pod_id]
      ,tabrptg.[w_pod_name]
      ,tabrptg.[w_hub_id]
      ,tabrptg.[w_hub_name]
      ,tabrptg.[w_som_hs_area_id]
      ,tabrptg.[w_som_hs_area_name]
      ,tabrptg.[w_upg_practice_flag]
      ,tabrptg.[w_upg_practice_region_id]
      ,tabrptg.[w_upg_practice_region_name]
      ,tabrptg.[w_upg_practice_id]
      ,tabrptg.[w_upg_practice_name]
      ,tabrptg.[w_serviceline_division_flag]
      ,tabrptg.[w_serviceline_division_id]
      ,tabrptg.[w_serviceline_division_name]
      ,tabrptg.[w_mc_operation_flag]
      ,tabrptg.[w_mc_operation_id]
      ,tabrptg.[w_mc_operation_name]
      ,tabrptg.[w_post_acute_flag]
      ,tabrptg.[w_ambulatory_operation_flag]
      ,tabrptg.[w_ambulatory_operation_id]
      ,tabrptg.[w_ambulatory_operation_name]
      ,tabrptg.[w_inpatient_adult_flag]
      ,tabrptg.[w_inpatient_adult_id]
      ,tabrptg.[w_inpatient_adult_name]
      ,tabrptg.[w_childrens_flag]
      ,tabrptg.[w_childrens_id]
      ,tabrptg.[w_childrens_name]
      ,tabrptg.[prov_service_line_id]
      ,tabrptg.[prov_service_line]
      ,tabrptg.[prov_hs_area_id]
      ,tabrptg.[prov_hs_area_name]
      ,tabrptg.[APPT_STATUS_FLAG]
      ,tabrptg.[CANCEL_REASON_C]
      ,tabrptg.[APPT_DT]
      ,tabrptg.[PAT_ENC_CSN_ID]
      ,tabrptg.[PRC_ID]
      ,tabrptg.[PRC_NAME]
      ,tabrptg.[UVaID]
      ,tabrptg.[VIS_NEW_TO_SYS_YN]
      ,tabrptg.[VIS_NEW_TO_DEP_YN]
      ,tabrptg.[VIS_NEW_TO_PROV_YN]
      ,tabrptg.[VIS_NEW_TO_SPEC_YN]
      ,tabrptg.[VIS_NEW_TO_SERV_AREA_YN]
      ,tabrptg.[VIS_NEW_TO_LOC_YN]
      ,tabrptg.[APPT_MADE_DATE]
      ,tabrptg.[ENTRY_DATE]
      ,tabrptg.[appt_event_No_Show]
      ,tabrptg.[appt_event_Canceled_Late]
      ,tabrptg.[appt_event_Canceled]
      ,tabrptg.[appt_event_Scheduled]
      ,tabrptg.[appt_event_Provider_Canceled]
      ,tabrptg.[appt_event_Completed]
      ,tabrptg.[appt_event_Arrived]
      ,tabrptg.[appt_event_New_to_Specialty]
      ,tabrptg.[DEPT_SPECIALTY_NAME]
      ,tabrptg.[PROV_SPECIALTY_NAME]
      ,tabrptg.[APPT_DTTM]
      ,tabrptg.[CANCEL_REASON_NAME]
      ,tabrptg.[financial_division]
      ,tabrptg.[financial_subdivision]
      ,tabrptg.[CANCEL_INITIATOR]
      ,tabrptg.[CANCEL_LEAD_HOURS]
      ,tabrptg.[APPT_CANC_DTTM]
      ,tabrptg.[Entry_UVaID]
      ,tabrptg.[Canc_UVaID]
      ,tabrptg.[PHONE_REM_STAT_NAME]
      ,tabrptg.[Cancel_Lead_Days]
      ,tabrptg.[APPT_MADE_DTTM]
      ,tabrptg.[Prov_Typ]
      ,tabrptg.[Staff_Resource]
      ,tabrptg.[APPT_SERIAL_NUM]
      ,tabrptg.[Appointment_Request_Date]
      ,tabrptg.[BILL_PROV_YN]
      ,tabrptg.[NoShow]
      ,tabrptg.[PatientCanceledLate]
      ,tabrptg.[Appointment]
      ,tabrptg.[F2F_Flag]
      ,tabrptg.[ENC_TYPE_C]
      ,tabrptg.[ENC_TYPE_TITLE]
      ,tabrptg.[Lip_Flag]
      ,tabrptg.[FINANCE_COST_CODE]
      ,tabrptg.[Prov_Based_Clinic]
      ,tabrptg.[Map_Type]
      ,tabrptg.[SUBLOC_ID]
      ,tabrptg.[SUBLOC_NAME]
      ,tabrptg.[Load_Dtm]

	  ,o.organization_id
	  ,o.organization_name
	  ,s.service_id
	  ,s.service_name
	  ,c.clinical_area_id
	  ,c.clinical_area_name
	  ,g.ambulatory_flag
	  ,g.community_health_flag

  INTO #TabRptg

  FROM [TabRptg].[Dash_AmbOpt_ApptNoShowMetric_Tiles] tabrptg

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id

WHERE event_date >= @locstartdate
      AND event_date <= @locenddate

SELECT
    'No Show + Late Cancel' AS Metric
   ,'Tab Table' AS Source
   ,'MTD' AS [Time Period]
   ,SUM(NoShow) AS NoShow
   ,SUM(PatientCanceledLate) AS PatientCanceledLate 
   ,SUM(Appointment) AS Appointment
FROM #TabRptg
WHERE event_category = 'Aggregate'
AND ambulatory_flag = 1 -- Ambulatory Operations Scorecard
--AND (ambulatory_flag = 1 OR community_health_flag = 1)

/*
New Patient Appointment Within 14 Calendar Days

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Check extract from Table TabRptg Dash_AmbOpt_NewPatientAppt_AccessWIn14CPT_Tiles.sql
*/
/*
IF OBJECT_ID('tempdb..#appts ') IS NOT NULL
DROP TABLE #appts

IF OBJECT_ID('tempdb..#cpts ') IS NOT NULL
DROP TABLE #cpts

IF OBJECT_ID('tempdb..#cptlist ') IS NOT NULL
DROP TABLE #cptlist

IF OBJECT_ID('tempdb..#events_list ') IS NOT NULL
DROP TABLE #events_list

IF OBJECT_ID('tempdb..#TabRptg2 ') IS NOT NULL
DROP TABLE #TabRptg2

SELECT DISTINCT
	  appt.sk_Fact_Pt_Enc_Clrt

INTO #appts

FROM
	DS_HSDM_App.Stage.Scheduled_Appointment appt
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date dd								ON	appt.CONTACT_DATE = dd.day_date
	INNER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master lm					ON	lm.EPIC_DEPARTMENT_ID = appt.DEPARTMENT_ID
	LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map									ON	map.Deptid = CAST(lm.FINANCE_COST_CODE AS INTEGER)
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient pat							ON	pat.sk_Dim_Pt = appt.sk_Dim_Pt
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc mdm		ON	appt.DEPARTMENT_ID = mdm.epic_department_id
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser						ON	ser.PROV_ID = appt.PROV_ID
	LEFT JOIN
		(
		SELECT 
			  sk_Dim_Physcn
			, UVaID
			, Service_Line
			, ProviderGroup
		FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
		WHERE current_flag = 1
		) AS doc																ON	ser.sk_Dim_Physcn = doc.sk_Dim_Physcn
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc				ON	physsvc.Physician_Roster_Name = CASE WHEN (ser.sk_Dim_Physcn > 0) THEN doc.Service_Line
																														ELSE 'No Value Specified'
																														END
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp				ON	entryemp.EMPlye_Usr_ID = appt.APPT_ENTRY_USER_ID
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep						ON dep.DEPARTMENT_ID = appt.DEPARTMENT_ID
	-- -------------------------------------
	-- Excluded departments--
	-- -------------------------------------
	LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl						ON excl.DEPARTMENT_ID = appt.DEPARTMENT_ID
	-- -------------------------------------
    -- SOM Financial Division Subdivision--
    -- -------------------------------------
	LEFT OUTER JOIN
		(
		SELECT
			  Epic_Financial_Division_Code
			, Epic_Financial_Subdivision_Code
			, Department
			, Department_ID
			, Organization
			, Org_Number
			, som_group_id
			, som_group_name
			, som_hs_area_id
			, som_hs_area_name
		FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv
		) dvsn																	ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.Financial_Division AS INT)
																				AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.Financial_SubDivision AS INT))
	-- -------------------------------------
	-- Identify transplant encounter--
	-- -------------------------------------
	LEFT OUTER JOIN
		(
		SELECT DISTINCT
			  btd.pat_enc_csn_id
			, btd.Event_Transplanted					AS transplant_surgery_dt
			, btd.hosp_admsn_time						AS Adm_Dtm
		FROM DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart AS btd
		WHERE (
				btd.TX_Episode_Phase = 'transplanted'
				AND btd.TX_Stat_Dt >= @locstartdate 
				AND btd.TX_Stat_Dt <  @locenddate
			  )
			AND btd.TX_GroupedPhaseStatus = 'TX-ADMIT'
		) AS tx															ON appt.PAT_ENC_CSN_ID = tx.pat_enc_csn_id

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = appt.DEPARTMENT_ID
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id

	WHERE 
	((excl.DEPARTMENT_ID IS NULL) OR (appt.DEPT_SPECIALTY_NAME = 'Lab'))
		--((excl.DEPARTMENT_ID IS NULL) OR (appt.DEPT_SPECIALTY_NAME = 'Lab') OR (appt.DEPARTMENT_ID IN (10242001,10243126))) -- 10242001	UVPC TELEMEDICINE, 10243126	UVHE URGENT VIDEO CL
		AND pat.IS_VALID_PAT_YN = 'Y'
		AND appt.ENC_TYPE_C NOT IN ('2505','2506')	--Erroneous Encounter, Erroneous Telephone Encounter

	SELECT
		  tdl.sk_Fact_Pt_Enc_Clrt
		  ,cpt.CPT_Cde
		  ,cpt.CPT_Nme
		  ,cpt.CPT_Dscr

	INTO #cpts

	FROM
		DS_HSDW_Prod.dbo.Fact_TDLTran tdl
		INNER JOIN #appts appts ON tdl.sk_Fact_Pt_Enc_Clrt = appts.sk_Fact_Pt_Enc_Clrt
		INNER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_TDL_Tran_TypeCharacteristics d	ON	tdl.sk_Dim_Clrt_TDL_Tran_TypeCharacteristics = d.sk_Dim_Clrt_TDL_Tran_TypeCharacteristics
		INNER JOIN DS_HSDW_Prod.dbo.Dim_CPT cpt								ON	tdl.sk_Dim_CPT = cpt.sk_Dim_CPT

	WHERE
		tdl.Orig_Service_Date >= @locstartdate 
		AND tdl.Orig_Service_Date <= @locenddate
		AND d.DETAIL_TYPE IN (1, 10)													--Limit transactions to only Charges and Voids
		--AND cpt.CPT_CDE IN	('99201','99202','99203','99204','99205'					--New patient office visit
		--					,'99381','99382','99383','99384','99385','99386','99387'	--New patient preventitive medicine servics
		--					,'92002','92003','92004'									--New patient general ophthalmological services and procedures
		--					,'99241','99242','99243','99244','99245'					--Office consultation for new patients
							--,'90791','90792' -- PSYCH DIAGNOSTIC EVALUATION, PSYCH DIAG EVAL W/MED SRVCS
		--					)
		AND tdl.AMOUNT > 0																--Filters out transactions that are net zero or voided.

SELECT DISTINCT
    tmenc.sk_Fact_Pt_Enc_Clrt
    , (SELECT res.CPT_Cde + ',' AS [text()]
        FROM #cpts res
		WHERE res.sk_Fact_Pt_Enc_Clrt = tmenc.sk_Fact_Pt_Enc_Clrt
		FOR XML PATH ('')
	) AS cpts
INTO #cptlist
FROM #cpts tmenc

;WITH cteNewPat (PAT_ID, PAT_ENC_CSN_ID) AS
	(
	SELECT
		  tdl.sk_Dim_Pt
		, tdl.sk_Fact_Pt_Enc_Clrt

	FROM
		DS_HSDW_Prod.dbo.Fact_TDLTran tdl
		INNER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_TDL_Tran_TypeCharacteristics d	ON	tdl.sk_Dim_Clrt_TDL_Tran_TypeCharacteristics = d.sk_Dim_Clrt_TDL_Tran_TypeCharacteristics
		INNER JOIN DS_HSDW_Prod.dbo.Dim_CPT cpt								ON	tdl.sk_Dim_CPT = cpt.sk_Dim_CPT

	WHERE
		--tdl.Orig_Service_Date >= @StartDate 
		--AND tdl.Orig_Service_Date <= @EndDate
		tdl.Orig_Service_Date >= @locstartdate 
		AND tdl.Orig_Service_Date <= @locenddate
		AND d.DETAIL_TYPE IN (1, 10)													--Limit transactions to only Charges and Voids
		AND cpt.CPT_CDE IN	('99201','99202','99203','99204','99205'					--New patient office visit
							,'99381','99382','99383','99384','99385','99386','99387'	--New patient preventitive medicine servics
							,'92002','92003','92004'									--New patient general ophthalmological services and procedures
							,'99241','99242','99243','99244','99245'					--Office consultation for new patients
							,'90791','90792' -- PSYCH DIAGNOSTIC EVALUATION, PSYCH DIAG EVAL W/MED SRVCS
							)

	GROUP BY
		  tdl.sk_Dim_Pt
		, tdl.sk_Fact_Pt_Enc_Clrt

	HAVING
		SUM(tdl.AMOUNT) > 0																--Filters out transactions that are net zero or voided.
	)

SELECT
	  CAST('New Patient Appointment' AS VARCHAR(50))																AS event_type
	, CASE WHEN DATEDIFF(DAY, appt.APPT_MADE_DATE, appt.APPT_DTTM) <= 14 THEN 1 ELSE 0 END							AS event_count
	, dd.day_date																									AS event_date
	, dd.fmonth_num
	, dd.FYear_num
	, dd.FYear_name
	, CAST(LEFT(DATENAME(MM, dd.day_date), 3) + ' ' + CAST(DAY(dd.day_date) AS VARCHAR(2)) AS VARCHAR(10))			AS report_period
	, CAST(CAST(dd.day_date AS DATE) AS SMALLDATETIME)																AS report_date
	, CAST(CASE WHEN DATEDIFF(DAY, appt.APPT_MADE_DATE, appt.APPT_DTTM) <= 14 THEN 'Activity' END AS VARCHAR(150))	AS event_category
	, lm.POD_ID																										AS pod_id
	, lm.PFA_POD																									AS pod_name
	, lm.HUB_ID																										AS hub_id
	, lm.HUB																										AS hub_name
	, appt.DEPARTMENT_ID																							AS epic_department_id
	, appt.DEPARTMENT_NAME																							AS epic_department_name
	, lm.EPIC_EXT_NAME																								AS epic_department_external
	, CAST(CASE WHEN FLOOR((CAST(appt.APPT_DT AS INTEGER) 
							- CAST(CAST(pat.BirthDate AS DATETIME) AS INTEGER)
							) / 365.25
							) < 18 THEN
					1
				ELSE
					0
				END AS SMALLINT)																					AS peds
	, CAST(CASE WHEN tx.pat_enc_csn_id IS NOT NULL THEN 1 ELSE 0 END AS SMALLINT)									AS transplant
	, appt.sk_Dim_Pt
	, appt.sk_Fact_Pt_Acct
	, appt.sk_Fact_Pt_Enc_Clrt
	, pat.BirthDate																									AS person_birth_date
	, pat.Sex																										AS person_sex
	, CAST(appt.IDENTITY_ID AS INT)																					AS person_id
	, pat.Name																										AS person_name
	, CAST(NULL AS INT)																								AS practice_group_id
	, CAST(NULL AS VARCHAR(150))																					AS practice_group_name
	, appt.PROV_ID																									AS provider_id
	, appt.PROV_NAME																								AS provider_name
	--MDM
	, mdm.service_line_id
	, mdm.service_line
	, physsvc.Service_Line_ID																						AS prov_service_line_id
	, physsvc.Service_Line																							AS prov_service_line
	, mdm.sub_service_line_id
	, mdm.sub_service_line
	, mdm.opnl_service_id
	, mdm.opnl_service_name
	, mdm.corp_service_line_id
	, mdm.corp_service_line
	, mdm.hs_area_id
	, mdm.hs_area_name
	, physsvc.hs_area_id																							AS prov_hs_area_id
	, physsvc.hs_area_name																							AS prov_hs_area_name
	, dvsn.som_group_id
	, dvsn.som_group_name
	, lm.LOC_ID																										AS rev_location_id
	, lm.REV_LOC_NAME																								AS rev_location
	-- SOM
	, TRY_CAST(ser.Financial_Division AS INT)																		AS financial_division_id
	, CASE WHEN ser.Financial_Division_Name <> 'na' THEN CAST(ser.Financial_Division_Name AS VARCHAR(150))
		   ELSE NULL END																							AS financial_division_name
	, TRY_CAST(ser.Financial_SubDivision AS INT)																	AS financial_sub_division_id
	, CASE WHEN ser.Financial_SubDivision_Name <> 'na' THEN CAST(ser.Financial_SubDivision_Name AS VARCHAR(150))
		   ELSE NULL END																							AS financial_sub_division_name

	, dvsn.Department_ID																							AS som_department_id
	, CAST(dvsn.Department AS VARCHAR(150))																			AS som_department_name
	, CAST(dvsn.Org_Number AS INT)																					AS som_division_id
	, CAST(dvsn.Organization AS VARCHAR(150))																		AS som_division_name
	, dvsn.som_hs_area_id
	, dvsn.som_hs_area_name
	--Select
	, appt.APPT_STATUS_FLAG
	, appt.APPT_STATUS_C
	, appt.APPT_MADE_DTTM
	, appt.APPT_MADE_DATE
	, appt.ENTRY_DATE
	, appt.CHANGE_DATE
	, appt.APPT_DTTM
	, appt.APPT_DT
	, CASE WHEN (appt.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd,appt.APPT_MADE_DATE, appt.APPT_DT)	-- Number of calendar days between the Appointment Made Date and the Scheduled Appointment Date
		   ELSE CAST(NULL AS INT)
		   END																										AS Appointment_Lag_Days
	, CAST(appt.IDENTITY_ID AS INTEGER)																				AS MRN_int
	, appt.CONTACT_DATE
	, appt.PRC_ID
	, appt.PRC_NAME
	, ser.sk_Dim_Physcn
	, doc.UVaID
	, appt.DEPT_SPECIALTY_NAME
	, appt.PROV_SPECIALTY_NAME
	, appt.ENC_TYPE_C
	, appt.ENC_TYPE_TITLE
	, appt.APPT_CONF_STAT_NAME
	, appt.ZIP
	, appt.APPT_CONF_DTTM
	, appt.SER_RPT_GRP_SIX																							AS financial_division
	, appt.SER_RPT_GRP_EIGHT																						AS financial_subdivision
	, appt.F2F_Flag
	, entryemp.EMPlye_Systm_Login																					AS Entry_UVaID
	, appt.PHONE_REM_STAT_NAME
	, lm.BUSINESS_UNIT
	, COALESCE(appt.PROV_TYPE_OT_NAME, ser.PROV_TYP, NULL)															AS Prov_Typ
	, ser.Staff_Resource
	, appt.BILL_PROV_YN
	, lm.UPG_PRACTICE_FLAG																							AS upg_practice_flag
	, CAST(lm.UPG_PRACTICE_REGION_ID AS INTEGER)																	AS upg_practice_region_id
	, CAST(lm.UPG_PRACTICE_REGION_NAME AS VARCHAR(150))																AS upg_practice_region_name
	, CAST(lm.UPG_PRACTICE_ID AS INTEGER)																			AS upg_practice_id
	, CAST(lm.UPG_PRACTICE_NAME AS VARCHAR(150))																	AS upg_practice_name
	, CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END												AS Lip_Flag
	, lm.FINANCE_COST_CODE
	, dep.Prov_Based_Clinic
	, map.Map_Type

	, o.organization_id
	, o.organization_name
	, s.service_id
	, s.service_name
	, c.clinical_area_id
	, c.clinical_area_name
	, g.ambulatory_flag
	, g.community_health_flag
	, CASE WHEN np.PAT_ENC_CSN_ID IS NULL THEN 0 ELSE 1 END AS NewPatient
	, CASE WHEN DATEDIFF(DAY, appt.APPT_MADE_DATE, appt.APPT_DTTM) <= 14 THEN 1 ELSE 0 END	AS AbleToAccess
	, appt.PAT_ENC_CSN_ID

INTO #events_list

FROM
	DS_HSDM_App.Stage.Scheduled_Appointment appt
	LEFT OUTER JOIN cteNewPat np														ON	appt.sk_Fact_Pt_Enc_Clrt = np.PAT_ENC_CSN_ID
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date dd								ON	appt.CONTACT_DATE = dd.day_date
	INNER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master lm					ON	lm.EPIC_DEPARTMENT_ID = appt.DEPARTMENT_ID
	LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map									ON	map.Deptid = CAST(lm.FINANCE_COST_CODE AS INTEGER)
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient pat							ON	pat.sk_Dim_Pt = appt.sk_Dim_Pt
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc mdm		ON	appt.DEPARTMENT_ID = mdm.epic_department_id
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser						ON	ser.PROV_ID = appt.PROV_ID
	LEFT JOIN
		(
		SELECT 
			  sk_Dim_Physcn
			, UVaID
			, Service_Line
			, ProviderGroup
		FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
		WHERE current_flag = 1
		) AS doc																ON	ser.sk_Dim_Physcn = doc.sk_Dim_Physcn
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc				ON	physsvc.Physician_Roster_Name = CASE WHEN (ser.sk_Dim_Physcn > 0) THEN doc.Service_Line
																														ELSE 'No Value Specified'
																														END
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp				ON	entryemp.EMPlye_Usr_ID = appt.APPT_ENTRY_USER_ID
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep						ON dep.DEPARTMENT_ID = appt.DEPARTMENT_ID
	-- -------------------------------------
	-- Excluded departments--
	-- -------------------------------------
	LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl						ON excl.DEPARTMENT_ID = appt.DEPARTMENT_ID
	-- -------------------------------------
    -- SOM Financial Division Subdivision--
    -- -------------------------------------
	LEFT OUTER JOIN
		(
		SELECT
			  Epic_Financial_Division_Code
			, Epic_Financial_Subdivision_Code
			, Department
			, Department_ID
			, Organization
			, Org_Number
			, som_group_id
			, som_group_name
			, som_hs_area_id
			, som_hs_area_name
		FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv
		) dvsn																	ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.Financial_Division AS INT)
																				AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.Financial_SubDivision AS INT))
	-- -------------------------------------
	-- Identify transplant encounter--
	-- -------------------------------------
	LEFT OUTER JOIN
		(
		SELECT DISTINCT
			  btd.pat_enc_csn_id
			, btd.Event_Transplanted					AS transplant_surgery_dt
			, btd.hosp_admsn_time						AS Adm_Dtm
		FROM DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart AS btd
		WHERE (
				btd.TX_Episode_Phase = 'transplanted'
				AND btd.TX_Stat_Dt >= @locstartdate 
				AND btd.TX_Stat_Dt <  @locenddate
			  )
			AND btd.TX_GroupedPhaseStatus = 'TX-ADMIT'
		) AS tx															ON appt.PAT_ENC_CSN_ID = tx.pat_enc_csn_id

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = appt.DEPARTMENT_ID
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id

	WHERE 
	((excl.DEPARTMENT_ID IS NULL) OR (appt.DEPT_SPECIALTY_NAME = 'Lab'))
		--((excl.DEPARTMENT_ID IS NULL) OR (appt.DEPT_SPECIALTY_NAME = 'Lab') OR (appt.DEPARTMENT_ID IN (10242001,10243126))) -- 10242001	UVPC TELEMEDICINE, 10243126	UVHE URGENT VIDEO CL
		AND pat.IS_VALID_PAT_YN = 'Y'
		AND appt.ENC_TYPE_C NOT IN ('2505','2506')	--Erroneous Encounter, Erroneous Telephone Encounter

SELECT
    'New Patient Appointment Within 14 Calendar Days' AS Metric
   ,'Stored Procedure' AS Source
   ,'MTD' AS [Time Period]
   ,SUM(CASE WHEN event_count = 1 THEN 1 ELSE 0 END) AS [AbleToAccess]
   ,COUNT(*) AS NewPatient
FROM #events_list
WHERE
ambulatory_flag = 1
--(ambulatory_flag = 1 OR community_health_flag = 1)
AND event_date >= @locstartdate AND event_date <= @locenddate
AND NewPatient = 1

SELECT tabrptg.[sk_Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles]
      ,tabrptg.[event_type]
      ,tabrptg.[event_count]
      ,tabrptg.[event_date]
      ,tabrptg.[event_id]
      ,tabrptg.[event_category]
      ,tabrptg.[epic_department_id]
      ,tabrptg.[epic_department_name]
      ,tabrptg.[epic_department_name_external]
      ,tabrptg.[fmonth_num]
      ,tabrptg.[fyear_num]
      ,tabrptg.[fyear_name]
      ,tabrptg.[report_period]
      ,tabrptg.[report_date]
      ,tabrptg.[peds]
      ,tabrptg.[transplant]
      ,tabrptg.[oncology]
      ,tabrptg.[sk_Dim_Pt]
      ,tabrptg.[sk_Fact_Pt_Acct]
      ,tabrptg.[sk_Fact_Pt_Enc_Clrt]
      ,tabrptg.[person_birth_date]
      ,tabrptg.[person_gender]
      ,tabrptg.[person_id]
      ,tabrptg.[person_name]
      ,tabrptg.[practice_group_id]
      ,tabrptg.[practice_group_name]
      ,tabrptg.[provider_id]
      ,tabrptg.[provider_name]
      ,tabrptg.[sk_dim_physcn]
      ,tabrptg.[hs_area_id]
      ,tabrptg.[hs_area_name]
      ,tabrptg.[financial_division_id]
      ,tabrptg.[financial_division_name]
      ,tabrptg.[financial_sub_division_id]
      ,tabrptg.[financial_sub_division_name]
      ,tabrptg.[rev_location_id]
      ,tabrptg.[rev_location]
      ,tabrptg.[som_group_id]
      ,tabrptg.[som_group_name]
      ,tabrptg.[som_department_id]
      ,tabrptg.[som_department_name]
      ,tabrptg.[som_division_id]
      ,tabrptg.[som_division_name]
      ,tabrptg.[service_line_id]
      ,tabrptg.[service_line]
      ,tabrptg.[sub_service_line_id]
      ,tabrptg.[sub_service_line]
      ,tabrptg.[opnl_service_id]
      ,tabrptg.[opnl_service_name]
      ,tabrptg.[corp_service_line_id]
      ,tabrptg.[corp_service_line_name]
      ,tabrptg.[w_service_line_id]
      ,tabrptg.[w_service_line_name]
      ,tabrptg.[w_sub_service_line_id]
      ,tabrptg.[w_sub_service_line_name]
      ,tabrptg.[w_opnl_service_id]
      ,tabrptg.[w_opnl_service_name]
      ,tabrptg.[w_corp_service_line_id]
      ,tabrptg.[w_corp_service_line_name]
      ,tabrptg.[w_department_id]
      ,tabrptg.[w_department_name]
      ,tabrptg.[w_department_name_external]
      ,tabrptg.[w_practice_group_id]
      ,tabrptg.[w_practice_group_name]
      ,tabrptg.[w_report_period]
      ,tabrptg.[w_report_date]
      ,tabrptg.[w_hs_area_id]
      ,tabrptg.[w_hs_area_name]
      ,tabrptg.[w_financial_division_id]
      ,tabrptg.[w_financial_division_name]
      ,tabrptg.[w_financial_sub_division_id]
      ,tabrptg.[w_financial_sub_division_name]
      ,tabrptg.[w_rev_location_id]
      ,tabrptg.[w_rev_location]
      ,tabrptg.[w_som_group_id]
      ,tabrptg.[w_som_group_name]
      ,tabrptg.[w_som_department_id]
      ,tabrptg.[w_som_department_name]
      ,tabrptg.[w_som_division_id]
      ,tabrptg.[w_som_division_name]
      ,tabrptg.[pod_id]
      ,tabrptg.[pod_name]
      ,tabrptg.[hub_id]
      ,tabrptg.[hub_name]
      ,tabrptg.[w_pod_id]
      ,tabrptg.[w_pod_name]
      ,tabrptg.[w_hub_id]
      ,tabrptg.[w_hub_name]
      ,tabrptg.[w_som_hs_area_id]
      ,tabrptg.[w_som_hs_area_name]
      ,tabrptg.[w_upg_practice_flag]
      ,tabrptg.[w_upg_practice_region_id]
      ,tabrptg.[w_upg_practice_region_name]
      ,tabrptg.[w_upg_practice_id]
      ,tabrptg.[w_upg_practice_name]
      ,tabrptg.[w_serviceline_division_flag]
      ,tabrptg.[w_serviceline_division_id]
      ,tabrptg.[w_serviceline_division_name]
      ,tabrptg.[w_mc_operation_flag]
      ,tabrptg.[w_mc_operation_id]
      ,tabrptg.[w_mc_operation_name]
      ,tabrptg.[w_post_acute_flag]
      ,tabrptg.[w_ambulatory_operation_flag]
      ,tabrptg.[w_ambulatory_operation_id]
      ,tabrptg.[w_ambulatory_operation_name]
      ,tabrptg.[w_inpatient_adult_flag]
      ,tabrptg.[w_inpatient_adult_id]
      ,tabrptg.[w_inpatient_adult_name]
      ,tabrptg.[w_childrens_flag]
      ,tabrptg.[w_childrens_id]
      ,tabrptg.[w_childrens_name]
      ,tabrptg.[epic_department_external]
      ,tabrptg.[person_sex]
      ,tabrptg.[prov_service_line_id]
      ,tabrptg.[prov_service_line]
      ,tabrptg.[prov_hs_area_id]
      ,tabrptg.[prov_hs_area_name]
      ,tabrptg.[APPT_STATUS_FLAG]
      ,tabrptg.[APPT_STATUS_C]
      ,tabrptg.[APPT_MADE_DTTM]
      ,tabrptg.[APPT_MADE_DATE]
      ,tabrptg.[ENTRY_DATE]
      ,tabrptg.[CHANGE_DATE]
      ,tabrptg.[APPT_DTTM]
      ,tabrptg.[APPT_DT]
      ,tabrptg.[Appointment_Lag_Days]
      ,tabrptg.[MRN_int]
      ,tabrptg.[CONTACT_DATE]
      ,tabrptg.[PRC_ID]
      ,tabrptg.[PRC_NAME]
      ,tabrptg.[UVaID]
      ,tabrptg.[DEPT_SPECIALTY_NAME]
      ,tabrptg.[PROV_SPECIALTY_NAME]
      ,tabrptg.[ENC_TYPE_C]
      ,tabrptg.[ENC_TYPE_TITLE]
      ,tabrptg.[APPT_CONF_STAT_NAME]
      ,tabrptg.[ZIP]
      ,tabrptg.[APPT_CONF_DTTM]
      ,tabrptg.[financial_division]
      ,tabrptg.[financial_subdivision]
      ,tabrptg.[F2F_Flag]
      ,tabrptg.[Entry_UVaID]
      ,tabrptg.[PHONE_REM_STAT_NAME]
      ,tabrptg.[BUSINESS_UNIT]
      ,tabrptg.[Prov_Typ]
      ,tabrptg.[Staff_Resource]
      ,tabrptg.[BILL_PROV_YN]
      ,tabrptg.[Lip_Flag]
      ,tabrptg.[FINANCE_COST_CODE]
      ,tabrptg.[Prov_Based_Clinic]
      ,tabrptg.[Map_Type]
      ,tabrptg.[Load_Dtm]

	  ,o.organization_id
	  ,o.organization_name
	  ,s.service_id
	  ,s.service_name
	  ,c.clinical_area_id
	  ,c.clinical_area_name
	  ,g.ambulatory_flag
	  ,g.community_health_flag

  INTO #TabRptg2

  FROM [DS_HSDM_App].[TabRptg].[Dash_AmbOpt_NewPatientAppt_AccessWin14CPT_Tiles] tabrptg

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id

 WHERE tabrptg.event_date >= @locstartdate AND tabrptg.event_date <= @locenddate

SELECT
    'New Patient Appointment Within 14 Calendar Days' AS Metric
   ,'Tab Table' AS Source
   ,'MTD' AS [Time Period]
   ,SUM(CASE WHEN event_count = 1 THEN 1 ELSE 0 END) AS [AbleToAccess]
   ,COUNT(*) AS NewPatient
FROM #TabRptg2
WHERE
ambulatory_flag = 1
--(ambulatory_flag = 1 OR community_health_flag = 1)
*/
/*
Provider-initiated Cancellation Rate

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure DM DS_HSDM_App ETL uspSrc_AmbOpt_Provider_Canceled_Appointment_Metric.sql
*/
/*
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
*/
/*
PXO - Likelihood of Recommending (Practice)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_AmbOpt_CGCAHPS_LklihdRecommendPractice.sql
*/

IF OBJECT_ID('tempdb..#cgcahps ') IS NOT NULL
DROP TABLE #cgcahps

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

    SELECT DISTINCT
            CAST('Outpatient-CGCAHPS' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,pm.sk_Fact_Pt_Acct
           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
		   ,pm.pod_name
           ,pm.hub_id
		   ,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
		   ,pm.BUSINESS_UNIT
	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
		   ,pm.Staff_Resource
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id -- int
		   ,pm.som_division_name
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   ,pm.upg_practice_flag
		   ,pm.upg_practice_region_id
		   ,pm.upg_practice_region_name
		   ,pm.upg_practice_id
		   ,pm.upg_practice_name
		   ,pm.F2F_Flag
		   ,pm.ENC_TYPE_C
		   ,pm.ENC_TYPE_TITLE
		   ,pm.Lip_Flag
		   ,pm.FINANCE_COST_CODE
		   ,pm.Prov_Based_Clinic
		   ,pm.Map_Type
		   ,CAST(CASE pm.VALUE
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS weighted_score -- DECIMAL(10,2)

		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.ambulatory_flag
		   ,g.community_health_flag
		   ,pm.Survey_Designator

    INTO #cgcahps

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
                   -- MDM
				,loc_master.service_line_id
				,loc_master.service_line
				,loc_master.sub_service_line_id
				,loc_master.sub_service_line
				,loc_master.opnl_service_id
				,loc_master.opnl_service_name
				,loc_master.corp_service_line_id
				,loc_master.corp_service_line
				,loc_master.hs_area_id
				,loc_master.hs_area_name
				,CAST(NULL AS INTEGER) AS practice_group_id
				,CAST(NULL AS VARCHAR(150)) AS practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,loc_master.EPIC_DEPT_NAME AS epic_department_name
				,loc_master.EPIC_EXT_NAME AS epic_department_name_external
				,loc_master.POD_ID AS pod_id
		        ,loc_master.pod_name
				,loc_master.HUB_ID AS hub_id
		        ,loc_master.hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn
				,resp.sk_Dim_Physcn
				,loc_master.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,loc_master.LOC_ID AS rev_location_id
				,loc_master.REV_LOC_NAME AS rev_location
				   -- SOM
				,TRY_CAST(prov.Financial_Division AS INT) AS financial_division_id
				,CASE WHEN prov.Financial_Division_Name <> 'na' THEN CAST(prov.Financial_Division_Name AS VARCHAR(150)) ELSE NULL END AS financial_division_name
				,TRY_CAST(prov.Financial_SubDivision AS INT) AS financial_sub_division_id
				,CASE WHEN prov.Financial_SubDivision_Name <> 'na' THEN CAST(prov.Financial_SubDivision_Name AS VARCHAR(150)) ELSE NULL END AS financial_sub_division_name
				,dvsn.som_group_id
				,dvsn.som_group_name
				,dvsn.Department_ID AS som_department_id
				,CAST(dvsn.Department AS VARCHAR(150)) AS som_department_name
				,CAST(dvsn.Org_Number AS INT) AS som_division_id
				,CAST(dvsn.Organization AS VARCHAR(150)) AS som_division_name
				,dvsn.som_hs_area_id
				,dvsn.som_hs_area_name
				,loc_master.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(loc_master.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(loc_master.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(loc_master.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(loc_master.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,loc_master.FINANCE_COST_CODE
				,dep.Prov_Based_Clinic
				,map.Map_Type
				,resp.Survey_Designator

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt	
		LEFT OUTER JOIN
		(
		SELECT
			sk_Dim_Physcn,
			NPI,
			PROV_ID,
			Prov_Nme,
			Prov_Typ,
			Staff_Resource,
			Financial_Division,
			Financial_Division_Name,
			Financial_SubDivision,
			Financial_SubDivision_Name,
			ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY Load_Dte DESC) AS ser_seq
		FROM [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc]
		) prov
		--provider table
		ON (prov.NPI = resp.NPI)
		AND prov.ser_seq = 1
        LEFT JOIN
		(
			SELECT sk_Dim_Physcn,
					UVaID,
					Service_Line,
					ProviderGroup
			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
			WHERE current_flag = 1
		) AS doc
			    ON doc.sk_Dim_Physcn = resp.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
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
		) AS loc_master
		ON (loc_master.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID)
		AND loc_master.Seq = 1
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                ON map.Deptid = CAST(loc_master.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
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
			ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(prov.Financial_Division AS INT)
				AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(prov.Financial_SubDivision AS INT))

 		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	            --AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
	            AND SUBSTRING(resp.Survey_Designator,1,2) IN ('MD','MT','TP')
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhgr
	ON mdmhgr.EPIC_DEPARTMENT_ID = pm.epic_department_id

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    SELECT organization_id
	              ,organization_name
				  ,service_id
	              ,service_name
				  ,clinical_area_id
				  ,clinical_area_name
				  ,som_division_id
				  ,som_division_name
				  ,som_department_id
				  ,som_department_name
				  ,epic_department_id
	              ,epic_department_name
				  ,weighted_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS LikelihoodRecommendPracticeResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator

	INTO #summary

	FROM #cgcahps

SELECT
    'PXO - Likelihood of Recommending (Practice)' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(LikelihoodRecommendPracticeResponse) AS LikelihoodRecommendPracticeResponse
	FROM #summary
	WHERE LikelihoodRecommendPracticeResponse = 1
	AND ambulatory_flag = 1
	--AND (ambulatory_flag = 1 and community_health_flag = 0)
	--AND (ambulatory_flag = 1 or community_health_flag = 1)

	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND SUBSTRING(Survey_Designator,1,2) IN ('MD','MT','TP')

SELECT
    'PXO - Likelihood of Recommending (Practice)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(event_count) AS LikelihoodRecommendPracticeResponse
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_CGCAHPSRecommendProvOffice_Tiles tabrptg
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   event_date>=@locstartdate
            AND event_date<@locenddate
			AND ambulatory_flag = 1
			--AND (ambulatory_flag = 1 and community_health_flag = 0)
			--AND (ambulatory_flag = 1 or community_health_flag = 1)

/*
PXO - Overall Rating HCAHPS

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_SvcLine_Inpatient_HCAHPS.sql
*/

IF OBJECT_ID('tempdb..#Proc ') IS NOT NULL
DROP TABLE #Proc

IF OBJECT_ID('tempdb..#TabRptg3 ') IS NOT NULL
DROP TABLE #TabRptg3

--    SELECT DISTINCT
--            CAST('Inpatient-HCAHPS' AS VARCHAR(50)) AS event_type		--this is the service code for inpatient-HCAHPS
--           ,pm.CMS_23 AS event_category	--overall question, will count 9's and 10's
--           ,rec.day_date AS event_date		--date survey received
--           ,CAST(COALESCE(loc.epic_department_name,pm.UNIT) AS VARCHAR(254)) AS UNIT
--           ,rec.fmonth_num
--           ,rec.FYear_name
--           ,rec.Fyear_num
--           ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
--           ,pm.MRN_int AS person_id		--patient
--           ,pm.PAT_NAME AS person_name		--patient
--           ,pm.BIRTH_DATE AS person_birth_date--patient
--           ,CAST(CASE WHEN pm.PT_SEX='F' THEN 'Female'
--                      WHEN pm.PT_SEX='M' THEN 'Male'
--                      ELSE NULL
--                 END AS VARCHAR(6)) AS person_gender
--           ,CASE WHEN pm.CMS_23 IS NULL THEN 0
--                 ELSE 1
--            END AS event_count		--count when the overall question has been answered
--           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
--           ,rec.day_date AS report_date
--           ,provider_id
--           ,provider_name
--			--handle UNIT of OBS, dividing between children and adult; handle when no unit is returned and set to medical center level
--           ,service_line_id = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.Service_Line_ID
--                                   WHEN loc.epic_department_name IS NULL AND pm.UNIT='No Unit' THEN NULL
--                                   ELSE COALESCE(loc.service_line_id, bscm.SERVICE_LINE_ID)
--                              END
--           ,service_line = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.Service_Line
--                                WHEN loc.epic_department_name IS NULL AND pm.UNIT='No Unit' THEN NULL
--                                ELSE COALESCE(loc.service_line,bscm.Service_Line)
--                           END
--           ,sub_service_line_id = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT='OBS'
--                                            AND AGE<18 THEN 1
--                                       WHEN loc.epic_department_name IS NULL AND pm.UNIT='obs'
--                                            AND AGE>=18 THEN 3
--                                       WHEN loc.epic_department_name IS NULL AND pm.UNIT='no unit' THEN NULL
--                                       ELSE COALESCE(loc.sub_service_line_id,bscm.Sub_Service_Line_ID)
--                                  END
--           ,sub_service_line = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT='OBS'
--                                         AND AGE<18 THEN 'Children'
--                                    WHEN loc.epic_department_name IS NULL AND pm.UNIT='obs'
--                                         AND AGE>=18 THEN 'Women'
--                                    WHEN loc.epic_department_name IS NULL AND pm.UNIT='no unit' THEN NULL
--                                    ELSE COALESCE(loc.sub_service_line,bscm.Sub_Service_Line)
--                               END
--           ,opnl_service_id = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT<>'OBS' THEN bscm.opnl_service_id
--                                   ELSE COALESCE(loc.opnl_service_id,NULL)
--                              END
--           ,opnl_service_name = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT<>'OBS' THEN bscm.opnl_service_name
--                                     ELSE COALESCE(loc.opnl_service_name,NULL)
--                                END
--           ,hs_area_id = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.hs_area_id
--                              WHEN loc.epic_department_name IS NULL AND pm.UNIT='no unit' THEN 1
--                              ELSE COALESCE(loc.hs_area_id,bscm.hs_area_id)
--                         END
--           ,hs_area_name = CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.hs_area_name
--                                WHEN loc.epic_department_name IS NULL AND pm.UNIT='no unit' THEN 'Medical Center'
--                                ELSE COALESCE(loc.hs_area_name,bscm.hs_area_name)
--                           END
			
--			--Add SOM grouping  04/12/2019  Mali A. 
--		   ,pm.som_group_id
--		   ,pm.som_group_name
--		   ,pm.rev_location_id
--		   ,pm.rev_location
--		   ,pm.financial_division_id
--		   ,pm.financial_division_name
--		   ,pm.financial_sub_division_id
--		   ,pm.financial_sub_division_name
--		   ,pm.som_department_id
--		   ,pm.som_department_name
--		   ,pm.som_division_id
--		   ,pm.som_division_name
--		   --Add SOM Grouping 05/15/2019 -Mali A.
--		    ,pm.som_division_5
--		   ,pm.som_hs_area_id
--		   ,pm.som_hs_area_name
--		   --Add sk_Dim_Pt 09/12/2019 -TMB
--		   ,pm.sk_Dim_Pt -- INTEGER
--		   ------
--		   ,pm.Clrt_DEPt_Nme
--		   ,pm.DEPARTMENT_ID
--		   ,CAST(COALESCE(loc.epic_department_id,bscm.EPIC_DEPARTMENT_ID) AS NUMERIC(18, 0)) AS epic_department_id
--           ,epic_department_name = COALESCE(loc.epic_department_name,department.Clrt_DEPt_Nme)
--           ,epic_department_name_external = COALESCE(loc.epic_department_name_external,department.Clrt_DEPt_Ext_Nme)
--           ,CASE WHEN pm.AGE<18 THEN 1
--                 ELSE 0
--            END AS peds
--           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
--                 ELSE 0
--            END AS transplant
--		   ,o.organization_id
--		   ,o.organization_name
--		   ,s.service_id
--		   ,s.service_name
--		   ,c.clinical_area_id
--		   ,c.clinical_area_name
--		   ,g.community_health_flag

--    INTO #Proc

--    FROM    DS_HSDW_Prod.dbo.Dim_Date AS rec
--    LEFT OUTER JOIN (
--                     --pm
--SELECT  unitemp.SURVEY_ID
--           ,unitemp.RECDATE
--		   ,unitemp.Load_Dtm
--           ,unitemp.MRN_int
--           ,unitemp.sk_Fact_Pt_Acct
--           ,unitemp.NPINumber
--           ,unitemp.sk_Dim_Physcn
--           ,unitemp.provider_id
--           ,unitemp.provider_name
--           ,unitemp.Service_Line_md
--           ,unitemp.PAT_NAME
--           ,unitemp.CMS_23
--           ,unitemp.AGE
--		   ,CASE WHEN unitemp.unit=pg_update.update_unit THEN unitemp.unit 
--		   WHEN pg_update.update_unit IS NOT NULL 
--		   THEN pg_update.update_unit 
--		   ELSE unitemp.unit END AS UNIT
--		   ,dept.DEPARTMENT_ID
--		   ,dept.Clrt_DEPt_Nme
--           ,unitemp.ADJSAMP
--           ,unitemp.BIRTH_DATE
--           ,unitemp.PT_SEX
--		   ,NULL AS som_group_id
--		   ,NULL AS som_group_name
--		   ,loc_master.LOC_ID AS rev_location_id
--			,loc_master.REV_LOC_NAME AS rev_location
--		    ,uwd.Clrt_Financial_Division AS financial_division_id
--			,uwd.Clrt_Financial_Division_Name AS financial_division_name
--			,uwd.Clrt_Financial_SubDivision AS financial_sub_division_id
--			,uwd.Clrt_Financial_SubDivision_Name financial_sub_division_name
--			,CAST(uwd.SOM_Department_ID AS INT) AS som_department_id
--			,CAST(uwd.SOM_Department AS VARCHAR(150)) AS som_department_name
--			,CAST(uwd.SOM_Division_ID AS INT) AS som_division_id
--			,CAST(uwd.SOM_Division_Name AS VARCHAR(150)) AS som_division_name
--		-- Add 05/15/2018 Mali A. 
--			,CAST(uwd.SOM_division_5 AS VARCHAR(150)) AS som_division_5

--  			    ,CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS SMALLINT) ELSE CAST(3 AS SMALLINT) END AS som_hs_area_id
--				,CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS VARCHAR(150)) ELSE CAST('School of Medicine' AS VARCHAR(150)) END AS som_hs_area_name
--		-- Add 09/12/2019 TMB
--		    ,unitemp.sk_Dim_Pt

--    FROM    (
--             --unitemp
--                     SELECT SURVEY_ID
--                           ,RECDATE
--                           ,MRN_int
--                           ,sk_Fact_Pt_Acct
--                           ,NPINumber
--                           ,sk_Dim_Physcn
--                           ,provider_id
--                           ,provider_name
--                           ,Service_Line_md
--						   ,p.Load_Dtm
--                           ,PAT_NAME
--                           ,[CMS_23]
--                           ,[AGE]
--						   --,p.UNIT
--                           ,CAST(CASE [UNIT]
--                                   WHEN 'ADMT (Closed)' THEN 'Other'
--                                   WHEN 'ER' THEN 'Other'
--                                   WHEN 'ADMT' THEN 'Other'
--                                   WHEN 'MSIC' THEN 'Other'
--                                   WHEN 'MSICU' THEN 'Other'
--                                   WHEN 'NNICU' THEN 'NNIC'
--                                   ELSE [UNIT]
--                                 END AS VARCHAR(8)) AS UNIT
--                           ,p.sk_Dim_Clrt_DEPt
--						   ,[ADJSAMP]
--                           ,BIRTH_DATE
--                           ,PT_SEX
--						   ,p.sk_Dim_Pt
--                     FROM   (
--                             --pivoted
--												SELECT DISTINCT
--                                                        pm.SURVEY_ID
--                                                       ,RECDATE
--                                                       ,LEFT(VALUE, 20) AS VALUE
--                                                       ,qstn.VARNAME
--                                                       ,fpa.MRN_int
--                                                       ,fpa.sk_Fact_Pt_Acct
--                                                       ,dp.NPINumber
--                                                       ,dp.sk_Dim_Physcn
--                                                       ,prov.PROV_ID AS provider_id
--                                                       ,dp.DisplayName AS provider_name
--                                                       ,dp.Service_Line AS Service_Line_md
--                                                       ,CAST(CONCAT(pat.PT_LNAME, ',', pat.PT_FNAME_MI) AS VARCHAR(200)) AS PAT_NAME
--                                                       ,pat.BIRTH_DT AS BIRTH_DATE
--                                                       ,pat.PT_SEX
--													   ,pm.sk_Dim_Clrt_DEPt
--													   ,pm.Load_Dtm
--													   ,pm.sk_Dim_Pt
--                                                FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS pm
--                                                INNER JOIN DS_HSDW_Prod.dbo.Dim_PG_Question AS qstn
--                                                        ON pm.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
--                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Acct AS fpa
--                                                        ON pm.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
--                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Pt AS pat
--                                                        ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
--                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Physcn AS dp
--                                                        ON CASE WHEN pm.sk_Dim_Physcn IN (0,-1) THEN -999 ELSE pm.sk_Dim_Physcn END=dp.sk_Dim_Physcn
--                                                LEFT OUTER JOIN [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc] prov
--                                                        --provider table
--                                                            ON dp.[sk_Dim_Physcn]=prov.[sk_Dim_Physcn]
--                                                WHERE   pm.Svc_Cde='IN'
--                                                        AND qstn.VARNAME IN ('CMS_23', 'AGE', 'UNIT', 'ADJSAMP')
--                                                        AND pm.RECDATE >= @locstartdate
--                                                        AND pm.RECDATE <  @locenddate
--                            ) AS pivoted PIVOT ( MAX(VALUE) FOR VARNAME IN ([CMS_23], [AGE], [UNIT], [ADJSAMP]) ) AS p
--            ) unitemp
--    LEFT OUTER JOIN (
--                     SELECT DISTINCT
--                            sk_Fact_Pt_Acct
--						    ,LAST_VALUE(loc.PRESSGANEY_NAME) OVER (PARTITION BY sk_Fact_Pt_Acct ORDER BY LOAD_DATE, LOAD_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS update_unit

--                     FROM   [DS_HSDM_Ext_OutPuts].[DwStage].[Press_Ganey_DW_Submission] 

--					 INNER JOIN ds_hsdw_prod.Rptg.vwRef_MDM_Location_Master_A2K3 AS a2unit ON a2unit.A2K3_NAME=Press_Ganey_DW_Submission.NURSTA
--					 INNER JOIN DS_HSDW_Prod.rptg.vwRef_MDM_Location_Master AS loc ON a2unit.epic_department_id = loc.EPIC_DEPARTMENT_ID
--                    ) AS pg_update
--            ON unitemp.sk_Fact_Pt_Acct=pg_update.sk_Fact_Pt_Acct
--	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt AS dept
--			ON unitemp.sk_Dim_Clrt_DEPt = dept.sk_Dim_Clrt_DEPt
--	LEFT OUTER JOIN
--        (
--            SELECT DISTINCT
--                   EPIC_DEPARTMENT_ID,
--                   SERVICE_LINE,
--				   POD_ID,
--                   PFA_POD,
--				   HUB_ID,
--                   HUB,
--			       BUSINESS_UNIT,
--				   LOC_ID,
--				   REV_LOC_NAME
--            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--        ) AS loc_master
--                ON dept.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
           
--	LEFT OUTER JOIN
--	     (
--					SELECT DISTINCT
--					    wd.sk_Dim_Physcn,
--						wd.PROV_ID,
--             			wd.Clrt_Financial_Division,
--			    		wd.Clrt_Financial_Division_Name,
--						wd.Clrt_Financial_SubDivision, 
--					    wd.Clrt_Financial_SubDivision_Name,
--					    wd.wd_Dept_Code,
--					    wd.SOM_Group_ID,
--					    wd.SOM_Group,
--						wd.SOM_department_id,
--					    wd.SOM_department,
--						wd.SOM_division_id,
--						wd.SOM_division_name,
--						wd.SOM_division_5
--					FROM
--					(
--					    SELECT
--						    cwlk.sk_Dim_Physcn,
--							cwlk.PROV_ID,
--             			    cwlk.Clrt_Financial_Division,
--			    		    cwlk.Clrt_Financial_Division_Name,
--						    cwlk.Clrt_Financial_SubDivision, 
--							cwlk.Clrt_Financial_SubDivision_Name,
--							cwlk.wd_Dept_Code,
--							som.SOM_Group_ID,
--							som.SOM_Group,
--							som.SOM_department_id,
--							som.SOM_department,
--							som.SOM_division_id,
--							som.SOM_division_name,
--							som.SOM_division_5,
--							ROW_NUMBER() OVER (PARTITION BY cwlk.sk_Dim_Physcn ORDER BY som.som_group_id ASC) AS [SOMSeq]
--						FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
--						    LEFT OUTER JOIN (SELECT DISTINCT
--							                     SOM_Group_ID,
--												 SOM_Group,
--												 SOM_department_id,
--												 SOM_department,
--												 SOM_division_id,
--												 SOM_division_name,
--												 SOM_division_5
--						                     FROM Rptg.vwRef_SOM_Hierarchy
--						                    ) AS som
--						        ON cwlk.wd_Dept_Code = som.SOM_division_5
--					    WHERE cwlk.wd_Is_Primary_Job = 1
--                              AND cwlk.wd_Is_Position_Active = 1
--					) AS wd
--					WHERE wd.SOMSeq = 1
--				) AS uwd
--					 ON uwd.sk_Dim_Physcn = unitemp.sk_Dim_Physcn      
--		  ) AS pm
--            ON rec.day_date=pm.RECDATE
--					   --AND	ADJSAMP<>'Not Included'  --remove adjusted internet surveys
--		-- -------------------------------------
--		-- Identify transplant encounter
--		-- -------------------------------------
--    LEFT OUTER JOIN (
--                     SELECT fpec.PAT_ENC_CSN_ID
--                           ,txsurg.day_date AS transplant_surgery_dt
--                           ,fpec.Adm_Dtm
--                           ,fpec.sk_Fact_Pt_Enc_Clrt
--                           ,fpec.sk_Fact_Pt_Acct
--                           ,fpec.sk_Dim_Clrt_Pt
--                           ,fpec.sk_Dim_Pt
--                     FROM   DS_HSDW_Prod.dbo.Fact_Pt_Trnsplnt_Clrt AS fptc
--                     INNER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt AS fpec
--                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
--                     INNER JOIN DS_HSDW_Prod.dbo.Dim_Date AS txsurg
--                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
--                     WHERE  txsurg.day_date BETWEEN CAST(fpec.Adm_Dtm AS DATE) AND fpec.Dsch_Dtm
--                            AND txsurg.day_date<>'1900-01-01 00:00:00'
--                    ) AS tx
--            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
--    LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master AS bscm
--            ON pm.UNIT=bscm.PRESSGANEY_NAME
--    LEFT OUTER JOIN DS_HSDW_Prod.Anlys.Ref_Service_Line AS sl
--            ON (CASE WHEN pm.UNIT='OBS' THEN 'Womens and Childrens'
--                     ELSE NULL
--                END)=sl.Service_Line
--   LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS loc
--			ON pm.DEPARTMENT_ID=loc.epic_department_id
--		-- ------------------------------------
--    LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt AS department
--            ON bscm.EPIC_DEPARTMENT_ID=department.DEPARTMENT_ID

--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
--	ON g.epic_department_id = pm.DEPARTMENT_ID
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
--	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
--	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
--	ON o.organization_id = s.organization_id
	
--    WHERE   rec.day_date >= @locstartdate
--        AND rec.day_date <  @locenddate
--		--AND CASE WHEN loc.epic_department_name IS NULL AND pm.UNIT='OBS'
--		--                                         AND AGE<18 THEN 'Children'
--		--                                    WHEN loc.epic_department_name IS NULL AND pm.UNIT='obs'
--		--                                         AND AGE>=18 THEN 'Women'
--		--                                    WHEN loc.epic_department_name IS NULL AND pm.UNIT='no unit' THEN NULL
--		--                                    ELSE COALESCE(loc.sub_service_line,bscm.Sub_Service_Line)
--		--                               END = 'Children';
--		AND CASE WHEN pm.CMS_23 IS NULL THEN 0
--		                 ELSE 1
--		            END = 1
--	    --;
--	ORDER BY event_category

    SELECT DISTINCT
            CAST('Inpatient-HCAHPS' AS VARCHAR(50)) AS event_type		--this is the service code for inpatient-HCAHPS
           ,pm.CMS_23 AS event_category	--overall question, will count 9's and 10's
           ,rec.day_date AS event_date		--date survey received
           ,CAST(COALESCE(pm.epic_department_name,pm.UNIT) AS VARCHAR(254)) AS UNIT
           ,rec.fmonth_num
           ,rec.FYear_name
           ,rec.Fyear_num
           ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,CAST(CASE WHEN pm.PT_SEX='F' THEN 'Female'
                      WHEN pm.PT_SEX='M' THEN 'Male'
                      ELSE NULL
                 END AS VARCHAR(6)) AS person_gender
           ,CASE WHEN pm.CMS_23 IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,provider_id
           ,provider_name
		   ,CASE WHEN dc_phys.type_of_HSF_contract = 'UVACHMG Employed' THEN 1 ELSE 0 END  CH_Hosp_Based_YN
			--handle UNIT of OBS, dividing between children and adult; handle when no unit is returned and set to medical center level
           ,service_line_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.Service_Line_ID
                                   WHEN pm.epic_department_name IS NULL AND pm.UNIT='No Unit' THEN NULL
                                   ELSE COALESCE(pm.service_line_id, bscm.SERVICE_LINE_ID)
                              END
           ,service_line = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.Service_Line
                                WHEN pm.epic_department_name IS NULL AND pm.UNIT='No Unit' THEN NULL
                                ELSE COALESCE(pm.service_line,bscm.Service_Line)
                           END
           ,sub_service_line_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS'
                                            AND AGE<18 THEN 1
                                       WHEN pm.epic_department_name IS NULL AND pm.UNIT='obs'
                                            AND AGE>=18 THEN 3
                                       WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN NULL
                                       ELSE COALESCE(pm.sub_service_line_id,bscm.Sub_Service_Line_ID)
                                  END
           ,sub_service_line = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS'
                                         AND AGE<18 THEN 'Children'
                                    WHEN pm.epic_department_name IS NULL AND pm.UNIT='obs'
                                         AND AGE>=18 THEN 'Women'
                                    WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN NULL
                                    ELSE COALESCE(pm.sub_service_line,bscm.Sub_Service_Line)
                               END
           ,opnl_service_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT<>'OBS' THEN bscm.opnl_service_id
                                   ELSE COALESCE(pm.opnl_service_id,NULL)
                              END
           ,opnl_service_name = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT<>'OBS' THEN bscm.opnl_service_name
                                     ELSE COALESCE(pm.opnl_service_name,NULL)
                                END
           ,hs_area_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.hs_area_id
                              WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN 1
                              ELSE COALESCE(pm.hs_area_id,bscm.hs_area_id)
                         END
           ,hs_area_name = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.hs_area_name
                                WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN 'Medical Center'
                                ELSE COALESCE(pm.hs_area_name,bscm.hs_area_name)
                           END
			
			--Add SOM grouping  04/12/2019  Mali A. 
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id
		   ,pm.som_division_name
		   --Add SOM Grouping 05/15/2019 -Mali A.
		    ,pm.som_division_5
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   --Add sk_Dim_Pt 09/12/2019 -TMB
		   ,pm.sk_Dim_Pt -- INTEGER
		   , pm.sk_Fact_Pt_Enc_Clrt
		   ------
		   ,CAST(COALESCE(pm.epic_department_id,bscm.EPIC_DEPARTMENT_ID) AS NUMERIC(18, 0)) AS epic_department_id
           ,epic_department_name = COALESCE(pm.epic_department_name,department.Clrt_DEPt_Nme)
           ,epic_department_name_external = COALESCE(pm.epic_department_name_external,department.Clrt_DEPt_Ext_Nme)
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.community_health_flag

    INTO #Proc

    FROM    DS_HSDW_Prod.dbo.Dim_Date AS rec
    
	LEFT OUTER JOIN (
                     --pm
SELECT  unitemp.SURVEY_ID
           ,unitemp.RECDATE
		   ,unitemp.Load_Dtm
           ,unitemp.MRN_int
           ,unitemp.sk_Fact_Pt_Acct
           ,unitemp.NPINumber
           ,unitemp.sk_Dim_Physcn
           ,unitemp.provider_id
           ,unitemp.provider_name
           ,unitemp.Service_Line_md
           ,unitemp.PAT_NAME
           ,unitemp.CMS_23
           ,unitemp.AGE
		   ,CASE WHEN unitemp.unit=pg_update.update_unit THEN unitemp.unit 
		   WHEN pg_update.update_unit IS NOT NULL 
		   THEN pg_update.update_unit 
		   ELSE unitemp.unit END AS UNIT
		   ,dept.DEPARTMENT_ID
           ,unitemp.ADJSAMP
           ,unitemp.BIRTH_DATE
           ,unitemp.PT_SEX
		   ,NULL AS som_group_id
		   ,NULL AS som_group_name
		   ,mdmhst.LOC_ID AS rev_location_id
			,mdmhst.REV_LOC_NAME AS rev_location
		    ,uwd.Clrt_Financial_Division AS financial_division_id
			,uwd.Clrt_Financial_Division_Name AS financial_division_name
			,uwd.Clrt_Financial_SubDivision AS financial_sub_division_id
			,uwd.Clrt_Financial_SubDivision_Name financial_sub_division_name
			,CAST(uwd.SOM_Department_ID AS INT) AS som_department_id
			,CAST(uwd.SOM_Department AS VARCHAR(150)) AS som_department_name
			,CAST(uwd.SOM_Division_ID AS INT) AS som_division_id
			,CAST(uwd.SOM_Division_Name AS VARCHAR(150)) AS som_division_name
		-- Add 05/15/2018 Mali A. 
			,CAST(uwd.SOM_division_5 AS VARCHAR(150)) AS som_division_5

  			    ,CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS SMALLINT) ELSE CAST(3 AS SMALLINT) END AS som_hs_area_id
				,CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS VARCHAR(150)) ELSE CAST('School of Medicine' AS VARCHAR(150)) END AS som_hs_area_name
		-- Add 09/12/2019 TMB
		    ,unitemp.sk_Dim_Pt
			, unitemp.sk_Fact_Pt_Enc_Clrt

			,mdmhst.EPIC_DEPARTMENT_ID
			,mdmhst.EPIC_DEPT_NAME AS epic_department_name
			,mdmhst.EPIC_EXT_NAME AS epic_department_name_external
			,mdmhst.SERVICE_LINE_ID
			,mdmhst.SERVICE_LINE
			,mdmhst.SUB_SERVICE_LINE_ID
			,mdmhst.SUB_SERVICE_LINE
			,mdmhst.OPNL_SERVICE_ID
			,mdmhst.OPNL_SERVICE_NAME
			,mdmhst.HS_AREA_ID
			,mdmhst.HS_AREA_NAME
			,mdmhst.PRESSGANEY_NAME

    FROM    (
             --unitemp
                     SELECT SURVEY_ID
                           ,RECDATE
                           ,MRN_int
                           ,sk_Fact_Pt_Acct
                           ,NPINumber
                           ,sk_Dim_Physcn
                           ,provider_id
                           ,provider_name
                           ,Service_Line_md
						   ,p.Load_Dtm
                           ,PAT_NAME
                           ,[CMS_23]
                           ,[AGE]
						   --,p.UNIT
                           ,CAST(CASE [UNIT]
                                   WHEN 'ADMT (Closed)' THEN 'Other'
                                   WHEN 'ER' THEN 'Other'
                                   WHEN 'ADMT' THEN 'Other'
                                   WHEN 'MSIC' THEN 'Other'
                                   WHEN 'MSICU' THEN 'Other'
                                   WHEN 'NNICU' THEN 'NNIC'
                                   ELSE [UNIT]
                                 END AS VARCHAR(8)) AS UNIT
                           ,p.sk_Dim_Clrt_DEPt
						   ,[ADJSAMP]
                           ,BIRTH_DATE
                           ,PT_SEX
						   ,p.sk_Dim_Pt
						   ,p.sk_Fact_Pt_Enc_Clrt
                     FROM   (
                             --pivoted
												SELECT DISTINCT
                                                        pm.SURVEY_ID
                                                       ,RECDATE
                                                       ,LEFT(VALUE, 20) AS VALUE
                                                       ,qstn.VARNAME
                                                       ,fpa.MRN_int
                                                       ,fpa.sk_Fact_Pt_Acct
                                                       ,dp.NPINumber
                                                       ,dp.sk_Dim_Physcn
                                                       ,prov.PROV_ID AS provider_id
                                                       ,dp.DisplayName AS provider_name
                                                       ,dp.Service_Line AS Service_Line_md
                                                       ,CAST(CONCAT(pat.PT_LNAME, ',', pat.PT_FNAME_MI) AS VARCHAR(200)) AS PAT_NAME
                                                       ,pat.BIRTH_DT AS BIRTH_DATE
                                                       ,pat.PT_SEX
													   ,pm.sk_Dim_Clrt_DEPt
													   ,pm.Load_Dtm
													   ,pm.sk_Dim_Pt
													   ,fptec.sk_Fact_Pt_Enc_Clrt
                                                FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS pm
                                                INNER JOIN DS_HSDW_Prod.dbo.Dim_PG_Question AS qstn
                                                        ON pm.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Acct AS fpa
                                                        ON pm.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Pt AS pat
                                                        ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Physcn AS dp
                                                        ON CASE WHEN pm.sk_Dim_Physcn IN (0,-1) THEN -999 ELSE pm.sk_Dim_Physcn END=dp.sk_Dim_Physcn
                                                LEFT OUTER JOIN [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc] prov
                                                        --provider table
                                                            ON dp.[sk_Dim_Physcn]=prov.[sk_Dim_Physcn]
												LEFT JOIN DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt fptec
														ON pm.Pat_Enc_CSN_Id=fptec.PAT_ENC_CSN_ID
                                                WHERE   pm.Svc_Cde='IN'
                                                        AND qstn.VARNAME IN ('CMS_23', 'AGE', 'UNIT', 'ADJSAMP')
                                                        AND pm.RECDATE >= @locstartdate
                                                        AND pm.RECDATE <  @locenddate
                            ) AS pivoted PIVOT ( MAX(VALUE) FOR VARNAME IN ([CMS_23], [AGE], [UNIT], [ADJSAMP]) ) AS p
            ) unitemp
    LEFT OUTER JOIN (
                     SELECT DISTINCT
                            sk_Fact_Pt_Acct
						    ,LAST_VALUE(loc.PRESSGANEY_NAME) OVER (PARTITION BY sk_Fact_Pt_Acct ORDER BY LOAD_DATE, LOAD_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS update_unit

                     FROM   [DS_HSDM_Ext_OutPuts].[DwStage].[Press_Ganey_DW_Submission] 

					 INNER JOIN ds_hsdw_prod.Rptg.vwRef_MDM_Location_Master_A2K3 AS a2unit ON a2unit.A2K3_NAME=Press_Ganey_DW_Submission.NURSTA
					 INNER JOIN DS_HSDW_Prod.rptg.vwRef_MDM_Location_Master AS loc ON a2unit.epic_department_id = loc.EPIC_DEPARTMENT_ID
                    ) AS pg_update
            ON unitemp.sk_Fact_Pt_Acct=pg_update.sk_Fact_Pt_Acct
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt AS dept
			ON unitemp.sk_Dim_Clrt_DEPt = dept.sk_Dim_Clrt_DEPt
	LEFT OUTER JOIN
		(
			SELECT
				history.MDM_BATCH_ID,
                history.EPIC_DEPARTMENT_ID,
                history.EPIC_DEPT_NAME,
				history.EPIC_EXT_NAME,
                history.SERVICE_LINE_ID,
                history.SERVICE_LINE,
                history.SUB_SERVICE_LINE_ID,
                history.SUB_SERVICE_LINE,
                history.LOC_ID,
                history.REV_LOC_NAME,
                history.HS_AREA_ID,
                history.HS_AREA_NAME,
                history.OPNL_SERVICE_ID,
                history.OPNL_SERVICE_NAME,
                history.PRESSGANEY_NAME
			FROM
			(
				SELECT
					MDM_BATCH_ID,
                    EPIC_DEPARTMENT_ID,
                    EPIC_DEPT_NAME,
					EPIC_EXT_NAME,
                    SERVICE_LINE_ID,
                    SERVICE_LINE,
                    SUB_SERVICE_LINE_ID,
                    SUB_SERVICE_LINE,
                    LOC_ID,
                    REV_LOC_NAME,
                    HS_AREA_ID,
                    HS_AREA_NAME,
                    OPNL_SERVICE_ID,
                    OPNL_SERVICE_NAME,
                    PRESSGANEY_NAME
				   ,ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
				FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
			) history
			WHERE history.seq = 1
		) mdmhst
		ON mdmhst.EPIC_DEPARTMENT_ID = dept.DEPARTMENT_ID
           
	LEFT OUTER JOIN
	     (
					SELECT DISTINCT
					    wd.sk_Dim_Physcn,
						wd.PROV_ID,
             			wd.Clrt_Financial_Division,
			    		wd.Clrt_Financial_Division_Name,
						wd.Clrt_Financial_SubDivision, 
					    wd.Clrt_Financial_SubDivision_Name,
					    wd.wd_Dept_Code,
					    wd.SOM_Group_ID,
					    wd.SOM_Group,
						wd.SOM_department_id,
					    wd.SOM_department,
						wd.SOM_division_id,
						wd.SOM_division_name,
						wd.SOM_division_5
					FROM
					(
					    SELECT
						    cwlk.sk_Dim_Physcn,
							cwlk.PROV_ID,
             			    cwlk.Clrt_Financial_Division,
			    		    cwlk.Clrt_Financial_Division_Name,
						    cwlk.Clrt_Financial_SubDivision, 
							cwlk.Clrt_Financial_SubDivision_Name,
							cwlk.wd_Dept_Code,
							som.SOM_Group_ID,
							som.SOM_Group,
							som.SOM_department_id,
							som.SOM_department,
							som.SOM_division_id,
							som.SOM_division_name,
							som.SOM_division_5,
							ROW_NUMBER() OVER (PARTITION BY cwlk.sk_Dim_Physcn ORDER BY som.som_group_id ASC) AS [SOMSeq]
						FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
						    LEFT OUTER JOIN (SELECT DISTINCT
							                     SOM_Group_ID,
												 SOM_Group,
												 SOM_department_id,
												 SOM_department,
												 SOM_division_id,
												 SOM_division_name,
												 SOM_division_5
						                     FROM Rptg.vwRef_SOM_Hierarchy
						                    ) AS som
						        ON cwlk.wd_Dept_Code = som.SOM_division_5
					    WHERE cwlk.wd_Is_Primary_Job = 1
                              AND cwlk.wd_Is_Position_Active = 1
					) AS wd
					WHERE wd.SOMSeq = 1
				) AS uwd
					 ON uwd.sk_Dim_Physcn = unitemp.sk_Dim_Physcn      
		  ) AS pm
            ON rec.day_date=pm.RECDATE
					   --AND	ADJSAMP<>'Not Included'  --remove adjusted internet surveys
		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.dbo.Fact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.dbo.Dim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN CAST(fpec.Adm_Dtm AS DATE) AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
    LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master AS bscm
            ON pm.UNIT=bscm.PRESSGANEY_NAME
    LEFT OUTER JOIN DS_HSDW_Prod.Anlys.Ref_Service_Line AS sl
            ON (CASE WHEN pm.UNIT='OBS' THEN 'Womens and Childrens'
                     ELSE NULL
                END)=sl.Service_Line
		-- ------------------------------------
    LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt AS department
            ON bscm.EPIC_DEPARTMENT_ID=department.DEPARTMENT_ID
	
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn           AS dc_phys ON pm.sk_Dim_Physcn = dc_phys.sk_Dim_Physcn

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.DEPARTMENT_ID
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id

    WHERE   rec.day_date >= @locstartdate
        AND rec.day_date <  @locenddate
		AND CASE WHEN pm.CMS_23 IS NULL THEN 0
		                 ELSE 1
		            END = 1;

SELECT
       'PXO - Overall Rating HCAHPS' AS Metric
	 , 'Stored Procedure' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 , CAST(
	   CAST(SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #Proc
WHERE event_count = 1
AND community_health_flag = 0
) [Proc]

SELECT [event_type]
      ,[event_count]
      ,[event_date]
      ,[event_id]
      ,[event_category]
      ,tabrptg.[epic_department_id]
      ,tabrptg.[epic_department_name]
      ,[epic_department_name_external]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[report_period]
      ,[report_date]
      ,[peds]
      ,[transplant]
      ,[age_flag]
      ,[sk_Dim_Pt]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[practice_group_id]
      ,[practice_group_name]
      ,[provider_id]
      ,[provider_name]
      ,[service_line_id]
      ,[service_line]
      ,[sub_service_line_id]
      ,[sub_service_line]
      ,[opnl_service_id]
      ,[opnl_service_name]
      ,[corp_service_line_id]
      ,[corp_service_line_name]
      ,[hs_area_id]
      ,[hs_area_name]
      ,[Unit]
      ,[w_department_id]
      ,[w_department_name]
      ,[w_department_name_external]
      ,[w_opnl_service_id]
      ,[w_opnl_service_name]
      ,[w_corp_service_line_id]
      ,[w_corp_service_line_name]
      ,[w_practice_group_id]
      ,[w_practice_group_name]
      ,[w_service_line_id]
      ,[w_service_line_name]
      ,[w_sub_service_line_id]
      ,[w_sub_service_line_name]
      ,[w_report_period]
      ,[w_report_date]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,tabrptg.[Load_Dtm]
      ,[orig_transplant]
      ,[orig_peds]
      ,[financial_division_id]
      ,[financial_division_name]
      ,[financial_sub_division_id]
      ,[financial_sub_division_name]
      ,[rev_location_id]
      ,[rev_location]
      ,[som_group_id]
      ,[som_group_name]
      ,[som_department_id]
      ,[som_department_name]
      ,[som_division_id]
      ,[som_division_name]
      ,[w_financial_division_id]
      ,[w_financial_division_name]
      ,[w_financial_sub_division_id]
      ,[w_financial_sub_division_name]
      ,[w_rev_location_id]
      ,[w_rev_location]
      ,[w_som_group_id]
      ,[w_som_group_name]
      ,[w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
      ,[som_division_5]
      ,[w_som_hs_area_id]
      ,[w_som_hs_area_name]

  INTO #TabRptg3

  FROM [DS_HSDM_App].[TabRptg].[Dash_BalancedScorecard_HCAHPS_Tiles] tabrptg
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id
  WHERE  event_date >= @locstartdate AND event_date <= @locenddate
  --AND event_count = 1
  AND g.community_health_flag = 0

SELECT
       'PXO - Overall Rating HCAHPS' AS Metric
	 , 'Tab Table' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 , CAST(
	   CAST(SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #TabRptg3
WHERE event_count = 1
) TabRptg

/*
PXO - Overall Rating ED

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\BalancedScorecard\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure DM DS_HSDM_App ETL uspSrc_SvcLine_ER_OverallCareRating.sql
*/


IF OBJECT_ID('tempdb..#ed ') IS NOT NULL
DROP TABLE #ed

IF OBJECT_ID('tempdb..#summary2 ') IS NOT NULL
DROP TABLE #summary2

    SELECT DISTINCT
            CAST('Emergency-Press Ganey' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,pm.sk_Fact_Pt_Acct
           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
		   ,pm.pod_name
           ,pm.hub_id
		   ,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
		   ,pm.BUSINESS_UNIT
	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
		   ,pm.Staff_Resource
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id -- int
		   ,pm.som_division_name
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   ,pm.upg_practice_flag
		   ,pm.upg_practice_region_id
		   ,pm.upg_practice_region_name
		   ,pm.upg_practice_id
		   ,pm.upg_practice_name
		   ,pm.F2F_Flag
		   ,pm.ENC_TYPE_C
		   ,pm.ENC_TYPE_TITLE
		   ,pm.Lip_Flag
		   ,pm.FINANCE_COST_CODE
		   ,pm.Prov_Based_Clinic
		   ,pm.Map_Type
		   ,CAST(CASE pm.VALUE
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS weighted_score -- DECIMAL(10,2)
		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.ambulatory_flag
		   ,g.community_health_flag
		   ,pm.Survey_Designator

    INTO #ed

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				--,resp.sk_Dim_Clrt_DEPt
				,enc.sk_Dim_Clrt_DEPt
                   -- MDM
				,mdm.service_line_id
				,mdm.service_line
				,mdm.sub_service_line_id
				,mdm.sub_service_line
				,mdm.opnl_service_id
				,mdm.opnl_service_name
				,mdm.corp_service_line_id
				,mdm.corp_service_line
				,mdm.hs_area_id
				,mdm.hs_area_name
				,mdm.practice_group_id
				,mdm.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,mdm.epic_department_name_external
				,mdm.POD_ID AS pod_id
		        ,mdm.PFA_POD AS pod_name
				,mdm.HUB_ID AS hub_id
		        ,mdm.HUB AS hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
				      ELSE -999
				 END AS sk_Dim_Physcn
				,mdm.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,mdm.LOC_ID AS rev_location_id
				,mdm.REV_LOC_NAME AS rev_location
				   -- SOM
				,physcn.Clrt_Financial_Division AS financial_division_id
				,physcn.Clrt_Financial_Division_Name AS financial_division_name
				,physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id
				,physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name
				,physcn.SOM_Group_ID AS som_group_id
				,physcn.SOM_group AS som_group_name
				,physcn.SOM_department_id AS som_department_id
				,physcn.SOM_department AS	som_department_name
				,physcn.SOM_division_5 AS	som_division_id
				,physcn.SOM_division_name AS som_division_name
				,physcn.som_hs_area_id AS	som_hs_area_id
				,physcn.som_hs_area_name AS som_hs_area_name
				,mdm.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(mdm.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(mdm.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(mdm.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(mdm.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,mdm.FINANCE_COST_CODE
				,dep.Prov_Based_Clinic
				,map.Map_Type
				,resp.Survey_Designator

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt		
		  LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
								, sk_Dim_Clrt_SERsrc
								, sk_Dim_Physcn
								, ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												   WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												   ELSE Atn_End_Dtm
																												 END DESC) AS 'Atn_Seq'
						   FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						   WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1		
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
        LEFT JOIN
		(
			SELECT sk_Dim_Physcn,
					UVaID,
					Service_Line,
					ProviderGroup
			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
			WHERE current_flag = 1
		) AS doc
			    ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = doc.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '326' -- Age question for ER
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt -- 20220725: Department key missing in response Fact table for some DS encs
				--ON dep.sk_Dim_Clrt_DEPt = enc.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN
		( --mdm history to replace vwRef_MDM_Location_Master_EpicSvc and vwRef_MDM_Location_Master
			SELECT DISTINCT
				   hx.max_dt
				  ,rmlmh.EPIC_DEPARTMENT_ID
				  ,rmlmh.EPIC_DEPT_NAME AS epic_department_name
				  ,rmlmh.EPIC_EXT_NAME  AS epic_department_name_external
				  ,rmlmh.SERVICE_LINE_ID
				  ,rmlmh.SERVICE_LINE
				  ,rmlmh.SUB_SERVICE_LINE_ID
				  ,rmlmh.SUB_SERVICE_LINE
				  ,rmlmh.HS_AREA_ID
				  ,rmlmh.HS_AREA_NAME
				  ,rmlmh.PRACTICE_GROUP_ID
				  ,rmlmh.PRACTICE_GROUP_NAME
				  ,rmlmh.OPNL_SERVICE_ID
				  ,rmlmh.OPNL_SERVICE_NAME
				  ,rmlmh.CORP_SERVICE_LINE_ID
				  ,rmlmh.CORP_SERVICE_LINE
				  ,rmlmh.LOC_ID
				  ,rmlmh.REV_LOC_NAME
				  ,rmlmh.POD_ID
				  ,rmlmh.PFA_POD
				  ,rmlmh.PBB_POD
				  ,rmlmh.HUB_ID
				  ,rmlmh.HUB
				  ,rmlmh.BUSINESS_UNIT
				  ,rmlmh.FINANCE_COST_CODE
				  ,rmlmh.UPG_PRACTICE_REGION_ID
				  ,rmlmh.UPG_PRACTICE_REGION_NAME
				  ,rmlmh.UPG_PRACTICE_ID
				  ,rmlmh.UPG_PRACTICE_NAME
				  ,rmlmh.UPG_PRACTICE_FLAG
			FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS rmlmh
				INNER JOIN
				( --hx--most recent batch date per dep id
					SELECT mdmhx.EPIC_DEPARTMENT_ID
						  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
					FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS mdmhx
					GROUP BY mdmhx.EPIC_DEPARTMENT_ID
				)                                                 AS hx
					ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
					   AND rmlmh.BATCH_RUN_DT = hx.max_dt
		)                                                        AS mdm
			ON mdm.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
       -- LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
       --         ON dep.DEPARTMENT_ID = mdm.epic_department_id
       -- LEFT OUTER JOIN
       -- (
       --     SELECT DISTINCT
       --            EPIC_DEPARTMENT_ID,
       --            SERVICE_LINE,
				   --POD_ID,
       --            PFA_POD,
				   --HUB_ID,
       --            HUB,
			    --   BUSINESS_UNIT,
				   --LOC_ID,
				   --REV_LOC_NAME,
				   --UPG_PRACTICE_FLAG,
				   --UPG_PRACTICE_REGION_ID,
				   --UPG_PRACTICE_REGION_NAME,
				   --UPG_PRACTICE_ID,
				   --UPG_PRACTICE_NAME,
				   --FINANCE_COST_CODE
       --     FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
       -- ) AS loc_master
       --         ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map --Missing in DMT>DS_HSDM_App
                ON map.Deptid = CAST(mdm.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl  --Missing in DMT>DS_HSDM_App, is in app_DEV
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = CASE
					                            WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
												WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
												WHEN resp.sk_Dim_Physcn = -1 THEN -999
												WHEN resp.sk_Dim_Physcn = 0 THEN -999
												ELSE -999
										      END
		WHERE   resp.Svc_Cde='ER' AND resp.sk_Dim_PG_Question IN ('389') -- Overall rating of care received during your visit
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
      
		-- ------------------------------------

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

--/* -- #summary
    SELECT organization_id
	              ,organization_name
				  ,service_id
	              ,service_name
				  ,clinical_area_id
				  ,clinical_area_name
				  ,som_division_id
				  ,som_division_name
				  ,som_department_id
				  ,som_department_name
				  ,epic_department_id
	              ,epic_department_name
				  ,weighted_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS OverallRatingOfCareResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator

	INTO #summary2

	FROM #ed

	SELECT
       'PXO - Overall Rating ED' AS Metric
	 , 'Stored Procedure' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(weighted_score) AS weighted_score
	 , SUM(OverallRatingOfCareResponse) AS OverallRatingOfCareResponse
	FROM #summary2
	WHERE OverallRatingOfCareResponse = 1
	AND community_health_flag = 0

SELECT
       'PXO - Overall Rating ED' AS Metric
	 , 'Tab Table' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(weighted_score) AS weighted_score
	 , SUM(event_count) AS OverallRatingOfCareResponse
FROM DS_HSDM_App.TabRptg.Dash_BalancedScorecard_ER_OverallCareRating_Tiles tabrptg
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   event_date>=@locstartdate
            AND event_date<@locenddate
			AND g.community_health_flag = 0

/*
PXO - Likelihood of Recommending (Telehealth)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\BalancedScorecard\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure DM DS_HSDM_App ETL uspSrc_BalancedScorecard_PatSat_Telemedicine_OverallAssess_Tiles.sql
*/
/*
IF OBJECT_ID('tempdb..#tm ') IS NOT NULL
DROP TABLE #tm

IF OBJECT_ID('tempdb..#summary3 ') IS NOT NULL
DROP TABLE #summary3

    SELECT DISTINCT
            CAST('Telemedicine-Press Ganey' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,CAST(CASE WHEN pm.VALUE=1  THEN 0
				      WHEN pm.VALUE=2  THEN 25
					  WHEN pm.VALUE=3  THEN 50
					  WHEN pm.VALUE=4  THEN 75
					  WHEN pm.VALUE=5  THEN 100		
				 END AS INT) 			   AS  event_score
		   ,pm.sk_Dim_PG_Question	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.PT_SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,CAST(pm.EPIC_DEPARTMENT_ID AS NUMERIC(18, 0)) AS epic_department_id
           ,epic_department_name = pm.EPIC_DEPT_NAME
           ,epic_department_name_external = pm.EPIC_EXT_NAME
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.Survey_Designator
		   ,pm.organization_id
		   ,pm.organization_name
		   ,pm.service_id
		   ,pm.service_name
		   ,pm.clinical_area_id
		   ,pm.clinical_area_name
		   ,pm.ambulatory_flag
		   ,pm.community_health_flag

    INTO #tm

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,CAST(VALUE AS NVARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
				,loc_master.EPIC_DEPARTMENT_ID
				,loc_master.EPIC_DEPT_NAME
				,loc_master.EPIC_EXT_NAME
				,loc_master.HS_AREA_ID
				,loc_master.HS_AREA_NAME
				,loc_master.SERVICE_LINE_ID
				,loc_master.SERVICE_LINE
				,loc_master.SUB_SERVICE_LINE_ID
				,loc_master.SUB_SERVICE_LINE
				,loc_master.OPNL_SERVICE_ID
				,loc_master.OPNL_SERVICE_NAME
				,loc_master.CORP_SERVICE_LINE_ID
				,loc_master.CORP_SERVICE_LINE
				,loc_master.PRACTICE_GROUP_ID
				,loc_master.PRACTICE_GROUP_NAME
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.PT_LNAME, ',', pat.PT_FNAME_MI) AS VARCHAR(200)) AS PAT_NAME
				,pat.BIRTH_DT AS BIRTH_DATE
				,pat.PT_SEX
				,resp.Load_Dtm
				,resp.Survey_Designator
				,o.organization_id
				,o.organization_name
				,s.service_id
				,s.service_name
				,c.clinical_area_id
				,c.clinical_area_name
				,g.ambulatory_flag
				,g.community_health_flag
		FROM    DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Pt AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
		LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn AS dp
				ON resp.sk_Dim_Physcn=dp.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN dp.[sk_Dim_Physcn] IN ('0','-1') THEN '-999' ELSE dp.sk_Dim_Physcn END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of 0,-1 in SERsrc
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS NVARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN DS_HSDW_Prod.rptg.vwRef_MDM_location_master loc_master
				ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID

		LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
		ON g.epic_department_id = dep.DEPARTMENT_ID
		LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
		ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
		LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
		ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
		LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
		ON o.organization_id = s.organization_id

		WHERE   resp.Svc_Cde='MD' AND SUBSTRING(resp.Survey_Designator, 1, 2) = 'MT' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

    WHERE   rec.day_date>=@locstartdate
           AND rec.day_date<@locenddate--;
		   AND SUBSTRING(pm.Survey_Designator,1,2) = 'MT'

    SELECT hs_area_id
	              ,epic_department_id
	              ,epic_department_name
				  ,event_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS LikelihoodRecommendPracticeResponse
				  ,ambulatory_flag
				  ,community_health_flag

	INTO #summary3

	FROM #tm

	SELECT
       'PXO - Likelihood of Recommending (Telehealth)' AS Metric
	 , 'Stored Procedure' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(event_score) AS event_score
	 , SUM(LikelihoodRecommendPracticeResponse) AS LikelihoodRecommendPracticeResponse
	FROM #summary3
	WHERE LikelihoodRecommendPracticeResponse = 1
	--AND hs_area_id = 1
	AND ambulatory_flag = 1
	--AND (ambulatory_flag = 1 and community_health_flag = 0)
	--AND (ambulatory_flag = 1 or community_health_flag = 1)


	SELECT
       'PXO - Likelihood of Recommending (Telehealth)' AS Metric
	 , 'Tab Table' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(event_score) AS event_score
	 , SUM(tabrptg.event_count) AS LikelihoodRecommendPracticeResponse
	FROM TabRptg.Dash_BalancedScorecard_PatSat_TelemedicineOverallAssess_Tiles tabrptg

		LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
		ON g.epic_department_id = tabrptg.epic_department_id
		LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
		ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
		LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
		ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
		LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
		ON o.organization_id = s.organization_id
	WHERE tabrptg.event_count = 1
	--AND hs_area_id = 1
	AND ambulatory_flag = 1
	--AND (ambulatory_flag = 1 and community_health_flag = 0)
	--AND (ambulatory_flag = 1 or community_health_flag = 1)

	AND tabrptg.event_date >= @locstartdate
           AND tabrptg.event_date<@locenddate
*/
-- FY24 -----------------------------------------------------------------------------------------------------------------------------------------------------------

/*
PXO - Staff Worked Together (Practice)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_AmbOpt_MD_StaffWorkedTogether.sql
*/

IF OBJECT_ID('tempdb..#md ') IS NOT NULL
DROP TABLE #md

IF OBJECT_ID('tempdb..#summary4 ') IS NOT NULL
DROP TABLE #summary4

    SELECT DISTINCT
            CAST('Outpatient-MD' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,pm.sk_Fact_Pt_Acct
           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
		   ,pm.pod_name
           ,pm.hub_id
		   ,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
		   ,pm.BUSINESS_UNIT
	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
		   ,pm.Staff_Resource
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id -- int
		   ,pm.som_division_name
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   ,pm.upg_practice_flag
		   ,pm.upg_practice_region_id
		   ,pm.upg_practice_region_name
		   ,pm.upg_practice_id
		   ,pm.upg_practice_name
		   ,pm.F2F_Flag
		   ,pm.ENC_TYPE_C
		   ,pm.ENC_TYPE_TITLE
		   ,pm.Lip_Flag
		   ,pm.FINANCE_COST_CODE
		   ,pm.Prov_Based_Clinic
		   ,pm.Map_Type
		   ,CAST(CASE pm.VALUE
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS weighted_score -- DECIMAL(10,2)

		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.ambulatory_flag
		   ,g.community_health_flag
		   ,pm.Survey_Designator
	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag

    INTO #md

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
                   -- MDM
				,mdm.service_line_id
				,mdm.service_line
				,mdm.sub_service_line_id
				,mdm.sub_service_line
				,mdm.opnl_service_id
				,mdm.opnl_service_name
				,mdm.corp_service_line_id
				,mdm.corp_service_line
				,mdm.hs_area_id
				,mdm.hs_area_name
				,mdm.practice_group_id
				,mdm.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,mdm.epic_department_name_external
				,mdm.POD_ID AS pod_id
		        ,mdm.PFA_POD AS pod_name
				,mdm.HUB_ID AS hub_id
		        ,mdm.HUB AS hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
				      ELSE -999
				 END AS sk_Dim_Physcn
				,mdm.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,mdm.LOC_ID AS rev_location_id
				,mdm.REV_LOC_NAME AS rev_location
				   -- SOM
				,physcn.Clrt_Financial_Division AS financial_division_id
				,physcn.Clrt_Financial_Division_Name AS financial_division_name
				,physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id
				,physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name
				,physcn.SOM_Group_ID AS som_group_id
				,physcn.SOM_group AS som_group_name
				,physcn.SOM_department_id AS som_department_id
				,physcn.SOM_department AS	som_department_name
				,physcn.SOM_division_5 AS	som_division_id
				,physcn.SOM_division_name AS som_division_name
				,physcn.som_hs_area_id AS	som_hs_area_id
				,physcn.som_hs_area_name AS som_hs_area_name
				,mdm.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(mdm.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(mdm.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(mdm.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(mdm.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,mdm.FINANCE_COST_CODE
				,dep.Prov_Based_Clinic
				,map.Map_Type
				,resp.Survey_Designator

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt		
		  LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
								, sk_Dim_Clrt_SERsrc
								, sk_Dim_Physcn
								, ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												   WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												   ELSE Atn_End_Dtm
																												 END DESC) AS 'Atn_Seq'
						   FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						   WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1	
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
        LEFT JOIN
		(
			SELECT sk_Dim_Physcn,
					UVaID,
					Service_Line,
					ProviderGroup
			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
			WHERE current_flag = 1
		) AS doc
			    ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = doc.sk_Dim_Physcn		
		--LEFT OUTER JOIN
		--(
		--SELECT
		--	sk_Dim_Physcn,
		--	NPI,
		--	PROV_ID,
		--	Prov_Nme,
		--	Prov_Typ,
		--	Staff_Resource,
		--	Financial_Division,
		--	Financial_Division_Name,
		--	Financial_SubDivision,
		--	Financial_SubDivision_Name,
		--	ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY Load_Dte DESC) AS ser_seq
		--FROM [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc]
		--) prov
		----provider table
		--ON (prov.NPI = resp.NPI)
		--AND prov.ser_seq = 1
  --      LEFT JOIN
		--(
		--	SELECT sk_Dim_Physcn,
		--			UVaID,
		--			Service_Line,
		--			ProviderGroup
		--	FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
		--	WHERE current_flag = 1
		--) AS doc
		--	    ON doc.sk_Dim_Physcn = resp.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN
		( --mdm history to replace vwRef_MDM_Location_Master_EpicSvc and vwRef_MDM_Location_Master
			SELECT DISTINCT
				   hx.max_dt
				  ,rmlmh.EPIC_DEPARTMENT_ID
				  ,rmlmh.EPIC_DEPT_NAME AS epic_department_name
				  ,rmlmh.EPIC_EXT_NAME  AS epic_department_name_external
				  ,rmlmh.SERVICE_LINE_ID
				  ,rmlmh.SERVICE_LINE
				  ,rmlmh.SUB_SERVICE_LINE_ID
				  ,rmlmh.SUB_SERVICE_LINE
				  ,rmlmh.HS_AREA_ID
				  ,rmlmh.HS_AREA_NAME
				  ,rmlmh.PRACTICE_GROUP_ID
				  ,rmlmh.PRACTICE_GROUP_NAME
				  ,rmlmh.OPNL_SERVICE_ID
				  ,rmlmh.OPNL_SERVICE_NAME
				  ,rmlmh.CORP_SERVICE_LINE_ID
				  ,rmlmh.CORP_SERVICE_LINE
				  ,rmlmh.LOC_ID
				  ,rmlmh.REV_LOC_NAME
				  ,rmlmh.POD_ID
				  ,rmlmh.PFA_POD
				  ,rmlmh.PBB_POD
				  ,rmlmh.HUB_ID
				  ,rmlmh.HUB
				  ,rmlmh.BUSINESS_UNIT
				  ,rmlmh.FINANCE_COST_CODE
				  ,rmlmh.UPG_PRACTICE_REGION_ID
				  ,rmlmh.UPG_PRACTICE_REGION_NAME
				  ,rmlmh.UPG_PRACTICE_ID
				  ,rmlmh.UPG_PRACTICE_NAME
				  ,rmlmh.UPG_PRACTICE_FLAG
			FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS rmlmh
				INNER JOIN
				( --hx--most recent batch date per dep id
					SELECT mdmhx.EPIC_DEPARTMENT_ID
						  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
					FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS mdmhx
					GROUP BY mdmhx.EPIC_DEPARTMENT_ID
				)                                                 AS hx
					ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
					   AND rmlmh.BATCH_RUN_DT = hx.max_dt
		)                                                        AS mdm
			ON mdm.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
       -- LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
       --         ON dep.DEPARTMENT_ID = mdm.epic_department_id
       -- LEFT OUTER JOIN
       -- (
       --     SELECT DISTINCT
       --            EPIC_DEPARTMENT_ID,
       --            SERVICE_LINE,
				   --POD_ID,
       --            PFA_POD,
				   --HUB_ID,
       --            HUB,
			    --   BUSINESS_UNIT,
				   --LOC_ID,
				   --REV_LOC_NAME,
				   --UPG_PRACTICE_FLAG,
				   --UPG_PRACTICE_REGION_ID,
				   --UPG_PRACTICE_REGION_NAME,
				   --UPG_PRACTICE_ID,
				   --UPG_PRACTICE_NAME,
				   --FINANCE_COST_CODE
       --     FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
       -- ) AS loc_master
       --         ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                ON map.Deptid = CAST(mdm.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = CASE
					                            WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
												WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
												WHEN resp.sk_Dim_Physcn = -1 THEN -999
												WHEN resp.sk_Dim_Physcn = 0 THEN -999
												ELSE -999
										      END
		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('2671') -- VARNAME: O15 - How well the staff worked together to care for you
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	            --AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
	            AND SUBSTRING(resp.Survey_Designator,1,2) IN ('MD','MT','TP')
	) AS pm
ON rec.day_date=pm.RECDATE
				-- -------------------------------------
				-- SOM Financial Division Subdivision --
				-- -------------------------------------
		--LEFT OUTER JOIN
		--(
		--	SELECT
		--		Epic_Financial_Division_Code,
		--		Epic_Financial_Subdivision_Code,
		--		Department,
		--		Department_ID,
		--		Organization,
		--		Org_Number,
		--		som_group_id,
		--		som_group_name,
		--		som_hs_area_id,
		--		som_hs_area_name
		--	FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
		--	ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(prov.Financial_Division AS INT)
		--		AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(prov.Financial_SubDivision AS INT))

-- 		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
--				AND resp.RECDATE>=@locstartdate
--				AND resp.RECDATE<@locenddate
--				--AND excl.DEPARTMENT_ID IS NULL
--			    AND pat.IS_VALID_PAT_YN = 'Y'
--			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
--	            AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
--	) AS pm
--ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhgr
	ON mdmhgr.EPIC_DEPARTMENT_ID = pm.epic_department_id

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    SELECT organization_id
	              ,organization_name
				  ,service_id
	              ,service_name
				  ,clinical_area_id
				  ,clinical_area_name
				  ,som_division_id
				  ,som_division_name
				  ,som_department_id
				  ,som_department_name
				  ,epic_department_id
	              ,epic_department_name
				  ,weighted_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS StaffWorkedTogetherResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator

	INTO #summary4

	FROM #md

SELECT
    'PXO - Staff Worked Together (Practice)' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(StaffWorkedTogetherResponse) AS StaffWorkedTogetherResponse
	FROM #summary4
	WHERE StaffWorkedTogetherResponse = 1
	AND ambulatory_flag = 1
	--AND (ambulatory_flag = 1 and community_health_flag = 0)
	--AND (ambulatory_flag = 1 or community_health_flag = 1)

	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND SUBSTRING(Survey_Designator,1,2) IN ('MD','MT','TP')

SELECT
    'PXO - Staff Worked Together (Practice)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(event_count) AS StaffWorkedTogetherResponse
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_MDStaffWorkedTogether_Tiles tabrptg
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   event_date>=@locstartdate
            AND event_date<@locenddate
			AND g.ambulatory_flag = 1
			--AND (ambulatory_flag = 1 and community_health_flag = 0)
			--AND (ambulatory_flag = 1 or community_health_flag = 1)

/*
PXO - Staff Worked Together (Adult Inpatient)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_AmbOpt_IN_StaffWorkedTogether.sql
*/

IF OBJECT_ID('tempdb..#in ') IS NOT NULL
DROP TABLE #in

IF OBJECT_ID('tempdb..#summary5 ') IS NOT NULL
DROP TABLE #summary5

--    SELECT DISTINCT
--            CAST('Inpatient-IN' AS VARCHAR(50)) AS event_type
--		   ,CASE WHEN pm.VALUE IS NULL THEN 0
--                 ELSE 1
--            END AS event_count		--count when the overall question has been answered
--		   ,rec.day_date AS event_date		--date survey received
--		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
--           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
--		   ,pm.sk_Dim_PG_Question
--		   ,pm.VARNAME AS PG_Question_Variable
--		   ,pm.QUESTION_TEXT AS PG_Question_Text	
--           ,rec.fmonth_num
--           ,rec.fyear_name
--           ,rec.fyear_num
--           ,pm.MRN_int AS person_id		--patient
--           ,pm.PAT_NAME AS person_name		--patient
--           ,pm.BIRTH_DATE AS person_birth_date--patient
--           ,pm.SEX AS person_gender
--           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
--           ,rec.day_date AS report_date
--           ,pm.service_line_id
--           ,pm.service_line
--           ,pm.sub_service_line_id
--           ,pm.sub_service_line
--           ,pm.opnl_service_id
--           ,pm.opnl_service_name
--           ,pm.hs_area_id
--           ,pm.hs_area_name
--		   ,pm.corp_service_line_id
--		   ,pm.corp_service_line
--		   ,pm.provider_id
--		   ,pm.provider_name
--		   ,pm.practice_group_id
--		   ,pm.practice_group_name
--		   ,pm.sk_Dim_Pt
--           ,pm.sk_Fact_Pt_Acct
--           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
--           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
--		   ,pm.pod_name
--           ,pm.hub_id
--		   ,pm.hub_name
--           ,pm.epic_department_id
--           ,pm.epic_department_name
--           ,pm.epic_department_name_external
--           ,CASE WHEN pm.AGE<18 THEN 1
--                 ELSE 0
--            END AS peds
--           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
--                 ELSE 0
--            END AS transplant
--		   ,pm.sk_Dim_Physcn
--		   ,pm.BUSINESS_UNIT
--	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
--		   ,pm.Staff_Resource
--		   ,pm.som_group_id
--		   ,pm.som_group_name
--		   ,pm.rev_location_id
--		   ,pm.rev_location
--		   ,pm.financial_division_id
--		   ,pm.financial_division_name
--		   ,pm.financial_sub_division_id
--		   ,pm.financial_sub_division_name
--		   ,pm.som_department_id
--		   ,pm.som_department_name
--		   ,pm.som_division_id -- int
--		   ,pm.som_division_name
--		   ,pm.som_hs_area_id
--		   ,pm.som_hs_area_name
--		   ,pm.upg_practice_flag
--		   ,pm.upg_practice_region_id
--		   ,pm.upg_practice_region_name
--		   ,pm.upg_practice_id
--		   ,pm.upg_practice_name
--		   ,pm.F2F_Flag
--		   ,pm.ENC_TYPE_C
--		   ,pm.ENC_TYPE_TITLE
--		   ,pm.Lip_Flag
--		   ,pm.FINANCE_COST_CODE
--		   ,pm.Prov_Based_Clinic
--		   ,pm.Map_Type
--		   ,CAST(CASE pm.VALUE
--                   WHEN 1 THEN 0
--                   WHEN 2 THEN 25
--                   WHEN 3 THEN 50
--                   WHEN 4 THEN 75
--                   WHEN 5 THEN 100
--                 END AS DECIMAL(10, 2)) AS weighted_score -- DECIMAL(10,2)

--		   ,o.organization_id
--		   ,o.organization_name
--		   ,s.service_id
--		   ,s.service_name
--		   ,c.clinical_area_id
--		   ,c.clinical_area_name
--		   ,g.ambulatory_flag
--		   ,g.community_health_flag
--		   ,pm.Survey_Designator
--	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag -- INTEGER

--    INTO #in

--    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
--    LEFT OUTER JOIN
--	(
--		SELECT DISTINCT
--				 resp.SURVEY_ID
--				,resp.RECDATE
--				,CAST(VALUE AS VARCHAR(500)) AS VALUE
--				,Resp_Age.AGE AS AGE
--				,qstn.sk_Dim_PG_Question
--				,resp.sk_Dim_Clrt_DEPt
--                   -- MDM
--				,mdm.service_line_id
--				,mdm.service_line
--				,mdm.sub_service_line_id
--				,mdm.sub_service_line
--				,mdm.opnl_service_id
--				,mdm.opnl_service_name
--				,mdm.corp_service_line_id
--				,mdm.corp_service_line
--				,mdm.hs_area_id
--				,mdm.hs_area_name
--				,mdm.practice_group_id
--				,mdm.practice_group_name
--				,dep.DEPARTMENT_ID AS epic_department_id
--				,mdm.epic_department_name
--				,mdm.epic_department_name_external
--				,loc_master.POD_ID AS pod_id
--		        ,loc_master.PFA_POD AS pod_name
--				,loc_master.HUB_ID AS hub_id
--		        ,loc_master.HUB AS hub_name
--				,fpa.MRN_int
--				,fpa.sk_Dim_Pt
--				,qstn.VARNAME
--				,qstn.QUESTION_TEXT
--				,fpa.sk_Fact_Pt_Acct
--				,prov.PROV_ID AS provider_id
--				,prov.Prov_Nme AS provider_name
--				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
--				,pat.BirthDate AS BIRTH_DATE
--				,pat.SEX
--				,resp.Load_Dtm
--				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
--				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
--				      ELSE -999
--				 END AS sk_Dim_Physcn
--				,loc_master.BUSINESS_UNIT
--				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
--				,prov.Staff_Resource
--				,loc_master.LOC_ID AS rev_location_id
--				,loc_master.REV_LOC_NAME AS rev_location
--				   -- SOM
--				,physcn.Clrt_Financial_Division AS financial_division_id
--				,physcn.Clrt_Financial_Division_Name AS financial_division_name
--				,physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id
--				,physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name
--				,physcn.SOM_Group_ID AS som_group_id
--				,physcn.SOM_group AS som_group_name
--				,physcn.SOM_department_id AS som_department_id
--				,physcn.SOM_department AS	som_department_name
--				,physcn.SOM_division_5 AS	som_division_id
--				,physcn.SOM_division_name AS som_division_name
--				,physcn.som_hs_area_id AS	som_hs_area_id
--				,physcn.som_hs_area_name AS som_hs_area_name
--				,loc_master.UPG_PRACTICE_FLAG AS upg_practice_flag
--				,CAST(loc_master.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
--				,CAST(loc_master.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
--				,CAST(loc_master.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
--				,CAST(loc_master.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
--				,appts.F2F_Flag
--				,appts.ENC_TYPE_C
--				,appts.ENC_TYPE_TITLE
--	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
--	            ,loc_master.FINANCE_COST_CODE
--				,dep.Prov_Based_Clinic
--				,map.Map_Type
--				,resp.Survey_Designator

--		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
--		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
--				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
--		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
--				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
--		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
--				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt		
--		  LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
--								, sk_Dim_Clrt_SERsrc
--								, sk_Dim_Physcn
--								, ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
--																												   WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
--																												   ELSE Atn_End_Dtm
--																												 END DESC) AS 'Atn_Seq'
--						   FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
--						   WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
--			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1	
--		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
--				--provider table
--				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--						WHEN resp.sk_Dim_Physcn = -1 THEN -999
--						WHEN resp.sk_Dim_Physcn = 0 THEN -999
--						ELSE -999
--				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
--        LEFT JOIN
--		(
--			SELECT sk_Dim_Physcn,
--					UVaID,
--					Service_Line,
--					ProviderGroup
--			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
--			WHERE current_flag = 1
--		) AS doc
--			    ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--						WHEN resp.sk_Dim_Physcn = -1 THEN -999
--						WHEN resp.sk_Dim_Physcn = 0 THEN -999
--						ELSE -999
--				   END = doc.sk_Dim_Physcn		
--		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
--		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
--		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
--		        ON ddte.date_key = enc.sk_Cont_Dte
--	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
--			    ON prov.PROV_ID = ptot.PROV_ID
--				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
--		LEFT OUTER JOIN
--			(
--				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
--				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
--				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '4' -- Age question for Inpatient
--				GROUP BY SURVEY_ID
--			) Resp_Age
--				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
--		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
--				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
--        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
--                ON dep.DEPARTMENT_ID = mdm.epic_department_id
--        LEFT OUTER JOIN
--        (
--            SELECT DISTINCT
--                   EPIC_DEPARTMENT_ID,
--                   SERVICE_LINE,
--				   POD_ID,
--                   PFA_POD,
--				   HUB_ID,
--                   HUB,
--			       BUSINESS_UNIT,
--				   LOC_ID,
--				   REV_LOC_NAME,
--				   UPG_PRACTICE_FLAG,
--				   UPG_PRACTICE_REGION_ID,
--				   UPG_PRACTICE_REGION_NAME,
--				   UPG_PRACTICE_ID,
--				   UPG_PRACTICE_NAME,
--				   FINANCE_COST_CODE
--            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--        ) AS loc_master
--                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
--        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
--                ON map.Deptid = CAST(loc_master.FINANCE_COST_CODE AS INTEGER)
--		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
--		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
--		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
--		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id

--                -- -------------------------------------
--                -- SOM Hierarchy--
--                -- -------------------------------------
--				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
--				    ON physcn.sk_Dim_Physcn = CASE
--					                            WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--												WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--												WHEN resp.sk_Dim_Physcn = -1 THEN -999
--												WHEN resp.sk_Dim_Physcn = 0 THEN -999
--												ELSE -999
--										      END
--		WHERE   resp.Svc_Cde='IN' AND resp.sk_Dim_PG_Question IN ('2733') -- VARNAME: O2PR - How well staff worked together to care for you
--				AND resp.RECDATE>=@locstartdate
--				AND resp.RECDATE<@locenddate
--				--AND excl.DEPARTMENT_ID IS NULL
--			    AND pat.IS_VALID_PAT_YN = 'Y'
--			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
--	) AS pm
--ON rec.day_date=pm.RECDATE

--		-- -------------------------------------
--		-- Identify transplant encounter
--		-- -------------------------------------
--    LEFT OUTER JOIN (
--                     SELECT fpec.PAT_ENC_CSN_ID
--                           ,txsurg.day_date AS transplant_surgery_dt
--                           ,fpec.Adm_Dtm
--                           ,fpec.sk_Fact_Pt_Enc_Clrt
--                           ,fpec.sk_Fact_Pt_Acct
--                           ,fpec.sk_Dim_Clrt_Pt
--                           ,fpec.sk_Dim_Pt
--                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
--                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
--                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
--                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
--                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
--                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
--                            AND txsurg.day_date<>'1900-01-01 00:00:00'
--                    ) AS tx
--            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
--		-- ------------------------------------

--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
--	ON g.epic_department_id = pm.epic_department_id
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
--	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
--	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
--	ON o.organization_id = s.organization_id
   
--		-- ------------------------------------

--	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhgr
--	ON mdmhgr.EPIC_DEPARTMENT_ID = pm.epic_department_id

--    WHERE   rec.day_date>=@locstartdate
--            AND rec.day_date<@locenddate

--    SELECT DISTINCT
--            CAST('Inpatient-IN' AS VARCHAR(50)) AS event_type
--		   ,CASE WHEN pm.VALUE IS NULL THEN 0
--                 ELSE 1
--            END AS event_count		--count when the overall question has been answered
--		   ,rec.day_date AS event_date		--date survey received
--		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
--           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
--		   ,pm.sk_Dim_PG_Question
--		   ,pm.VARNAME AS PG_Question_Variable
--		   ,pm.QUESTION_TEXT AS PG_Question_Text	
--           ,rec.fmonth_num
--           ,rec.fyear_name
--           ,rec.fyear_num
--           ,pm.MRN_int AS person_id		--patient
--           ,pm.PAT_NAME AS person_name		--patient
--           ,pm.BIRTH_DATE AS person_birth_date--patient
--           ,pm.SEX AS person_gender
--           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
--           ,rec.day_date AS report_date
--           ,pm.service_line_id
--           ,pm.service_line
--           ,pm.sub_service_line_id
--           ,pm.sub_service_line
--           ,pm.opnl_service_id
--           ,pm.opnl_service_name
--           ,pm.hs_area_id
--           ,pm.hs_area_name
--		   ,pm.corp_service_line_id
--		   ,pm.corp_service_line
--		   ,pm.provider_id
--		   ,pm.provider_name
--		   ,CASE WHEN dc_phys.type_of_HSF_contract = 'UVACHMG Employed' THEN 1 ELSE 0 END  CH_Hosp_Based_YN
--		   ,pm.practice_group_id
--		   ,pm.practice_group_name
--		   ,pm.sk_Dim_Pt
--           ,pm.sk_Fact_Pt_Acct
--           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
--           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
--		   ,pm.pod_name
--           ,pm.hub_id
--		   ,pm.hub_name
--           ,pm.epic_department_id
--           ,pm.epic_department_name
--           ,pm.epic_department_name_external
--           ,CASE WHEN pm.AGE<18 THEN 1
--                 ELSE 0
--            END AS peds
--           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
--                 ELSE 0
--            END AS transplant
--		   ,pm.sk_Dim_Physcn
--		   ,pm.BUSINESS_UNIT
--	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
--		   ,pm.Staff_Resource
--		   ,pm.som_group_id
--		   ,pm.som_group_name
--		   ,pm.rev_location_id
--		   ,pm.rev_location
--		   ,pm.financial_division_id
--		   ,pm.financial_division_name
--		   ,pm.financial_sub_division_id
--		   ,pm.financial_sub_division_name
--		   ,pm.som_department_id
--		   ,pm.som_department_name
--		   ,pm.som_division_id -- int
--		   ,pm.som_division_name
--		   ,pm.som_hs_area_id
--		   ,pm.som_hs_area_name
--		   ,pm.upg_practice_flag
--		   ,pm.upg_practice_region_id
--		   ,pm.upg_practice_region_name
--		   ,pm.upg_practice_id
--		   ,pm.upg_practice_name
--		   ,pm.F2F_Flag
--		   ,pm.ENC_TYPE_C
--		   ,pm.ENC_TYPE_TITLE
--		   ,pm.Lip_Flag
--		   ,pm.FINANCE_COST_CODE
--		   ,pm.Prov_Based_Clinic
--		   ,pm.Map_Type
--		   ,CAST(CASE pm.VALUE
--                   WHEN 1 THEN 0
--                   WHEN 2 THEN 25
--                   WHEN 3 THEN 50
--                   WHEN 4 THEN 75
--                   WHEN 5 THEN 100
--                 END AS DECIMAL(10, 2)) AS weighted_score
--		   ,o.organization_id
--		   ,o.organization_name
--		   ,s.service_id
--		   ,s.service_name
--		   ,c.clinical_area_id
--		   ,c.clinical_area_name
--		   ,g.ambulatory_flag
--		   ,g.community_health_flag
--		   ,pm.Survey_Designator
--	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag

--    INTO #in

--    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
--    LEFT OUTER JOIN
--	(
--		SELECT DISTINCT
--				 resp.SURVEY_ID
--				,resp.RECDATE
--				,CAST(VALUE AS VARCHAR(500)) AS VALUE
--				,Resp_Age.AGE AS AGE
--				,qstn.sk_Dim_PG_Question
--				,resp.sk_Dim_Clrt_DEPt
--                   -- MDM
--				,mdmhst.service_line_id
--				,mdmhst.service_line
--				,mdmhst.sub_service_line_id
--				,mdmhst.sub_service_line
--				,mdmhst.opnl_service_id
--				,mdmhst.opnl_service_name
--				,mdmhst.corp_service_line_id
--				,mdmhst.corp_service_line
--				,mdmhst.hs_area_id
--				,mdmhst.hs_area_name
--				,mdmhst.practice_group_id
--				,mdmhst.practice_group_name
--				,dep.DEPARTMENT_ID AS epic_department_id
--				,mdmhst.epic_department_name
--				,mdmhst.epic_department_name_external
--				,mdmhst.POD_ID AS pod_id
--		        ,mdmhst.PFA_POD AS pod_name
--				,mdmhst.HUB_ID AS hub_id
--		        ,mdmhst.HUB AS hub_name
--				,fpa.MRN_int
--				,fpa.sk_Dim_Pt
--				,qstn.VARNAME
--				,qstn.QUESTION_TEXT
--				,fpa.sk_Fact_Pt_Acct
--				,prov.PROV_ID AS provider_id
--				,prov.Prov_Nme AS provider_name
--				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
--				,pat.BirthDate AS BIRTH_DATE
--				,pat.SEX
--				,resp.Load_Dtm
--				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
--				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
--				      ELSE -999
--				 END AS sk_Dim_Physcn
--				,mdmhst.BUSINESS_UNIT
--				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
--				,prov.Staff_Resource
--				,mdmhst.LOC_ID AS rev_location_id
--				,mdmhst.REV_LOC_NAME AS rev_location
--				   -- SOM
--				,physcn.Clrt_Financial_Division AS financial_division_id
--				,physcn.Clrt_Financial_Division_Name AS financial_division_name
--				,physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id
--				,physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name
--				,physcn.SOM_Group_ID AS som_group_id
--				,physcn.SOM_group AS som_group_name
--				,physcn.SOM_department_id AS som_department_id
--				,physcn.SOM_department AS	som_department_name
--				,physcn.SOM_division_5 AS	som_division_id
--				,physcn.SOM_division_name AS som_division_name
--				,physcn.som_hs_area_id AS	som_hs_area_id
--				,physcn.som_hs_area_name AS som_hs_area_name
--				,upg.UPG_PRACTICE_FLAG AS upg_practice_flag
--				,CAST(upg.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
--				,CAST(upg.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
--				,CAST(upg.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
--				,CAST(upg.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
--				,appts.F2F_Flag
--				,appts.ENC_TYPE_C
--				,appts.ENC_TYPE_TITLE
--	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
--	            ,mdmhst.FINANCE_COST_CODE
--				,dep.Prov_Based_Clinic
--				,map.Map_Type
--				,resp.Survey_Designator

--		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
--		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
--				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
--		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
--				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
--		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
--				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt		
--		  LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
--								, sk_Dim_Clrt_SERsrc
--								, sk_Dim_Physcn
--								, ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
--																												   WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
--																												   ELSE Atn_End_Dtm
--																												 END DESC) AS 'Atn_Seq'
--						   FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
--						   WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
--			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1		
--		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
--				--provider table
--				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--						WHEN resp.sk_Dim_Physcn = -1 THEN -999
--						WHEN resp.sk_Dim_Physcn = 0 THEN -999
--						ELSE -999
--				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
--        LEFT JOIN
--		(
--			SELECT sk_Dim_Physcn,
--					UVaID,
--					Service_Line,
--					ProviderGroup
--			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
--			WHERE current_flag = 1
--		) AS doc
--			    ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--						WHEN resp.sk_Dim_Physcn = -1 THEN -999
--						WHEN resp.sk_Dim_Physcn = 0 THEN -999
--						ELSE -999
--				   END = doc.sk_Dim_Physcn
--		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
--		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
--		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
--		        ON ddte.date_key = enc.sk_Cont_Dte
--	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
--			    ON prov.PROV_ID = ptot.PROV_ID
--				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
--		LEFT OUTER JOIN
--			(
--				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
--				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
--				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '4' -- Age question for Inpatient
--				GROUP BY SURVEY_ID
--			) Resp_Age
--				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
--		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
--				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
--		LEFT OUTER JOIN
--			(
--				SELECT
--					history.MDM_BATCH_ID,
--					history.EPIC_DEPARTMENT_ID,
--					history.EPIC_DEPT_NAME AS epic_department_name,
--					history.EPIC_EXT_NAME AS epic_department_name_external,
--					history.SERVICE_LINE_ID,
--					history.SERVICE_LINE,
--					history.SUB_SERVICE_LINE_ID,
--					history.SUB_SERVICE_LINE,
--					history.LOC_ID,
--					history.REV_LOC_NAME,
--					history.HS_AREA_ID,
--					history.HS_AREA_NAME,
--					history.OPNL_SERVICE_ID,
--					history.OPNL_SERVICE_NAME,
--					history.PRESSGANEY_NAME,
--					history.FINANCE_COST_CODE,
--					history.CORP_SERVICE_LINE_ID,
--					history.CORP_SERVICE_LINE,
--					history.PRACTICE_GROUP_ID,
--					history.PRACTICE_GROUP_NAME,
--					history.POD_ID,
--					history.PFA_POD,
--					history.HUB_ID,
--					history.	HUB,
--					history.BUSINESS_UNIT
--				FROM
--				(
--					SELECT
--						MDM_BATCH_ID,
--						EPIC_DEPARTMENT_ID,
--						EPIC_DEPT_NAME,
--						EPIC_EXT_NAME,
--						SERVICE_LINE_ID,
--						SERVICE_LINE,
--						SUB_SERVICE_LINE_ID,
--						SUB_SERVICE_LINE,
--						LOC_ID,
--						REV_LOC_NAME,
--						HS_AREA_ID,
--						HS_AREA_NAME,
--						OPNL_SERVICE_ID,
--						OPNL_SERVICE_NAME,
--						PRESSGANEY_NAME,
--						FINANCE_COST_CODE,
--						CORP_SERVICE_LINE_ID,
--						CORP_SERVICE_LINE,
--						PRACTICE_GROUP_ID,
--						PRACTICE_GROUP_NAME,
--						POD_ID,
--						PFA_POD,
--						HUB_ID,
--						HUB,
--						BUSINESS_UNIT,
--						ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
--					FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
--				) history
--				WHERE history.seq = 1
--			) mdmhst
--			ON mdmhst.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
--        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
--                ON map.Deptid = CAST(mdmhst.FINANCE_COST_CODE AS INTEGER)
--		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
--		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
--		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
--		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id

--                -- -------------------------------------
--                -- SOM Hierarchy--
--                -- -------------------------------------
--				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
--				    ON physcn.sk_Dim_Physcn = CASE
--					                            WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
--												WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
--												WHEN resp.sk_Dim_Physcn = -1 THEN -999
--												WHEN resp.sk_Dim_Physcn = 0 THEN -999
--												ELSE -999
--										      END

--                -- -------------------------------------
--                -- Department UPG Practice--
--                -- -------------------------------------
--				LEFT OUTER JOIN Rptg.vwClarity_DEP_UPG upg
--				    ON upg.DEPARTMENT_ID = dep.DEPARTMENT_ID

--		WHERE   resp.Svc_Cde='IN'
--				AND ((resp.sk_Dim_PG_Question = '2733' AND resp.RECDATE <= '12/31/2024') -- VARNAME: O2PR - How well staff worked together to care for you
--							OR
--						  (resp.sk_Dim_PG_Question IN ('2733','1046') AND resp.RECDATE > '12/31/2024') -- VARNAME: O2PR, O2 - How well staff worked together to care for you
--						)
--				AND resp.RECDATE>=@locstartdate
--				AND resp.RECDATE<@locenddate
--				--AND excl.DEPARTMENT_ID IS NULL
--			    AND pat.IS_VALID_PAT_YN = 'Y'
--			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
--	) AS pm
--ON rec.day_date=pm.RECDATE

--		-- -------------------------------------
--		-- Identify transplant encounter
--		-- -------------------------------------
    
--	 LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn           AS dc_phys ON pm.sk_Dim_Physcn = dc_phys.sk_Dim_Physcn
--	LEFT OUTER JOIN (
--                     SELECT fpec.PAT_ENC_CSN_ID
--                           ,txsurg.day_date AS transplant_surgery_dt
--                           ,fpec.Adm_Dtm
--                           ,fpec.sk_Fact_Pt_Enc_Clrt
--                           ,fpec.sk_Fact_Pt_Acct
--                           ,fpec.sk_Dim_Clrt_Pt
--                           ,fpec.sk_Dim_Pt
--                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
--                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
--                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
--                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
--                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
--                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
--                            AND txsurg.day_date<>'1900-01-01 00:00:00'
--                    ) AS tx
--            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
--		-- ------------------------------------

--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
--	ON g.epic_department_id = pm.epic_department_id
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
--	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
--	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
--	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
--	ON o.organization_id = s.organization_id
   
--		-- ------------------------------------

--    WHERE   rec.day_date>=@locstartdate
--            AND rec.day_date<@locenddate

    SELECT DISTINCT
            CAST('HCAHPS' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the HCAHPS question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,CASE WHEN dc_phys.type_of_HSF_contract = 'UVACHMG Employed' THEN 1 ELSE 0 END  CH_Hosp_Based_YN
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,pm.sk_Fact_Pt_Acct
           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
		   ,pm.pod_name
           ,pm.hub_id
		   ,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
		   ,pm.BUSINESS_UNIT
	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
		   ,pm.Staff_Resource
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id -- int
		   ,pm.som_division_name
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   ,pm.upg_practice_flag
		   ,pm.upg_practice_region_id
		   ,pm.upg_practice_region_name
		   ,pm.upg_practice_id
		   ,pm.upg_practice_name
		   ,pm.F2F_Flag
		   ,pm.ENC_TYPE_C
		   ,pm.ENC_TYPE_TITLE
		   ,pm.Lip_Flag
		   ,pm.FINANCE_COST_CODE
		   ,pm.Prov_Based_Clinic
		   ,pm.Map_Type
		   ,CAST(NULL AS DECIMAL(10, 2)) AS weighted_score
		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.ambulatory_flag
		   ,g.community_health_flag
		   ,pm.Survey_Designator
	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag

    INTO #in

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
                   -- MDM
				,mdmhst.service_line_id
				,mdmhst.service_line
				,mdmhst.sub_service_line_id
				,mdmhst.sub_service_line
				,mdmhst.opnl_service_id
				,mdmhst.opnl_service_name
				,mdmhst.corp_service_line_id
				,mdmhst.corp_service_line
				,mdmhst.hs_area_id
				,mdmhst.hs_area_name
				,mdmhst.practice_group_id
				,mdmhst.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdmhst.epic_department_name
				,mdmhst.epic_department_name_external
				,mdmhst.POD_ID AS pod_id
		        ,mdmhst.PFA_POD AS pod_name
				,mdmhst.HUB_ID AS hub_id
		        ,mdmhst.HUB AS hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
				      ELSE -999
				 END AS sk_Dim_Physcn
				,mdmhst.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,mdmhst.LOC_ID AS rev_location_id
				,mdmhst.REV_LOC_NAME AS rev_location
				   -- SOM
				,physcn.Clrt_Financial_Division AS financial_division_id
				,physcn.Clrt_Financial_Division_Name AS financial_division_name
				,physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id
				,physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name
				,physcn.SOM_Group_ID AS som_group_id
				,physcn.SOM_group AS som_group_name
				,physcn.SOM_department_id AS som_department_id
				,physcn.SOM_department AS	som_department_name
				,physcn.SOM_division_5 AS	som_division_id
				,physcn.SOM_division_name AS som_division_name
				,physcn.som_hs_area_id AS	som_hs_area_id
				,physcn.som_hs_area_name AS som_hs_area_name
				,upg.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(upg.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(upg.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(upg.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(upg.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,mdmhst.FINANCE_COST_CODE
				,dep.Prov_Based_Clinic
				,map.Map_Type
				,resp.Survey_Designator

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt		
		  LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
								, sk_Dim_Clrt_SERsrc
								, sk_Dim_Physcn
								, ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												   WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												   ELSE Atn_End_Dtm
																												 END DESC) AS 'Atn_Seq'
						   FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						   WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1		
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
        LEFT JOIN
		(
			SELECT sk_Dim_Physcn,
					UVaID,
					Service_Line,
					ProviderGroup
			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
			WHERE current_flag = 1
		) AS doc
			    ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = doc.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '4' -- Age question for Inpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN
			(
				SELECT
					history.MDM_BATCH_ID,
					history.EPIC_DEPARTMENT_ID,
					history.EPIC_DEPT_NAME AS epic_department_name,
					history.EPIC_EXT_NAME AS epic_department_name_external,
					history.SERVICE_LINE_ID,
					history.SERVICE_LINE,
					history.SUB_SERVICE_LINE_ID,
					history.SUB_SERVICE_LINE,
					history.LOC_ID,
					history.REV_LOC_NAME,
					history.HS_AREA_ID,
					history.HS_AREA_NAME,
					history.OPNL_SERVICE_ID,
					history.OPNL_SERVICE_NAME,
					history.PRESSGANEY_NAME,
					history.FINANCE_COST_CODE,
					history.CORP_SERVICE_LINE_ID,
					history.CORP_SERVICE_LINE,
					history.PRACTICE_GROUP_ID,
					history.PRACTICE_GROUP_NAME,
					history.POD_ID,
					history.PFA_POD,
					history.HUB_ID,
					history.	HUB,
					history.BUSINESS_UNIT
				FROM
				(
					SELECT
						MDM_BATCH_ID,
						EPIC_DEPARTMENT_ID,
						EPIC_DEPT_NAME,
						EPIC_EXT_NAME,
						SERVICE_LINE_ID,
						SERVICE_LINE,
						SUB_SERVICE_LINE_ID,
						SUB_SERVICE_LINE,
						LOC_ID,
						REV_LOC_NAME,
						HS_AREA_ID,
						HS_AREA_NAME,
						OPNL_SERVICE_ID,
						OPNL_SERVICE_NAME,
						PRESSGANEY_NAME,
						FINANCE_COST_CODE,
						CORP_SERVICE_LINE_ID,
						CORP_SERVICE_LINE,
						PRACTICE_GROUP_ID,
						PRACTICE_GROUP_NAME,
						POD_ID,
						PFA_POD,
						HUB_ID,
						HUB,
						BUSINESS_UNIT,
						ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
					FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
				) history
				WHERE history.seq = 1
			) mdmhst
			ON mdmhst.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                ON map.Deptid = CAST(mdmhst.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = CASE
					                            WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
												WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
												WHEN resp.sk_Dim_Physcn = -1 THEN -999
												WHEN resp.sk_Dim_Physcn = 0 THEN -999
												ELSE -999
										      END

                -- -------------------------------------
                -- Department UPG Practice--
                -- -------------------------------------
				LEFT OUTER JOIN Rptg.vwClarity_DEP_UPG upg
				    ON upg.DEPARTMENT_ID = dep.DEPARTMENT_ID

		WHERE   resp.Svc_Cde='IN'
				AND (resp.sk_Dim_PG_Question = '3834' AND resp.RECDATE > '12/31/2024') -- VARNAME: CMS_47 - During this hospital stay, how often did doctors, nurses and other hospital staff work well together to care for you?
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    
	 LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn           AS dc_phys ON pm.sk_Dim_Physcn = dc_phys.sk_Dim_Physcn
	LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
   
		-- ------------------------------------

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    SELECT organization_id
	              ,organization_name
				  ,service_id
	              ,service_name
				  ,clinical_area_id
				  ,clinical_area_name
				  ,som_division_id
				  ,som_division_name
				  ,som_department_id
				  ,som_department_name
				  ,epic_department_id
	              ,epic_department_name
				  ,event_category
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS StaffWorkedTogetherResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator

	INTO #summary5

	FROM #in

SELECT
    'PXO - Staff Worked Together (Adult Inpatient)' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(CASE WHEN [summary5].event_category IN ('Always') THEN 1 ELSE 0 END) AS Numerator,
	SUM(StaffWorkedTogetherResponse) AS StaffWorkedTogetherResponse,
	CAST(
	   CAST(SUM(CASE WHEN [summary5].event_category IN ('Always') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN [summary5].StaffWorkedTogetherResponse = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #summary5
WHERE StaffWorkedTogetherResponse = 1
AND community_health_flag = 0
) [summary5]

/*

SELECT
       'PXO - Overall Rating HCAHPS' AS Metric
	 , 'Tab Table' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 , CAST(
	   CAST(SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #TabRptg3
WHERE event_count = 1
) TabRptg*/

SELECT
    'PXO - Staff Worked Together (Adult Inpatient)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(CASE WHEN tabrptg.event_category IN ('Always') THEN 1 ELSE 0 END) AS Numerator,
	SUM(event_count) AS StaffWorkedTogetherResponse,
	CAST(
	   CAST(SUM(CASE WHEN tabrptg.event_category IN ('Always') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN tabrptg.event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_HCAHPSStaffWorkTogether_Tiles tabrptg
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   tabrptg.event_date>=@locstartdate
            AND tabrptg.event_date<@locenddate
	        AND g.community_health_flag = 0

/*
PXO - Staff Worked Together (Emergency)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_AmbOpt_ER_StaffWorkedTogether.sql
*/

IF OBJECT_ID('tempdb..#er ') IS NOT NULL
DROP TABLE #er

IF OBJECT_ID('tempdb..#summary6 ') IS NOT NULL
DROP TABLE #summary6

    SELECT DISTINCT
            CAST('Emergency' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,pm.sk_Fact_Pt_Acct
           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
		   ,pm.pod_name
           ,pm.hub_id
		   ,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
		   ,pm.BUSINESS_UNIT
	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
		   ,pm.Staff_Resource
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id -- int
		   ,pm.som_division_name
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   ,pm.upg_practice_flag
		   ,pm.upg_practice_region_id
		   ,pm.upg_practice_region_name
		   ,pm.upg_practice_id
		   ,pm.upg_practice_name
		   ,pm.F2F_Flag
		   ,pm.ENC_TYPE_C
		   ,pm.ENC_TYPE_TITLE
		   ,pm.Lip_Flag
		   ,pm.FINANCE_COST_CODE
		   ,pm.Prov_Based_Clinic
		   ,pm.Map_Type
		   ,CAST(CASE pm.VALUE
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS weighted_score -- DECIMAL(10,2)

		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.ambulatory_flag
		   ,g.community_health_flag
		   ,pm.Survey_Designator
	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag -- INTEGER

    INTO #er

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
                   -- MDM
				,mdm.service_line_id
				,mdm.service_line
				,mdm.sub_service_line_id
				,mdm.sub_service_line
				,mdm.opnl_service_id
				,mdm.opnl_service_name
				,mdm.corp_service_line_id
				,mdm.corp_service_line
				,mdm.hs_area_id
				,mdm.hs_area_name
				,mdm.practice_group_id
				,mdm.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,mdm.epic_department_name_external
				,mdm.POD_ID AS pod_id
		        ,mdm.PFA_POD AS pod_name
				,mdm.HUB_ID AS hub_id
		        ,mdm.HUB AS hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
				      ELSE -999
				 END AS sk_Dim_Physcn
				,mdm.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,mdm.LOC_ID AS rev_location_id
				,mdm.REV_LOC_NAME AS rev_location
				   -- SOM
				,physcn.Clrt_Financial_Division AS financial_division_id
				,physcn.Clrt_Financial_Division_Name AS financial_division_name
				,physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id
				,physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name
				,physcn.SOM_Group_ID AS som_group_id
				,physcn.SOM_group AS som_group_name
				,physcn.SOM_department_id AS som_department_id
				,physcn.SOM_department AS	som_department_name
				,physcn.SOM_division_5 AS	som_division_id
				,physcn.SOM_division_name AS som_division_name
				,physcn.som_hs_area_id AS	som_hs_area_id
				,physcn.som_hs_area_name AS som_hs_area_name
				,mdm.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(mdm.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(mdm.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(mdm.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(mdm.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,mdm.FINANCE_COST_CODE
				,dep.Prov_Based_Clinic
				,map.Map_Type
				,resp.Survey_Designator

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt		
		  LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
								, sk_Dim_Clrt_SERsrc
								, sk_Dim_Physcn
								, ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												   WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												   ELSE Atn_End_Dtm
																												 END DESC) AS 'Atn_Seq'
						   FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						   WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1	
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
        LEFT JOIN
		(
			SELECT sk_Dim_Physcn,
					UVaID,
					Service_Line,
					ProviderGroup
			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
			WHERE current_flag = 1
		) AS doc
			    ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = doc.sk_Dim_Physcn		
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question ='326' -- Age question for Emergency
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN
		( --mdm history to replace vwRef_MDM_Location_Master_EpicSvc and vwRef_MDM_Location_Master
			SELECT DISTINCT
				   hx.max_dt
				  ,rmlmh.EPIC_DEPARTMENT_ID
				  ,rmlmh.EPIC_DEPT_NAME AS epic_department_name
				  ,rmlmh.EPIC_EXT_NAME  AS epic_department_name_external
				  ,rmlmh.SERVICE_LINE_ID
				  ,rmlmh.SERVICE_LINE
				  ,rmlmh.SUB_SERVICE_LINE_ID
				  ,rmlmh.SUB_SERVICE_LINE
				  ,rmlmh.HS_AREA_ID
				  ,rmlmh.HS_AREA_NAME
				  ,rmlmh.PRACTICE_GROUP_ID
				  ,rmlmh.PRACTICE_GROUP_NAME
				  ,rmlmh.OPNL_SERVICE_ID
				  ,rmlmh.OPNL_SERVICE_NAME
				  ,rmlmh.CORP_SERVICE_LINE_ID
				  ,rmlmh.CORP_SERVICE_LINE
				  ,rmlmh.LOC_ID
				  ,rmlmh.REV_LOC_NAME
				  ,rmlmh.POD_ID
				  ,rmlmh.PFA_POD
				  ,rmlmh.PBB_POD
				  ,rmlmh.HUB_ID
				  ,rmlmh.HUB
				  ,rmlmh.BUSINESS_UNIT
				  ,rmlmh.FINANCE_COST_CODE
				  ,rmlmh.UPG_PRACTICE_REGION_ID
				  ,rmlmh.UPG_PRACTICE_REGION_NAME
				  ,rmlmh.UPG_PRACTICE_ID
				  ,rmlmh.UPG_PRACTICE_NAME
				  ,rmlmh.UPG_PRACTICE_FLAG
			FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS rmlmh
				INNER JOIN
				( --hx--most recent batch date per dep id
					SELECT mdmhx.EPIC_DEPARTMENT_ID
						  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
					FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS mdmhx
					GROUP BY mdmhx.EPIC_DEPARTMENT_ID
				)                                                 AS hx
					ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
					   AND rmlmh.BATCH_RUN_DT = hx.max_dt
		)                                                        AS mdm
			ON mdm.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
       -- LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
       --         ON dep.DEPARTMENT_ID = mdm.epic_department_id
       -- LEFT OUTER JOIN
       -- (
       --     SELECT DISTINCT
       --            EPIC_DEPARTMENT_ID,
       --            SERVICE_LINE,
				   --POD_ID,
       --            PFA_POD,
				   --HUB_ID,
       --            HUB,
			    --   BUSINESS_UNIT,
				   --LOC_ID,
				   --REV_LOC_NAME,
				   --UPG_PRACTICE_FLAG,
				   --UPG_PRACTICE_REGION_ID,
				   --UPG_PRACTICE_REGION_NAME,
				   --UPG_PRACTICE_ID,
				   --UPG_PRACTICE_NAME,
				   --FINANCE_COST_CODE
       --     FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
       -- ) AS loc_master
       --         ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                ON map.Deptid = CAST(mdm.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = CASE
					                            WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
												WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
												WHEN resp.sk_Dim_Physcn = -1 THEN -999
												WHEN resp.sk_Dim_Physcn = 0 THEN -999
												ELSE -999
										      END
		WHERE   resp.Svc_Cde='ER' AND resp.sk_Dim_PG_Question IN ('2720') -- VARNAME: F81 - How well the staff worked together to care for you
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhgr
	ON mdmhgr.EPIC_DEPARTMENT_ID = pm.epic_department_id

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    SELECT organization_id
	              ,organization_name
				  ,service_id
	              ,service_name
				  ,clinical_area_id
				  ,clinical_area_name
				  ,som_division_id
				  ,som_division_name
				  ,som_department_id
				  ,som_department_name
				  ,epic_department_id
	              ,epic_department_name
				  ,weighted_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS StaffWorkedTogetherResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator

	INTO #summary6

	FROM #er

SELECT
    'PXO - Staff Worked Together (Emergency)' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(StaffWorkedTogetherResponse) AS StaffWorkedTogetherResponse
	FROM #summary6
	WHERE StaffWorkedTogetherResponse = 1
	AND community_health_flag = 0

SELECT
    'PXO - Staff Worked Together (Emergency)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(event_count) AS StaffWorkedTogetherResponse
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_ERStaffWorkedTogether_Tiles tabrptg
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   event_date>=@locstartdate
            AND event_date<@locenddate
			AND g.community_health_flag = 0

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
   --,SUM(Denominator) AS Regular_Openings
   ,SUM(Regular_Openings_Available) AS Regular_Openings
FROM #TabRptg4

GO