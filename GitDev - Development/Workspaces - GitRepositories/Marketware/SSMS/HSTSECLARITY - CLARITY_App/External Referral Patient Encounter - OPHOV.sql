USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME = NULL, @EndDate SMALLDATETIME = NULL

SET @StartDate = '12/15/2024 00:00'
--SET @EndDate = CAST(CAST(DATEADD(DAY, -1, GETDATE()) AS DATE) AS SMALLDATETIME);
SET @EndDate = '1/31/2025 00:00'

--ALTER PROCEDURE [Rptg].[uspSrc_Completed_Referral_Scheduling_Detail]
--       (
--        @StartDate SMALLDATETIME = NULL,
--        @EndDate SMALLDATETIME = NULL,
--		@DepartmentGrouperColumn VARCHAR(12),
--		@DepartmentGrouperNoValue VARCHAR(25),
--        @in_pods_servLine VARCHAR(MAX),
--        @in_depid VARCHAR(MAX)
--	   )
--AS
/****************************************************************************************************************************************
WHAT: Create procedure Rptg.uspSrc_Completed_Referral_Scheduling_Detail
WHO : Tom Burgan
WHEN: 03/19/2020
WHY : Referral Completion report
----------------------------------------------------------------------------------------------------------------------------------------
INFO:
      INPUTS:   CLARITY_App.ETL.fn_ParmParse
	            CLARITY.dbo.REFERRAL
                CLARITY.dbo.REFERRAL_3
				CLARITY.dbo.PATIENT
				CLARITY.dbo.CLARITY_DEP
				CLARITY.dbo.CLARITY_SER
				CLARITY.dbo.ZC_SPECIALTY
				CLARITY.dbo.ZC_RFL_TYPE
				CLARITY.dbo.ZC_RFL_CLASS
				CLARITY.dbo.ZC_RFL_STATUS
				CLARITY.dbo.REFERRAL_SOURCE
				CLARITY.dbo.REFERRAL_HIST
                CLARITY.dbo.CLARITY_EMP
				CLARITY.dbo.ZC_RFL_HST_CHG_TYP
				CLARITY.dbo.V_SCHED_APPT
				CLARITY_App.Rptg.vwDim_Date
                CLARITY_App.Rptg.vwRef_MDM_Location_Master_EpicSvc

      OUTPUTS:
                CLARITY_App.Rptg.uspSrc_Completed_Referral_Scheduling_Detail
----------------------------------------------------------------------------------------------------------------------------------------
MODS:     03/19/2020--TMB-- Create new stored procedure
*****************************************************************************************************************************************/

  SET NOCOUNT ON;

---------------------------------------------------
---Default referral expiration date range is the current FYTD
  DECLARE @CurrDate SMALLDATETIME;

  SET @CurrDate = CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME);
    
  IF @StartDate IS NULL
      BEGIN
		SET @StartDate=CASE WHEN DATEPART(mm, @CurrDate)<7 
 	                        THEN CAST('07/01/'+CAST(DATEPART(yy, @CurrDate)-1 AS CHAR(4)) AS SMALLDATETIME)
                            ELSE CAST('07/01/'+CAST(DATEPART(yy, @CurrDate) AS CHAR(4)) AS SMALLDATETIME)
                       END;
	  END;

  --IF @EndDate IS NULL
  --    BEGIN
  --      SET @EndDate = CAST(CAST(DATEADD(DAY, -1, @CurrDate) AS DATE) AS SMALLDATETIME);
  --    END;
----------------------------------------------------

DECLARE @locstartdate DATETIME,
        @locenddate DATETIME

SET @locstartdate = CAST(@StartDate AS DATETIME)
SET @locenddate   = CAST(@EndDate AS DATETIME)

IF OBJECT_ID('tempdb..#mdm ') IS NOT NULL
DROP TABLE #mdm

IF OBJECT_ID('tempdb..#mdmhsp ') IS NOT NULL
DROP TABLE #mdmhsp

IF OBJECT_ID('tempdb..#rfls ') IS NOT NULL
DROP TABLE #rfls

IF OBJECT_ID('tempdb..#rflpts ') IS NOT NULL
DROP TABLE #rflpts

IF OBJECT_ID('tempdb..#rflencs ') IS NOT NULL
DROP TABLE #rflencs

IF OBJECT_ID('tempdb..#pts ') IS NOT NULL
DROP TABLE #pts

IF OBJECT_ID('tempdb..#encs ') IS NOT NULL
DROP TABLE #encs

		SELECT DISTINCT
				   rmlmh.EPIC_DEPARTMENT_ID
				  ,rmlmh.EPIC_DEPT_TYPE
				  ,rmlmh.SERVICE_LINE_ID
				  ,rmlmh.SERVICE_LINE
				  ,rmlmh.OPNL_SERVICE_ID
				  ,rmlmh.OPNL_SERVICE_NAME
				  ,rmlmh.HS_AREA_ID
				  ,rmlmh.HS_AREA_NAME
				  ,rmlmh.POD_ID
				  ,rmlmh.PFA_POD

			INTO #mdm

			FROM dbo.Ref_MDM_Location_Master_History AS rmlmh
				INNER JOIN
				( --hx--most recent batch date per dep id
					SELECT mdmhx.EPIC_DEPARTMENT_ID
						  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
					FROM dbo.Ref_MDM_Location_Master_History AS mdmhx
					GROUP BY mdmhx.EPIC_DEPARTMENT_ID
				)                                                 AS hx
					ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
					   AND rmlmh.BATCH_RUN_DT = hx.max_dt

	ORDER BY 
          rmlmh.EPIC_DEPARTMENT_ID

  CREATE UNIQUE CLUSTERED INDEX IX_mdm ON #mdm (EPIC_DEPARTMENT_ID)

			SELECT DISTINCT
			       mdmhsp.[EPIC_DEPARTMENT_ID]
				  ,mdmhsp.[REV_LOC_ID]
				  ,mdmhsp.[REV_LOC_NAME]
				  ,mdmhsp.[HOSPITAL_CODE]
				  ,mdmhsp.[DE_HOSPITAL_CODE]
				  ,mdmhsp.[HOSPITAL_GROUP]
				  ,mdmhsp.[LOC_RPT_GRP_NINE_NAME]
				  ,mdmhsp.[LOC_RPT_GRP_NINE]
				  ,mdmhsp.[RECORD_STATUS]
				  ,mdmhsp.[RECORD_STATUS_TITLE]

			INTO #mdmhsp

			FROM [CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group_ALL_History] mdmhsp
			INNER JOIN
			( --hx--most recent update date per dep id
				SELECT mdmhx.EPIC_DEPARTMENT_ID
						,MAX(mdmhx.Update_Dtm) AS max_dt
				FROM [Rptg].[vwRef_MDM_Location_Master_Hospital_Group_ALL_History] AS mdmhx
				GROUP BY mdmhx.EPIC_DEPARTMENT_ID
			)                                                 AS hx
				ON hx.EPIC_DEPARTMENT_ID = mdmhsp.EPIC_DEPARTMENT_ID
					AND mdmhsp.Update_Dtm = hx.max_dt
			--WHERE RECORD_STATUS_TITLE = 'ACTIVE'

	ORDER BY 
          mdmhsp.EPIC_DEPARTMENT_ID

  CREATE UNIQUE CLUSTERED INDEX IX_mdmhsp ON #mdmhsp (EPIC_DEPARTMENT_ID)

SELECT DISTINCT
   RFL.REFERRAL_ID,
   RFL.ENTRY_DATE AS event_date,
   ROW_NUMBER() OVER(PARTITION BY PT.PAT_ID ORDER BY RFL.ENTRY_DATE) AS rflseq,
   date_dim.fmonth_num,
   date_dim.Fyear_num,
   date_dim.FYear_name,
   CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
   CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date,
   RFL.REFD_BY_DEPT_ID AS REFERRED_BY_DEPT_ID,
   REFERRING_DEP.DEPARTMENT_NAME AS REFERRED_BY_DEPT_NAME,
   RFL.PAT_ID,
   CAST(PT.PAT_MRN_ID AS INTEGER) AS person_id,
   PT.PAT_NAME AS person_name,
   REFERRAL_SOURCE.REF_PROVIDER_ID AS provider_id,
   REFERRING_SER.PROV_NAME AS provider_name,
   RFL.ENTRY_DATE,
   RFL.EXP_DATE,
   --AUTH_CHANGE.CHANGE_DATETIME AS AUTH_DATE,
   ZC_RFL_CLASS.NAME AS REFERRAL_CLASS,
   ZC_RFL_TYPE.NAME AS REFERRAL_TYPE,
   REFERRED_TO_DEP.DEPARTMENT_ID AS REFERRED_TO_DEPT_ID,
   REFERRED_TO_DEP.DEPARTMENT_NAME AS REFERRED_TO_DEPT_NAME,
   CLARITY_SER.PROV_ID AS REFERRED_TO_PROV_ID,
   CLARITY_SER.PROV_NAME AS REFERRED_TO_PROV_NAME,
   ZC_SPECIALTY.NAME AS REFERRED_TO_PROV_SPEC,
   PT.PAT_NAME AS PATIENT_NAME,
/*
   SCHED_CHANGE.CHANGE_DATETIME AS RFL_CHANGE_DTTM,
   SCHED_CHANGE.CHANGE_TYPE_NAME AS RFL_CHANGE_TYPE,
   SCHED_CHANGE.CHANGE_USER_ID AS RFL_CHANGE_USER_ID,
   SCHED_CHANGE.CHANGE_USER_NAME AS RFL_CHANGE_USER,
   SCHED_CHANGE.PREVIOUS_VALUE AS RFL_CHANGE_TEXT,
   RFL_APPTS.ACTUAL_CNT AS SCHEDULED_VISITS,
   RFL_APPTS.APPT_MADE_DTTM AS FIRST_APPT_MADE,
   RFL_APPTS.APPT_DATE AS FIRST_APPT,
   RFL_APPTS.COMPLETED_CNT,
   RFL_APPTS.CANCELED_CNT,
   RFL_APPTS.RFL_ID,
*/
/*
   RFL_APPTS.RFL_ID,
   RFL_APPTS.PAT_ENC_CSN_ID,
   RFL_APPTS.APPT_MADE_DATE,
   RFL_APPTS.APPT_TIME,
   RFL_APPTS.CONTACT_DATE,
   RFL_APPTS.APPT_STATUS_C,
   RFL_APPTS.APPT_STATUS_NAME,
   RFL_APPTS.HOSP_ADMSN_TIME,
   RFL_APPTS.HOSP_ADMSN_TYPE_C,
   RFL_APPTS.HOSP_ADMSN_TYPE_NAME,
   RFL_APPTS.HOSP_DISCHRG_TIME,
   RFL_APPTS.ENC_TYPE_NAME,
*/
   mdm.service_line_id,
   mdm.service_line,
   mdm.POD_ID,
   mdm.PFA_POD

INTO #rfls

FROM 
   CLARITY.dbo.REFERRAL RFL
   LEFT OUTER JOIN CLARITY.dbo.REFERRAL_3 ON RFL.REFERRAL_ID=REFERRAL_3.REFERRAL_ID
   LEFT OUTER JOIN CLARITY.dbo.PATIENT AS PT ON RFL.PAT_ID = PT.PAT_ID
   LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS REFERRED_TO_DEP ON RFL.REFD_TO_DEPT_ID = REFERRED_TO_DEP.DEPARTMENT_ID
   LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ON RFL.REFERRAL_PROV_ID = CLARITY_SER.PROV_ID 
   LEFT OUTER JOIN CLARITY.dbo.ZC_SPECIALTY ON RFL.PROV_SPEC_C = ZC_SPECIALTY.SPECIALTY_C
   LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_TYPE ON RFL.RFL_TYPE_C = ZC_RFL_TYPE.RFL_TYPE_C
   LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_CLASS ON RFL.RFL_CLASS_C = ZC_RFL_CLASS.RFL_CLASS_C
   LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_STATUS ON RFL.RFL_STATUS_C = ZC_RFL_STATUS.RFL_STATUS_C
   LEFT OUTER JOIN CLARITY.dbo.REFERRAL_SOURCE ON RFL.REFERRING_PROV_ID = REFERRAL_SOURCE.REFERRING_PROV_ID
   LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS REFERRING_SER  ON REFERRING_SER.PROV_ID = REFERRAL_SOURCE.REF_PROVIDER_ID
   LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS REFERRING_DEP  ON REFERRING_DEP.DEPARTMENT_ID = RFL.REFD_BY_DEPT_ID
   LEFT OUTER JOIN
      ( SELECT REFERRAL_ID, CHANGE_DATETIME
         FROM CLARITY.dbo.REFERRAL_HIST
         WHERE REFERRAL_HIST.LINE = (	SELECT MAX(RFLHX.LINE) 
                                 FROM CLARITY.dbo.REFERRAL_HIST RFLHX
                                 WHERE RFLHX.CHANGE_TYPE_C IN (53,39) -- Change Status, Set Auto-status
                                 AND RFLHX.REFERRAL_ID = REFERRAL_HIST.REFERRAL_ID)) AS AUTH_CHANGE ON AUTH_CHANGE.REFERRAL_ID = RFL.REFERRAL_ID

   LEFT OUTER JOIN
      ( SELECT rh.REFERRAL_ID, rh.LINE, rh.CHANGE_DATE, rh.CHANGE_TYPE_C, rhct.NAME AS CHANGE_TYPE_NAME, rh.CHANGE_USER_ID, emp.NAME AS CHANGE_USER_NAME, rh.PREVIOUS_VALUE, rh.CHANGE_DATETIME
         FROM CLARITY.dbo.REFERRAL_HIST rh
         LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP emp ON emp.USER_ID = rh.CHANGE_USER_ID
         LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_HST_CHG_TYP rhct ON rhct.CHANGE_TYPE_C = rh.CHANGE_TYPE_C
         WHERE rh.LINE = (	SELECT MAX(RFLHX.LINE) 
                                 FROM CLARITY.dbo.REFERRAL_HIST RFLHX
                                 LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_HST_CHG_TYP rhct ON rhct.CHANGE_TYPE_C    = RFLHX.CHANGE_TYPE_C
                                 WHERE RFLHX.CHANGE_USER_ID NOT IN ('RFLEOD','RTEUSERIN','CADENCEEOD','KBSQL')
								 AND (rhct.NAME LIKE '%Schedul%' AND rhct.NAME NOT IN ('RFLEOD','CADENCEEOD'))
                                 AND RFLHX.REFERRAL_ID = rh.REFERRAL_ID)) AS SCHED_CHANGE ON SCHED_CHANGE.REFERRAL_ID = RFL.REFERRAL_ID
   LEFT OUTER JOIN
    (
	    SELECT day_date,
               fmonth_num,
               Fyear_num,
               FYear_name
        FROM CLARITY_App.Rptg.vwDim_Date
    ) date_dim
	--ON (date_dim.day_date = CAST(RFL.EXP_DATE AS SMALLDATETIME))
	ON (date_dim.day_date = CAST(RFL.ENTRY_DATE AS SMALLDATETIME))
    --LEFT OUTER JOIN Rptg.vwRef_MDM_Location_Master_EpicSvc mdm
    LEFT OUTER JOIN #mdm  mdm
    --ON RFL.REFD_BY_DEPT_ID = mdm.epic_department_id
    ON REFERRED_TO_DEP.DEPARTMENT_ID = mdm.epic_department_id
   
WHERE (REFERRAL_3.AUTH_CERT_YN IS NULL OR REFERRAL_3.AUTH_CERT_YN='N')
      AND RFL.ACTUAL_NUM_VISITS IS NOT NULL
	  --AND (RFL.EXP_DATE >= @CompletedStartDate AND RFL.EXP_DATE <= @CompletedEndDate)
	  --AND (RFL.EXP_DATE >= @locstartdate  AND RFL.EXP_DATE <= @locenddate)
	  --AND (RFL.ENTRY_DATE >= @locstartdate)
	  AND (RFL.ENTRY_DATE >= @locstartdate  AND RFL.ENTRY_DATE <= @locenddate)
      --AND (COALESCE(mdm.' + @DepartmentGrouperColumn + ',''' + @DepartmentGrouperNoValue + ''') IN (SELECT pod_Service_Line FROM cte_pods_servLine)) AND (RFL.REFD_BY_DEPT_ID IN (SELECT DepartmentId FROM cte_depid))
	  AND RFL.RFL_TYPE_C NOT IN ('42','510','102','502') -- Home Health Care, Home Health Pharmacy, Insurance Referral, Lab
	  AND ZC_RFL_CLASS.NAME = 'Incoming'
	  --AND RFL_APPTS.APPT_MADE_DATE >= RFL.ENTRY_DATE

--ORDER BY
--	person_id,
--	REFERRAL_ID--,
--	--APPT_TIME,
--	--RFL_APPTS.PAT_ENC_CSN_ID

--ORDER BY
--	PT.PAT_ID,
--	REFERRAL_ID--,
--	--APPT_TIME,
--	--RFL_APPTS.PAT_ENC_CSN_ID

ORDER BY
	RFL.PAT_ID,
	rflseq
	
  -- Create index for temp table #rflpts

  --CREATE CLUSTERED INDEX IX_rflpts ON #rflpts (person_id, REFERRAL_ID, APPT_MADE_DTTM, PAT_ENC_CSN_ID)
  --CREATE CLUSTERED INDEX IX_rfls ON #rfls (PAT_ID, REFERRAL_ID)
  CREATE CLUSTERED INDEX IX_rfls ON #rfls (PAT_ID, rflseq)

--SELECT
--    PAT_ID,
--	rflseq,
--	REFERRAL_ID,
--    event_date,
--    fmonth_num,
--    Fyear_num,
--    FYear_name,
--    report_period,
--    report_date,
--    REFERRED_BY_DEPT_ID,
--    REFERRED_BY_DEPT_NAME,
--    person_id,
--    person_name,
--    provider_id,
--    provider_name,
--    ENTRY_DATE,
--    EXP_DATE,
--    REFERRAL_CLASS,
--    REFERRAL_TYPE,
--    REFERRED_TO_DEPT_ID,
--    REFERRED_TO_DEPT_NAME,
--    REFERRED_TO_PROV_ID,
--    REFERRED_TO_PROV_NAME,
--    REFERRED_TO_PROV_SPEC,
--    PATIENT_NAME,
--    service_line_id,
--    service_line,
--    POD_ID,
--    PFA_POD
--FROM #rfls
--ORDER BY
--	PAT_ID,
--	REFERRAL_ID

SELECT --DISTINCT
	PAT_ID,
	PATIENT_NAME,
	event_date
INTO #rflpts
FROM #rfls
WHERE rflseq = 1
ORDER BY
	PAT_ID
	
  -- Create index for temp table #rflpts

  CREATE CLUSTERED INDEX IX_rflpts ON #rflpts (PAT_ID)

;WITH ophov_cte AS (
SELECT
	cte.PAT_ID,
	cte.PATIENT_NAME,
    ENC.PAT_ENC_CSN_ID,
	ENC.PAT_ENC_CSN_ID AS HSP_PAT_ENC_CSN_ID,
	ENC.CONTACT_DATE,
	ENC.APPT_TIME,
	CASE WHEN ENC.APPT_STATUS_C = 1 THEN 'SCHEDULED'
			WHEN ENC.APPT_STATUS_C = 2 THEN 'COMPLETED'
			WHEN ENC.APPT_STATUS_C = 3 THEN 'CANCELED'
			WHEN ENC.APPT_STATUS_C = 4 THEN 'NO_SHOW'
			WHEN ENC.APPT_STATUS_C = 6 THEN 'ARRIVED'
			ELSE NULL
	END AS APPT_STATUS_NAME,
	NULL AS HOSP_ADMSN_TIME,
	NULL AS HOSP_DISCH_TIME,
	NULL AS HSP_HOSP_ADMSN_TIME,
	NULL AS HSP_HOSP_DISCH_TIME,
	zdet.NAME AS Enc_Type,
	zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
	ENC.VISIT_PROV_ID AS Prov_Id,
	ENC.DEPARTMENT_ID,
	DEP.DEPARTMENT_NAME,
	ENC.REFERRAL_ID,
	'OUTPATIENT' UOS,
	'ophov_cte' AS [extract],
	NULL AS LOG_ID,
	NULL AS PRIMARY_PROCEDURE_ID,
	NULL AS PRIMARY_PROCEDURE_NM,
	ENC.HSP_ACCOUNT_ID,
    cpt1.CPT_CODE [CPT1],
	cpt1.CPT_CODE_DESC,
	cpt1.CPT_QUANTITY,
	har.TOT_CHGS,
	har.TOT_PMTS,
	icd10.CURRENT_ICD10_LIST,
	icd10.DX_NAME,
	dx.DX_ID AS dx_DX_ID,
	dx.NAME AS dx_DX_ID_NAME,
	dx.REF_BILL_CODE_SET_NAME,
	dx.REF_BILL_CODE AS dx_REF_BILL_CODE,
	cpt.NAME AS cpt_NAME,
	cpt.REF_BILL_CODE AS cpt_REF_BILL_CODE,
	zcsh.NAME AS CODING_STATUS_NAME, -- 1 Not Started, 2 In Progress, 3 Waiting, 4 Completed, 5 Ready To Start, 6 On Hold
	har.PRIMARY_PAYOR_ID,
	har.PRIMARY_PLAN_ID

    FROM CLARITY.dbo.PAT_ENC ENC
    LEFT OUTER JOIN CLARITY.dbo.PATIENT pt ON pt.PAT_ID = ENC.PAT_ID
	INNER JOIN #rflpts cte ON pt.PAT_ID = cte.PAT_ID
	LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = ENC.HOSP_ADMSN_TYPE_C
	LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet ON zdet.DISP_ENC_TYPE_C = ENC.ENC_TYPE_C
    LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP  ON DEP.DEPARTMENT_ID = ENC.DEPARTMENT_ID
	LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT har ON har.HSP_ACCOUNT_ID = enc.HSP_ACCOUNT_ID

	LEFT OUTER JOIN 
			(SELECT cptt.HSP_ACCOUNT_ID,
                    cptt.LINE,
                    cptt.CPT_CODE,
                    cptt.CPT_CODE_DESC,
                    cptt.CPT_QUANTITY
			FROM CLARITY.dbo.HSP_ACCT_CPT_CODES cptt	
			WHERE cptt.LINE='1') cpt1									ON cpt1.HSP_ACCOUNT_ID=har.HSP_ACCOUNT_ID
		
		LEFT OUTER JOIN 
				(SELECT *
				FROM CLARITY.dbo.HSP_ACCT_DX_LIST opdx
				WHERE opdx.LINE='1'	) opdx										ON opdx.HSP_ACCOUNT_ID=har.HSP_ACCOUNT_ID
	
		LEFT OUTER JOIN 
				(SELECT *
				FROM CLARITY.dbo.CLARITY_EDG edg
				WHERE edg.REF_BILL_CODE_SET_C='2') icd10							ON icd10.DX_ID=opdx.DX_ID

		LEFT JOIN CLARITY.dbo.ZC_CODING_STS_HA zcsh ON zcsh.CODING_STATUS_C = har.CODING_STATUS_C
	
		LEFT OUTER JOIN 
				(SELECT
				        vcde.HSP_ACCOUNT_ID,
                        vcde.DX_ID,
                        vcde.NAME,
                        vcde.REF_BILL_CODE_SET_NAME,
                        vcde.REF_BILL_CODE
				FROM CLARITY.dbo.V_CODING_ALL_DX_PX_LIST vcde
				WHERE vcde.REF_BILL_CODE_SET_C='2' AND vcde.LINE = 1) dx							ON dx.HSP_ACCOUNT_ID = har.HSP_ACCOUNT_ID
	
		LEFT OUTER JOIN 
				(SELECT
				        vcde.HSP_ACCOUNT_ID,
                        vcde.NAME,
                        vcde.REF_BILL_CODE
				FROM CLARITY.dbo.V_CODING_ALL_DX_PX_LIST vcde
				WHERE vcde.SOURCE_KEY = 23) cpt							ON cpt.HSP_ACCOUNT_ID = har.HSP_ACCOUNT_ID -- Combined CPT Code

	WHERE ENC.CONTACT_DATE >= cte.event_date
	AND ENC.ENC_TYPE_C IN (
	'1001', -- Anti-coag visit
		'1003', -- Procedure visit
		'101', -- Office Visit
		'1200', -- Routine Prenatal
		'1201', -- Initial Prenatal
		'201', -- Nurse Only
		'2101', -- Clinical Support
		'2104200001', -- Telemedicine Clinical Support
		'2105100001', -- Therapy Visit
		'2105700001', -- Prof Remote/Non Face-to-Face Encounter
		'213', -- Dentistry Visit
		'2502', -- Follow-Up
		'3', -- Hospital Encounter
		'50', -- Appointment
		'51', -- Surgery
		'76'  -- Telemedicine
)
	AND ENC.VISIT_PROV_ID IS NOT NULL

	AND ENC.APPT_STATUS_C IN (1 --  'SCHEDULED'
	,2 -- 'COMPLETED'
	,6 -- 'ARRIVED'
	)
)

SELECT
	encs.PAT_ID,
	encs.PATIENT_NAME,
    encs.PAT_ENC_CSN_ID,
    encs.HSP_PAT_ENC_CSN_ID,
    encs.CONTACT_DATE,
    encs.APPT_TIME,
    encs.APPT_STATUS_NAME,
    encs.HOSP_ADMSN_TIME,
    encs.HOSP_DISCH_TIME,
	encs.HSP_HOSP_ADMSN_TIME,
	encs.HSP_HOSP_DISCH_TIME,
    encs.Enc_Type,
    encs.HOSP_ADMSN_TYPE_NAME,
    encs.Prov_Id,
    encs.DEPARTMENT_ID,
    encs.DEPARTMENT_NAME,
    encs.REFERRAL_ID,
    encs.UOS,
    encs.[extract],
	encs.LOG_ID,
	encs.PRIMARY_PROCEDURE_ID,
	encs.PRIMARY_PROCEDURE_NM,
	encs.HSP_ACCOUNT_ID,
	encs.[CPT1],
	encs.CPT_CODE_DESC,
	encs.CPT_QUANTITY,
	encs.TOT_CHGS,
	encs.TOT_PMTS,
	encs.CURRENT_ICD10_LIST,
	encs.DX_NAME,
	encs.dx_DX_ID,
	encs.dx_DX_ID_NAME,
	encs.REF_BILL_CODE_SET_NAME,
	encs.dx_REF_BILL_CODE,
	encs.cpt_NAME,
	encs.cpt_REF_BILL_CODE,
	encs.CODING_STATUS_NAME,
	encs.PRIMARY_PAYOR_ID,
	encs.PRIMARY_PLAN_ID
INTO #encs
FROM
(
--SELECT
--	ophov_case.PAT_ID,
--	ophov_case.PATIENT_NAME,
--    ophov_case.PAT_ENC_CSN_ID,
--    ophov_case.HSP_PAT_ENC_CSN_ID,
--    ophov_case.CONTACT_DATE,
--    ophov_case.APPT_TIME,
--    ophov_case.APPT_STATUS_NAME,
--    ophov_case.HOSP_ADMSN_TIME,
--    ophov_case.HOSP_DISCH_TIME,
--	ophov_case.HSP_HOSP_ADMSN_TIME,
--	ophov_case.HSP_HOSP_DISCH_TIME,
--    ophov_case.Enc_Type,
--    ophov_case.HOSP_ADMSN_TYPE_NAME,
--    ophov_case.Prov_Id,
--    ophov_case.DEPARTMENT_ID,
--    ophov_case.DEPARTMENT_NAME,
--	ophov_case.REFERRAL_ID,
--    ophov_case.UOS,
--	ophov_case.[extract],
--	ophov_case.LOG_ID,
--	ophov_case.PRIMARY_PROCEDURE_ID,
--	ophov_case.PRIMARY_PROCEDURE_NM,
--	ophov_case.HSP_ACCOUNT_ID,
--	ophov_case.[CPT1],
--	ophov_case.CPT_CODE_DESC,
--	ophov_case.CPT_QUANTITY,
--	ophov_case.TOT_CHGS,
--	ophov_case.TOT_PMTS,
--	ophov_case.CURRENT_ICD10_LIST,
--	ophov_case.DX_NAME,
--	ophov_case.dx_DX_ID,
--	ophov_case.dx_DX_ID_NAME,
--	ophov_case.REF_BILL_CODE_SET_NAME,
--	ophov_case.dx_REF_BILL_CODE,
--	ophov_case.cpt_NAME,
--	ophov_case.cpt_REF_BILL_CODE,
--	ophov_case.CODING_STATUS_NAME,
--	ophov_case.PRIMARY_PAYOR_ID,
--	ophov_case.PRIMARY_PLAN_ID
--FROM
--(
--SELECT
--	ophov.PAT_ID,
--	ophov.PATIENT_NAME,
--    ophov.PAT_ENC_CSN_ID,
--    ophov.HSP_PAT_ENC_CSN_ID,
--    ophov.CONTACT_DATE,
--    ophov.APPT_TIME,
--    ophov.APPT_STATUS_NAME,
--    ophov.HOSP_ADMSN_TIME,
--    ophov.HOSP_DISCH_TIME,
--	ophov.HSP_HOSP_ADMSN_TIME,
--	ophov.HSP_HOSP_DISCH_TIME,
--    ophov.Enc_Type,
--    ophov.HOSP_ADMSN_TYPE_NAME,
--    ophov.Prov_Id,
--    ophov.DEPARTMENT_ID,
--    ophov.DEPARTMENT_NAME,
--	ophov.REFERRAL_ID,
--    ophov.UOS,
--	ophov.[extract],
--	ophov.LOG_ID,
--	ophov.PRIMARY_PROCEDURE_ID,
--	ophov.PRIMARY_PROCEDURE_NM,
--	ophov.HSP_ACCOUNT_ID,
--	ophov.[CPT1],
--	ophov.CPT_CODE_DESC,
--	ophov.CPT_QUANTITY,
--	ophov.TOT_CHGS,
--	ophov.TOT_PMTS,
--	ophov.CURRENT_ICD10_LIST,
--	ophov.DX_NAME,
--	ophov.dx_DX_ID,
--	ophov.dx_DX_ID_NAME,
--	ophov.REF_BILL_CODE_SET_NAME,
--	ophov.dx_REF_BILL_CODE,
--	ophov.cpt_NAME,
--	ophov.cpt_REF_BILL_CODE,
--	ophov.CODING_STATUS_NAME,
--	ophov.PRIMARY_PAYOR_ID,
--	ophov.PRIMARY_PLAN_ID
--FROM
--(
SELECT
	ophov_cte.PAT_ID,
	ophov_cte.PATIENT_NAME,
    ophov_cte.PAT_ENC_CSN_ID,
    ophov_cte.HSP_PAT_ENC_CSN_ID,
    ophov_cte.CONTACT_DATE,
    ophov_cte.APPT_TIME,
    ophov_cte.APPT_STATUS_NAME,
    ophov_cte.HOSP_ADMSN_TIME,
    ophov_cte.HOSP_DISCH_TIME,
	ophov_cte.HSP_HOSP_ADMSN_TIME,
	ophov_cte.HSP_HOSP_DISCH_TIME,
    ophov_cte.Enc_Type,
    ophov_cte.HOSP_ADMSN_TYPE_NAME,
    ophov_cte.Prov_Id,
    ophov_cte.DEPARTMENT_ID,
    ophov_cte.DEPARTMENT_NAME,
	ophov_cte.REFERRAL_ID,
    ophov_cte.UOS,
    ophov_cte.[extract],
	ophov_cte.LOG_ID,
	ophov_cte.PRIMARY_PROCEDURE_ID,
	ophov_cte.PRIMARY_PROCEDURE_NM,
	ophov_cte.HSP_ACCOUNT_ID,
	ophov_cte.[CPT1],
	ophov_cte.CPT_CODE_DESC,
	ophov_cte.CPT_QUANTITY,
	ophov_cte.TOT_CHGS,
	ophov_cte.TOT_PMTS,
	ophov_cte.CURRENT_ICD10_LIST,
	ophov_cte.DX_NAME,
	ophov_cte.dx_DX_ID,
	ophov_cte.dx_DX_ID_NAME,
	ophov_cte.REF_BILL_CODE_SET_NAME,
	ophov_cte.dx_REF_BILL_CODE,
	ophov_cte.cpt_NAME,
	ophov_cte.cpt_REF_BILL_CODE,
	ophov_cte.CODING_STATUS_NAME,
	ophov_cte.PRIMARY_PAYOR_ID,
	ophov_cte.PRIMARY_PLAN_ID
FROM ophov_cte
--) ophov
--) ophov_case
) encs
--WHERE extract = 'ophov_cte'
--/*
SELECT
	--enc.person_id AS MRN,
    enc.PAT_ID AS Epic_Patient_Id,
	enc.PATIENT_NAME,
    --enc.REFERRAL_ID,
	--rflencs.REFERRAL_ID AS ENCOUNTER_REFERRAL_ID,
	--ISNULL(CONVERT(VARCHAR(100),rflencs.REFERRAL_ID),'') AS Encounter_Referral_Id,
    --CASE WHEN rflpts.REFERRAL_ID IS NOT NULL THEN enc.REFERRAL_ID ELSE NULL END AS EARLIEST_REFERRAL_ID,
    --enc.REFERRAL_ID AS Earliest_Referral_Id,
	--LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) AS REFERRAL_IDS,
	--CASE WHEN rflpts.REFERRAL_ID IS NOT NULL THEN LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) ELSE NULL END AS ADDITIONAL_REFERRAL_IDS,
	--LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) AS ADDITIONAL_REFERRAL_IDS,
	--ISNULL(CONVERT(VARCHAR(200),LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1)),'')  AS Additional_Referral_Ids,
	--ISNULL(CONVERT(VARCHAR(200),'"' + LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) + '"'),'')  AS Additional_Referral_Ids,
	--ISNULL(CONVERT(VARCHAR(200),LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1)),'')  AS Additional_Referral_Ids,
    enc.PAT_ENC_CSN_ID AS Encounter_CSN,
    enc.HSP_PAT_ENC_CSN_ID AS HSP_Encounter_CSN,
    enc.CONTACT_DATE AS Contact_Date,
    --enc.ENTRY_DATE AS Referral_Entry_Date,
    --enc.ENTRY_DATE AS Referral_Entry_Date,
    --enc.APPT_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.APPT_TIME,121),'') AS Appointment_Time,
    --enc.APPT_STATUS_NAME,
    ISNULL(CONVERT(VARCHAR(100),enc.APPT_STATUS_NAME),'') AS Appointment_Status,
    --enc.HOSP_ADMSN_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.HOSP_ADMSN_TIME,121),'') AS Hosp_Admsn_Time,
    --enc.HOSP_DISCH_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.HOSP_DISCH_TIME,121),'') AS Hosp_Disch_Time,
	enc.HSP_HOSP_ADMSN_TIME,
	enc.HSP_HOSP_DISCH_TIME,
    enc.Enc_Type,
    enc.HOSP_ADMSN_TYPE_NAME AS Hosp_Admsn_Type,
    enc.Prov_Id,
    enc.DEPARTMENT_ID AS Department_Id,
    enc.DEPARTMENT_NAME AS Department_Name,
	enc.REFERRAL_ID AS Referral_Id,
	--enc.APPT_PRC_ID AS Appt_Proc_Id,
	--enc.PROC_NAME AS Appt_Proc_Name,
    '"' + TRIM(ser.PROV_NAME) + '"' AS Provider_Name,
    ser.PROV_TYPE AS Provider_Type,
    ser.STAFF_RESOURCE Provider_Resource_Type,
	--zsp.NAME AS SPECIALTY_NAME,
    ISNULL(CONVERT(VARCHAR(100),zsp.NAME),'') AS Provider_Specialty,
	--dvsn.Epic_Financial_Division,
    ISNULL(CONVERT(VARCHAR(100),dvsn.Epic_Financial_Division),'') AS Epic_Financial_Division,
	--dvsn.Epic_Financial_Subdivision
    ISNULL(CONVERT(VARCHAR(100),dvsn.Epic_Financial_Subdivision),'') AS Epic_Financial_Subdivision,
	mdmhsp.HOSPITAL_CODE,
	enc.UOS,
	enc.[extract],
	enc.LOG_ID,
	enc.PRIMARY_PROCEDURE_ID,
	enc.PRIMARY_PROCEDURE_NM,
	enc.HSP_ACCOUNT_ID,
	enc.CPT1,
	enc.CPT_CODE_DESC,
	enc.CPT_QUANTITY,
	enc.TOT_CHGS,
	enc.TOT_PMTS,
	enc.CURRENT_ICD10_LIST,
	enc.DX_NAME,
	dx_DX_ID,
	dx_DX_ID_NAME,
	REF_BILL_CODE_SET_NAME,
	dx_REF_BILL_CODE,
	cpt_NAME,
	cpt_REF_BILL_CODE,
	CODING_STATUS_NAME,
	PRIMARY_PAYOR_ID,
	PRIMARY_PLAN_ID,
	epm.PAYOR_NAME,
	epm.FINANCIAL_CLASS,
	FIN_CLASS.NAME AS FIN_CLASS_NAME,
	epp.BENEFIT_PLAN_NAME
--*/
/*
SELECT DISTINCT
    enc.DEPARTMENT_ID AS Department_Id,
    enc.DEPARTMENT_NAME AS Department_Name,
	mdmhsp.HOSPITAL_CODE
*/
FROM #encs enc
LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser ON ser.PROV_ID = enc.Prov_Id
--LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER_SPEC serspc ON serspc.PROV_ID = enc.Prov_Id
LEFT OUTER JOIN
(
SELECT
	PROV_ID,
    spc.SPECIALTY_C,
	zs.NAME AS SPECIALTY_NAME
FROM CLARITY..CLARITY_SER_SPEC spc
LEFT OUTER JOIN CLARITY..ZC_SPECIALTY zs
ON zs.SPECIALTY_C = spc.SPECIALTY_C
WHERE LINE = 1
) sersp
ON sersp.PROV_ID = enc.Prov_Id
LEFT OUTER JOIN CLARITY.dbo.[ZC_SPECIALTY_DEP] zsp ON zsp.SPECIALTY_DEP_C = sersp.SPECIALTY_C
--LEFT OUTER JOIN #rflpts rflpts
--	ON rflpts.person_id = enc.person_id
--	AND rflpts.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
--LEFT OUTER JOIN #pts rflpts
--	ON rflpts.person_id = enc.person_id
--LEFT OUTER JOIN #rflencs rflencs
--	ON rflencs.person_id = enc.person_id
--	AND rflencs.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
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
	FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
	ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.RPT_GRP_SIX AS INT)
		AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.RPT_GRP_EIGHT AS INT))
LEFT OUTER JOIN #mdmhsp mdmhsp ON mdmhsp.EPIC_DEPARTMENT_ID = enc.DEPARTMENT_ID
LEFT JOIN CLARITY.dbo.CLARITY_EPM epm ON epm.PAYOR_ID = enc.PRIMARY_PAYOR_ID
LEFT JOIN CLARITY..ZC_FINANCIAL_CLASS FIN_CLASS ON FIN_CLASS.FINANCIAL_CLASS = epm.FINANCIAL_CLASS
LEFT JOIN CLARITY.dbo.CLARITY_EPP epp ON epp.BENEFIT_PLAN_ID = enc.PRIMARY_PLAN_ID
--WHERE enc.[extract] <> 'ophov_cte'
--AND mdmhsp.HOSPITAL_CODE IS NULL
ORDER BY
	enc.PAT_ID,
	enc.PAT_ENC_CSN_ID,
	COALESCE(enc.APPT_TIME, enc.HOSP_ADMSN_TIME)
/*
ORDER BY
	enc.DEPARTMENT_ID,
	enc.DEPARTMENT_NAME
*/

GO


