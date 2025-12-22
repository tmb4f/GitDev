USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME = NULL, @EndDate SMALLDATETIME = NULL

SET @StartDate = '1/1/2024 00:00'
--SET @EndDate = CAST(CAST(DATEADD(DAY, -1, GETDATE()) AS DATE) AS SMALLDATETIME);
SET @EndDate = '6/30/2024 00:00'

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

--DECLARE @SelectString NVARCHAR(MAX),
--		@ParmDefinition NVARCHAR(500)

--;WITH cte_pods_servLine (pod_Service_Line)
--AS
--(
--SELECT Param FROM CLARITY_App.ETL.fn_ParmParse(@PodServiceLine, '','')
--)
--,cte_depid (DepartmentId)
--AS
--(
--SELECT Param FROM CLARITY_App.ETL.fn_ParmParse(@DepartmentId, '','')
--)

		SELECT DISTINCT
				   rmlmh.EPIC_DEPARTMENT_ID
				  ,rmlmh.EPIC_DEPT_TYPE
				  ,rmlmh.SERVICE_LINE_ID
				  ,rmlmh.SERVICE_LINE
				  ,rmlmh.OPNL_SERVICE_ID
				  ,rmlmh.OPNL_SERVICE_NAME
				  ,rmlmh.HS_AREA_ID
				  ,rmlmh.HS_AREA_NAME

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
			WHERE RECORD_STATUS_TITLE = 'ACTIVE'

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
/*   
   LEFT OUTER JOIN 
      ( SELECT RFL.REFERRAL_ID RFL_ID,
         RFL_ENC.PAT_ENC_CSN_ID,
         RFL_ENC.APPT_MADE_DATE,
         RFL_ENC.APPT_MADE_DTTM,
         RFL_ENC.CONTACT_DATE,
		 RFL_ENC.APPT_STATUS_C,
		 CASE WHEN RFL_ENC.APPT_STATUS_C = 1 THEN 'SCHEDULED'
					WHEN RFL_ENC.APPT_STATUS_C = 2 THEN 'COMPLETED'
					WHEN RFL_ENC.APPT_STATUS_C = 3 THEN 'CANCELED'
					WHEN RFL_ENC.APPT_STATUS_C = 4 THEN 'NO_SHOW'
					WHEN RFL_ENC.APPT_STATUS_C = 6 THEN 'ARRIVED'
					ELSE NULL
		 END AS APPT_STATUS_NAME
         FROM CLARITY.dbo.REFERRAL RFL
         INNER JOIN CLARITY.dbo.V_SCHED_APPT RFL_ENC ON RFL_ENC.REFERRAL_ID = RFL.REFERRAL_ID
         --GROUP BY RFL.REFERRAL_ID
		 ) RFL_APPTS ON RFL_APPTS.RFL_ID = RFL.REFERRAL_ID
*/
/*
   LEFT OUTER JOIN 
      ( SELECT RFL.REFERRAL_ID RFL_ID,
         RFL_ENC.PAT_ENC_CSN_ID,
         RFL_ENC.APPT_MADE_DATE,
         --RFL_ENC.APPT_MADE_DTTM,
		 RFL_ENC.APPT_TIME,
         RFL_ENC.CONTACT_DATE,
		 RFL_ENC.APPT_STATUS_C,
		 CASE WHEN RFL_ENC.APPT_STATUS_C = 1 THEN 'SCHEDULED'
					WHEN RFL_ENC.APPT_STATUS_C = 2 THEN 'COMPLETED'
					WHEN RFL_ENC.APPT_STATUS_C = 3 THEN 'CANCELED'
					WHEN RFL_ENC.APPT_STATUS_C = 4 THEN 'NO_SHOW'
					WHEN RFL_ENC.APPT_STATUS_C = 6 THEN 'ARRIVED'
					ELSE NULL
		 END AS APPT_STATUS_NAME,
		 RFL_ENC.HOSP_ADMSN_TIME,
		 RFL_ENC.HOSP_ADMSN_TYPE_C,
		 zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
		 RFL_ENC.HOSP_DISCHRG_TIME,
		 RFL_ENC.ENC_TYPE_C,
		 zdet.NAME AS ENC_TYPE_NAME
         FROM CLARITY.dbo.REFERRAL RFL
         INNER JOIN CLARITY.dbo.PAT_ENC RFL_ENC ON RFL_ENC.REFERRAL_ID = RFL.REFERRAL_ID
		 LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = RFL_ENC.HOSP_ADMSN_TYPE_C
		 LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet ON zdet.DISP_ENC_TYPE_C = RFL_ENC.ENC_TYPE_C
         --GROUP BY RFL.REFERRAL_ID
		 ) RFL_APPTS ON RFL_APPTS.RFL_ID = RFL.REFERRAL_ID
*/
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
    LEFT OUTER JOIN Rptg.vwRef_MDM_Location_Master_EpicSvc mdm
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
	--cte.person_id,
	cte.PAT_ID,
	--cte.REFERRAL_ID,
	--cte.REFERRAL_IDS,
    ENC.PAT_ENC_CSN_ID,
	NULL AS HSP_PAT_ENC_CSN_ID,
	ENC.CONTACT_DATE,
	--cte.ENTRY_DATE,
    --ENC.APPT_MADE_DATE,
	ENC.APPT_TIME,
	--ENC.APPT_STATUS_C,
	CASE WHEN ENC.APPT_STATUS_C = 1 THEN 'SCHEDULED'
			WHEN ENC.APPT_STATUS_C = 2 THEN 'COMPLETED'
			WHEN ENC.APPT_STATUS_C = 3 THEN 'CANCELED'
			WHEN ENC.APPT_STATUS_C = 4 THEN 'NO_SHOW'
			WHEN ENC.APPT_STATUS_C = 6 THEN 'ARRIVED'
			ELSE NULL
	END AS APPT_STATUS_NAME,
	--ENC.HOSP_ADMSN_TIME,
	NULL AS HOSP_ADMSN_TIME,
	--ENC.HOSP_DISCHRG_TIME,
	NULL AS HOSP_DISCH_TIME,
	--ENC.ENC_TYPE_C,
	--zdet.NAME AS ENC_TYPE_NAME,
	zdet.NAME AS Enc_Type,
	--ENC.HOSP_ADMSN_TYPE_C,
	zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
	--ENC.VISIT_PROV_ID,
	ENC.VISIT_PROV_ID AS Prov_Id,
	--ENC.ATTND_PROV_ID,
	ENC.DEPARTMENT_ID,
	DEP.DEPARTMENT_NAME,
	ENC.REFERRAL_ID,
	--ENC.APPT_PRC_ID,
	--EAP.PROC_NAME
	'OUTPATIENT' UOS,
	'ophov_cte' AS [extract]

--SELECT DISTINCT
--	ENC.ENC_TYPE_C,
--	zdet.NAME AS ENC_TYPE_NAME
    FROM CLARITY.dbo.PAT_ENC ENC
    LEFT OUTER JOIN CLARITY.dbo.PATIENT pt ON pt.PAT_ID = ENC.PAT_ID
	--INNER JOIN cte_rflpts cte ON pt.PAT_ID = cte.PAT_ID
	INNER JOIN #rflpts cte ON pt.PAT_ID = cte.PAT_ID
	LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = ENC.HOSP_ADMSN_TYPE_C
	LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet ON zdet.DISP_ENC_TYPE_C = ENC.ENC_TYPE_C
    LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP  ON DEP.DEPARTMENT_ID = ENC.DEPARTMENT_ID
	--LEFT OUTER JOIN CLARITY.dbo.CLARITY_EAP AS EAP	ON EAP.PROC_NAME = enc.APPT_PRC_ID

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

	--ORDER BY
	--	cte.person_id,
	--	ENC.CONTACT_DATE

	--ORDER BY
	--	ENC.ENC_TYPE_C
),
--*/
ip_cte AS (
SELECT
	   --cte.person_id,
	   hsp.[PAT_ID],
	   --cte.REFERRAL_ID,
	   --cte.REFERRAL_IDS,
	   hsp.PAT_ENC_CSN_ID,
	   hsp.PAT_ENC_CSN_ID AS HSP_PAT_ENC_CSN_ID,
       --[CONTACT_DATE],
       CAST(hsp.HOSP_ADMSN_TIME AS DATE) AS [CONTACT_DATE],
	   --cte.ENTRY_DATE,
	   NULL AS APPT_TIME,
	   NULL AS APPT_STATUS_NAME,
       [HOSP_ADMSN_TIME]
      ,[HOSP_DISCH_TIME]
      --,hsp.[ADT_PAT_CLASS_C]
	  --,zpl.NAME AS ADT_PAT_CLASS_NAME
	  ,zpl.NAME AS Enc_Type
      --,hsp.[HOSP_ADMSN_TYPE_C]
	  ,zhat.NAME AS HOSP_ADMSN_TYPE_NAME
   --   ,[ADT_PATIENT_STAT_C]
   --   ,hsp.[ADMIT_SOURCE_C]
	  --,zas.NAME AS ADMIT_SOURCE_NAME
   --   ,[DISCHARGE_PROV_ID]
      --,[ADMISSION_PROV_ID]
      ,[ADMISSION_PROV_ID] AS Prov_Id
	  --,SER.PROV_NAME AS ADMISSION_PROV_NAME
   --   ,hsp.[HOSP_ADMSN_TYPE_C]
	  --,zhat.NAME AS HOSP_ADMSN_TYPE_NAME
      ,hsp.[DEPARTMENT_ID]
	  ,DEP.DEPARTMENT_NAME
      --,[HSP_ACCOUNT_ID]
      --,[INPATIENT_DATA_ID]
      --,[OP_ADM_DATE]
      --,[OP_ADM_EVENT_ID]
      --,[EMER_ADM_EVENT_ID]
	  ,'INPATIENT' UOS
	  ,	'ip_cte' AS [extract]
  FROM [CLARITY].[dbo].[PAT_ENC_HSP] hsp
  INNER JOIN #rflpts cte ON hsp.PAT_ID = cte.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS zpl ON zpl.ADT_PAT_CLASS_C = hsp.ADT_PAT_CLASS_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_ADM_SOURCE zas ON zas.ADMIT_SOURCE_C = hsp.ADMIT_SOURCE_C
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP  ON DEP.DEPARTMENT_ID = hsp.DEPARTMENT_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS SER  ON SER.PROV_ID = hsp.ADMISSION_PROV_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C

  --WHERE PAT_ID = 'Z245973'
  WHERE 1 = 1
  AND CAST(hsp.HOSP_ADMSN_TIME AS DATE) >= cte.event_date
  AND hsp.ADMISSION_PROV_ID IS NOT NULL
 -- ORDER BY
	--cte.person_id,
	--hsp.HOSP_ADMSN_TIME
),
/*
or1_cte AS (
SELECT
	   vs.[PAT_ID],
	   adm.PAT_ENC_CSN_ID,
	   vs.SURGERY_DATE AS CONTACT_DATE,
	   NULL AS APPT_TIME,
	   NULL AS APPT_STATUS_NAME,
       hspa.HOSP_ADMSN_TIME,
       hspa.HOSP_DISCH_TIME,
	   zpl.NAME AS Enc_Type,
	   zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
       cs.PROV_ID AS Prov_Id,
       hspa.[DEPARTMENT_ID],
	   dept.DEPARTMENT_NAME,
			 -- --,hspa.BILL_NUM
			 -- ,BILL_NUM = CASE
				--WHEN hspa.HSP_ACCOUNT_ID IS NOT NULL 
				--	THEN hspa.HSP_ACCOUNT_ID
				--WHEN ISNUMERIC(hspa.BILL_NUM) = 1 
				--	THEN CAST(hspa.BILL_NUM AS BIGINT) 
				--ELSE 0 END  --06/30/17
			 -- ,'Surgery' AS Event
			 -- ,zpc.NAME AS Pt_Class
			 -- ,loc.LOC_NAME AS Unit
			 -- ,cs.PROV_NAME AS Room
			 -- ,vs.CASE_ID	AS id
			 -- ,'Surgery'			"Current_Service"
			 -- ,coalesce(zcp_h.name, '')		"Service"
			 -- ,dept.REV_LOC_ID
	   CASE WHEN vs.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS,
	   'or1_cte' AS [extract]
		FROM clarity.dbo.OR_LOG AS vs
	    INNER JOIN #rflpts cte ON vs.PAT_ID = cte.PAT_ID
		LEFT JOIN clarity.dbo.ZC_PAT_CLASS AS zpc
				ON vs.PAT_TYPE_C =zpc.ADT_PAT_CLASS_C
		--INNER JOIN clarity.dbo.CLARITY_SER AS cs
		--		ON vs.ROOM_ID=cs.PROV_ID
		left join clarity.dbo.PAT_OR_ADM_LINK adm
				on adm.OR_CASELOG_ID = vs.LOG_ID
		left join clarity.dbo.clarity_loc as loc
				on loc.LOC_ID = vs.LOC_ID
		INNER JOIN clarity.dbo.pat_enc_hsp hspa
				ON hspa.pat_enc_csn_id = adm.PAT_ENC_CSN_ID
		LEFT JOIN clarity.dbo.zc_pat_service zcp_h
				on zcp_h.hosp_serv_c = hspa.hosp_serv_c
		LEFT JOIN clarity.dbo.zc_disch_disp zcd
				on zcd.disch_disp_c = hspa.disch_disp_c
        INNER JOIN CLARITY.dbo.CLARITY_DEP dept				ON hspa.DEPARTMENT_ID = dept.DEPARTMENT_ID
		LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS zpl ON zpl.ADT_PAT_CLASS_C = hspa.ADT_PAT_CLASS_C
		LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hspa.HOSP_ADMSN_TYPE_C
		LEFT JOIN CLARITY.dbo.OR_LOG_ALL_SURG AS PRIM_SURG_ID  ON vs.LOG_ID = PRIM_SURG_ID.LOG_ID AND PRIM_SURG_ID.ROLE_C=1 AND PRIM_SURG_ID.PANEL=1
		LEFT JOIN CLARITY.dbo.CLARITY_SER AS cs  ON PRIM_SURG_ID.SURG_ID = cs.PROV_ID

  WHERE 1 = 1
  AND CAST(hspa.HOSP_ADMSN_TIME AS DATE) >= cte.event_date
),
*/
/*
or2_cte AS (
SELECT
	   ENC.[PAT_ID],
	   LOGLIST.OR_CSN_ID AS PAT_ENC_CSN_ID,
	   ORL.SURGERY_DATE AS CONTACT_DATE,
	   NULL AS APPT_TIME,
	   NULL AS APPT_STATUS_NAME,
       ENC_HSP.HOSP_ADMSN_TIME,
       ENC_HSP.HOSP_DISCH_TIME,
	   zpl.NAME AS Enc_Type,
	   zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
       cs.PROV_ID AS Prov_Id,
       ENC_HSP.[DEPARTMENT_ID],
	   DEP.DEPARTMENT_NAME,
		 --    HSP.HSP_ACCOUNT_ID AS ACCTNBR_INT
   --         ,LNK.PAT_ENC_CSN_ID 
			--,ORL.LOG_ID AS UNIQUE_ID
			--,PAT.PAT_MRN_ID
			--,ROUND(DATEDIFF(DAY, PAT.BIRTH_DATE, ORL.SURGERY_DATE)/365.0,2) AGE_AT_SERVICE
			--,ORL.SURGERY_DATE 
			--,CASE	
			--	WHEN LOC.LOC_NAME  = 'UVHE Main OR' THEN 'Main_OR'
			--	WHEN LOC.LOC_NAME LIKE '%IMRI%' THEN 'IMRI'
			--	WHEN LOC.LOC_NAME LIKE '%Gen%Op%' THEN 'GUOR'
			--	WHEN LOC.LOC_NAME LIKE '%Ou%pa%Su%Ce%' THEN 'OPSC'
			--	WHEN LOC.LOC_NAME LIKE '%EP%LABS%' THEN 'Off_Site'
			--	WHEN LOC.LOC_NAME LIKE '%W%C%' THEN 'Off_Site'
			--	WHEN LOC.LOC_NAME LIKE '%Anes%Ser%' THEN 'Off_Site'
			--	ELSE NULL
			--	END AS DEPARTMENT
	  --      ,CASE WHEN LOGLIST.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS
			--,CASE WHEN ORT.TRACKING_EVENT_C = 60 THEN ORT.TRACKING_TIME_IN  END AS IN_ROOM
			--ROW_NUMBER() OVER (PARTITION BY hsp.HSP_ACCOUNT_ID, orl.LOG_ID ORDER BY ort.TRACKING_TIME_IN DESC) "Seq"
	   CASE WHEN LOGLIST.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS,
	   'or2_cte' AS [extract]

		FROM
		(SELECT l.LOG_ID
		,COALESCE(a.OR_LINK_CSN, a.PAT_ENC_CSN_ID) OR_CSN_ID  /* THIS COALESCES THE SURGERY CSN AND THE PROCEDURE CSN TO CREATE A REFERENCE CSN USED IN MAIN QUERY */
		,l.LOG_TYPE_C
		FROM CLARITY..OR_LOG l 
		LEFT OUTER JOIN CLARITY..PAT_OR_ADM_LINK a ON l.LOG_ID = a.OR_CASELOG_ID
		WHERE 1=1 
		--AND l.SURGERY_DATE >= @locSTARTDATE 
		--AND l.SURGERY_DATE < DATEADD(d,1,@locENDDATE)
		AND l.STATUS_C NOT IN (4,6)       /* NOT VOIDED, NOT CANCELED */
		--AND l.LOC_ID IN (SELECT L.LOC_ID FROM CLARITY..CLARITY_LOC L WHERE ( L.LOC_NAME  = 'UVHE Main OR'  OR  L.LOC_NAME LIKE '%IMRI%' OR L.LOC_NAME LIKE '%Gen%Op%'))
		) LOGLIST 
		LEFT OUTER JOIN CLARITY..OR_LOG AS ORL ON LOGLIST.LOG_ID = ORL.LOG_ID
		LEFT OUTER JOIN CLARITY..PAT_OR_ADM_LINK LNK ON ORL.LOG_ID = LNK.OR_CASELOG_ID
		LEFT OUTER JOIN CLARITY..PAT_ENC AS ENC ON LOGLIST.OR_CSN_ID = ENC.PAT_ENC_CSN_ID 
		LEFT OUTER JOIN CLARITY..PAT_ENC_HSP AS ENC_HSP ON LOGLIST.OR_CSN_ID = ENC_HSP.PAT_ENC_CSN_ID 
	    LEFT OUTER JOIN CLARITY..CLARITY_LOC AS LOC ON ORL.LOC_ID = LOC.LOC_ID
		LEFT OUTER JOIN CLARITY..PATIENT AS PAT ON PAT.PAT_ID = ENC.PAT_ID
		--INNER JOIN CLARITY..IDENTITY_ID AS id ON id.PAT_ID = pat.PAT_ID
	    INNER JOIN #rflpts cte ON ENC.PAT_ID = cte.PAT_ID
		LEFT OUTER JOIN CLARITY..OR_LOG_CASE_TIMES AS ORT ON ORT.LOG_ID = orl.LOG_ID AND ORT.TRACKING_EVENT_C = 60
		LEFT OUTER JOIN (
							SELECT 
											HAR.HSP_ACCOUNT_ID,
											HAR.PRIM_ENC_CSN_ID,
											har.ACCT_CLASS_HA_C
							FROM
											CLARITY..HSP_ACCOUNT HAR
											INNER JOIN CLARITY..HSP_ACCT_SBO SBO ON HAR.HSP_ACCOUNT_ID = SBO.HSP_ACCOUNT_ID 
											LEFT JOIN CLARITY..HSP_ACCT_TYPE TYP ON HAR.HSP_ACCOUNT_ID = TYP.HSP_ACCOUNT_ID 
							WHERE
											SBO.SBO_HAR_TYPE_C = '0'
											AND (TYP.HAR_TYPE_C <> '5' OR TYP.HAR_TYPE_C IS NULL)
							) AS HSP ON ENC.HSP_ACCOUNT_ID = HSP.HSP_ACCOUNT_ID
		LEFT OUTER JOIN CLARITY..CLARITY_DEP DEP ON DEP.DEPARTMENT_ID = ENC_HSP.DEPARTMENT_ID
		INNER JOIN #mdm MDM_DEP ON (MDM_DEP.EPIC_DEPARTMENT_ID = DEP.DEPARTMENT_ID)
		LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS zpl ON zpl.ADT_PAT_CLASS_C = ENC_HSP.ADT_PAT_CLASS_C
		LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = ENC_HSP.HOSP_ADMSN_TYPE_C
		LEFT JOIN CLARITY.dbo.OR_LOG_ALL_SURG AS PRIM_SURG_ID  ON LOGLIST.LOG_ID = PRIM_SURG_ID.LOG_ID AND PRIM_SURG_ID.ROLE_C=1 AND PRIM_SURG_ID.PANEL=1
		LEFT JOIN CLARITY.dbo.CLARITY_SER AS cs  ON PRIM_SURG_ID.SURG_ID = cs.PROV_ID

  WHERE 1 = 1
  AND (hsp.ACCT_CLASS_HA_C <> '123' OR hsp.ACCT_CLASS_HA_C IS NULL)
  AND CAST(ENC_HSP.HOSP_ADMSN_TIME AS DATE) >= cte.event_date
)
*/
or3_cte AS (
SELECT
	   vsurg.[PAT_ID],
	   lnk.PAT_ENC_CSN_ID AS PAT_ENC_CSN_ID,
	   lnk.OR_LINK_CSN AS HSP_PAT_ENC_CSN_ID,
	   lgb.PROC_DATE AS CONTACT_DATE,
	   NULL AS APPT_TIME,
	   NULL AS APPT_STATUS_NAME,
	   lgb.IN_OR_DTTM AS HOSP_ADMSN_TIME,
       lgb.OUT_OR_DTTM AS HOSP_DISCH_TIME,
	   lgb.PATIENT_CLASS_NM AS Enc_Type,
	   zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
       lgb.PRIMARY_PHYSICIAN_ID AS Prov_Id,
	   lgb.LOCATION_ID AS DEPARTMENT_ID,
	   lgb.LOCATION_NM AS DEPARTMENT_NAME,
	   CASE WHEN orl.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS,
/*
		 --    HSP.HSP_ACCOUNT_ID AS ACCTNBR_INT
   --         ,LNK.PAT_ENC_CSN_ID 
			--,ORL.LOG_ID AS UNIQUE_ID
			--,PAT.PAT_MRN_ID
			--,ROUND(DATEDIFF(DAY, PAT.BIRTH_DATE, ORL.SURGERY_DATE)/365.0,2) AGE_AT_SERVICE
			--,ORL.SURGERY_DATE 
			--,CASE	
			--	WHEN LOC.LOC_NAME  = 'UVHE Main OR' THEN 'Main_OR'
			--	WHEN LOC.LOC_NAME LIKE '%IMRI%' THEN 'IMRI'
			--	WHEN LOC.LOC_NAME LIKE '%Gen%Op%' THEN 'GUOR'
			--	WHEN LOC.LOC_NAME LIKE '%Ou%pa%Su%Ce%' THEN 'OPSC'
			--	WHEN LOC.LOC_NAME LIKE '%EP%LABS%' THEN 'Off_Site'
			--	WHEN LOC.LOC_NAME LIKE '%W%C%' THEN 'Off_Site'
			--	WHEN LOC.LOC_NAME LIKE '%Anes%Ser%' THEN 'Off_Site'
			--	ELSE NULL
			--	END AS DEPARTMENT
	  --      ,CASE WHEN LOGLIST.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS
			--,CASE WHEN ORT.TRACKING_EVENT_C = 60 THEN ORT.TRACKING_TIME_IN  END AS IN_ROOM
			--ROW_NUMBER() OVER (PARTITION BY hsp.HSP_ACCOUNT_ID, orl.LOG_ID ORDER BY ort.TRACKING_TIME_IN DESC) "Seq"
*/
	   'or3_cte' AS [extract]	
	
		FROM CLARITY.dbo.OR_CASE vsurg
	    INNER JOIN #rflpts cte ON vsurg.PAT_ID = cte.PAT_ID
		LEFT OUTER JOIN CLARITY.dbo.OR_LOG orl											ON orl.CASE_ID = vsurg.OR_CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb							ON lgb.CASE_ID=vsurg.OR_CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.PAT_OR_ADM_LINK lnk						ON lnk.CASE_ID = vsurg.OR_CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.	PAT_ENC_HSP	hsp						ON lnk.OR_LINK_CSN = hsp.PAT_ENC_CSN_ID
		LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT har							ON har.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
	    LEFT OUTER JOIN CLARITY.dbo.OR_CASE_2 cs2									ON vsurg.OR_CASE_ID=cs2.CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C
	WHERE 1=1
		AND 
			--(
			-- COALESCE(CAST(vsurg.TIME_SCHEDULED AS DATE), CAST(cs2.CASE_REQUESTED_DTTM AS DATE)) >=@StartDate 
			--	AND COALESCE(CAST(vsurg.TIME_SCHEDULED AS DATE), CAST(cs2.CASE_REQUESTED_DTTM AS DATE)) <=@EndDate
			--) 
			lgb.PROC_DATE >= cte.event_date

				
		--AND har.ASSOC_AUTHCERT_ID IS NOT NULL 
		AND (vsurg.SCHED_STATUS_C NOT IN ('2','5')  OR lgb.LOG_STATUS_C NOT IN ('4','6')) /* Not Voided or Canceled */
		--AND idx.IDENTITY_TYPE_ID='14'
		--AND (grp8.NAME =@FinDiv OR @FinDiv IS NULL )
		--AND  (@MRN IS NULL OR idx.IDENTITY_ID =@MRN)
		--AND ((ser.PROV_NAME LIKE @prov OR surg_2.PROV_NAME LIKE @prov) OR @prov IS NULL )
)
/*
SELECT
	ophov_cte.PAT_ID,
    ophov_cte.PAT_ENC_CSN_ID,
    ophov_cte.CONTACT_DATE,
    ophov_cte.APPT_TIME,
    ophov_cte.APPT_STATUS_NAME,
    ophov_cte.HOSP_ADMSN_TIME,
    ophov_cte.HOSP_DISCH_TIME,
    ophov_cte.Enc_Type,
    ophov_cte.HOSP_ADMSN_TYPE_NAME,
    ophov_cte.Prov_Id,
    ophov_cte.DEPARTMENT_ID,
    ophov_cte.DEPARTMENT_NAME,
    ophov_cte.REFERRAL_ID,
	ophov_cte.APPT_PRC_ID
FROM ophov_cte
ORDER BY
	ophov_cte.PAT_ID,
	ophov_cte.CONTACT_DATE
*/
/*
SELECT
	ip_cte.PAT_ID,
    ip_cte.PAT_ENC_CSN_ID,
	ip_cte.CONTACT_DATE,
    ip_cte.APPT_TIME,
    ip_cte.APPT_STATUS_NAME,
    ip_cte.HOSP_ADMSN_TIME,
    ip_cte.HOSP_DISCH_TIME,
    ip_cte.Enc_Type,
    ip_cte.HOSP_ADMSN_TYPE_NAME,
    ip_cte.Prov_Id,
    ip_cte.DEPARTMENT_ID,
    ip_cte.DEPARTMENT_NAME
FROM ip_cte
ORDER BY
	ip_cte.PAT_ID,
	ip_cte.CONTACT_DATE
*/
,
enc_cte AS (
SELECT
	ophov_ip.PAT_ID,
    ophov_ip.PAT_ENC_CSN_ID,
	ophov_ip.HSP_PAT_ENC_CSN_ID,
    ophov_ip.CONTACT_DATE,
    ophov_ip.APPT_TIME,
    ophov_ip.APPT_STATUS_NAME,
    ophov_ip.HOSP_ADMSN_TIME,
    ophov_ip.HOSP_DISCH_TIME,
    ophov_ip.Enc_Type,
    ophov_ip.HOSP_ADMSN_TYPE_NAME,
    ophov_ip.Prov_Id,
    ophov_ip.DEPARTMENT_ID,
    ophov_ip.DEPARTMENT_NAME,
    ophov_ip.REFERRAL_ID,
    --ophov_ip.APPT_PRC_ID,
    --ophov_ip.PROC_NAME
	ophov_ip.UOS,
	ophov_ip.[extract]
FROM
(
SELECT
	ophov_cte.PAT_ID,
    ophov_cte.PAT_ENC_CSN_ID,
    ophov_cte.HSP_PAT_ENC_CSN_ID,
    ophov_cte.CONTACT_DATE,
    ophov_cte.APPT_TIME,
    ophov_cte.APPT_STATUS_NAME,
    ophov_cte.HOSP_ADMSN_TIME,
    ophov_cte.HOSP_DISCH_TIME,
    ophov_cte.Enc_Type,
    ophov_cte.HOSP_ADMSN_TYPE_NAME,
    ophov_cte.Prov_Id,
    ophov_cte.DEPARTMENT_ID,
    ophov_cte.DEPARTMENT_NAME,
    ophov_cte.REFERRAL_ID,
 --   ophov_cte.APPT_PRC_ID,
	--ophov_cte.PROC_NAME
    ophov_cte.UOS,
	ophov_cte.[extract]
FROM ophov_cte
UNION ALL
SELECT
	ip_cte.PAT_ID,
    ip_cte.PAT_ENC_CSN_ID,
    ip_cte.HSP_PAT_ENC_CSN_ID,
    ip_cte.CONTACT_DATE,
    ip_cte.APPT_TIME,
    ip_cte.APPT_STATUS_NAME,
    ip_cte.HOSP_ADMSN_TIME,
    ip_cte.HOSP_DISCH_TIME,
    ip_cte.Enc_Type,
    ip_cte.HOSP_ADMSN_TYPE_NAME,
    ip_cte.Prov_Id,
    ip_cte.DEPARTMENT_ID,
    ip_cte.DEPARTMENT_NAME,
    NULL AS REFERRAL_ID,
    --NULL AS APPT_PRC_ID,
    --NULL AS PROC_NAME
    ip_cte.UOS,
	ip_cte.[extract]
FROM ip_cte
UNION ALL
/*
SELECT
	or1_cte.PAT_ID,
    or1_cte.PAT_ENC_CSN_ID,
    or1_cte.CONTACT_DATE,
    or1_cte.APPT_TIME,
    or1_cte.APPT_STATUS_NAME,
    or1_cte.HOSP_ADMSN_TIME,
    or1_cte.HOSP_DISCH_TIME,
    or1_cte.Enc_Type,
    or1_cte.HOSP_ADMSN_TYPE_NAME,
    or1_cte.Prov_Id,
    or1_cte.DEPARTMENT_ID,
    or1_cte.DEPARTMENT_NAME,
    NULL AS REFERRAL_ID,
    --NULL AS APPT_PRC_ID,
    --NULL AS PROC_NAME
    or1_cte.UOS,
	or1_cte.[extract]
FROM or1_cte
UNION ALL
SELECT
	or2_cte.PAT_ID,
    or2_cte.PAT_ENC_CSN_ID,
    or2_cte.CONTACT_DATE,
    or2_cte.APPT_TIME,
    or2_cte.APPT_STATUS_NAME,
    or2_cte.HOSP_ADMSN_TIME,
    or2_cte.HOSP_DISCH_TIME,
    or2_cte.Enc_Type,
    or2_cte.HOSP_ADMSN_TYPE_NAME,
    or2_cte.Prov_Id,
    or2_cte.DEPARTMENT_ID,
    or2_cte.DEPARTMENT_NAME,
    NULL AS REFERRAL_ID,
    --NULL AS APPT_PRC_ID,
    --NULL AS PROC_NAME
    or2_cte.UOS,
	or2_cte.[extract]
FROM or2_cte
*/
SELECT
	or3_cte.PAT_ID,
    or3_cte.PAT_ENC_CSN_ID,
    or3_cte.HSP_PAT_ENC_CSN_ID,
    or3_cte.CONTACT_DATE,
    or3_cte.APPT_TIME,
    or3_cte.APPT_STATUS_NAME,
    or3_cte.HOSP_ADMSN_TIME,
    or3_cte.HOSP_DISCH_TIME,
    or3_cte.Enc_Type,
    or3_cte.HOSP_ADMSN_TYPE_NAME,
    or3_cte.Prov_Id,
    or3_cte.DEPARTMENT_ID,
    or3_cte.DEPARTMENT_NAME,
    NULL AS REFERRAL_ID,
    --NULL AS APPT_PRC_ID,
    --NULL AS PROC_NAME
    or3_cte.UOS,
	or3_cte.[extract]
FROM or3_cte
) ophov_ip
)
--/*
SELECT
	--enc.person_id AS MRN,
    enc.PAT_ID AS Epic_Patient_Id,
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
	enc.[extract]
FROM enc_cte enc
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
WHERE enc.[extract] <> 'ophov_cte'
ORDER BY
	enc.PAT_ID,
	enc.PAT_ENC_CSN_ID,
	COALESCE(enc.APPT_TIME, enc.HOSP_ADMSN_TIME)
--*/
/*
,
proc_cte AS (
SELECT
	   op.ORDER_PROC_ID,
       op.PAT_ID,
       --op.PAT_ENC_DATE_REAL,
       op.PAT_ENC_CSN_ID,
       --op.RESULT_LAB_ID,
       op.ORDERING_DATE,
       op.ORDER_TYPE_C,
	   zot.NAME AS ORDER_TYPE_NAME,
       op.PROC_ID,
       --op.PROC_CODE,
       op.DESCRIPTION,
       op.ORDER_CLASS_C,
	   zoc.NAME AS ORDER_CLASS_NAME,
       op.AUTHRZING_PROV_ID,
       --op.ABNORMAL_YN,
       --op.BILLING_PROV_ID,
       --op.COSIGNER_USER_ID,
       --op.ORD_CREATR_USER_ID,
       --op.LAB_STATUS_C,
       op.ORDER_STATUS_C,
	   zos.NAME AS ORDER_STATUS_NAME,
       --op.MODIFIER1_ID,
       --op.MODIFIER2_ID,
       --op.MODIFIER3_ID,
       --op.MODIFIER4_ID,
       --op.QUANTITY,
       --op.REASON_FOR_CANC_C,
       --op.FUTURE_OR_STAND,
       --op.STANDING_EXP_DATE,
       --op.FUT_EXPECT_COMP_DT,
       --op.STANDING_OCCURS,
       --op.STAND_ORIG_OCCUR,
       --op.RESULT_TYPE,
       op.REFERRING_PROV_ID,
       op.REFD_TO_LOC_ID,
       op.REFD_TO_SPECLTY_C,
       op.REQUESTED_SPEC_C,
       op.RFL_PRIORITY,
       op.RFL_CLASS_C,
       op.RFL_TYPE_C,
       op.RSN_FOR_RFL_C,
       op.RFL_NUM_VIS,
       op.RFL_EXPIRE_DT,
       --op.INTERFACE_STAT_C,
       op.CPT_CODE,
       --op.UPDATE_DATE,
       op.SERV_AREA_ID,
       --op.ABN_NOTE_ID,
       --op.RADIOLOGY_STATUS_C,
       --op.INT_STUDY_C,
       --op.INT_STUDY_USER_ID,
       --op.TECHNOLOGIST_ID,
       --op.FILMS_USED,
       --op.FILM_SIZE_C,
       --op.NUMBER_OF_REPEATS,
       --op.DOSE,
       --op.PROC_BGN_TIME,
       --op.PROC_END_TIME,
       --op.RIS_TRANS_ID,
       op.ORDER_INST,
       op.DISPLAY_NAME,
       --op.HV_HOSPITALIST_YN,
       --op.PROV_STATUS,
       op.ORDER_PRIORITY_C,
       --op.CHRG_DROPPED_TIME,
       --op.PANEL_PROC_ID,
       --op.COSIGNER_AUTH_TIME,
       --op.COSIGNED_USER_ID,
       --op.STAND_INTERVAL,
       --op.DISCRETE_INTERVAL,
       op.INSTANTIATED_TIME,
       --op.INSTNTOR_USER_ID,
       --op.DEPT_REF_PROV_ID,
       op.SPECIALTY_DEP_C,
	   zsd.NAME AS SPECIALTY_DEP_NAME,
       --op.ORDERING_MODE,
       --op.SPECIMEN_TYPE_C,
       --op.SPECIMEN_SOURCE_C,
       --op.ORDER_TIME,
       --op.RESULT_TIME,
       --op.REVIEW_TIME,
       --op.IS_PENDING_ORD_YN,
       --op.PROC_START_TIME,
       --op.PROBLEM_LIST_ID,
       --op.RSLTS_INTERPRETER,
       --op.PROC_ENDING_TIME,
       --op.CM_PHY_OWNER_ID,
       --op.CM_LOG_OWNER_ID,
       --op.SPECIFIED_FIRST_TM,
       --op.SCHED_START_TM,
       --op.SESSION_KEY,
       --op.PROC_PERF_DEPT_ID,
       --op.PROC_PERF_PROV_ID,
       --op.PROC_PAT_CLASS_C,
       --op.PROC_LATERALITY_C,
       --op.PROC_POSSIBLE_YN,
       --op.PROC_DATE,
       --op.LABCORP_BILL_TYPE_C,
       --op.LABCORP_CLIENT_ID,
       --op.LABCORP_CONTROL_NUM,
       --op.NO_CHG_RSN_C,
       --op.MRK_RSLT_MSG_IMP_YN,
       --op.CHNG_ORDER_PROC_ID,
       --op.REC_ARCHIVED_YN
	   eap.PROC_CAT
  FROM [CLARITY].[dbo].[ORDER_PROC] op
  INNER JOIN #rflpts cte ON op.PAT_ID = cte.PAT_ID 
  LEFT OUTER JOIN CLARITY..ORDER_INSTANTIATED AS oi ON op.ORDER_PROC_ID = oi.ORDER_ID
  LEFT OUTER JOIN CLARITY..CLARITY_EAP AS eap ON op.PROC_ID = eap.PROC_ID
  LEFT OUTER JOIN CLARITY..ZC_ORDER_TYPE zot ON zot.ORDER_TYPE_C = op.ORDER_TYPE_C
  LEFT OUTER JOIN CLARITY..ZC_ORDER_CLASS zoc ON zoc.ORDER_CLASS_C = op.ORDER_CLASS_C
  LEFT OUTER JOIN CLARITY..ZC_ORDER_STATUS zos ON zos.ORDER_STATUS_C = op.ORDER_STATUS_C
  LEFT OUTER JOIN CLARITY..ZC_SPECIALTY_DEP zsd ON zsd.SPECIALTY_DEP_C = op.SPECIALTY_DEP_C
  --LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP  ON DEP.DEPARTMENT_ID = hsp.DEPARTMENT_ID
  --LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS SER  ON SER.PROV_ID = hsp.ADMISSION_PROV_ID

  --WHERE PAT_ID = 'Z245973'
  WHERE 1 = 1
  AND op.ORDERING_DATE >= cte.event_date
  AND (op.FUTURE_OR_STAND IS NULL OR oi.ORDER_ID IS  NULL)
  AND op.ORDER_STATUS_C <> 4 --Canceled
  AND op.ORDER_CLASS_C NOT IN (4, 45, 11, 2, 43, 63) -- Lab Collect, Historical, Unit Collect, Point Of Care, Clinic Collect, Discharge Instructions
  AND op.ORDER_TYPE_C NOT IN (5) -- Imaging
)

  SELECT
	 *
  FROM proc_cte
  ORDER BY
	proc_cte.PAT_ID,
	proc_cte.PAT_ENC_CSN_ID,
	proc_cte.INSTANTIATED_TIME
*/
/*SELECT DISTINCT (op.PAT_ID) AS PAT_ID
			FROM CLARITY..ORDER_PROC AS op 
			LEFT OUTER JOIN CLARITY..ORDER_INSTANTIATED AS oi 
			  ON op.ORDER_PROC_ID = oi.ORDER_ID
			LEFT OUTER JOIN CLARITY..CLARITY_EAP AS eap
			  ON op.PROC_ID = eap.PROC_ID
			WHERE op.ORDERING_DATE >= '12/09/2013'
			AND eap.PROC_CAT =  'CV ECHO ORDERABLES'
			AND (op.FUTURE_OR_STAND IS NULL  
			OR oi.ORDER_ID IS  NULL )
			AND op.ORDER_STATUS_C <> 4 --Canceled
	   UNION --Invasive patients
			SELECT DISTINCT(op.PAT_ID)
			FROM   CLARITY..ORDER_PROC AS op 
			INNER JOIN CLARITY..OR_CASE_ORDER_IDS AS CASEORD --links the order to the log
					ON op.ORDER_PROC_ID = CASEORD.ORDER_ID 
			INNER JOIN CLARITY..OR_LOG AS ol 
					ON CASEORD.CASE_ID = ol.CASE_ID 
			WHERE ol.SURGERY_DATE >= '12/09/2013'
*/
/*
SELECT
    rflencs.person_id,
	rflencs.PAT_ID,
	rflencs.REFERRAL_ID,
	--frstrfl.rfl_seq,
	CASE WHEN frstrfl.rfl_seq = 1 THEN 'Y' ELSE 'N' END AS frstrfl,
	--ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY REFERRAL_ID, APPT_MADE_DTTM) AS rfl_seq,
    rflencs.APPT_MADE_DATE,
    --rflencs.APPT_MADE_DTTM,
	rflencs.APPT_TIME,
    rflencs.PAT_ENC_CSN_ID,
    event_date,
    rflencs.HOSP_ADMSN_TIME,
    rflencs.HOSP_ADMSN_TYPE_NAME,
    rflencs.HOSP_DISCHRG_TIME,
    rflencs.ENC_TYPE_NAME,
    fmonth_num,
    Fyear_num,
    FYear_name,
    report_period,
    report_date,
    rflencs.REFERRED_BY_DEPT_ID,
    rflencs.REFERRED_BY_DEPT_NAME,
    person_name,
    provider_id,
    provider_name,
    ENTRY_DATE,
    EXP_DATE,
    REFERRAL_CLASS,
    REFERRAL_TYPE,
    REFERRED_TO_DEPT_ID,
    REFERRED_TO_DEPT_NAME,
    REFERRED_TO_PROV_ID,
    REFERRED_TO_PROV_NAME,
    REFERRED_TO_PROV_SPEC,
    PATIENT_NAME,
    CONTACT_DATE,
    APPT_STATUS_C,
    APPT_STATUS_NAME,
    service_line_id,
    service_line,
    POD_ID,
    PFA_POD

INTO #rflencs

FROM #rflpts rflencs
LEFT OUTER JOIN
(
SELECT
	person_id,
	REFERRAL_ID,
	--APPT_MADE_DTTM,
	APPT_TIME,
	PAT_ENC_CSN_ID,
	--ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY REFERRAL_ID, APPT_MADE_DTTM, PAT_ENC_CSN_ID) AS rfl_seq
	ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY REFERRAL_ID, APPT_TIME, PAT_ENC_CSN_ID) AS rfl_seq
FROM #rflpts
) frstrfl
ON frstrfl.person_id = rflencs.person_id
AND frstrfl.REFERRAL_ID = rflencs.REFERRAL_ID
--AND frstrfl.APPT_MADE_DTTM = rflencs.APPT_MADE_DTTM
AND frstrfl.APPT_TIME = rflencs.APPT_TIME
AND frstrfl.PAT_ENC_CSN_ID = rflencs.PAT_ENC_CSN_ID
--AND frstrfl.rfl_seq = 1

ORDER BY
	person_id,
	REFERRAL_ID,
	APPT_TIME,
	PAT_ENC_CSN_ID
	
  -- Create index for temp table #rflencs

  --CREATE CLUSTERED INDEX IX_rflpts ON #rflpts (person_id, REFERRAL_ID, APPT_MADE_DTTM, PAT_ENC_CSN_ID)
  CREATE CLUSTERED INDEX IX_rflencs ON #rflencs (person_id, REFERRAL_ID, APPT_TIME, PAT_ENC_CSN_ID)

--SELECT
--	*
--FROM #rflencs
--ORDER BY
--	person_id,
--	REFERRAL_ID,
--	APPT_TIME,
--	PAT_ENC_CSN_ID

--;WITH cte_rflpts AS
--(
SELECT
	rflpts.person_id,
	rflpts.PAT_ID,
    rflpts.REFERRAL_ID,
	rflpts.ENTRY_DATE,
    ptrfls.REFERRAL_IDS
INTO #pts
FROM
(
SELECT
	person_id,
	PAT_ID,
	REFERRAL_ID,
	ENTRY_DATE
FROM #rflencs
WHERE frstrfl = 'Y'
) rflpts
LEFT OUTER JOIN
(
SELECT DISTINCT
	rflencs.person_id
--, (SELECT COALESCE(MAX(CAST(rfl.REFERRAL_ID AS VARCHAR(10))),'')  + ',' AS [text()]
, (SELECT COALESCE(MAX(CAST(rfl.REFERRAL_ID AS VARCHAR(10))),'')  + '|' AS [text()]
	FROM #rflencs rfl
	INNER JOIN
	(
	SELECT
		person_id,
		REFERRAL_ID
	FROM #rflencs
	WHERE frstrfl = 'Y'
	) frstrfl
	ON rfl.person_id = frstrfl.person_id
	WHERE rfl.person_id = rflencs.person_id
	AND rfl.REFERRAL_ID <> frstrfl.REFERRAL_ID
	GROUP BY rfl.person_id
		    , rfl.REFERRAL_ID
	FOR XML PATH ('')
	) AS REFERRAL_IDS
FROM #rflencs rflencs
) ptrfls
ON ptrfls.person_id = rflpts.person_id
--WHERE rflpts.person_id = 10004
ORDER BY
	PAT_ID
--)
	
  -- Create index for temp table #pts

  CREATE CLUSTERED INDEX IX_pts ON #pts (PAT_ID)

--SELECT
--	*
----FROM cte_rflpts
--FROM #pts cte_rflpts
--ORDER BY
--	cte_rflpts.person_id

--/*
;WITH ophov_cte AS (
SELECT
	cte.person_id,
	cte.PAT_ID,
	cte.REFERRAL_ID,
	cte.REFERRAL_IDS,
    ENC.PAT_ENC_CSN_ID,
	ENC.CONTACT_DATE,
	cte.ENTRY_DATE,
    --ENC.APPT_MADE_DATE,
	ENC.APPT_TIME,
	--ENC.APPT_STATUS_C,
	CASE WHEN ENC.APPT_STATUS_C = 1 THEN 'SCHEDULED'
			WHEN ENC.APPT_STATUS_C = 2 THEN 'COMPLETED'
			WHEN ENC.APPT_STATUS_C = 3 THEN 'CANCELED'
			WHEN ENC.APPT_STATUS_C = 4 THEN 'NO_SHOW'
			WHEN ENC.APPT_STATUS_C = 6 THEN 'ARRIVED'
			ELSE NULL
	END AS APPT_STATUS_NAME,
	--ENC.HOSP_ADMSN_TIME,
	NULL AS HOSP_ADMSN_TIME,
	--ENC.HOSP_DISCHRG_TIME,
	NULL AS HOSP_DISCH_TIME,
	--ENC.ENC_TYPE_C,
	--zdet.NAME AS ENC_TYPE_NAME,
	zdet.NAME AS Enc_Type,
	--ENC.HOSP_ADMSN_TYPE_C,
	zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
	--ENC.VISIT_PROV_ID,
	ENC.VISIT_PROV_ID AS Prov_Id,
	--ENC.ATTND_PROV_ID,
	ENC.DEPARTMENT_ID,
	DEP.DEPARTMENT_NAME

--SELECT DISTINCT
--	ENC.ENC_TYPE_C,
--	zdet.NAME AS ENC_TYPE_NAME
    FROM CLARITY.dbo.PAT_ENC ENC
    LEFT OUTER JOIN CLARITY.dbo.PATIENT pt ON pt.PAT_ID = ENC.PAT_ID
	--INNER JOIN cte_rflpts cte ON pt.PAT_ID = cte.PAT_ID
	INNER JOIN #pts cte ON pt.PAT_ID = cte.PAT_ID
	LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = ENC.HOSP_ADMSN_TYPE_C
	LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet ON zdet.DISP_ENC_TYPE_C = ENC.ENC_TYPE_C
    LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP  ON DEP.DEPARTMENT_ID = ENC.DEPARTMENT_ID

	WHERE ENC.CONTACT_DATE >= cte.ENTRY_DATE
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

	--ORDER BY
	--	cte.person_id,
	--	ENC.CONTACT_DATE

	--ORDER BY
	--	ENC.ENC_TYPE_C
),
--*/
ip_cte AS (
SELECT
	   cte.person_id,
	   hsp.[PAT_ID],
	   cte.REFERRAL_ID,
	   cte.REFERRAL_IDS,
	   hsp.PAT_ENC_CSN_ID,
       [CONTACT_DATE],
	   cte.ENTRY_DATE,
	   NULL AS APPT_TIME,
	   NULL AS APPT_STATUS_NAME,
       [HOSP_ADMSN_TIME]
      ,[HOSP_DISCH_TIME]
      --,hsp.[ADT_PAT_CLASS_C]
	  --,zpl.NAME AS ADT_PAT_CLASS_NAME
	  ,zpl.NAME AS Enc_Type
      --,hsp.[HOSP_ADMSN_TYPE_C]
	  ,zhat.NAME AS HOSP_ADMSN_TYPE_NAME
   --   ,[ADT_PATIENT_STAT_C]
   --   ,hsp.[ADMIT_SOURCE_C]
	  --,zas.NAME AS ADMIT_SOURCE_NAME
   --   ,[DISCHARGE_PROV_ID]
      --,[ADMISSION_PROV_ID]
      ,[ADMISSION_PROV_ID] AS Prov_Id
	  --,SER.PROV_NAME AS ADMISSION_PROV_NAME
   --   ,hsp.[HOSP_ADMSN_TYPE_C]
	  --,zhat.NAME AS HOSP_ADMSN_TYPE_NAME
      ,hsp.[DEPARTMENT_ID]
	  ,DEP.DEPARTMENT_NAME
      --,[HSP_ACCOUNT_ID]
      --,[INPATIENT_DATA_ID]
      --,[OP_ADM_DATE]
      --,[OP_ADM_EVENT_ID]
      --,[EMER_ADM_EVENT_ID]
  FROM [CLARITY].[dbo].[PAT_ENC_HSP] hsp
  INNER JOIN #pts cte ON hsp.PAT_ID = cte.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS zpl ON zpl.ADT_PAT_CLASS_C = hsp.ADT_PAT_CLASS_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_ADM_SOURCE zas ON zas.ADMIT_SOURCE_C = hsp.ADMIT_SOURCE_C
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP  ON DEP.DEPARTMENT_ID = hsp.DEPARTMENT_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS SER  ON SER.PROV_ID = hsp.ADMISSION_PROV_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C

  --WHERE PAT_ID = 'Z245973'
  WHERE 1 = 1
  AND CAST(hsp.HOSP_ADMSN_TIME AS DATE) >= cte.ENTRY_DATE
  AND hsp.ADMISSION_PROV_ID IS NOT NULL
 -- ORDER BY
	--cte.person_id,
	--hsp.HOSP_ADMSN_TIME
),
enc_cte AS (
SELECT
	*
FROM
(
SELECT
	*
FROM ophov_cte
UNION ALL
SELECT
	*
FROM ip_cte
) ophov_ip
)

SELECT
	enc.person_id AS MRN,
    enc.PAT_ID AS Epic_Patient_Id,
    --enc.REFERRAL_ID,
	--rflencs.REFERRAL_ID AS ENCOUNTER_REFERRAL_ID,
	ISNULL(CONVERT(VARCHAR(100),rflencs.REFERRAL_ID),'') AS Encounter_Referral_Id,
    --CASE WHEN rflpts.REFERRAL_ID IS NOT NULL THEN enc.REFERRAL_ID ELSE NULL END AS EARLIEST_REFERRAL_ID,
    enc.REFERRAL_ID AS Earliest_Referral_Id,
	--LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) AS REFERRAL_IDS,
	--CASE WHEN rflpts.REFERRAL_ID IS NOT NULL THEN LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) ELSE NULL END AS ADDITIONAL_REFERRAL_IDS,
	--LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) AS ADDITIONAL_REFERRAL_IDS,
	ISNULL(CONVERT(VARCHAR(200),LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1)),'')  AS Additional_Referral_Ids,
	--ISNULL(CONVERT(VARCHAR(200),'"' + LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1) + '"'),'')  AS Additional_Referral_Ids,
	--ISNULL(CONVERT(VARCHAR(200),LEFT(enc.REFERRAL_IDS,LEN(enc.REFERRAL_IDS) - 1)),'')  AS Additional_Referral_Ids,
    enc.PAT_ENC_CSN_ID AS Encounter_CSN,
    enc.CONTACT_DATE AS Contact_Date,
    enc.ENTRY_DATE AS Referral_Entry_Date,
    --enc.APPT_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.APPT_TIME,121),'') AS Appointment_Time,
    --enc.APPT_STATUS_NAME,
    ISNULL(CONVERT(VARCHAR(100),enc.APPT_STATUS_NAME),'') AS Appointment_Status,
    --enc.HOSP_ADMSN_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.HOSP_ADMSN_TIME,121),'') AS Hosp_Admsn_Time,
    --enc.HOSP_DISCH_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.HOSP_DISCH_TIME,121),'') AS Hosp_Disch_Time,
    enc.Enc_Type,
    enc.HOSP_ADMSN_TYPE_NAME AS Hosp_Admsn_Type,
    enc.Prov_Id,
    enc.DEPARTMENT_ID AS Department_Id,
    enc.DEPARTMENT_NAME AS Department_Name,
    '"' + TRIM(ser.PROV_NAME) + '"' AS Provider_Name,
    ser.PROV_TYPE AS Provider_Type,
    ser.STAFF_RESOURCE Provider_Resource_Type,
	--zsp.NAME AS SPECIALTY_NAME,
    ISNULL(CONVERT(VARCHAR(100),zsp.NAME),'') AS Provider_Specialty,
	--dvsn.Epic_Financial_Division,
    ISNULL(CONVERT(VARCHAR(100),dvsn.Epic_Financial_Division),'') AS Epic_Financial_Division,
	--dvsn.Epic_Financial_Subdivision
    ISNULL(CONVERT(VARCHAR(100),dvsn.Epic_Financial_Subdivision),'') AS Epic_Financial_Subdivision
FROM enc_cte enc
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
LEFT OUTER JOIN #pts rflpts
	ON rflpts.person_id = enc.person_id
LEFT OUTER JOIN #rflencs rflencs
	ON rflencs.person_id = enc.person_id
	AND rflencs.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
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
ORDER BY
	enc.PAT_ID,
	enc.PAT_ENC_CSN_ID,
	COALESCE(enc.APPT_TIME, enc.HOSP_ADMSN_TIME)
*/
GO


