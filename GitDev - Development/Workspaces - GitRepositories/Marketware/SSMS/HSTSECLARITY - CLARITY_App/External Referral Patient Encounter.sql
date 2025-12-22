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
   RFL.REFERRAL_ID,
   RFL.ENTRY_DATE AS event_date,
   date_dim.fmonth_num,
   date_dim.Fyear_num,
   date_dim.FYear_name,
   CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
   CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date,
   RFL.REFD_BY_DEPT_ID AS REFERRED_BY_DEPT_ID,
   REFERRING_DEP.DEPARTMENT_NAME AS REFERRED_BY_DEPT_NAME,
   RFL.PAT_ID,
   CAST(PATIENT.PAT_MRN_ID AS INTEGER) AS person_id,
   PATIENT.PAT_NAME AS person_name,
   REFERRAL_SOURCE.REF_PROVIDER_ID AS provider_id,
   REFERRING_SER.PROV_NAME AS provider_name,
   RFL.ENTRY_DATE,
   RFL.EXP_DATE,
   --AUTH_CHANGE.CHANGE_DATETIME AS AUTH_DATE,
   ZC_RFL_CLASS.NAME AS REFERRAL_CLASS,
   ZC_RFL_TYPE.NAME AS REFERRAL_TYPE,
   CLARITY_DEP.DEPARTMENT_ID AS REFERRED_TO_DEPT_ID,
   CLARITY_DEP.DEPARTMENT_NAME AS REFERRED_TO_DEPT_NAME,
   CLARITY_SER.PROV_ID AS REFERRED_TO_PROV_ID,
   CLARITY_SER.PROV_NAME AS REFERRED_TO_PROV_NAME,
   ZC_SPECIALTY.NAME AS REFERRED_TO_PROV_SPEC,
   PATIENT.PAT_NAME AS PATIENT_NAME,
   --SCHED_CHANGE.CHANGE_DATETIME AS RFL_CHANGE_DTTM,
   --SCHED_CHANGE.CHANGE_TYPE_NAME AS RFL_CHANGE_TYPE,
   --SCHED_CHANGE.CHANGE_USER_ID AS RFL_CHANGE_USER_ID,
   --SCHED_CHANGE.CHANGE_USER_NAME AS RFL_CHANGE_USER,
   --SCHED_CHANGE.PREVIOUS_VALUE AS RFL_CHANGE_TEXT,
   --RFL_APPTS.ACTUAL_CNT AS SCHEDULED_VISITS,
   --RFL_APPTS.APPT_MADE_DTTM AS FIRST_APPT_MADE,
   --RFL_APPTS.APPT_DATE AS FIRST_APPT,
   --RFL_APPTS.COMPLETED_CNT,
   --RFL_APPTS.CANCELED_CNT,
   --RFL_APPTS.RFL_ID,
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
   mdm.service_line_id,
   mdm.service_line,
   mdm.POD_ID,
   mdm.PFA_POD

INTO #rflpts

FROM 
   CLARITY.dbo.REFERRAL RFL
   LEFT OUTER JOIN CLARITY.dbo.REFERRAL_3 ON RFL.REFERRAL_ID=REFERRAL_3.REFERRAL_ID
   LEFT OUTER JOIN CLARITY.dbo.PATIENT ON RFL.PAT_ID = PATIENT.PAT_ID
   LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP ON RFL.REFD_TO_DEPT_ID = CLARITY_DEP.DEPARTMENT_ID
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
    ON RFL.REFD_BY_DEPT_ID = mdm.epic_department_id
   
WHERE (REFERRAL_3.AUTH_CERT_YN IS NULL OR REFERRAL_3.AUTH_CERT_YN='N')
      AND RFL.ACTUAL_NUM_VISITS IS NOT NULL
	  --AND (RFL.EXP_DATE >= @CompletedStartDate AND RFL.EXP_DATE <= @CompletedEndDate)
	  --AND (RFL.EXP_DATE >= @locstartdate  AND RFL.EXP_DATE <= @locenddate)
	  --AND (RFL.ENTRY_DATE >= @locstartdate)
	  AND (RFL.ENTRY_DATE >= @locstartdate  AND RFL.ENTRY_DATE <= @locenddate)
      --AND (COALESCE(mdm.' + @DepartmentGrouperColumn + ',''' + @DepartmentGrouperNoValue + ''') IN (SELECT pod_Service_Line FROM cte_pods_servLine)) AND (RFL.REFD_BY_DEPT_ID IN (SELECT DepartmentId FROM cte_depid))
	  AND RFL.RFL_TYPE_C NOT IN ('42','510','102','502') -- Home Health Care, Home Health Pharmacy, Insurance Referral, Lab
	  AND ZC_RFL_CLASS.NAME = 'Incoming'
	  AND RFL_APPTS.APPT_MADE_DATE >= RFL.ENTRY_DATE

ORDER BY
	person_id,
	REFERRAL_ID,
	APPT_TIME,
	RFL_APPTS.PAT_ENC_CSN_ID
	
  -- Create index for temp table #rflpts

  --CREATE CLUSTERED INDEX IX_rflpts ON #rflpts (person_id, REFERRAL_ID, APPT_MADE_DTTM, PAT_ENC_CSN_ID)
  CREATE CLUSTERED INDEX IX_rflpts ON #rflpts (person_id, REFERRAL_ID, APPT_TIME, PAT_ENC_CSN_ID)

--SELECT DISTINCT
--	REFERRAL_TYPE
--FROM #rflpts
--ORDER BY
--	REFERRAL_TYPE

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

GO


