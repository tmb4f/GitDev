USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME = NULL, @EndDate SMALLDATETIME = NULL

--SET @StartDate = '10/1/2024 00:00'
--SET @StartDate = '1/1/2025 00:00'
--SET @StartDate = '7/1/2025 00:00'
SET @StartDate = '9/22/2025 00:00'
--SET @EndDate = CAST(CAST(DATEADD(DAY, -1, GETDATE()) AS DATE) AS SMALLDATETIME);
--SET @EndDate = '12/31/2024 00:00'
--SET @EndDate = '4/30/2025 00:00'
SET @EndDate = '9/30/2025 00:00'


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

DECLARE @YestDate SMALLDATETIME;

SET @YestDate = CAST(CAST(DATEADD(DAY, -1, @CurrDate) AS DATE) AS SMALLDATETIME);

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

IF OBJECT_ID('tempdb..#chgs ') IS NOT NULL
DROP TABLE #chgs

IF OBJECT_ID('tempdb..#chgs2 ') IS NOT NULL
DROP TABLE #chgs2

IF OBJECT_ID('tempdb..#rflencs ') IS NOT NULL
DROP TABLE #rflencs

IF OBJECT_ID('tempdb..#pts ') IS NOT NULL
DROP TABLE #pts

IF OBJECT_ID('tempdb..#encs ') IS NOT NULL
DROP TABLE #encs

IF OBJECT_ID('tempdb..#encs_plus ') IS NOT NULL
DROP TABLE #encs_plus

IF OBJECT_ID('tempdb..#encs_plus_plus ') IS NOT NULL
DROP TABLE #encs_plus_plus

IF OBJECT_ID('tempdb..#ip_proc ') IS NOT NULL
DROP TABLE #ip_proc

IF OBJECT_ID('tempdb..#or_proc ') IS NOT NULL
DROP TABLE #or_proc

IF OBJECT_ID('tempdb..#op_proc ') IS NOT NULL
DROP TABLE #op_proc

IF OBJECT_ID('tempdb..#RptgTbl ') IS NOT NULL
DROP TABLE #RptgTbl

IF OBJECT_ID('tempdb..#RptgTbl2 ') IS NOT NULL
DROP TABLE #RptgTbl2

--	SELECT DISTINCT
--			   rmlmh.EPIC_DEPARTMENT_ID
--			  ,rmlmh.EPIC_DEPT_TYPE
--			  ,rmlmh.SERVICE_LINE_ID
--			  ,rmlmh.SERVICE_LINE
--			  ,rmlmh.OPNL_SERVICE_ID
--			  ,rmlmh.OPNL_SERVICE_NAME
--			  ,rmlmh.HS_AREA_ID
--			  ,rmlmh.HS_AREA_NAME
--			  ,rmlmh.POD_ID
--			  ,rmlmh.PFA_POD

--		INTO #mdm

--		FROM dbo.Ref_MDM_Location_Master_History AS rmlmh
--			INNER JOIN
--			( --hx--most recent batch date per dep id
--				SELECT mdmhx.EPIC_DEPARTMENT_ID
--					  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
--				FROM dbo.Ref_MDM_Location_Master_History AS mdmhx
--				GROUP BY mdmhx.EPIC_DEPARTMENT_ID
--			)                                                 AS hx
--				ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
--				   AND rmlmh.BATCH_RUN_DT = hx.max_dt

--ORDER BY 
--         rmlmh.EPIC_DEPARTMENT_ID

-- CREATE UNIQUE CLUSTERED INDEX IX_mdm ON #mdm (EPIC_DEPARTMENT_ID)

SELECT DISTINCT
    mdmhsp.[EPIC_DEPARTMENT_ID]
				  , mdmhsp.[REV_LOC_ID]
				  , mdmhsp.[REV_LOC_NAME]
				  , mdmhsp.[HOSPITAL_CODE]
				  , mdmhsp.[DE_HOSPITAL_CODE]
				  , mdmhsp.[HOSPITAL_GROUP]
				  , mdmhsp.[LOC_RPT_GRP_NINE_NAME]
				  , mdmhsp.[LOC_RPT_GRP_NINE]
				  , mdmhsp.[RECORD_STATUS]
				  , mdmhsp.[RECORD_STATUS_TITLE]

INTO #mdmhsp

FROM [CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group_ALL_History] mdmhsp
    INNER JOIN
    ( --hx--most recent update date per dep id
				SELECT mdmhx.EPIC_DEPARTMENT_ID
						, MAX(mdmhx.Update_Dtm) AS max_dt
    FROM [Rptg].[vwRef_MDM_Location_Master_Hospital_Group_ALL_History] AS mdmhx
    GROUP BY mdmhx.EPIC_DEPARTMENT_ID
			)                                                 AS hx
    ON hx.EPIC_DEPARTMENT_ID = mdmhsp.EPIC_DEPARTMENT_ID
        AND mdmhsp.Update_Dtm = hx.max_dt
--WHERE RECORD_STATUS_TITLE = 'ACTIVE'

ORDER BY 
          mdmhsp.EPIC_DEPARTMENT_ID

CREATE UNIQUE CLUSTERED INDEX IX_mdmhsp ON #mdmhsp (EPIC_DEPARTMENT_ID)

DECLARE @ORtoHospital TABLE
(
    [Department_Id] NUMERIC(18,0),
    [Department_Name] VARCHAR(254),
    [Hospital_Code] VARCHAR(7)
)

INSERT INTO @ORtoHospital
    (
    Department_Id,
    Department_Name,
    Hospital_Code
    )
VALUES
    (10104500, 'UVHE CARDIAC CATH AND EP LABS', 'UVA-MC')
,    (11804503, 'PWMC CARDIAC CATH AND EP LABS', 'UVA-PW')
,    (1071024300, 'UVHE Main OR', 'UVA-MC')
,    (1071024302, 'UVHE Womens Center OR', 'UVA-MC')
,    (1071024304, 'UVHE ENDOSCOPY/BRONCHOSCOPY SUITE', 'UVA-MC')
,    (1071024305, 'UVA GI TRAVEL', 'UVA-MC')
,    (1071029500, 'CPSA OR', 'UVA-CP')
,    (1071035400, 'UVBB Outpatient Surgery Center', 'UVA-MC')
,    (1071035402, 'CPSN OPSC OR', 'UVA-CP')
,    (1071036300, 'CVSL SURGICAL CARE RIVERSIDE OR', 'UVA-SCR')
,    (1071037900, 'UVML Endoscopy/Bronchoscopy Suite', 'UVA-MC')
,    (1071038800, 'PWMC OR', 'UVA-PW')
,    (1071038801, 'PWMC L&D OR', 'UVA-PW')
,    (1071038802, 'PWMC ENDOSCOPY', 'UVA-PW')
,    (1071041900, 'OCIR IVY ROAD OR', 'UVA-MC')
,    (1071074300, 'HYMC OR', 'UVA-HM')
,    (1071074302, 'HYMC ENDOSCOPY', 'UVA-HM')
;

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
    --REFERRING_DEP.DEPARTMENT_NAME AS REFERRED_BY_DEPT_NAME,
    RFL.PAT_ID,
    CAST(PT.PAT_MRN_ID AS INTEGER) AS person_id,
    PT.PAT_NAME AS person_name,
    REFERRAL_SOURCE.REF_PROVIDER_ID AS provider_id,
    REFERRING_SER.Prov_Nme AS provider_name,
    REFERRING_SER.NPI,
    RFL.ENTRY_DATE,
    RFL.EXP_DATE,
    --AUTH_CHANGE.CHANGE_DATETIME AS AUTH_DATE,
    ZC_RFL_CLASS.NAME AS REFERRAL_CLASS,
    ZC_RFL_TYPE.NAME AS REFERRAL_TYPE,
    --REFERRED_TO_DEP.DEPARTMENT_ID AS REFERRED_TO_DEPT_ID,
    --REFERRED_TO_DEP.DEPARTMENT_NAME AS REFERRED_TO_DEPT_NAME,
    --CLARITY_SER.PROV_ID AS REFERRED_TO_PROV_ID,
    --CLARITY_SER.PROV_NAME AS REFERRED_TO_PROV_NAME,
    ZC_SPECIALTY.NAME AS REFERRED_TO_PROV_SPEC,
    PT.PAT_NAME AS PATIENT_NAME--,
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
--mdm.service_line_id,
--mdm.service_line,
--mdm.POD_ID,
--mdm.PFA_POD

INTO #rfls

FROM
    CLARITY.dbo.REFERRAL RFL
    LEFT OUTER JOIN CLARITY.dbo.REFERRAL_3 ON RFL.REFERRAL_ID=REFERRAL_3.REFERRAL_ID
    LEFT OUTER JOIN CLARITY.dbo.PATIENT AS PT ON RFL.PAT_ID = PT.PAT_ID
    --LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS REFERRED_TO_DEP ON RFL.REFD_TO_DEPT_ID = REFERRED_TO_DEP.DEPARTMENT_ID
    --LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ON RFL.REFERRAL_PROV_ID = CLARITY_SER.PROV_ID 
    LEFT OUTER JOIN CLARITY.dbo.ZC_SPECIALTY ON RFL.PROV_SPEC_C = ZC_SPECIALTY.SPECIALTY_C
    LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_TYPE ON RFL.RFL_TYPE_C = ZC_RFL_TYPE.RFL_TYPE_C
    LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_CLASS ON RFL.RFL_CLASS_C = ZC_RFL_CLASS.RFL_CLASS_C
    LEFT OUTER JOIN CLARITY.dbo.ZC_RFL_STATUS ON RFL.RFL_STATUS_C = ZC_RFL_STATUS.RFL_STATUS_C
    LEFT OUTER JOIN CLARITY.dbo.REFERRAL_SOURCE ON RFL.REFERRING_PROV_ID = REFERRAL_SOURCE.REFERRING_PROV_ID
    --LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS REFERRING_SER  ON REFERRING_SER.PROV_ID = REFERRAL_SOURCE.REF_PROVIDER_ID
    LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_SERsrc REFERRING_SER ON REFERRING_SER.PROV_ID = REFERRAL_SOURCE.REF_PROVIDER_ID
    --LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS REFERRING_DEP  ON REFERRING_DEP.DEPARTMENT_ID = RFL.REFD_BY_DEPT_ID
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
--LEFT OUTER JOIN #mdm  mdm
----ON RFL.REFD_BY_DEPT_ID = mdm.epic_department_id
--ON REFERRED_TO_DEP.DEPARTMENT_ID = mdm.epic_department_id

WHERE (REFERRAL_3.AUTH_CERT_YN IS NULL OR REFERRAL_3.AUTH_CERT_YN='N')
    AND RFL.ACTUAL_NUM_VISITS IS NOT NULL
    --AND (RFL.EXP_DATE >= @CompletedStartDate AND RFL.EXP_DATE <= @CompletedEndDate)
    --AND (RFL.EXP_DATE >= @locstartdate  AND RFL.EXP_DATE <= @locenddate)
    --AND (RFL.ENTRY_DATE >= @locstartdate)
    AND (RFL.ENTRY_DATE >= @locstartdate AND RFL.ENTRY_DATE <= @locenddate)
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

SELECT --DISTINCT
    PAT_ID,
    person_id,
    PATIENT_NAME,
    event_date -- Referral entry date
INTO #rflpts
FROM #rfls
WHERE rflseq = 1 -- Earliest referral entry date for a patient
--AND PAT_ID = 'Z1001295'
ORDER BY
	PAT_ID

-- Create index for temp table #rflpts

CREATE CLUSTERED INDEX IX_rflpts ON #rflpts (PAT_ID)
/*
SELECT
	rflpts.PAT_ID,
	rflpts.person_id,
	enc.HSP_ACCOUNT_ID,
	--cpt.HSP_ACCOUNT_ID,
    cpt.LINE,
    cpt.CPT_CODE,
    cpt.CPT_CODE_DATE,
    cpt.CPT_PERF_PROV_ID,
    --cpt.CPT_EVENT_NUMBER,
    --cpt.CPT_MODIFIERS,
    --cpt.LMRP_CODE,
    cpt.CPT_CODE_DESC,
    --cpt.CM_PHY_OWNER_ID,
    --cpt.CM_LOG_OWNER_ID,
    --cpt.PX_APC_PMT_STS_IND,
    --cpt.PX_CODE_AFF_DRG_YN,
    --cpt.PX_APC_PMT_IND,
    --cpt.PX_APC_WEIGHT,
    --cpt.PX_APC_FAC_RMB_AMT,
    --cpt.PX_OCE_EDIT_CODE,
    --cpt.PX_APC_CODE,
    --cpt.PX_HCFA_PAYMT_AMT,
    --cpt.PX_REIMB_TYPE,
    --cpt.PX_COPAY_AMT,
    --cpt.PX_PAY_RT_UNIT_AMT,
    --cpt.PX_REV_CODE_ID,
    --cpt.CPT_EXCLD_RPT_YN,
    cpt.CPT_QUANTITY--,
    --cpt.CPT_POS_TYPE_C
FROM #rflpts rflpts
LEFT OUTER JOIN  CLARITY.dbo.PAT_ENC enc
	ON enc.PAT_ID = rflpts.PAT_ID
LEFT OUTER JOIN CLARITY.dbo.HSP_ACCT_CPT_CODES cpt
	ON cpt.HSP_ACCOUNT_ID =enc.HSP_ACCOUNT_ID
WHERE enc.CONTACT_DATE >= rflpts.event_date AND enc.CONTACT_DATE <= CAST(GETDATE() AS DATE)
ORDER BY
	rflpts.PAT_ID,
	rflpts.event_date,
	enc.HSP_ACCOUNT_ID,
	enc.CONTACT_DATE,
	cpt.LINE
*/
--/*
SELECT	c.PAT_ID
      , c.EncounterNumber
      , c.PAT_ENC_CSN_ID
	  , c.CPTCode
	  --, c.CPT_CODE_DESC
	  , c.CPTCodeSequence
	  , c.CPTDate
	  , ISNULL(c.CPTModifier1, '') AS CPTModifier1
	  , ISNULL(c.CPTModifier2, '') AS CPTModifier2
	  , ISNULL(c.CPTModifier3, '') AS CPTModifier3
	  , c.CPTCharge
	  , CAST(c.QUANTITY AS INT) AS CPTQuantity
	  , c.DFLT_PROC_DESC
	  , c.PROCEDURE_DESC
	  , c.PROC_ID
	  , c.PROC_NAME
	  , c.PT_FRIENDLY_NAME
	  , c.PROC_CODE
	  , c.PROC_CAT
	  , c.TYPE_C
	  , zpt.NAME AS TYPE_NAME
	  , c.PROC_TYPE
	  , c.ORDER_ID
	  , op.ORDER_CLASS_C
	  , zoc.NAME AS ORDER_CLASS_NAME
	  , op.ORDER_TYPE_C
	  , zot.NAME AS ORDER_TYPE_NAME
	  , c.ORDER_DISPLAY_NAME
	  , c.BILLING_CAT_C
	  , zbc.NAME AS BILLING_CAT_NAME
	  , op.AUTHRZING_PROV_ID
	  , op.REFERRING_PROV_ID
	  , op.PROC_PERF_PROV_ID
	  , COUNT(*) OVER(PARTITION BY c.PAT_ENC_CSN_ID) AS CSN_COUNT
	  /*
        lgb2.IN_OR_DTTM,
        lgb2.OUT_OR_DTTM*/
	  , NULL	AS	CASE_ID
	  , NULL	AS	LOG_ID
	  --, lnk.CASE_ID
	  --, lgb.LOG_ID,
   --     lgb.CASE_ID,
   --     --lgb.PAT_ID,
   --     lgb.PAT_AGE,
   --     lgb.PATIENT_CLASS_C,
      , NULL	AS	PATIENT_CLASS_NM
   --     lgb.PATIENT_CLASS_NM,
      , NULL	AS	PATIENT_CLASS_GROUP
   --     lgb.PATIENT_CLASS_GROUP,
   --     lgb.CASE_CLASS_C,
      , NULL	AS	CASE_CLASS_NM
   --     lgb.CASE_CLASS_NM,
   --     lgb.LOG_STATUS_C,
      , NULL	AS	LOG_STATUS_NM
   --     lgb.LOG_STATUS_NM,
   --     lgb.ADD_ON_CASE_MAN_YN,
   --     lgb.ADD_ON_CASE_SCH_YN,
      , NULL	AS	PRIMARY_PHYSICIAN_ID
   --     lgb.PRIMARY_PHYSICIAN_ID,
   --     lgb.PRIMARY_PHYSICIAN_NM,
   --     lgb.PRIMARY_PHYSICIAN_NM_WID,
   --     lgb.PRIMARY_PHYSICIAN_CRED,
      , NULL	AS	SECONDARY_PHYSICIAN_ID
   --     lgb.SECONDARY_PHYSICIAN_ID,
   --     lgb.SECONDARY_PHYSICIAN_NM,
   --     lgb.SECONDARY_PHYSICIAN_NM_WID,
   --     lgb.SECONDARY_PHYSICIAN_CRED,
   --     lgb.SERVICE_C,
      , NULL	AS	SERVICE_NM
   --     lgb.SERVICE_NM,
      , NULL	AS	PRIMARY_PROCEDURE_ID
   --     lgb.PRIMARY_PROCEDURE_ID,
      , NULL	AS	PRIMARY_PROCEDURE_NM
   --     lgb.PRIMARY_PROCEDURE_NM,
   --     lgb.PRIMARY_PROCEDURE_NM_WID,
   --     lgb.LOCATION_ID,
   --     lgb.LOCATION_NM,
   --     lgb.LOCATION_NM_WID,
   --     lgb.ROOM_ID,
   --     lgb.ROOM_NM,
   --     lgb.ROOM_NM_WID,
   --     lgb.PRIMARY_CIRCULATOR_ID,
   --     lgb.PRIMARY_CIRCULATOR_NM,
   --     lgb.PRIMARY_CIRCULATOR_NM_WID,
   --     lgb.PRIMARY_CIRCULATOR_CRED,
   --     lgb.PRIMARY_SURG_TECH_ID,
   --     lgb.PRIMARY_SURG_TECH_NM,
   --     lgb.PRIMARY_SURG_TECH_NM_WID,
   --     lgb.PRIMARY_SURG_TECH_CRED,
   --     lgb.FIRST_ANES_ID,
   --     lgb.FIRST_ANES_NM,
   --     lgb.FIRST_ANES_NM_WID,
   --     lgb.FIRST_ANES_CRED,
      , NULL	AS	PROC_DATE
   --     lgb.PROC_DATE,
   --     lgb.PROC_DAY_NUM_OF_WEEK,
   --     lgb.PROC_DAY_OF_WEEK,
   --     lgb.PROC_MONTH_NUMBER,
   --     lgb.PROC_MONTH_NAME,
   --     lgb.PROC_YEAR,
   --     lgb.PROC_YEAR_AND_MONTH,
   --     lgb.PROC_WEEK_OF_YEAR,
   --     lgb.PROC_WEEKEND_YN,
   --     lgb.PROC_HOLIDAY_YN,
   --     lgb.NUMBER_OF_PROCEDURES,
   --     lgb.NUMBER_OF_PANELS,
   --     lgb.PROC_NOT_PERF_C,
      , NULL	AS	PROC_NOT_PERF_NM
   --     lgb.PROC_NOT_PERF_NM,
      , NULL	AS	IN_OR_DTTM
   --     lgb.IN_OR_DTTM,
      , NULL	AS	OUT_OR_DTTM
   --     lgb.OUT_OR_DTTM,
   --     lgb.MINUTES_IN_OR,
   --     lgb.COUNT_IN_OR,
   --     lgb.PRIMARY_PREOP_NURSE_ID,
   --     lgb.PRIMARY_PREOP_NURSE_NM,
   --     lgb.PRIMARY_PREOP_NURSE_NM_WID,
   --     lgb.PRIMARY_PREOP_NURSE_CRED,
   --     lgb.IN_PREOP_DTTM,
   --     lgb.OUT_PREOP_DTTM,
   --     lgb.MINUTES_IN_PREOP,
   --     lgb.COUNT_IN_PREOP,
   --     lgb.PRIMARY_RECOVERY_NURSE_ID,
   --     lgb.PRIMARY_RECOVERY_NURSE_NM,
   --     lgb.PRIMARY_RECOVERY_NURSE_NM_WID,
   --     lgb.PRIMARY_RECOVERY_NURSE_CRED,
   --     lgb.IN_RECOVERY_DTTM,
   --     lgb.COMP_RECOVERY_DTTM,
   --     lgb.OUT_RECOVERY_DTTM,
   --     lgb.MINUTES_BOARD_RECOVERY,
   --     lgb.MINUTES_IN_RECOVERY,
   --     lgb.COUNT_IN_RECOVERY,
   --     lgb.PRIMARY_PHASEII_NURSE_ID,
   --     lgb.PRIMARY_PHASEII_NURSE_NM,
   --     lgb.PRIMARY_PHASEII_NURSE_NM_WID,
   --     lgb.PRIMARY_PHASEII_NURSE_CRED,
   --     lgb.IN_PHASEII_DTTM,
   --     lgb.COMP_PHASEII_DTTM,
   --     lgb.OUT_PHASEII_DTTM,
   --     lgb.MINUTES_BOARD_PHASEII,
   --     lgb.MINUTES_IN_PHASEII,
   --     lgb.COUNT_IN_PHASEII,
   --     lgb.CASE_SCHEDULED_START_DTTM,
   --     lgb.SETUP_LENGTH,
   --     lgb.CLEANUP_LENGTH,
   --     lgb.CASE_SCHEDULED_END_DTTM,
   --     lgb.ROOM_PREVIOUS_LOG_ID,
   --     lgb.ROOM_PREVIOUS_CASE_ID,
   --     lgb.RESP_ANES_ID,
   --     lgb.RESP_ANES_NM,
   --     lgb.RESP_ANES_NM_WID,
   --     lgb.RESP_ANES_CRED,
   --     lgb.PRIMARY_ANES_TYPE_C,
   --     lgb.PRIMARY_ANES_TYPE_NM,
   --     lgb.LOG_EXCLUSION_REASON_C
INTO #chgs
FROM
(

	-- Transactions with no CPT modifiers
	SELECT	ha.PAT_ID
	      , ha.HSP_ACCOUNT_ID AS EncounterNumber
	      , ht.PAT_ENC_CSN_ID
		  , ht.CPT_CODE AS CPTCode
		  --, cpt.CPT_CODE_DESC
		  , 1 AS CPTCodeSequence
		  --, CONVERT(VARCHAR(10), ht.SERVICE_DATE, 23) AS CPTDate
		  , CAST(ht.SERVICE_DATE AS DATE) AS CPTDate
		  , ht.TX_ID
		  , ht.QUANTITY
		  , COALESCE(ht.MODIFIERS, '') AS All_Modifiers
		  , CAST('' AS VARCHAR(254)) AS CPTModifier1
		  , CAST('' AS VARCHAR(254)) AS CPTModifier2
		  , CAST('' AS VARCHAR(254)) AS CPTModifier3
		  , ht.TX_AMOUNT AS CPTCharge
		  , ht.DFLT_PROC_DESC
		  , ht.PROCEDURE_DESC
		  , ht.PROC_ID
		  , eap.PROC_NAME
		  , eap.PT_FRIENDLY_NAME
		  , eap.PROC_CODE
		  , eap.PROC_CAT
		  , eap.TYPE_C
		  , eap.PROC_TYPE
		  , ht.ORDER_ID
		  , eap.ORDER_DISPLAY_NAME
		  , eap.BILLING_CAT_C

	FROM	CLARITY..HSP_TRANSACTIONS AS ht

	-- population has already been selected by MRN_List proc, so use that
	--INNER JOIN Biome_CTE AS biome
	--	ON ht.HSP_ACCOUNT_ID = biome.HSP_ACCOUNT_ID
	--	   AND	ht.CPT_CODE = biome.SELECTED_CODE

	LEFT OUTER JOIN CLARITY..F_ARHB_INACTIVE_TX AS itx -- weed out transactions that were later credited out (see WHERE clause)
		ON ht.TX_ID = itx.TX_ID
	INNER JOIN CLARITY..HSP_ACCOUNT AS ha
		ON ha.HSP_ACCOUNT_ID = ht.HSP_ACCOUNT_ID
	INNER JOIN CLARITY..HSP_ACCT_SBO AS sbo
		ON ha.HSP_ACCOUNT_ID = sbo.HSP_ACCOUNT_ID
	INNER JOIN CLARITY..VALID_PATIENT vp
		ON ha.PAT_ID = vp.PAT_ID
	INNER JOIN CLARITY..CLARITY_EAP AS eap
		ON eap.PROC_ID = ht.PROC_ID
	INNER JOIN CLARITY..CLARITY_DEP AS dep
		ON dep.DEPARTMENT_ID = ht.DEPARTMENT

	INNER JOIN #rflpts rflpts
		ON ha.PAT_ID = rflpts.PAT_ID
	--LEFT JOIN CLARITY.dbo.HSP_ACCT_CPT_CODES cpt
	--		ON cpt.HSP_ACCOUNT_ID = cpt.HSP_ACCOUNT_ID
	--		AND cpt.CPT_CODE = ht.CPT_CODE


	WHERE	1 = 1
			AND
			(
				(
					ha.ACCT_BASECLS_HA_C IN ( '1' ) -- Inpatient
					--AND ha.DISCH_DATE_TIME >= @StartDate
					--AND ha.DISCH_DATE_TIME <= @EndDate
					AND ha.DISCH_DATE_TIME >= rflpts.event_date
				)
				OR
				(
					ha.ACCT_BASECLS_HA_C IN ( '2', '3' ) -- outpatient
					--AND ha.ADM_DATE_TIME >= @StartDate
					--AND ha.ADM_DATE_TIME <= @EndDate
					AND ha.ADM_DATE_TIME >= rflpts.event_date
				)
			)
			AND
			(
				sbo.SBO_HAR_TYPE_C IS NULL
				OR	sbo.SBO_HAR_TYPE_C = 0
			)
			AND NOT ha.PRIM_ENC_CSN_ID IS NULL
			AND ha.TOT_CHGS > 0
			AND ht.TX_TYPE_HA_C = 1 -- charges
			AND itx.TX_ID IS NULL -- not credited out later
			AND ha.ACCT_BILLSTS_HA_C <> '40' -- not voided
			AND LEN(ht.CPT_CODE) = 5
			AND vp.IS_VALID_PAT_YN = 'Y'
			AND ht.MODIFIERS IS NULL
	--AND ht.HSP_ACCOUNT_ID = 13016228956

			--AND ht.SERVICE_DATE >= rflpts.event_date


	UNION ALL


	-- transactions with CPT modifiers that need unpacking
	SELECT	b.PAT_ID
	      , b.EncounterNumber
	      , b.PAT_ENC_CSN_ID
		  , b.CPTCode
	      --, b.CPT_CODE_DESC
		  , b.CPTCodeSequence
		  , b.CPTDate
		  , b.TX_ID
		  , b.QUANTITY
		  , b.All_Modifiers
		  , MAX(b.CPTModifier1) AS CPTModifier1
		  , MAX(b.CPTModifier2) AS CPTModifier2
		  , MAX(b.CPTModifier3) AS CPTModifier3
		  , b.CPTCharge
		  , b.DFLT_PROC_DESC
		  , b.PROCEDURE_DESC
		  , b.PROC_ID
		  , b.PROC_NAME
		  , b.PT_FRIENDLY_NAME
		  , b.PROC_CODE
		  , b.PROC_CAT
		  , b.TYPE_C
		  , b.PROC_TYPE
		  , b.ORDER_ID
		  , b.ORDER_DISPLAY_NAME
		  , b.BILLING_CAT_C
	FROM
	( -- b
		SELECT	a.PAT_ID
		      , a.EncounterNumber
	          , a.PAT_ENC_CSN_ID
			  , a.CPTCode
	          --, a.CPT_CODE_DESC
			  , a.CPTCodeSequence
			  , a.CPTDate
			  , a.TX_ID
			  , a.QUANTITY
			  , a.All_Modifiers
			  , CASE
				WHEN a.row_num = 1 THEN a.Modifier_value
				ELSE CAST('' AS VARCHAR(254))
				END AS CPTModifier1
			  , CASE
				WHEN a.row_num = 2 THEN a.Modifier_value
				ELSE CAST('' AS VARCHAR(254))
				END AS CPTModifier2
			  , CASE
				WHEN a.row_num = 3 THEN a.Modifier_value
				ELSE CAST('' AS VARCHAR(254))
				END AS CPTModifier3
			  , a.Modifier_value
			  , a.row_num
			  , a.CPTCharge
			  , a.DFLT_PROC_DESC
			  , a.PROCEDURE_DESC
			  , a.PROC_ID
			  , a.PROC_NAME
			  , a.PT_FRIENDLY_NAME
			  , a.PROC_CODE
			  , a.PROC_CAT
			  , a.TYPE_C
			  , a.PROC_TYPE
			  , a.ORDER_ID
			  , a.ORDER_DISPLAY_NAME
			  , a.BILLING_CAT_C
		FROM
		( -- a
			SELECT	ha.PAT_ID
			      , ha.HSP_ACCOUNT_ID AS EncounterNumber
	              , ht.PAT_ENC_CSN_ID
				  , ht.CPT_CODE AS CPTCode
				  --, cpt.CPT_CODE_DESC
				  , 1 AS CPTCodeSequence
				  --, CONVERT(VARCHAR(10), ht.SERVICE_DATE, 23) AS CPTDate
				  , CAST(ht.SERVICE_DATE AS DATE) AS CPTDate
				  , ht.TX_ID
				  , ht.QUANTITY
				  , COALESCE(ht.MODIFIERS, '') AS All_Modifiers

				  , CAST(mods.value AS VARCHAR(254))   AS Modifier_value
				  , ROW_NUMBER() OVER (PARTITION BY ha.HSP_ACCOUNT_ID
												  , ht.CPT_CODE
												  , ht.SERVICE_DATE
												  , ht.TX_ID
									   ORDER BY mods.value
									  ) AS row_num
				  , ht.TX_AMOUNT AS CPTCharge
				  , ht.DFLT_PROC_DESC
				  , ht.PROCEDURE_DESC
				  , ht.PROC_ID
				  , eap.PROC_NAME
				  , eap.PT_FRIENDLY_NAME
				  , eap.PROC_CODE
				  , eap.PROC_CAT
		          , eap.TYPE_C
				  , eap.PROC_TYPE
				  , ht.ORDER_ID
				  , eap.ORDER_DISPLAY_NAME
				  , eap.BILLING_CAT_C

			FROM	CLARITY..HSP_TRANSACTIONS AS ht

			-- population has already been selected by MRN_List proc, so use that
			--INNER JOIN Biome_CTE AS biome
			--	ON biome.HSP_ACCOUNT_ID = ht.HSP_ACCOUNT_ID
			--	   AND	ht.CPT_CODE = biome.SELECTED_CODE

			LEFT OUTER JOIN CLARITY..F_ARHB_INACTIVE_TX AS itx -- weed out transactions that were later credited out (see WHERE clause)
				ON ht.TX_ID = itx.TX_ID
			INNER JOIN CLARITY..HSP_ACCOUNT AS ha
				ON ha.HSP_ACCOUNT_ID = ht.HSP_ACCOUNT_ID
			INNER JOIN CLARITY..HSP_ACCT_SBO AS sbo
				ON ha.HSP_ACCOUNT_ID = sbo.HSP_ACCOUNT_ID
			INNER JOIN CLARITY..VALID_PATIENT vp
				ON ha.PAT_ID = vp.PAT_ID
			INNER JOIN CLARITY..CLARITY_EAP AS eap
				ON eap.PROC_ID = ht.PROC_ID
			INNER JOIN CLARITY..CLARITY_DEP AS dep
				ON dep.DEPARTMENT_ID = ht.DEPARTMENT

		   INNER JOIN #rflpts rflpts
			    ON ha.PAT_ID = rflpts.PAT_ID
	--LEFT JOIN CLARITY.dbo.HSP_ACCT_CPT_CODES cpt
	--		ON cpt.HSP_ACCOUNT_ID = cpt.HSP_ACCOUNT_ID
	--		AND cpt.CPT_CODE = ht.CPT_CODE

			CROSS APPLY STRING_SPLIT(ht.MODIFIERS, ',') AS mods

			WHERE	1 = 1
					AND
					(
						(
							ha.ACCT_BASECLS_HA_C IN ( '1' ) -- Inpatient
							--AND ha.DISCH_DATE_TIME >= @StartDate
							--AND ha.DISCH_DATE_TIME <= @EndDate
							AND ha.DISCH_DATE_TIME >= rflpts.event_date
						)
						OR
						(
							ha.ACCT_BASECLS_HA_C IN ( '2', '3' ) -- outpatient
							--AND ha.ADM_DATE_TIME >= @StartDate
							--AND ha.ADM_DATE_TIME <= @EndDate
							AND ha.ADM_DATE_TIME >= rflpts.event_date
						)
					)
					AND
					(
						sbo.SBO_HAR_TYPE_C IS NULL
						OR	sbo.SBO_HAR_TYPE_C = 0
					)
					AND NOT ha.PRIM_ENC_CSN_ID IS NULL
					AND ha.TOT_CHGS > 0
					AND ht.TX_TYPE_HA_C = 1 -- charges
					AND itx.TX_ID IS NULL -- not credited out later
					AND ha.ACCT_BILLSTS_HA_C <> '40' -- voided
					AND LEN(ht.CPT_CODE) = 5
					AND dep.SERV_AREA_ID = '10' --uva service area
					AND vp.IS_VALID_PAT_YN = 'Y'
					AND ht.MODIFIERS IS NOT NULL

					--AND ht.SERVICE_DATE >= rflpts.event_date

		--AND ht.HSP_ACCOUNT_ID = 13016228956

		) AS a
	) AS b
	GROUP BY b.PAT_ID
	       , b.EncounterNumber
	       , b.PAT_ENC_CSN_ID
		   , b.CPTCode
	       --, b.CPT_CODE_DESC
		   , b.CPTDate
		   , b.TX_ID
		   , b.QUANTITY
		   , b.CPTCodeSequence
		   , b.All_Modifiers
		   , b.CPTCharge
		   , b.DFLT_PROC_DESC
		   , b.PROCEDURE_DESC
		   , b.PROC_ID
		   , b.PROC_NAME
		   , b.PT_FRIENDLY_NAME
		   , b.PROC_CODE
		   , b.PROC_CAT
		   , b.TYPE_C
		   , b.PROC_TYPE
		   , b.ORDER_ID
		   , b.ORDER_DISPLAY_NAME
		   , b.BILLING_CAT_C

) AS c
--LEFT OUTER JOIN CLARITY.dbo.CLARITY_EAP eap
--	ON eap.PROC_ID = c.PROC_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_PROCEDURE_TYPE zpt
	ON zpt.PROC_TYPE = c.TYPE_C
LEFT OUTER JOIN CLARITY.dbo.ORDER_PROC op
	ON op.ORDER_PROC_ID = c.ORDER_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_CLASS zoc
	ON zoc.ORDER_CLASS_C = op.ORDER_CLASS_C
LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_TYPE zot
	ON zot.ORDER_TYPE_C = op.ORDER_TYPE_C
LEFT OUTER JOIN CLARITY.dbo.ZC_BILLING_CAT zbc
	ON zbc.BILLING_CAT_C = c.BILLING_CAT_C
--LEFT OUTER JOIN CLARITY.dbo.PAT_OR_ADM_LINK lnk
--	ON lnk.OR_LINK_CSN = c.PAT_ENC_CSN_ID
--LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb
--	ON lgb.CASE_ID=lnk.CASE_ID
WHERE 1 = 1
AND c.BILLING_CAT_C NOT IN (
	6000, -- Labs
	15700, -- Clinic Visit
	14000, -- Home Health
	1500, -- Room/Bed
	15150 -- CRNA's
)
	--ORDER BY PAT_ID
	--	   , CPTDate
	--       , EncounterNumber
	--       , PAT_ENC_CSN_ID
	--ORDER BY EncounterNumber
	--       , c.CPTCode
	ORDER BY c.PAT_ENC_CSN_ID
			, c.CPTDate
			, c.CPTCode

-- Create index for temp table #chgs

--CREATE CLUSTERED INDEX IX_chgs ON #chgs (EncounterNumber, CPTCode)
CREATE CLUSTERED INDEX IX_chgs ON #chgs (PAT_ENC_CSN_ID, CPTDate, CPTCode)

SELECT
	*
FROM #chgs
--ORDER BY
--	PAT_ID,
--	EncounterNumber,
--	CPTCode
ORDER BY
	PAT_ENC_CSN_ID,
	CPTDate,
	CPTCode

SELECT
	chgs.PAT_ID,
    chgs.EncounterNumber,
    chgs.PAT_ENC_CSN_ID,
    chgs.CPTCode,
    chgs.CPTCodeSequence,
    chgs.CPTDate,
    chgs.CPTModifier1,
    chgs.CPTModifier2,
    chgs.CPTModifier3,
    chgs.CPTCharge,
    chgs.CPTQuantity,
    chgs.DFLT_PROC_DESC,
    chgs.PROCEDURE_DESC,
    chgs.PROC_ID,
    chgs.PROC_NAME,
    chgs.PT_FRIENDLY_NAME,
    chgs.PROC_CODE,
    chgs.PROC_CAT,
    chgs.TYPE_C,
    chgs.TYPE_NAME,
    chgs.PROC_TYPE,
    chgs.ORDER_ID,
    chgs.ORDER_CLASS_C,
    chgs.ORDER_CLASS_NAME,
    chgs.ORDER_TYPE_C,
    chgs.ORDER_TYPE_NAME,
    chgs.ORDER_DISPLAY_NAME,
    chgs.BILLING_CAT_C,
    chgs.BILLING_CAT_NAME,
    chgs.AUTHRZING_PROV_ID,
    chgs.REFERRING_PROV_ID,
    chgs.PROC_PERF_PROV_ID,
    chgs.CSN_COUNT,
	    lgb2.CASE_ID
	  , lgb2.LOG_ID,
     --   lgb2.CASE_ID AS lgb_CASE_ID,
	    --lgb2.LOG_ID AS lgb_LOG_ID--,
        ----lgb2.PAT_ID,
        --lgb2.PAT_AGE,
        --lgb2.PATIENT_CLASS_C,
        lgb2.PATIENT_CLASS_NM,
        lgb2.PATIENT_CLASS_GROUP,
        --lgb2.CASE_CLASS_C,
        lgb2.CASE_CLASS_NM,
        --lgb2.LOG_STATUS_C,
        lgb2.LOG_STATUS_NM,
        --lgb2.ADD_ON_CASE_MAN_YN,
        --lgb2.ADD_ON_CASE_SCH_YN,
        lgb2.PRIMARY_PHYSICIAN_ID,
        --lgb2.PRIMARY_PHYSICIAN_NM,
        --lgb2.PRIMARY_PHYSICIAN_NM_WID,
        --lgb2.PRIMARY_PHYSICIAN_CRED,
        lgb2.SECONDARY_PHYSICIAN_ID,
        --lgb2.SECONDARY_PHYSICIAN_NM,
        --lgb2.SECONDARY_PHYSICIAN_NM_WID,
        --lgb2.SECONDARY_PHYSICIAN_CRED,
        --lgb2.SERVICE_C,
        lgb2.SERVICE_NM,
        lgb2.PRIMARY_PROCEDURE_ID,
        lgb2.PRIMARY_PROCEDURE_NM,
        --lgb2.PRIMARY_PROCEDURE_NM_WID,
        --lgb2.LOCATION_ID,
        --lgb2.LOCATION_NM,
        --lgb2.LOCATION_NM_WID,
        --lgb2.ROOM_ID,
        --lgb2.ROOM_NM,
        --lgb2.ROOM_NM_WID,
        --lgb2.PRIMARY_CIRCULATOR_ID,
        --lgb2.PRIMARY_CIRCULATOR_NM,
        --lgb2.PRIMARY_CIRCULATOR_NM_WID,
        --lgb2.PRIMARY_CIRCULATOR_CRED,
        --lgb2.PRIMARY_SURG_TECH_ID,
        --lgb2.PRIMARY_SURG_TECH_NM,
        --lgb2.PRIMARY_SURG_TECH_NM_WID,
        --lgb2.PRIMARY_SURG_TECH_CRED,
        --lgb2.FIRST_ANES_ID,
        --lgb2.FIRST_ANES_NM,
        --lgb2.FIRST_ANES_NM_WID,
        --lgb2.FIRST_ANES_CRED,
        lgb2.PROC_DATE,
        --lgb2.PROC_DAY_NUM_OF_WEEK,
        --lgb2.PROC_DAY_OF_WEEK,
        --lgb2.PROC_MONTH_NUMBER,
        --lgb2.PROC_MONTH_NAME,
        --lgb2.PROC_YEAR,
        --lgb2.PROC_YEAR_AND_MONTH,
        --lgb2.PROC_WEEK_OF_YEAR,
        --lgb2.PROC_WEEKEND_YN,
        --lgb2.PROC_HOLIDAY_YN,
        --lgb2.NUMBER_OF_PROCEDURES,
        --lgb2.NUMBER_OF_PANELS,
        --lgb2.PROC_NOT_PERF_C,
        lgb2.PROC_NOT_PERF_NM,
        lgb2.IN_OR_DTTM,
        lgb2.OUT_OR_DTTM--,
        --lgb.MINUTES_IN_OR,
        --lgb.COUNT_IN_OR,
        --lgb.PRIMARY_PREOP_NURSE_ID,
        --lgb.PRIMARY_PREOP_NURSE_NM,
        --lgb.PRIMARY_PREOP_NURSE_NM_WID,
        --lgb.PRIMARY_PREOP_NURSE_CRED,
        --lgb.IN_PREOP_DTTM,
        --lgb.OUT_PREOP_DTTM,
        --lgb.MINUTES_IN_PREOP,
        --lgb.COUNT_IN_PREOP,
        --lgb.PRIMARY_RECOVERY_NURSE_ID,
        --lgb.PRIMARY_RECOVERY_NURSE_NM,
        --lgb.PRIMARY_RECOVERY_NURSE_NM_WID,
        --lgb.PRIMARY_RECOVERY_NURSE_CRED,
        --lgb.IN_RECOVERY_DTTM,
        --lgb.COMP_RECOVERY_DTTM,
        --lgb.OUT_RECOVERY_DTTM,
        --lgb.MINUTES_BOARD_RECOVERY,
        --lgb.MINUTES_IN_RECOVERY,
        --lgb.COUNT_IN_RECOVERY,
        --lgb.PRIMARY_PHASEII_NURSE_ID,
        --lgb.PRIMARY_PHASEII_NURSE_NM,
        --lgb.PRIMARY_PHASEII_NURSE_NM_WID,
        --lgb.PRIMARY_PHASEII_NURSE_CRED,
        --lgb.IN_PHASEII_DTTM,
        --lgb.COMP_PHASEII_DTTM,
        --lgb.OUT_PHASEII_DTTM,
        --lgb.MINUTES_BOARD_PHASEII,
        --lgb.MINUTES_IN_PHASEII,
        --lgb.COUNT_IN_PHASEII,
        --lgb.CASE_SCHEDULED_START_DTTM,
        --lgb.SETUP_LENGTH,
        --lgb.CLEANUP_LENGTH,
        --lgb.CASE_SCHEDULED_END_DTTM,
        --lgb.ROOM_PREVIOUS_LOG_ID,
        --lgb.ROOM_PREVIOUS_CASE_ID,
        --lgb.RESP_ANES_ID,
        --lgb.RESP_ANES_NM,
        --lgb.RESP_ANES_NM_WID,
        --lgb.RESP_ANES_CRED,
        --lgb.PRIMARY_ANES_TYPE_C,
        --lgb.PRIMARY_ANES_TYPE_NM,
        --lgb.LOG_EXCLUSION_REASON_C
INTO #chgs2
FROM #chgs chgs
--LEFT OUTER JOIN CLARITY.dbo.PAT_OR_ADM_LINK lnk
--	ON lnk.OR_LINK_CSN = chgs.PAT_ENC_CSN_ID
--LEFT OUTER JOIN CLARITY.dbo.PAT_OR_ADM_LINK lnk
--	ON lnk.OR_SRC_VISIT_CSN = chgs.PAT_ENC_CSN_ID
/*
LEFT OUTER JOIN
(
SELECT
	OR_LINK_CSN,
	CASE_ID,
	LOG_ID
FROM CLARITY.dbo.PAT_OR_ADM_LINK
WHERE LOG_ID IS NOT NULL
) lnk
	ON lnk.OR_LINK_CSN = chgs.PAT_ENC_CSN_ID
--LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb
--	ON lgb.CASE_ID=lnk.CASE_ID
--	AND lgb.LOG_ID = lnk.LOG_ID
LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb
	ON lgb.CASE_ID=lnk.CASE_ID
	AND lgb.LOG_ID = lnk.LOG_ID
*/
LEFT OUTER JOIN
(
SELECT
	lnk.OR_LINK_CSN,
	lnk.CASE_ID,
	lnk.LOG_ID,
	lgb.PATIENT_CLASS_NM,
	lgb.PATIENT_CLASS_GROUP,
	lgb.CASE_CLASS_NM,
	lgb.LOG_STATUS_NM,
	lgb.PRIMARY_PHYSICIAN_ID,
	lgb.SECONDARY_PHYSICIAN_ID,
	lgb.SERVICE_NM,
	lgb.PRIMARY_PROCEDURE_ID,
    lgb.PRIMARY_PROCEDURE_NM,
	lgb.PROC_DATE,
	lgb.PROC_NOT_PERF_NM,
    lgb.IN_OR_DTTM,
    lgb.OUT_OR_DTTM
FROM
(
SELECT
	OR_LINK_CSN,
	CASE_ID,
	LOG_ID
FROM CLARITY.dbo.PAT_OR_ADM_LINK
WHERE LOG_ID IS NOT NULL
) lnk
LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb
	ON lgb.CASE_ID=lnk.CASE_ID
	AND lgb.LOG_ID = lnk.LOG_ID
) lgb2
--ON  lgb2.PROC_DATE = chgs.CPTDate
ON lgb2.OR_LINK_CSN = chgs.PAT_ENC_CSN_ID
AND  lgb2.PROC_DATE = chgs.CPTDate
--WHERE chgs.PAT_ID = 'Z2653762'
--WHERE 1 = 1
--AND lgb.PROC_DATE = chgs.CPTDate
--ORDER BY
--	chgs.PAT_ENC_CSN_ID,
--	chgs.CPTDate,
--	chgs.CPTCode

SELECT
	*
FROM #chgs2 chgs
ORDER BY
    chgs.PAT_ID,
	chgs.PAT_ENC_CSN_ID,
	chgs.CPTDate,
	chgs.CPTCode

--*/
/*
;WITH
    ip_cte
    AS
    (
        SELECT
            hsp.[PAT_ID],
            cte.person_id,
            cte.PATIENT_NAME,
            hsp.PAT_ENC_CSN_ID,
            hsp.PAT_ENC_CSN_ID AS HSP_PAT_ENC_CSN_ID,
            CAST(hsp.HOSP_ADMSN_TIME AS DATE) AS [CONTACT_DATE],
            --NULL AS APPT_TIME,
            [HOSP_ADMSN_TIME] AS APPT_TIME,
            NULL AS APPT_STATUS_NAME,
            [HOSP_ADMSN_TIME],
            [HOSP_DISCH_TIME],
            hsp.HOSP_ADMSN_TIME AS HSP_HOSP_ADMSN_TIME,
            hsp.HOSP_DISCH_TIME AS HSP_HOSP_DISCH_TIME,
            hsp.ADT_PAT_CLASS_C AS ENC_TYPE_C,
            zpl.NAME AS Enc_Type,
            zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
            [ADMISSION_PROV_ID] AS Prov_Id,
            hsp.[DEPARTMENT_ID],
            DEP.DEPARTMENT_NAME,
            NULL AS REFERRAL_ID,
            --NULL AS Referral_Entry_Date,
            'INPATIENT' UOS,
            'ip_cte' AS [extract],

            NULL AS LOG_ID,
            NULL AS PRIMARY_PROCEDURE_ID,
            NULL AS PRIMARY_PROCEDURE_NM,
            hsp.HSP_ACCOUNT_ID,
            har.TOT_CHGS,
            har.TOT_PMTS,
            zcsh.NAME AS CODING_STATUS_NAME, -- 1 Not Started, 2 In Progress, 3 Waiting, 4 Completed, 5 Ready To Start, 6 On Hold
            har.PRIMARY_PAYOR_ID,
            har.PRIMARY_PLAN_ID
        FROM [CLARITY].[dbo].[PAT_ENC_HSP] hsp
            INNER JOIN #rflpts cte ON hsp.PAT_ID = cte.PAT_ID
            LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS zpl ON zpl.ADT_PAT_CLASS_C = hsp.ADT_PAT_CLASS_C
            LEFT OUTER JOIN CLARITY.dbo.ZC_ADM_SOURCE zas ON zas.ADMIT_SOURCE_C = hsp.ADMIT_SOURCE_C
            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP ON DEP.DEPARTMENT_ID = hsp.DEPARTMENT_ID
            LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER AS SER ON SER.PROV_ID = hsp.ADMISSION_PROV_ID
            LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C
            LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT har ON har.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID

            LEFT JOIN CLARITY.dbo.ZC_CODING_STS_HA zcsh ON zcsh.CODING_STATUS_C = har.CODING_STATUS_C

        --WHERE PAT_ID = 'Z245973'
        WHERE 1 = 1
            AND CAST(hsp.HOSP_ADMSN_TIME AS DATE) >= cte.event_date
			AND (hsp.HOSP_DISCH_TIME IS NOT NULL AND CAST(hsp.HOSP_DISCH_TIME AS DATE) <= @YestDate)
            AND hsp.ADMISSION_PROV_ID IS NOT NULL
			AND zcsh.NAME LIKE '%Complete%'
        -- ORDER BY
        --cte.person_id,
        --hsp.HOSP_ADMSN_TIME
    ),

    or_cte
    AS
    (
    SELECT
        vsurg.[PAT_ID],
        cte.person_id,
        cte.PATIENT_NAME,
        lnk.PAT_ENC_CSN_ID AS PAT_ENC_CSN_ID,
        lnk.OR_LINK_CSN AS HSP_PAT_ENC_CSN_ID,
        lgb.PROC_DATE AS CONTACT_DATE,
        --NULL AS APPT_TIME,
        COALESCE(lgb.IN_OR_DTTM, vsurg.CASE_BEGIN_INSTANT) AS APPT_TIME,
        NULL AS APPT_STATUS_NAME,
        COALESCE(lgb.IN_OR_DTTM, vsurg.CASE_BEGIN_INSTANT) AS HOSP_ADMSN_TIME,
        COALESCE(lgb.OUT_OR_DTTM, vsurg.CASE_END_INSTANT) AS HOSP_DISCH_TIME,
        hsp.HOSP_ADMSN_TIME AS HSP_HOSP_ADMSN_TIME,
        hsp.HOSP_DISCH_TIME AS HSP_HOSP_DISCH_TIME,
        lgb.PATIENT_CLASS_C AS ENC_TYPE_C,
        lgb.PATIENT_CLASS_NM AS Enc_Type,
        zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
        lgb.PRIMARY_PHYSICIAN_ID AS Prov_Id,
        lgb.LOCATION_ID AS DEPARTMENT_ID,
        lgb.LOCATION_NM AS DEPARTMENT_NAME,
        NULL AS REFERRAL_ID,
        --NULL AS Referral_Entry_Date,
        CASE WHEN orl.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS,
        'or_cte' AS [extract]	,

        orl.LOG_ID,
        lgb.PRIMARY_PROCEDURE_ID,
        lgb.PRIMARY_PROCEDURE_NM,
        hsp.HSP_ACCOUNT_ID,
        har.TOT_CHGS,
        har.TOT_PMTS,
        zcsh.NAME AS CODING_STATUS_NAME, -- 1 Not Started, 2 In Progress, 3 Waiting, 4 Completed, 5 Ready To Start, 6 On Hold
        har.PRIMARY_PAYOR_ID,
        har.PRIMARY_PLAN_ID

    FROM CLARITY.dbo.OR_CASE vsurg
        INNER JOIN #rflpts cte ON vsurg.PAT_ID = cte.PAT_ID
        LEFT OUTER JOIN CLARITY.dbo.OR_LOG orl ON orl.CASE_ID = vsurg.OR_CASE_ID
        LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb ON lgb.CASE_ID=vsurg.OR_CASE_ID
        LEFT OUTER JOIN CLARITY.dbo.PAT_OR_ADM_LINK lnk ON lnk.CASE_ID = vsurg.OR_CASE_ID
        LEFT OUTER JOIN CLARITY.dbo.	PAT_ENC_HSP	hsp ON lnk.OR_LINK_CSN = hsp.PAT_ENC_CSN_ID
        LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT har ON har.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
        LEFT OUTER JOIN CLARITY.dbo.OR_CASE_2 cs2 ON vsurg.OR_CASE_ID=cs2.CASE_ID
        LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C

        LEFT JOIN CLARITY.dbo.ZC_CODING_STS_HA zcsh ON zcsh.CODING_STATUS_C = har.CODING_STATUS_C

    WHERE 1=1
        AND lgb.PROC_DATE >= cte.event_date
		AND lgb.PROC_DATE <= @YestDate

        AND vsurg.SCHED_STATUS_C NOT IN ('2','5') /* Not  Canceled or Voided */
        AND lgb.LOG_STATUS_C NOT IN ('4','6') /* Not Voided or Canceled */
        ----AND vsurg.CANCEL_REASON_C IS NULL /* No Cancel Reason */
        --AND (vsurg.CANCEL_REASON_C IS NULL /* No Cancel Reason */
        --	OR (vsurg.CANCEL_REASON_C = 999 AND zcsh.NAME = 'Completed'))
        AND lgb.PROC_NOT_PERF_C IS NULL
		AND zcsh.NAME LIKE '%Complete%'
        /* Procedure Performed */
    ),

    ophov_cte
    AS
    (
        SELECT
            cte.PAT_ID,
            cte.person_id,
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
            ENC.ENC_TYPE_C,
            zdet.NAME AS Enc_Type,
            zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
            ENC.VISIT_PROV_ID AS Prov_Id,
            ENC.DEPARTMENT_ID,
            DEP.DEPARTMENT_NAME,
            ENC.REFERRAL_ID,
            --cte.event_date AS Referral_Entry_Date,
            'OUTPATIENT' UOS,
            'ophov_cte' AS [extract],
            NULL AS LOG_ID,
            NULL AS PRIMARY_PROCEDURE_ID,
            NULL AS PRIMARY_PROCEDURE_NM,
            ENC.HSP_ACCOUNT_ID,
            har.TOT_CHGS,
            har.TOT_PMTS,
            zcsh.NAME AS CODING_STATUS_NAME, -- 1 Not Started, 2 In Progress, 3 Waiting, 4 Completed, 5 Ready To Start, 6 On Hold
            har.PRIMARY_PAYOR_ID,
            har.PRIMARY_PLAN_ID

        FROM CLARITY.dbo.PAT_ENC ENC
            LEFT OUTER JOIN CLARITY.dbo.PATIENT pt ON pt.PAT_ID = ENC.PAT_ID
            INNER JOIN #rflpts cte ON pt.PAT_ID = cte.PAT_ID
            LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = ENC.HOSP_ADMSN_TYPE_C
            LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet ON zdet.DISP_ENC_TYPE_C = ENC.ENC_TYPE_C
            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP AS DEP ON DEP.DEPARTMENT_ID = ENC.DEPARTMENT_ID
            LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT har ON har.HSP_ACCOUNT_ID = enc.HSP_ACCOUNT_ID

            LEFT JOIN CLARITY.dbo.ZC_CODING_STS_HA zcsh ON zcsh.CODING_STATUS_C = har.CODING_STATUS_C

        WHERE ENC.CONTACT_DATE >= cte.event_date
			AND ENC.CONTACT_DATE <= @YestDate
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
			AND zcsh.NAME LIKE '%Complete%'
    )

--SELECT
--	*
--FROM ophov_cte
--WHERE ophov_cte.PAT_ENC_CSN_ID = 200113815905 -- 200109673978
*/ -- ctes
/*
SELECT
    encs.PAT_ID,
    encs.person_id,
    encs.PATIENT_NAME,
    encs.PAT_ENC_CSN_ID,
    encs.HSP_PAT_ENC_CSN_ID,
    encs.CONTACT_DATE,
	encs.ENC_TIME,
    encs.APPT_TIME,
    encs.APPT_STATUS_NAME,
    encs.HOSP_ADMSN_TIME,
    encs.HOSP_DISCH_TIME,
    encs.HSP_HOSP_ADMSN_TIME,
    encs.HSP_HOSP_DISCH_TIME,
    encs.[extract],
    encs.ENC_TYPE_C,
    encs.Enc_Type,
    encs.HOSP_ADMSN_TYPE_NAME,
    encs.Prov_Id,
    encs.DEPARTMENT_ID,
    encs.DEPARTMENT_NAME,
    encs.REFERRAL_ID,
    --encs.Referral_Entry_Date,
    encs.UOS,
    encs.LOG_ID,
    encs.PRIMARY_PROCEDURE_ID,
    encs.PRIMARY_PROCEDURE_NM,
    encs.HSP_ACCOUNT_ID,
    encs.TOT_CHGS,
    encs.TOT_PMTS,
    encs.CODING_STATUS_NAME,
    encs.PRIMARY_PAYOR_ID,
    encs.PRIMARY_PLAN_ID
INTO #encs
FROM
    (
        SELECT
            ip_or_case.PAT_ID,
            ip_or_case.person_id,
            ip_or_case.PATIENT_NAME,
            ip_or_case.PAT_ENC_CSN_ID,
            ip_or_case.HSP_PAT_ENC_CSN_ID,
            ip_or_case.CONTACT_DATE,
            ip_or_case.ENC_TIME,
            ip_or_case.APPT_TIME,
            ip_or_case.APPT_STATUS_NAME,
            ip_or_case.HOSP_ADMSN_TIME,
            ip_or_case.HOSP_DISCH_TIME,
            ip_or_case.HSP_HOSP_ADMSN_TIME,
            ip_or_case.HSP_HOSP_DISCH_TIME,
            ip_or_case.ENC_TYPE_C,
            ip_or_case.Enc_Type,
            ip_or_case.HOSP_ADMSN_TYPE_NAME,
            ip_or_case.Prov_Id,
            ip_or_case.DEPARTMENT_ID,
            ip_or_case.DEPARTMENT_NAME,
            ip_or_case.REFERRAL_ID,
            --ip_or_case.Referral_Entry_Date,
            ip_or_case.UOS,
            --'ipor_cte' AS [extract]
            ip_or_case.[extract],
            ip_or_case.LOG_ID,
            ip_or_case.PRIMARY_PROCEDURE_ID,
            ip_or_case.PRIMARY_PROCEDURE_NM,
            ip_or_case.HSP_ACCOUNT_ID,
            ip_or_case.TOT_CHGS,
            ip_or_case.TOT_PMTS,
            CODING_STATUS_NAME,
            ip_or_case.PRIMARY_PAYOR_ID,
            ip_or_case.PRIMARY_PLAN_ID
        FROM
            (
SELECT
                ip_or.PAT_ID,
                ip_or.person_id,
                ip_or.PATIENT_NAME,
                ip_or.PAT_ENC_CSN_ID,
                ip_or.HSP_PAT_ENC_CSN_ID,
                ip_or.CONTACT_DATE,
                ip_or.ENC_TIME,
                ip_or.APPT_TIME,
                ip_or.APPT_STATUS_NAME,
                ip_or.HOSP_ADMSN_TIME,
                ip_or.HOSP_DISCH_TIME,
                ip_or.HSP_HOSP_ADMSN_TIME,
                ip_or.HSP_HOSP_DISCH_TIME,
                ip_or.ENC_TYPE_C,
                ip_or.Enc_Type,
                ip_or.HOSP_ADMSN_TYPE_NAME,
                ip_or.Prov_Id,
                ip_or.DEPARTMENT_ID,
                ip_or.DEPARTMENT_NAME,
                ip_or.REFERRAL_ID,
                --ip_or.Referral_Entry_Date,
                ip_or.UOS,
                ip_or.[extract],
                ROW_NUMBER() OVER(PARTITION BY ip_or.PAT_ID, ip_or.HSP_PAT_ENC_CSN_ID ORDER BY ip_or.[extract] DESC) AS case_seq,
                ip_or.LOG_ID,
                ip_or.PRIMARY_PROCEDURE_ID,
                ip_or.PRIMARY_PROCEDURE_NM,
                ip_or.HSP_ACCOUNT_ID,
                ip_or.TOT_CHGS,
                ip_or.TOT_PMTS,
                CODING_STATUS_NAME,
                ip_or.PRIMARY_PAYOR_ID,
                ip_or.PRIMARY_PLAN_ID
            FROM
                (
                    SELECT
                        ip_cte.PAT_ID,
                        ip_cte.person_id,
                        ip_cte.PATIENT_NAME,
                        ip_cte.PAT_ENC_CSN_ID,
                        ip_cte.HSP_PAT_ENC_CSN_ID,
                        ip_cte.CONTACT_DATE,
                        ip_cte.APPT_TIME AS ENC_TIME,
                        CAST(NULL AS DATETIME) AS APPT_TIME,
                        ip_cte.APPT_STATUS_NAME,
                        ip_cte.HOSP_ADMSN_TIME,
                        ip_cte.HOSP_DISCH_TIME,
                        ip_cte.HSP_HOSP_ADMSN_TIME,
                        ip_cte.HSP_HOSP_DISCH_TIME,
                        ip_cte.ENC_TYPE_C,
                        ip_cte.Enc_Type,
                        ip_cte.HOSP_ADMSN_TYPE_NAME,
                        ip_cte.Prov_Id,
                        ip_cte.DEPARTMENT_ID,
                        ip_cte.DEPARTMENT_NAME,
                        ip_cte.REFERRAL_ID,
                        --ip_cte.Referral_Entry_Date,
                        ip_cte.UOS,
                        ip_cte.[extract],
                        ip_cte.LOG_ID,
                        ip_cte.PRIMARY_PROCEDURE_ID,
                        ip_cte.PRIMARY_PROCEDURE_NM,
                        ip_cte.HSP_ACCOUNT_ID,
                        ip_cte.TOT_CHGS,
                        ip_cte.TOT_PMTS,
                        ip_cte.CODING_STATUS_NAME,
                        ip_cte.PRIMARY_PAYOR_ID,
                        ip_cte.PRIMARY_PLAN_ID
                    FROM ip_cte
                UNION ALL
                    SELECT
                        or_cte.PAT_ID,
                        or_cte.person_id,
                        or_cte.PATIENT_NAME,
                        or_cte.PAT_ENC_CSN_ID,
                        or_cte.HSP_PAT_ENC_CSN_ID,
                        or_cte.CONTACT_DATE,
                        or_cte.APPT_TIME AS ENC_TIME,
                        CAST(NULL AS DATETIME) AS APPT_TIME,
                        or_cte.APPT_STATUS_NAME,
                        or_cte.HOSP_ADMSN_TIME,
                        or_cte.HOSP_DISCH_TIME,
                        or_cte.HSP_HOSP_ADMSN_TIME,
                        or_cte.HSP_HOSP_DISCH_TIME,
                        or_cte.ENC_TYPE_C,
                        or_cte.Enc_Type,
                        or_cte.HOSP_ADMSN_TYPE_NAME,
                        or_cte.Prov_Id,
                        or_cte.DEPARTMENT_ID,
                        or_cte.DEPARTMENT_NAME,
                        or_cte.REFERRAL_ID,
                        --or_cte.Referral_Entry_Date,
                        or_cte.UOS,
                        or_cte.[extract],
                        or_cte.LOG_ID,
                        or_cte.PRIMARY_PROCEDURE_ID,
                        or_cte.PRIMARY_PROCEDURE_NM,
                        or_cte.HSP_ACCOUNT_ID,
                        or_cte.TOT_CHGS,
                        or_cte.TOT_PMTS,
                        or_cte.CODING_STATUS_NAME,
                        or_cte.PRIMARY_PAYOR_ID,
                        or_cte.PRIMARY_PLAN_ID
                    FROM or_cte
) ip_or
) ip_or_case
        WHERE ip_or_case.case_seq = 1
    UNION ALL
        SELECT
            ophov_cte.PAT_ID,
            ophov_cte.person_id,
            ophov_cte.PATIENT_NAME,
            ophov_cte.PAT_ENC_CSN_ID,
            ophov_cte.HSP_PAT_ENC_CSN_ID,
            ophov_cte.CONTACT_DATE,
            ophov_cte.APPT_TIME AS ENC_TIME,
            ophov_cte.APPT_TIME,
            ophov_cte.APPT_STATUS_NAME,
            ophov_cte.HOSP_ADMSN_TIME,
            ophov_cte.HOSP_DISCH_TIME,
            ophov_cte.HSP_HOSP_ADMSN_TIME,
            ophov_cte.HSP_HOSP_DISCH_TIME,
            ophov_cte.ENC_TYPE_C,
            ophov_cte.Enc_Type,
            ophov_cte.HOSP_ADMSN_TYPE_NAME,
            ophov_cte.Prov_Id,
            ophov_cte.DEPARTMENT_ID,
            ophov_cte.DEPARTMENT_NAME,
            ophov_cte.REFERRAL_ID,
            --ophov_cte.Referral_Entry_Date,
            ophov_cte.UOS,
            ophov_cte.[extract],
            ophov_cte.LOG_ID,
            ophov_cte.PRIMARY_PROCEDURE_ID,
            ophov_cte.PRIMARY_PROCEDURE_NM,
            ophov_cte.HSP_ACCOUNT_ID,
            ophov_cte.TOT_CHGS,
            ophov_cte.TOT_PMTS,
            ophov_cte.CODING_STATUS_NAME,
            ophov_cte.PRIMARY_PAYOR_ID,
            ophov_cte.PRIMARY_PLAN_ID
        FROM ophov_cte
) encs
--WHERE extract = 'ip_cte'
ORDER BY
	encs.Prov_Id,
	encs.REFERRAL_ID,
	encs.DEPARTMENT_ID,
	encs.HSP_ACCOUNT_ID,
	encs.PAT_ENC_CSN_ID

-- Create index for temp table #encs

--CREATE CLUSTERED INDEX IX_encs ON #encs (Prov_Id, REFERRAL_ID, DEPARTMENT_ID, HSP_ACCOUNT_ID, PAT_ENC_CSN_ID)
CREATE CLUSTERED INDEX IX_encs ON #encs (Prov_Id, REFERRAL_ID, DEPARTMENT_ID, HSP_ACCOUNT_ID, HSP_PAT_ENC_CSN_ID)
*/ -- encs
/*
SELECT
    --enc.person_id AS MRN,
    enc.PAT_ID AS Epic_Patient_Id,
    enc.person_id,
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
	enc.ENC_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.APPT_TIME,121),'') AS Appointment_Time,
    ISNULL(CONVERT(VARCHAR(100),enc.HOSP_ADMSN_TIME,121),'') AS Hosp_Admsn_Time,
    COALESCE(enc.APPT_TIME, enc.HOSP_ADMSN_TIME) AS Appt_Admsn_Time,
    enc.[extract],
    enc.PAT_ENC_CSN_ID AS Encounter_CSN,
    enc.HSP_PAT_ENC_CSN_ID AS HSP_Encounter_CSN,
    enc.CONTACT_DATE AS Contact_Date,
    rfls.ENTRY_DATE AS Referral_Entry_Date,
    --enc.Referral_Entry_Date,
    --enc.APPT_TIME,
    --enc.APPT_STATUS_NAME,
    ISNULL(CONVERT(VARCHAR(100),enc.APPT_STATUS_NAME),'') AS Appointment_Status,
    --enc.HOSP_ADMSN_TIME,
    --enc.HOSP_DISCH_TIME,
    ISNULL(CONVERT(VARCHAR(100),enc.HOSP_DISCH_TIME,121),'') AS Hosp_Disch_Time,
    enc.HSP_HOSP_ADMSN_TIME,
    enc.HSP_HOSP_DISCH_TIME,
    enc.ENC_TYPE_C,
    enc.Enc_Type,
    enc.HOSP_ADMSN_TYPE_NAME AS Hosp_Admsn_Type,
    enc.Prov_Id,
    enc.DEPARTMENT_ID AS Department_Id,
    enc.DEPARTMENT_NAME AS Department_Name,
    enc.REFERRAL_ID AS Referral_Id,
    --rfls.NPI AS Referral_NPI,
    CASE WHEN (rfls.NPI IS NULL OR (rfls.NPI IS NOT NULL AND rfls.NPI = 0)) THEN enc.Prov_Id ELSE CAST(rfls.NPI AS VARCHAR(18)) END AS Referral_NPI,
    --enc.APPT_PRC_ID AS Appt_Proc_Id,
    --enc.PROC_NAME AS Appt_Proc_Name,
    '"' + TRIM(ser.Prov_Nme) + '"' AS Provider_Name,
    ser.Prov_Typ AS Provider_Type,
    ser.STAFF_RESOURCE Provider_Resource_Type,
    ser.NPI AS provider_NPI,
    --zsp.NAME AS SPECIALTY_NAME,
    ISNULL(CONVERT(VARCHAR(100),zsp.NAME),'') AS Provider_Specialty,
    --dvsn.Epic_Financial_Division,
    ISNULL(CONVERT(VARCHAR(100),dvsn.Epic_Financial_Division),'') AS Epic_Financial_Division,
    --dvsn.Epic_Financial_Subdivision
    ISNULL(CONVERT(VARCHAR(100),dvsn.Epic_Financial_Subdivision),'') AS Epic_Financial_Subdivision,
    --mdmhsp.HOSPITAL_CODE,
    COALESCE(mdmhsp.HOSPITAL_CODE,orhsp.Hospital_Code,NULL) AS HOSPITAL_CODE,
    enc.UOS,
    enc.LOG_ID,
    enc.PRIMARY_PROCEDURE_ID,
    enc.PRIMARY_PROCEDURE_NM,
    enc.HSP_ACCOUNT_ID,
    enc.TOT_CHGS,
    enc.TOT_PMTS,
    enc.CODING_STATUS_NAME,
    enc.PRIMARY_PAYOR_ID,
    enc.PRIMARY_PLAN_ID,
    epm.PAYOR_NAME,
    epm.FINANCIAL_CLASS,
    FIN_CLASS.NAME AS FIN_CLASS_NAME,
    epp.BENEFIT_PLAN_NAME,
	ROW_NUMBER() OVER(PARTITION BY enc.HSP_PAT_ENC_CSN_ID ORDER BY enc.ENC_TIME) AS enc_seq

INTO #encs_plus
FROM #encs enc
    --LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser ON ser.PROV_ID = enc.Prov_Id
    LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Clrt_SERsrc ser ON ser.PROV_ID = enc.Prov_Id
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
    LEFT OUTER JOIN #rfls rfls
    ON rfls.REFERRAL_ID = enc.REFERRAL_ID
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
    --ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.RPT_GRP_SIX AS INT)
    --	AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.RPT_GRP_EIGHT AS INT))
    ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.Financial_Division AS INT)
        AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.Financial_SubDivision AS INT))
    LEFT OUTER JOIN #mdmhsp mdmhsp ON mdmhsp.EPIC_DEPARTMENT_ID = enc.DEPARTMENT_ID
    LEFT OUTER JOIN @ORtoHospital orhsp ON orhsp.Department_Id = enc.DEPARTMENT_ID
    LEFT JOIN CLARITY..CLARITY_EPM epm ON epm.PAYOR_ID = enc.PRIMARY_PAYOR_ID
    LEFT JOIN CLARITY..ZC_FINANCIAL_CLASS FIN_CLASS ON FIN_CLASS.FINANCIAL_CLASS = epm.FINANCIAL_CLASS
    LEFT JOIN CLARITY.dbo.CLARITY_EPP epp ON epp.BENEFIT_PLAN_ID = enc.PRIMARY_PLAN_ID
--ORDER BY
--	enc.extract,
--	enc.HSP_ACCOUNT_ID,
--	enc.PAT_ENC_CSN_ID
ORDER BY
	enc.HSP_PAT_ENC_CSN_ID

-- Create index for temp table #encs_plus

--CREATE CLUSTERED INDEX IX_encs_plus ON #encs_plus (extract, HSP_ACCOUNT_ID, Encounter_CSN)
--CREATE CLUSTERED INDEX IX_encs_plus ON #encs_plus (Encounter_CSN)
CREATE CLUSTERED INDEX IX_encs_plus ON #encs_plus (HSP_Encounter_CSN, enc_seq)
*/ -- encs_plus
/*
SELECT
	*
INTO #encs_plus_plus
FROM #encs_plus encs
WHERE encs.enc_seq = 1
ORDER BY
	encs.HSP_Encounter_CSN

-- Create index for temp table #encs_plus_lpus

CREATE CLUSTERED INDEX IX_encs_plus_plus ON #encs_plus_plus (HSP_Encounter_CSN)

SELECT DISTINCT
	extract,
	Epic_Patient_Id,
	Appt_Admsn_Time,
	Encounter_CSN,
	HSP_Encounter_CSN,
	HSP_ACCOUNT_ID,
	Department_Id,
	Department_Name
FROM #encs_plus_plus
ORDER BY
    Epic_Patient_Id,
	Appt_Admsn_Time,
	extract,
	HSP_Encounter_CSN
*/ -- encs_plus_plus
/*
SELECT
        fin2.extract
	  , fin2.Epic_Patient_Id
	  , fin2.Appt_Admsn_Time
      , fin2.PAT_ENC_CSN_ID
	  , fin2.HSP_PAT_ENC_CSN_ID
      , fin2.HSP_ACCOUNT_ID AS EncounterNumber
      , fin2.DEPARTMENT AS DepartmentCode
      , fin2.DEPARTMENT_NAME AS DepartmentName
      , fin2.PROC_CODE AS SvcItemChargeCode
      , fin2.PROCEDURE_DESC AS ChargeCodeName
      , fin2.UB_REV_CODE_ID AS UB92RevCode
      , fin2.REVENUE_CODE_NAME AS UB92RevCodeDescription
	  , fin2.UB_GROUPER_NAME
	  , fin2.UB_TYPE_OF_SERVICE_NAME
      , fin2.CPT4HCPCSCode
	  , fin2.CPT4HCPCSName
	  , fin2.GUDID
	  , fin2.NationalDrugClassCode
	  , fin2.VendorName
	  , fin2.SvcItemDate
	  , fin2.SvcItemTime
	  , fin2.Units
	  , fin2.TX_AMOUNT
	  , fin2.TX_NUM_IN_HOSPACCT
	  , fin2.Direct_Cost
	  , fin2.Indirect_Cost

    ----,tx.TX_ID

/*
SELECT DISTINCT
        fin2.extract
	  , fin2.Epic_Patient_Id
	  , fin2.Appt_Admsn_Time
      , fin2.PAT_ENC_CSN_ID
	  , fin2.HSP_PAT_ENC_CSN_ID
      , fin2.HSP_ACCOUNT_ID AS EncounterNumber
      , fin2.DEPARTMENT AS DepartmentCode
      , fin2.DEPARTMENT_NAME AS DepartmentName
   --   , fin2.PROC_CODE AS SvcItemChargeCode
   --   , fin2.PROCEDURE_DESC AS ChargeCodeName
   --   , fin2.UB_REV_CODE_ID AS UB92RevCode
   --   , fin2.REVENUE_CODE_NAME AS UB92RevCodeDescription
	  --, fin2.UB_GROUPER_NAME
	  --, fin2.UB_TYPE_OF_SERVICE_NAME
   --   , fin2.CPT4HCPCSCode
	  --, fin2.CPT4HCPCSName
	  --, fin2.GUDID
	  --, fin2.NationalDrugClassCode
	  --, fin2.VendorName
	  --, fin2.SvcItemDate
	  --, fin2.SvcItemTime
	  --, fin2.Units
	  --, fin2.TX_AMOUNT
	  --, fin2.TX_NUM_IN_HOSPACCT
	  --, fin2.Direct_Cost
	  --, fin2.Indirect_Cost
*/
	FROM
	(
	SELECT DISTINCT
		encs.extract,
		encs.Epic_Patient_Id,
		encs.Appt_Admsn_Time,
		encs.Encounter_CSN AS PAT_ENC_CSN_ID,
		encs.HSP_Encounter_CSN AS HSP_PAT_ENC_CSN_ID,
		fin.HSP_ACCOUNT_ID,
		fin.DEPARTMENT,
		fin.DEPARTMENT_NAME,
		fin.PROC_CODE,
		fin.PROCEDURE_DESC,
		fin.UB_REV_CODE_ID,
		fin.REVENUE_CODE_NAME,
		fin.CPT4HCPCSCode,
		fin.CPT4HCPCSName,
		fin.GUDID,
		fin.NationalDrugClassCode,
		fin.VendorName,
		fin.SvcItemDate,
		fin.SvcItemTime,
		fin.Units,
		fin.TX_AMOUNT,
		fin.Direct_Cost,
		fin.Indirect_Cost,
		fin.TX_NUM_IN_HOSPACCT,
		fin.UB_GROUPER_NAME,
		fin.UB_TYPE_OF_SERVICE_NAME
	FROM #encs_plus_plus encs
	LEFT OUTER JOIN
	(
	SELECT
		ha.HSP_ACCOUNT_ID,
		tx.PAT_ENC_CSN_ID,
		tx.DEPARTMENT,
		txdep.DEPARTMENT_NAME,
		eap.PROC_CODE,
		tx.PROCEDURE_DESC,
		tx.UB_REV_CODE_ID,
		cl_ub.REVENUE_CODE_NAME,
		CASE
            WHEN COALESCE(tx.HCPCS_CODE /*HTR 254*/, tx.CPT_CODE /*EAP 2000*/) = '9990' THEN '0' --9990 is conversion code!
            WHEN LEN(COALESCE(tx.HCPCS_CODE /*HCPCS Code for this transaction.*/, tx.CPT_CODE /*The CPT™ code stored in a charge transaction.*/)) > 5 THEN
                 '' --custom/junk codes
            ELSE COALESCE(tx.HCPCS_CODE, tx.CPT_CODE) END AS CPT4HCPCSCode,
			'N/A' AS CPT4HCPCSName,
		CASE WHEN NOT imp.STATIC_UDI LIKE '%[^0-9]%' THEN imp.STATIC_UDI ELSE '' END AS GUDID, --(Primary Device ID Number, numeric)
		COALESCE(ndc.NDC_CODE, '')  AS NationalDrugClassCode,
		COALESCE(zcmanu.NAME, '')  AS VendorName,
		CONVERT(VARCHAR(10), tx.SERVICE_DATE, 23) AS SvcItemDate,
		'' AS SvcItemTime,
		CAST(tx.QUANTITY AS INT) AS Units,
		tx.TX_AMOUNT,
		cst.Direct_Cost,
		cst.Indirect_Cost,
		tx.TX_NUM_IN_HOSPACCT,
		zug.NAME AS UB_GROUPER_NAME,
		zutos.NAME AS UB_TYPE_OF_SERVICE_NAME
	FROM CLARITY..HSP_ACCOUNT AS ha
    INNER JOIN
        (
        SELECT
            PAT_ID
          , IDENTITY_ID
          , IDENTITY_TYPE_ID
        FROM CLARITY..IDENTITY_ID
        WHERE IDENTITY_TYPE_ID = '14'
        ) AS idx -- MRN
        ON idx.PAT_ID = ha.PAT_ID
    INNER JOIN CLARITY..VALID_PATIENT vp
        ON ha.PAT_ID = vp.PAT_ID
    INNER JOIN CLARITY..HSP_ACCT_SBO AS sbo
        ON ha.HSP_ACCOUNT_ID = sbo.HSP_ACCOUNT_ID
    INNER JOIN CLARITY..CLARITY_DEP AS sa
        ON ha.DISCH_DEPT_ID = sa.DEPARTMENT_ID
    -- transactions
    INNER JOIN CLARITY..HSP_TRANSACTIONS AS tx
        ON tx.HSP_ACCOUNT_ID = ha.HSP_ACCOUNT_ID
    LEFT OUTER JOIN CLARITY..CLARITY_DEP AS chg_dep
        ON chg_dep.DEPARTMENT_ID = tx.DEPARTMENT
    LEFT OUTER JOIN CLARITY_App.Rptg.TX_COSTS_STRATA AS cst
        ON cst.TX_ID = tx.TX_ID
    INNER JOIN CLARITY..CLARITY_DEP AS txdep
        ON tx.DEPARTMENT = txdep.DEPARTMENT_ID
    INNER JOIN CLARITY..CLARITY_EAP AS eap
        ON tx.PROC_ID = eap.PROC_ID
    LEFT OUTER JOIN CLARITY..CL_UB_REV_CODE AS cl_ub
        ON cl_ub.UB_REV_CODE_ID = tx.UB_REV_CODE_ID
    LEFT OUTER JOIN CLARITY..RX_NDC AS ndc
        ON ndc.NDC_ID = tx.NDC_ID
    LEFT OUTER JOIN CLARITY..OR_SPLY AS osp
        ON tx.SUP_ID = osp.SUPPLY_ID
    LEFT OUTER JOIN CLARITY..OR_SPLY_MANFACTR manu
        ON osp.SUPPLY_ID = manu.ITEM_ID
    LEFT OUTER JOIN CLARITY..ZC_OR_MANUFACTURER zcmanu
        ON manu.MANUFACTURER_C = zcmanu.MANUFACTURER_C
    LEFT OUTER JOIN CLARITY..OR_IMP imp
        ON osp.SUPPLY_ID = imp.IMPLANT_ID
    LEFT OUTER JOIN CLARITY..ZC_UBC_GROUPER zug
		ON zug.UBC_GROUPER_C = cl_ub.UBC_GROUPER_C
    LEFT OUTER JOIN CLARITY..ZC_UBC_TYPE_OF_SER zutos
		ON zutos.UBC_TYPE_OF_SER_C = cl_ub.TYPE_OF_SERVICE_C
    WHERE
        1 = 1
        AND
            (
            (
            ha.ACCT_BASECLS_HA_C IN ('1') -- Inpatient
            --AND ha.DISCH_DATE_TIME >= @StartDate
            --AND ha.DISCH_DATE_TIME <= @EndDate
            )
            OR
                (
                ha.ACCT_BASECLS_HA_C IN ('2', '3') -- outpatient
                --AND ha.ADM_DATE_TIME >= @StartDate
                --AND ha.ADM_DATE_TIME <= @EndDate
                )
            )
        AND
            (
            sbo.SBO_HAR_TYPE_C IS NULL
            OR sbo.SBO_HAR_TYPE_C = 0
            )
        AND NOT ha.PRIM_ENC_CSN_ID IS NULL
        AND ha.TOT_CHGS > 0
        AND ha.ACCT_BILLSTS_HA_C <> '40' -- voided
        --AND ha.DISCH_DATE_TIME IS NOT NULL
        AND sa.SERV_AREA_ID = '10' --uva service area
        AND vp.IS_VALID_PAT_YN = 'Y'
        AND tx.IS_SYSTEM_ADJ_YN IS NULL
        AND tx.TX_TYPE_HA_C = 1 -- charge
--		AND tx.PAT_ENC_CSN_ID IN (200112367096
--,200112397966
--,200117281370
--)
		--AND cl_ub.REVENUE_CODE_NAME NOT LIKE 'PHARMACY%'
		--AND cl_ub.REVENUE_CODE_NAME NOT LIKE 'LABORATORY%'
		--AND cl_ub.REVENUE_CODE_NAME NOT LIKE '%ROOM%'
		--AND tx.UB_REV_CODE_ID <> '510' -- CLINIC - GENERAL CLASSIFICATION
		) fin
		--ON fin.PAT_ENC_CSN_ID = encs.Encounter_CSN
		ON fin.PAT_ENC_CSN_ID = encs.HSP_Encounter_CSN
		AND fin.DEPARTMENT = encs.Department_Id
		AND fin.HSP_ACCOUNT_ID = encs.HSP_ACCOUNT_ID
		) fin2
--    WHERE
--        1 = 1
--        AND
--            (
--            (
--            ha.ACCT_BASECLS_HA_C IN ('1') -- Inpatient
--            --AND ha.DISCH_DATE_TIME >= @StartDate
--            --AND ha.DISCH_DATE_TIME <= @EndDate
--            )
--            OR
--                (
--                ha.ACCT_BASECLS_HA_C IN ('2', '3') -- outpatient
--                --AND ha.ADM_DATE_TIME >= @StartDate
--                --AND ha.ADM_DATE_TIME <= @EndDate
--                )
--            )
--        AND
--            (
--            sbo.SBO_HAR_TYPE_C IS NULL
--            OR sbo.SBO_HAR_TYPE_C = 0
--            )
--        AND NOT ha.PRIM_ENC_CSN_ID IS NULL
--        AND ha.TOT_CHGS > 0
--        AND ha.ACCT_BILLSTS_HA_C <> '40' -- voided
--        --AND ha.DISCH_DATE_TIME IS NOT NULL
--        AND sa.SERV_AREA_ID = '10' --uva service area
--        AND vp.IS_VALID_PAT_YN = 'Y'
--        AND tx.IS_SYSTEM_ADJ_YN IS NULL
--        AND tx.TX_TYPE_HA_C = 1 -- charge
--		AND tx.PAT_ENC_CSN_ID IN (200112367096
--,200112397966
--,200117281370
--)
--ORDER BY
--	fin2.extract,
--	fin2.PAT_ENC_CSN_ID
--ORDER BY
--	fin2.extract,
--	fin2.PAT_ENC_CSN_ID,
--	fin2.TX_NUM_IN_HOSPACCT
--ORDER BY
--    fin2.Epic_Patient_Id,
--	fin2.Appt_Admsn_Time,
--	fin2.extract,
--	fin2.PAT_ENC_CSN_ID,
--	fin2.TX_NUM_IN_HOSPACCT
ORDER BY
    fin2.Epic_Patient_Id,
	fin2.Appt_Admsn_Time,
	fin2.extract,
	fin2.PAT_ENC_CSN_ID--,
	--fin2.TX_NUM_IN_HOSPACCT
*/
/*
/* =================== INPATIENT ================== */

SELECT
	rnlist.extract,
	rnlist.HSP_ACCOUNT_ID,
	--rnlist.PRIMARY_PROCEDURE_ID,
	--rnlist.PRIMARY_PROCEDURE_NM,
 --   rnlist.SOURCE_KEY,
 --   rnlist.LINE,
 --   rnlist.SOURCE_NAME,
 --   rnlist.SOURCE_ABBR,
 --   rnlist.rn,
    --rnlist.DX_ID,
    rnlist.ICD_PX_ID,
    rnlist.NAME,
    --rnlist.REF_BILL_CODE_SET_C,
    rnlist.REF_BILL_CODE_SET_NAME--,
    --rnlist.REF_BILL_CODE,
    --rnlist.EXCLUDE_YN,
    --rnlist.AFFECTS_SOI_YN,
    --rnlist.AFFECTS_ROM_YN,
    --rnlist.DX_POA_C,
    --rnlist.DX_POA_NAME,
    --rnlist.DX_COMORBIDITY_C,
    --rnlist.DX_COMORBIDITY_NAME,
    --rnlist.DX_HAC_YN,
    --rnlist.DX_AFFECTS_DRG_YN,
    --rnlist.DX_SOI_C,
    --rnlist.DX_SOI_NAME,
    --rnlist.DX_ROM_C,
    --rnlist.DX_ROM_NAME,
    --rnlist.PX_PERF_PROV_ID,
    --rnlist.PX_PERF_PROV_NMWID,
    --rnlist.PX_EVENT_NUMBER,
    --rnlist.PX_DATE,
    --rnlist.PX_CPT_MODIFIERS,
    --rnlist.PX_CPT_LCD_CODE,
    --rnlist.PX_CPT_OCE_EDIT_CODE,
    --rnlist.PX_CPT_APC_CODE,
    --rnlist.PX_CPT_APC_PMT_IND,
    --rnlist.PX_CPT_APC_PMT_STS_IND,
    --rnlist.PX_CPT_APC_WEIGHT,
    --rnlist.PX_CPT_APC_FAC_RMB_AMT,
    --rnlist.PX_CPT_HCFA_PAYMT_AMT,
    --rnlist.PX_CPT_COPAY_AMT,
    --rnlist.PX_CPT_PAY_RT_UNIT_AMT,
    --rnlist.PX_CPT_CODE_AFF_DRG_YN,
    --rnlist.PX_CPT_REIMB_TYPE,
    --rnlist.PX_CPT_REV_CODE_ID,
    --rnlist.PX_CPT_REV_CODE,
    --rnlist.PX_CPT_REV_CODE_NAME,
    --rnlist.PX_CPT_QUANTITY,
    --rnlist.CODE_INT_MOD_1_ID,
    --rnlist.CODE_INT_MOD_1_CODE,
    --rnlist.CODE_INT_MOD_1_NAME,
    --rnlist.CODE_INT_MOD_2_ID,
    --rnlist.CODE_INT_MOD_2_CODE,
    --rnlist.CODE_INT_MOD_2_NAME,
    --rnlist.CODE_INT_MOD_3_ID,
    --rnlist.CODE_INT_MOD_3_CODE,
    --rnlist.CODE_INT_MOD_3_NAME,
    --rnlist.CODE_INT_MOD_4_ID,
    --rnlist.CODE_INT_MOD_4_CODE,
    --rnlist.CODE_INT_MOD_4_NAME,
    --rnlist.CODE_INT_ROOM_RATE,
    --rnlist.CODE_INT_UNUSED_YN,
    --rnlist.CODE_INT_UNUSED_RSN_C,
    --rnlist.CODE_INT_UNUSED_RSN_NAME,
    --rnlist.PX_EVENT_DATE,
    --rnlist.PX_EVENT_PROV_ID,
    --rnlist.PX_EVENT_PROV_NM_WID,
    --rnlist.PX_EVENT_COMMENT,
    --rnlist.ASA_CLASS,
    --rnlist.ANESTH_TYPE_HA_C,
    --rnlist.ANESTH_TYPE_HA_NAME,
    --rnlist.ANESTH_PROV_ID,
    --rnlist.ANESTH_PROV_NM_WID,
    --rnlist.CODING_INFO_CPT_LINE
INTO #ip_proc
FROM
(
SELECT
	ip_encs.extract,
    ip_encs.HSP_ACCOUNT_ID,
    --vlist.HSP_ACCOUNT_ID,
	--ip_encs.PRIMARY_PROCEDURE_ID,
	--ip_encs.PRIMARY_PROCEDURE_NM,
    vlist.SOURCE_KEY,
    vlist.LINE,
    vlist.SOURCE_NAME,
    vlist.SOURCE_ABBR,
    vlist.rn,
    vlist.DX_ID,
    vlist.ICD_PX_ID,
    vlist.NAME,
    vlist.REF_BILL_CODE_SET_C,
    vlist.REF_BILL_CODE_SET_NAME,
    vlist.REF_BILL_CODE,
    vlist.EXCLUDE_YN,
    vlist.AFFECTS_SOI_YN,
    vlist.AFFECTS_ROM_YN,
    vlist.DX_POA_C,
    vlist.DX_POA_NAME,
    vlist.DX_COMORBIDITY_C,
    vlist.DX_COMORBIDITY_NAME,
    vlist.DX_HAC_YN,
    vlist.DX_AFFECTS_DRG_YN,
    vlist.DX_SOI_C,
    vlist.DX_SOI_NAME,
    vlist.DX_ROM_C,
    vlist.DX_ROM_NAME,
    vlist.PX_PERF_PROV_ID,
    vlist.PX_PERF_PROV_NMWID,
    vlist.PX_EVENT_NUMBER,
    vlist.PX_DATE,
    vlist.PX_CPT_MODIFIERS,
    vlist.PX_CPT_LCD_CODE,
    vlist.PX_CPT_OCE_EDIT_CODE,
    vlist.PX_CPT_APC_CODE,
    vlist.PX_CPT_APC_PMT_IND,
    vlist.PX_CPT_APC_PMT_STS_IND,
    vlist.PX_CPT_APC_WEIGHT,
    vlist.PX_CPT_APC_FAC_RMB_AMT,
    vlist.PX_CPT_HCFA_PAYMT_AMT,
    vlist.PX_CPT_COPAY_AMT,
    vlist.PX_CPT_PAY_RT_UNIT_AMT,
    vlist.PX_CPT_CODE_AFF_DRG_YN,
    vlist.PX_CPT_REIMB_TYPE,
    vlist.PX_CPT_REV_CODE_ID,
    vlist.PX_CPT_REV_CODE,
    vlist.PX_CPT_REV_CODE_NAME,
    vlist.PX_CPT_QUANTITY,
    vlist.CODE_INT_MOD_1_ID,
    vlist.CODE_INT_MOD_1_CODE,
    vlist.CODE_INT_MOD_1_NAME,
    vlist.CODE_INT_MOD_2_ID,
    vlist.CODE_INT_MOD_2_CODE,
    vlist.CODE_INT_MOD_2_NAME,
    vlist.CODE_INT_MOD_3_ID,
    vlist.CODE_INT_MOD_3_CODE,
    vlist.CODE_INT_MOD_3_NAME,
    vlist.CODE_INT_MOD_4_ID,
    vlist.CODE_INT_MOD_4_CODE,
    vlist.CODE_INT_MOD_4_NAME,
    vlist.CODE_INT_ROOM_RATE,
    vlist.CODE_INT_UNUSED_YN,
    vlist.CODE_INT_UNUSED_RSN_C,
    vlist.CODE_INT_UNUSED_RSN_NAME,
    vlist.PX_EVENT_DATE,
    vlist.PX_EVENT_PROV_ID,
    vlist.PX_EVENT_PROV_NM_WID,
    vlist.PX_EVENT_COMMENT,
    vlist.ASA_CLASS,
    vlist.ANESTH_TYPE_HA_C,
    vlist.ANESTH_TYPE_HA_NAME,
    vlist.ANESTH_PROV_ID,
    vlist.ANESTH_PROV_NM_WID,
    vlist.CODING_INFO_CPT_LINE
FROM
(
SELECT DISTINCT
	encs.extract,
	encs.HSP_ACCOUNT_ID--,
	--encs.PRIMARY_PROCEDURE_ID,
	--encs.PRIMARY_PROCEDURE_NM
FROM #encs_plus encs
WHERE encs.extract = 'ip_cte'
) ip_encs
LEFT OUTER JOIN
(
SELECT
	HSP_ACCOUNT_ID,
    SOURCE_KEY,
    LINE,
    SOURCE_NAME,
    SOURCE_ABBR,
	ROW_NUMBER() OVER(PARTITION BY HSP_ACCOUNT_ID, SOURCE_KEY ORDER BY LINE) AS rn,
    DX_ID,
    ICD_PX_ID,
    NAME,
    REF_BILL_CODE_SET_C,
    REF_BILL_CODE_SET_NAME,
    REF_BILL_CODE,
    EXCLUDE_YN,
    AFFECTS_SOI_YN,
    AFFECTS_ROM_YN,
    DX_POA_C,
    DX_POA_NAME,
    DX_COMORBIDITY_C,
    DX_COMORBIDITY_NAME,
    DX_HAC_YN,
    DX_AFFECTS_DRG_YN,
    DX_SOI_C,
    DX_SOI_NAME,
    DX_ROM_C,
    DX_ROM_NAME,
    PX_PERF_PROV_ID,
    PX_PERF_PROV_NMWID,
    PX_EVENT_NUMBER,
    PX_DATE,
    PX_CPT_MODIFIERS,
    PX_CPT_LCD_CODE,
    PX_CPT_OCE_EDIT_CODE,
    PX_CPT_APC_CODE,
    PX_CPT_APC_PMT_IND,
    PX_CPT_APC_PMT_STS_IND,
    PX_CPT_APC_WEIGHT,
    PX_CPT_APC_FAC_RMB_AMT,
    PX_CPT_HCFA_PAYMT_AMT,
    PX_CPT_COPAY_AMT,
    PX_CPT_PAY_RT_UNIT_AMT,
    PX_CPT_CODE_AFF_DRG_YN,
    PX_CPT_REIMB_TYPE,
    PX_CPT_REV_CODE_ID,
    PX_CPT_REV_CODE,
    PX_CPT_REV_CODE_NAME,
    PX_CPT_QUANTITY,
    CODE_INT_MOD_1_ID,
    CODE_INT_MOD_1_CODE,
    CODE_INT_MOD_1_NAME,
    CODE_INT_MOD_2_ID,
    CODE_INT_MOD_2_CODE,
    CODE_INT_MOD_2_NAME,
    CODE_INT_MOD_3_ID,
    CODE_INT_MOD_3_CODE,
    CODE_INT_MOD_3_NAME,
    CODE_INT_MOD_4_ID,
    CODE_INT_MOD_4_CODE,
    CODE_INT_MOD_4_NAME,
    CODE_INT_ROOM_RATE,
    CODE_INT_UNUSED_YN,
    CODE_INT_UNUSED_RSN_C,
    CODE_INT_UNUSED_RSN_NAME,
    PX_EVENT_DATE,
    PX_EVENT_PROV_ID,
    PX_EVENT_PROV_NM_WID,
    PX_EVENT_COMMENT,
    ASA_CLASS,
    ANESTH_TYPE_HA_C,
    ANESTH_TYPE_HA_NAME,
    ANESTH_PROV_ID,
    ANESTH_PROV_NM_WID,
    CODING_INFO_CPT_LINE
FROM CLARITY.dbo.V_CODING_ALL_DX_PX_LIST
WHERE SOURCE_KEY = 11
) vlist
ON vlist.HSP_ACCOUNT_ID = ip_encs.HSP_ACCOUNT_ID
) rnlist
WHERE rnlist.rn = 1
/* =================== INPATIENT ================== */

/* =================== SURGERY ================== */

SELECT
	rnlist.extract,
	rnlist.HSP_ACCOUNT_ID,
	rnlist.PRIMARY_PROCEDURE_ID,
	rnlist.PRIMARY_PROCEDURE_NM--,
    --rnlist.SOURCE_KEY,
    --rnlist.LINE,
    --rnlist.SOURCE_NAME,
    --rnlist.SOURCE_ABBR,
    --rnlist.rn,
    --rnlist.DX_ID,
    --rnlist.ICD_PX_ID,
    --rnlist.NAME,
    --rnlist.REF_BILL_CODE_SET_C,
    --rnlist.REF_BILL_CODE_SET_NAME,
    --rnlist.REF_BILL_CODE,
    --rnlist.EXCLUDE_YN,
    --rnlist.AFFECTS_SOI_YN,
    --rnlist.AFFECTS_ROM_YN,
    --rnlist.DX_POA_C,
    --rnlist.DX_POA_NAME,
    --rnlist.DX_COMORBIDITY_C,
    --rnlist.DX_COMORBIDITY_NAME,
    --rnlist.DX_HAC_YN,
    --rnlist.DX_AFFECTS_DRG_YN,
    --rnlist.DX_SOI_C,
    --rnlist.DX_SOI_NAME,
    --rnlist.DX_ROM_C,
    --rnlist.DX_ROM_NAME,
    --rnlist.PX_PERF_PROV_ID,
    --rnlist.PX_PERF_PROV_NMWID,
    --rnlist.PX_EVENT_NUMBER,
    --rnlist.PX_DATE,
    --rnlist.PX_CPT_MODIFIERS,
    --rnlist.PX_CPT_LCD_CODE,
    --rnlist.PX_CPT_OCE_EDIT_CODE,
    --rnlist.PX_CPT_APC_CODE,
    --rnlist.PX_CPT_APC_PMT_IND,
    --rnlist.PX_CPT_APC_PMT_STS_IND,
    --rnlist.PX_CPT_APC_WEIGHT,
    --rnlist.PX_CPT_APC_FAC_RMB_AMT,
    --rnlist.PX_CPT_HCFA_PAYMT_AMT,
    --rnlist.PX_CPT_COPAY_AMT,
    --rnlist.PX_CPT_PAY_RT_UNIT_AMT,
    --rnlist.PX_CPT_CODE_AFF_DRG_YN,
    --rnlist.PX_CPT_REIMB_TYPE,
    --rnlist.PX_CPT_REV_CODE_ID,
    --rnlist.PX_CPT_REV_CODE,
    --rnlist.PX_CPT_REV_CODE_NAME,
    --rnlist.PX_CPT_QUANTITY,
    --rnlist.CODE_INT_MOD_1_ID,
    --rnlist.CODE_INT_MOD_1_CODE,
    --rnlist.CODE_INT_MOD_1_NAME,
    --rnlist.CODE_INT_MOD_2_ID,
    --rnlist.CODE_INT_MOD_2_CODE,
    --rnlist.CODE_INT_MOD_2_NAME,
    --rnlist.CODE_INT_MOD_3_ID,
    --rnlist.CODE_INT_MOD_3_CODE,
    --rnlist.CODE_INT_MOD_3_NAME,
    --rnlist.CODE_INT_MOD_4_ID,
    --rnlist.CODE_INT_MOD_4_CODE,
    --rnlist.CODE_INT_MOD_4_NAME,
    --rnlist.CODE_INT_ROOM_RATE,
    --rnlist.CODE_INT_UNUSED_YN,
    --rnlist.CODE_INT_UNUSED_RSN_C,
    --rnlist.CODE_INT_UNUSED_RSN_NAME,
    --rnlist.PX_EVENT_DATE,
    --rnlist.PX_EVENT_PROV_ID,
    --rnlist.PX_EVENT_PROV_NM_WID,
    --rnlist.PX_EVENT_COMMENT,
    --rnlist.ASA_CLASS,
    --rnlist.ANESTH_TYPE_HA_C,
    --rnlist.ANESTH_TYPE_HA_NAME,
    --rnlist.ANESTH_PROV_ID,
    --rnlist.ANESTH_PROV_NM_WID,
    --rnlist.CODING_INFO_CPT_LINE
INTO #or_proc
FROM
(
SELECT
	or_encs.extract,
    or_encs.HSP_ACCOUNT_ID,
	or_encs.PRIMARY_PROCEDURE_ID,
	or_encs.PRIMARY_PROCEDURE_NM--,
    ----vlist.HSP_ACCOUNT_ID,
    --vlist.SOURCE_KEY,
    --vlist.LINE,
    --vlist.SOURCE_NAME,
    --vlist.SOURCE_ABBR,
    --vlist.rn,
    --vlist.DX_ID,
    --vlist.ICD_PX_ID,
    --vlist.NAME,
    --vlist.REF_BILL_CODE_SET_C,
    --vlist.REF_BILL_CODE_SET_NAME,
    --vlist.REF_BILL_CODE,
    --vlist.EXCLUDE_YN,
    --vlist.AFFECTS_SOI_YN,
    --vlist.AFFECTS_ROM_YN,
    --vlist.DX_POA_C,
    --vlist.DX_POA_NAME,
    --vlist.DX_COMORBIDITY_C,
    --vlist.DX_COMORBIDITY_NAME,
    --vlist.DX_HAC_YN,
    --vlist.DX_AFFECTS_DRG_YN,
    --vlist.DX_SOI_C,
    --vlist.DX_SOI_NAME,
    --vlist.DX_ROM_C,
    --vlist.DX_ROM_NAME,
    --vlist.PX_PERF_PROV_ID,
    --vlist.PX_PERF_PROV_NMWID,
    --vlist.PX_EVENT_NUMBER,
    --vlist.PX_DATE,
    --vlist.PX_CPT_MODIFIERS,
    --vlist.PX_CPT_LCD_CODE,
    --vlist.PX_CPT_OCE_EDIT_CODE,
    --vlist.PX_CPT_APC_CODE,
    --vlist.PX_CPT_APC_PMT_IND,
    --vlist.PX_CPT_APC_PMT_STS_IND,
    --vlist.PX_CPT_APC_WEIGHT,
    --vlist.PX_CPT_APC_FAC_RMB_AMT,
    --vlist.PX_CPT_HCFA_PAYMT_AMT,
    --vlist.PX_CPT_COPAY_AMT,
    --vlist.PX_CPT_PAY_RT_UNIT_AMT,
    --vlist.PX_CPT_CODE_AFF_DRG_YN,
    --vlist.PX_CPT_REIMB_TYPE,
    --vlist.PX_CPT_REV_CODE_ID,
    --vlist.PX_CPT_REV_CODE,
    --vlist.PX_CPT_REV_CODE_NAME,
    --vlist.PX_CPT_QUANTITY,
    --vlist.CODE_INT_MOD_1_ID,
    --vlist.CODE_INT_MOD_1_CODE,
    --vlist.CODE_INT_MOD_1_NAME,
    --vlist.CODE_INT_MOD_2_ID,
    --vlist.CODE_INT_MOD_2_CODE,
    --vlist.CODE_INT_MOD_2_NAME,
    --vlist.CODE_INT_MOD_3_ID,
    --vlist.CODE_INT_MOD_3_CODE,
    --vlist.CODE_INT_MOD_3_NAME,
    --vlist.CODE_INT_MOD_4_ID,
    --vlist.CODE_INT_MOD_4_CODE,
    --vlist.CODE_INT_MOD_4_NAME,
    --vlist.CODE_INT_ROOM_RATE,
    --vlist.CODE_INT_UNUSED_YN,
    --vlist.CODE_INT_UNUSED_RSN_C,
    --vlist.CODE_INT_UNUSED_RSN_NAME,
    --vlist.PX_EVENT_DATE,
    --vlist.PX_EVENT_PROV_ID,
    --vlist.PX_EVENT_PROV_NM_WID,
    --vlist.PX_EVENT_COMMENT,
    --vlist.ASA_CLASS,
    --vlist.ANESTH_TYPE_HA_C,
    --vlist.ANESTH_TYPE_HA_NAME,
    --vlist.ANESTH_PROV_ID,
    --vlist.ANESTH_PROV_NM_WID,
    --vlist.CODING_INFO_CPT_LINE
FROM
(
SELECT DISTINCT
	encs.extract,
	encs.HSP_ACCOUNT_ID,
	encs.PRIMARY_PROCEDURE_ID,
	encs.PRIMARY_PROCEDURE_NM
FROM #encs_plus encs
WHERE encs.extract = 'or_cte'
) or_encs
--LEFT OUTER JOIN
--(
--SELECT
--	HSP_ACCOUNT_ID,
--    SOURCE_KEY,
--    LINE,
--    SOURCE_NAME,
--    SOURCE_ABBR,
--	ROW_NUMBER() OVER(PARTITION BY HSP_ACCOUNT_ID, SOURCE_KEY ORDER BY LINE) AS rn,
--    DX_ID,
--    ICD_PX_ID,
--    NAME,
--    REF_BILL_CODE_SET_C,
--    REF_BILL_CODE_SET_NAME,
--    REF_BILL_CODE,
--    EXCLUDE_YN,
--    AFFECTS_SOI_YN,
--    AFFECTS_ROM_YN,
--    DX_POA_C,
--    DX_POA_NAME,
--    DX_COMORBIDITY_C,
--    DX_COMORBIDITY_NAME,
--    DX_HAC_YN,
--    DX_AFFECTS_DRG_YN,
--    DX_SOI_C,
--    DX_SOI_NAME,
--    DX_ROM_C,
--    DX_ROM_NAME,
--    PX_PERF_PROV_ID,
--    PX_PERF_PROV_NMWID,
--    PX_EVENT_NUMBER,
--    PX_DATE,
--    PX_CPT_MODIFIERS,
--    PX_CPT_LCD_CODE,
--    PX_CPT_OCE_EDIT_CODE,
--    PX_CPT_APC_CODE,
--    PX_CPT_APC_PMT_IND,
--    PX_CPT_APC_PMT_STS_IND,
--    PX_CPT_APC_WEIGHT,
--    PX_CPT_APC_FAC_RMB_AMT,
--    PX_CPT_HCFA_PAYMT_AMT,
--    PX_CPT_COPAY_AMT,
--    PX_CPT_PAY_RT_UNIT_AMT,
--    PX_CPT_CODE_AFF_DRG_YN,
--    PX_CPT_REIMB_TYPE,
--    PX_CPT_REV_CODE_ID,
--    PX_CPT_REV_CODE,
--    PX_CPT_REV_CODE_NAME,
--    PX_CPT_QUANTITY,
--    CODE_INT_MOD_1_ID,
--    CODE_INT_MOD_1_CODE,
--    CODE_INT_MOD_1_NAME,
--    CODE_INT_MOD_2_ID,
--    CODE_INT_MOD_2_CODE,
--    CODE_INT_MOD_2_NAME,
--    CODE_INT_MOD_3_ID,
--    CODE_INT_MOD_3_CODE,
--    CODE_INT_MOD_3_NAME,
--    CODE_INT_MOD_4_ID,
--    CODE_INT_MOD_4_CODE,
--    CODE_INT_MOD_4_NAME,
--    CODE_INT_ROOM_RATE,
--    CODE_INT_UNUSED_YN,
--    CODE_INT_UNUSED_RSN_C,
--    CODE_INT_UNUSED_RSN_NAME,
--    PX_EVENT_DATE,
--    PX_EVENT_PROV_ID,
--    PX_EVENT_PROV_NM_WID,
--    PX_EVENT_COMMENT,
--    ASA_CLASS,
--    ANESTH_TYPE_HA_C,
--    ANESTH_TYPE_HA_NAME,
--    ANESTH_PROV_ID,
--    ANESTH_PROV_NM_WID,
--    CODING_INFO_CPT_LINE
--FROM CLARITY.dbo.V_CODING_ALL_DX_PX_LIST
--WHERE SOURCE_KEY = 23
--) vlist
--ON vlist.HSP_ACCOUNT_ID = ip_encs.HSP_ACCOUNT_ID
) rnlist
--WHERE rnlist.rn = 1
/* =================== SURGERY ================== */
/* =================== OUTPATIENT ================== */

SELECT
	ophovlist.extract,
    ophovlist.HSP_ACCOUNT_ID,
    ophovlist.Encounter_CSN,
    --ophovlist.Epic_Patient_Id,
    --ophovlist.ENC_TYPE_C,
    --ophovlist.encounter_class,
    --ophovlist.PAT_ENC_CSN_ID,
    --ophovlist.ORDER_INST,
    ophovlist.PROC_ID,
    ophovlist.PROC_CODE,
    --ophovlist.PROC_NAME,
    COALESCE(ophovlist.DISPLAY_NAME,ophovlist.PROC_NAME) AS PROC_NAME--,
    --ophovlist.ORDER_CLASS_C,
    --ophovlist.ORDER_CLASS_NAME,
    --ophovlist.ORDER_TYPE_C,
    --ophovlist.ORDER_TYPE_NAME,
    --ophovlist.ORDER_STATUS_C,
    --ophovlist.ORDER_STATUS_NAME,
    --ophovlist.DISPLAY_NAME,
    --ophovlist.RN
INTO #op_proc
FROM
(
SELECT
    rnlist.extract,
    rnlist.HSP_ACCOUNT_ID,
    rnlist.Encounter_CSN,
    rnlist.Epic_Patient_Id,
    rnlist.ENC_TYPE_C,
    rnlist.encounter_class,
    rnlist.PAT_ENC_CSN_ID,
	rnlist.ORDER_INST,
    rnlist.PROC_ID,
    rnlist.PROC_CODE,
	rnlist.PROC_NAME,
    rnlist.ORDER_CLASS_C,
    rnlist.ORDER_CLASS_NAME,
    rnlist.ORDER_TYPE_C,
    rnlist.ORDER_TYPE_NAME,
    rnlist.ORDER_STATUS_C,
    rnlist.ORDER_STATUS_NAME,
    rnlist.DISPLAY_NAME,
    --rnlist.RN
ROW_NUMBER() OVER(PARTITION BY rnlist.Encounter_CSN ORDER BY rnlist.ORDER_INST) RN
--INTO #op_proc
FROM
    (
SELECT
        op_encs.extract,
        op_encs.HSP_ACCOUNT_ID,
        op_encs.Encounter_CSN,
        op_encs.Epic_Patient_Id,
        op_encs.ENC_TYPE_C,
        [encounter_class]			 = ZC_ACCT_CLASS_HA.NAME, -- 7 Encounter Class								string(254) The classification of the encounter.
        op.PAT_ENC_CSN_ID,
		op.ORDER_INST,
        op.PROC_ID,
        op.PROC_CODE,
		op.PROC_NAME,
        op.ORDER_CLASS_C,
        op.ORDER_CLASS_NAME,
        op.ORDER_TYPE_C,
        op.ORDER_TYPE_NAME,
        op.ORDER_STATUS_C,
        op.ORDER_STATUS_NAME,
        op.DISPLAY_NAME--,
        --op.RN
    FROM
        (
SELECT DISTINCT
            encs.extract,
            encs.HSP_ACCOUNT_ID,
            encs.Encounter_CSN,
            encs.Epic_Patient_Id,
            encs.ENC_TYPE_C
        FROM #encs_plus encs
        WHERE encs.extract = 'ophov_cte'
) op_encs
        LEFT OUTER JOIN
        (
SELECT
            ORDER_PROC.PAT_ENC_CSN_ID
,ORDER_PROC.ORDER_INST
 --,[encounter_class]			 = ZC_ACCT_CLASS_HA.NAME		-- 7 Encounter Class								string(254) The classification of the encounter.
--,[proc_code]				 = CL_ICD_PX.REF_BILL_CODE		--10 Procedure Code (proc_code) string(100) The procedure code. This can be a string separated by commas
--,[proc_code_sys]			 = ZC_HCD_CODE_SET.NAME 		--11 Procedure Code System (proc_code_sys) string(20) ICD9PCS ICD10PCS etc. An identifier for the diagnosis code system.
--,[line_num]					 = HSP_ACCT_PX_LIST.LINE		--12 Procedure Line Number (line_num) integer The line number or sequence for this procedure from 1 to N.
--,[proc_performed_datetime]   = CONVERT(VARCHAR(19), HSP_ACCT_PX_LIST.PROC_DATE, 120)	--13 Procedure Performed Datetime (proc_performed_datetime) datetime YYYY-MM-DD HH24:MM:SS The date and time the procedure was performed. 
, ORDER_PROC.PROC_ID
, CLARITY_EAP.PROC_CODE
--,ORDER_PROC.PROC_CODE
, ORDER_PROC.ORDER_CLASS_C
, zoc.NAME AS ORDER_CLASS_NAME
, ORDER_PROC.ORDER_TYPE_C
, zot.NAME AS ORDER_TYPE_NAME
, ORDER_PROC.ORDER_STATUS_C
, zos.NAME AS ORDER_STATUS_NAME
, ORDER_PROC.DISPLAY_NAME
--,CLARITY_EAP.ORDER_DISPLAY_NAME
,CLARITY_EAP.PROC_NAME
--,CLARITY_EAP.PROC_CAT
--,CLARITY_EAP.PROC_TYPE
--,ROW_NUMBER() OVER(PARTITION BY ip_encs.EncounterCSN ORDER BY ORDER_PROC.ORDER_INST) RN
--, ROW_NUMBER() OVER(PARTITION BY ORDER_PROC.PAT_ENC_CSN_ID ORDER BY ORDER_PROC.ORDER_INST) RN
        FROM CLARITY.dbo.ORDER_PROC	ORDER_PROC--	ON ORDER_PROC.PAT_ENC_CSN_ID = HA.EncounterCSN AND CAST(ORDER_PROC.ORDER_INST AS DATE) = HA.AdmitDate AND ORDER_PROC.ORDER_CLASS_C IN ('22','23','44') -- Hospital Performed, Clinic Performed, Ancillary Performed
            LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_CLASS zoc ON zoc.ORDER_CLASS_C = ORDER_PROC.ORDER_CLASS_C
            LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_TYPE zot ON zot.ORDER_TYPE_C = ORDER_PROC.ORDER_TYPE_C
            LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_STATUS zos ON zos.ORDER_STATUS_C = ORDER_PROC.ORDER_STATUS_C
            LEFT JOIN CLARITY.dbo.CLARITY_EAP		CLARITY_EAP ON CLARITY_EAP.PROC_ID = ORDER_PROC.PROC_ID
--LEFT JOIN		CLARITY.dbo.PAT_ENC				PAT_ENC				ON ip_encs.EncounterCSN = PAT_ENC.PAT_ENC_CSN_ID
--LEFT JOIN		CLARITY.dbo.PATIENT				PATIENT				ON PATIENT.PAT_ID					= ip_encs.Epic_Patient_Id
--LEFT JOIN		CLARITY.dbo.PAT_ENC_2           PAT_ENC_2			ON PAT_ENC.PAT_ENC_CSN_ID			= PAT_ENC_2.PAT_ENC_CSN_ID
--LEFT JOIN		CLARITY.dbo.PAT_ENC_HSP			PAT_ENC_HSP			ON PAT_ENC.PAT_ENC_CSN_ID			= PAT_ENC_HSP.PAT_ENC_CSN_ID
--LEFT JOIN		CLARITY.dbo.ZC_ACCT_CLASS_HA	ZC_ACCT_CLASS_HA    ON PAT_ENC_2.ADT_PAT_CLASS_C		= ZC_ACCT_CLASS_HA.ACCT_CLASS_HA_C
) op
        ON op.PAT_ENC_CSN_ID = op_encs.Encounter_CSN
        LEFT JOIN CLARITY.dbo.PAT_ENC				PAT_ENC ON PAT_ENC.PAT_ENC_CSN_ID = op_encs.Encounter_CSN
        LEFT JOIN CLARITY.dbo.PATIENT				PATIENT ON PATIENT.PAT_ID					= op_encs.Epic_Patient_Id
        LEFT JOIN CLARITY.dbo.PAT_ENC_2           PAT_ENC_2 ON PAT_ENC.PAT_ENC_CSN_ID			= PAT_ENC_2.PAT_ENC_CSN_ID
        LEFT JOIN CLARITY.dbo.PAT_ENC_HSP			PAT_ENC_HSP ON PAT_ENC.PAT_ENC_CSN_ID			= PAT_ENC_HSP.PAT_ENC_CSN_ID
        LEFT JOIN CLARITY.dbo.ZC_ACCT_CLASS_HA	ZC_ACCT_CLASS_HA ON PAT_ENC_2.ADT_PAT_CLASS_C		= ZC_ACCT_CLASS_HA.ACCT_CLASS_HA_C

    /*'1003', -- Procedure visit
		'213', -- Dentistry Visit
		'3', -- Hospital Encounter
		'51', -- Surgery */
    WHERE op_encs.[extract] = 'ophov_cte'
        --AND HA.AdmitDate = CAST(HSP_ACCT_PX_LIST.PROC_DATE AS DATE)
        AND op_encs.ENC_TYPE_C IN ('1003','213','3','51')
        AND op.ORDER_STATUS_C <> 4 -- Canceled
        AND op.ORDER_TYPE_C NOT IN (
 '41' -- PR Charge
--,'5' -- Imaging
--,'101' -- Dental
,'22' -- Charge
,'78' -- Case Request
,'7' -- Lab
,'8' -- Outpatient Referral
,'1230100001' -- Pathology and Cytology Orders
--,'1004' -- Vascular Services
,'1005' -- Device Interrogation
,'26' -- Point of Care Testing
--,'29' -- Echocardiography
,'1153900001' -- Appointment Requests
--,'35' -- PFT
--,'1011' -- Stress Echocardiogram
,'10' -- Nursing
--,'1007' -- Peds Fetal Echo
,'47' -- Admission
,'3' -- Microbiology
--,'46' -- Dialysis
--,'28' -- ECG
--,'1014' -- Stress
--,'33' -- Neurology
--,'1008' -- Nuclear
,'6' -- Immunization/Injection
--,'54' -- GI
--,'38' -- Sleep Center
,'31' -- Blood Bank
,'12' -- Consult
,'48' -- Transfer
,'4' -- Radiation Oncology
,'60' -- Nursing Transfusion
,'49' -- Discharge
,'1009' -- Holter/Cardiac Event Monitor
--,'17' -- PT
--,'16' -- OT
--,'24' -- Cardiac Services
--,'1' -- Procedures
,'56' -- Privilege Level
--,'43' -- IV
,'1010' -- Peds Holter/Cardiac Event Monitor
,'11' -- Code Status
,'13' -- Isolation
,'9' -- Diet
,'62' -- Continue Foley
,'64' -- Activity
--,'18' -- Respiratory Care
--,'39' -- Audiology
,'1000' -- DURABLE MEDICAL EQUIPMENT
--,'37' -- Ophthalmology
,'2' -- General Supply
,'2100800001' -- NonTOC Referral
--,'36' -- OB
--,'100' -- Dermatology
)
) rnlist
--WHERE rnlist.rn = 1
) ophovlist
WHERE ophovlist.RN = 1

SELECT
    enc_cte.extract,
    enc_cte.HSP_ACCOUNT_ID,
    enc_cte.Encounter_CSN,
	--ROW_NUMBER() OVER(PARTITION BY enc_cte.Encounter_CSN ORDER BY enc_cte.HSP_ACCOUNT_ID) AS rn,
	--ROW_NUMBER() OVER(PARTITION BY enc_cte.Encounter_CSN ORDER BY enc_cte.PROCEDURE_CODE DESC) AS rn,
	enc_cte.Epic_Patient_Id,
    enc_cte.person_id,
    enc_cte.PATIENT_NAME,
	enc_cte.ENC_TIME,
    enc_cte.Appointment_Time,
    enc_cte.Hosp_Admsn_Time,
    enc_cte.Appt_Admsn_Time,
    enc_cte.HSP_Encounter_CSN,
    enc_cte.Contact_Date,
    enc_cte.Referral_Entry_Date,
    enc_cte.Appointment_Status,
    enc_cte.Hosp_Disch_Time,
    enc_cte.HSP_HOSP_ADMSN_TIME,
    enc_cte.HSP_HOSP_DISCH_TIME,
    enc_cte.ENC_TYPE_C,
    enc_cte.Enc_Type,
    enc_cte.Hosp_Admsn_Type,
    enc_cte.Prov_Id,
    enc_cte.Department_Id,
    enc_cte.Department_Name,
    enc_cte.Referral_Id,
    enc_cte.Referral_NPI,
    enc_cte.Provider_Name,
    enc_cte.Provider_Type,
    enc_cte.Provider_Resource_Type,
    enc_cte.provider_NPI,
    enc_cte.Provider_Specialty,
    enc_cte.Epic_Financial_Division,
    enc_cte.Epic_Financial_Subdivision,
    enc_cte.HOSPITAL_CODE,
    enc_cte.UOS,
    enc_cte.LOG_ID,
    --enc_cte.PRIMARY_PROCEDURE_ID,
    --enc_cte.PRIMARY_PROCEDURE_NM,
	--enc_cte.PROCEDURE_CODE,
    ISNULL(CONVERT(VARCHAR(255),enc_cte.PROCEDURE_CODE),'') AS PROCEDURE_CODE,
	--enc_cte.[PROCEDURE_NAME],
    ISNULL(CONVERT(VARCHAR(255),enc_cte.[PROCEDURE_NAME]),'') AS [PROCEDURE_NAME],
    enc_cte.TOT_CHGS,
    enc_cte.TOT_PMTS,
    enc_cte.CODING_STATUS_NAME,
    enc_cte.PRIMARY_PAYOR_ID,
    enc_cte.PRIMARY_PLAN_ID,
    enc_cte.PAYOR_NAME,
    enc_cte.FINANCIAL_CLASS,
    enc_cte.FIN_CLASS_NAME,
    enc_cte.BENEFIT_PLAN_NAME--,
    --enc_cte.ICD_PX_ID,
    --enc_cte.NAME,
    --enc_cte.REF_BILL_CODE_SET_NAME,
    --enc_cte.PROC_ID,
    --enc_cte.PROC_CODE,
    --enc_cte.PROC_NAME
INTO  #RptgTbl
FROM
(
SELECT
	ip_cte.Epic_Patient_Id,
    ip_cte.person_id,
    ip_cte.PATIENT_NAME,
	ip_cte.ENC_TIME,
    ip_cte.Appointment_Time,
    ip_cte.Hosp_Admsn_Time,
    ip_cte.Appt_Admsn_Time,
    ip_cte.extract,
    ip_cte.Encounter_CSN,
    ip_cte.HSP_Encounter_CSN,
    ip_cte.Contact_Date,
    ip_cte.Referral_Entry_Date,
    ip_cte.Appointment_Status,
    ip_cte.Hosp_Disch_Time,
    ip_cte.HSP_HOSP_ADMSN_TIME,
    ip_cte.HSP_HOSP_DISCH_TIME,
    ip_cte.ENC_TYPE_C,
    ip_cte.Enc_Type,
    ip_cte.Hosp_Admsn_Type,
    ip_cte.Prov_Id,
    ip_cte.Department_Id,
    ip_cte.Department_Name,
    ip_cte.Referral_Id,
    ip_cte.Referral_NPI,
    ip_cte.Provider_Name,
    ip_cte.Provider_Type,
    ip_cte.Provider_Resource_Type,
    ip_cte.provider_NPI,
    ip_cte.Provider_Specialty,
    ip_cte.Epic_Financial_Division,
    ip_cte.Epic_Financial_Subdivision,
    ip_cte.HOSPITAL_CODE,
    ip_cte.UOS,
    ip_cte.LOG_ID,
    ip_cte.PRIMARY_PROCEDURE_ID,
    ip_cte.PRIMARY_PROCEDURE_NM,
    ip_proc.ICD_PX_ID AS PROCEDURE_CODE,
    ip_proc.NAME AS [PROCEDURE_NAME],
    ip_cte.HSP_ACCOUNT_ID,
    ip_cte.TOT_CHGS,
    ip_cte.TOT_PMTS,
    ip_cte.CODING_STATUS_NAME,
    ip_cte.PRIMARY_PAYOR_ID,
    ip_cte.PRIMARY_PLAN_ID,
    ip_cte.PAYOR_NAME,
    ip_cte.FINANCIAL_CLASS,
    ip_cte.FIN_CLASS_NAME,
    ip_cte.BENEFIT_PLAN_NAME,
    ip_proc.ICD_PX_ID,
    ip_proc.NAME,
    ip_proc.REF_BILL_CODE_SET_NAME,
    NULL AS PROC_ID,
    NULL AS PROC_CODE,
    NULL AS PROC_NAME
FROM
(
SELECT
	*
FROM #encs_plus
WHERE extract = 'ip_cte'
) ip_cte
LEFT OUTER JOIN #ip_proc ip_proc
ON ip_proc.extract = ip_cte.extract
AND ip_proc.HSP_ACCOUNT_ID = ip_cte.HSP_ACCOUNT_ID
UNION ALL
SELECT
	or_cte.Epic_Patient_Id,
    or_cte.person_id,
    or_cte.PATIENT_NAME,
	or_cte.ENC_TIME,
    or_cte.Appointment_Time,
    or_cte.Hosp_Admsn_Time,
    or_cte.Appt_Admsn_Time,
    or_cte.extract,
    or_cte.Encounter_CSN,
    or_cte.HSP_Encounter_CSN,
    or_cte.Contact_Date,
    or_cte.Referral_Entry_Date,
    or_cte.Appointment_Status,
    or_cte.Hosp_Disch_Time,
    or_cte.HSP_HOSP_ADMSN_TIME,
    or_cte.HSP_HOSP_DISCH_TIME,
    or_cte.ENC_TYPE_C,
    or_cte.Enc_Type,
    or_cte.Hosp_Admsn_Type,
    or_cte.Prov_Id,
    or_cte.Department_Id,
    or_cte.Department_Name,
    or_cte.Referral_Id,
    or_cte.Referral_NPI,
    or_cte.Provider_Name,
    or_cte.Provider_Type,
    or_cte.Provider_Resource_Type,
    or_cte.provider_NPI,
    or_cte.Provider_Specialty,
    or_cte.Epic_Financial_Division,
    or_cte.Epic_Financial_Subdivision,
    or_cte.HOSPITAL_CODE,
    or_cte.UOS,
    or_cte.LOG_ID,
    or_cte.PRIMARY_PROCEDURE_ID,
    or_cte.PRIMARY_PROCEDURE_NM,
    or_cte.PRIMARY_PROCEDURE_ID AS PROCEDURE_CODE,
    or_cte.PRIMARY_PROCEDURE_NM AS [PROCEDURE_NAME],
    or_cte.HSP_ACCOUNT_ID,
    or_cte.TOT_CHGS,
    or_cte.TOT_PMTS,
    or_cte.CODING_STATUS_NAME,
    or_cte.PRIMARY_PAYOR_ID,
    or_cte.PRIMARY_PLAN_ID,
    or_cte.PAYOR_NAME,
    or_cte.FINANCIAL_CLASS,
    or_cte.FIN_CLASS_NAME,
    or_cte.BENEFIT_PLAN_NAME,
    NULL AS ICD_PX_ID,
    NULL AS NAME,
    NULL AS REF_BILL_CODE_SET_NAME,
    NULL AS PROC_ID,
    NULL AS PROC_CODE,
    NULL AS PROC_NAME
FROM
(
SELECT
	*
FROM #encs_plus
WHERE extract = 'or_cte'
) or_cte
LEFT OUTER JOIN #or_proc or_proc
ON or_proc.extract = or_cte.extract
AND or_proc.HSP_ACCOUNT_ID = or_cte.HSP_ACCOUNT_ID
UNION ALL
SELECT
	ophov_cte.Epic_Patient_Id,
    ophov_cte.person_id,
    ophov_cte.PATIENT_NAME,
	ophov_cte.ENC_TIME,
    ophov_cte.Appointment_Time,
    ophov_cte.Hosp_Admsn_Time,
    ophov_cte.Appt_Admsn_Time,
    ophov_cte.extract,
    ophov_cte.Encounter_CSN,
    ophov_cte.HSP_Encounter_CSN,
    ophov_cte.Contact_Date,
    ophov_cte.Referral_Entry_Date,
    ophov_cte.Appointment_Status,
    ophov_cte.Hosp_Disch_Time,
    ophov_cte.HSP_HOSP_ADMSN_TIME,
    ophov_cte.HSP_HOSP_DISCH_TIME,
    ophov_cte.ENC_TYPE_C,
    ophov_cte.Enc_Type,
    ophov_cte.Hosp_Admsn_Type,
    ophov_cte.Prov_Id,
    ophov_cte.Department_Id,
    ophov_cte.Department_Name,
    ophov_cte.Referral_Id,
    ophov_cte.Referral_NPI,
    ophov_cte.Provider_Name,
    ophov_cte.Provider_Type,
    ophov_cte.Provider_Resource_Type,
    ophov_cte.provider_NPI,
    ophov_cte.Provider_Specialty,
    ophov_cte.Epic_Financial_Division,
    ophov_cte.Epic_Financial_Subdivision,
    ophov_cte.HOSPITAL_CODE,
    ophov_cte.UOS,
    ophov_cte.LOG_ID,
    ophov_cte.PRIMARY_PROCEDURE_ID,
    ophov_cte.PRIMARY_PROCEDURE_NM,
    op_proc.PROC_ID AS PROCEDURE_CODE,
    op_proc.PROC_NAME AS [PROCEDURE_NAME],
    ophov_cte.HSP_ACCOUNT_ID,
    ophov_cte.TOT_CHGS,
    ophov_cte.TOT_PMTS,
    ophov_cte.CODING_STATUS_NAME,
    ophov_cte.PRIMARY_PAYOR_ID,
    ophov_cte.PRIMARY_PLAN_ID,
    ophov_cte.PAYOR_NAME,
    ophov_cte.FINANCIAL_CLASS,
    ophov_cte.FIN_CLASS_NAME,
    ophov_cte.BENEFIT_PLAN_NAME,
    NULL AS ICD_PX_ID,
    NULL AS NAME,
    NULL AS REF_BILL_CODE_SET_NAME,
    op_proc.PROC_ID,
    op_proc.PROC_CODE,
    op_proc.PROC_NAME
FROM
(
SELECT
	*
FROM #encs_plus
WHERE extract = 'ophov_cte'
) ophov_cte
LEFT OUTER JOIN #op_proc op_proc
ON op_proc.extract = ophov_cte.extract
AND op_proc.HSP_ACCOUNT_ID = ophov_cte.HSP_ACCOUNT_ID
AND op_proc.Encounter_CSN = ophov_cte.Encounter_CSN
) enc_cte
/*
	--ROW_NUMBER() OVER(PARTITION BY enc_cte.Encounter_CSN ORDER BY enc_cte.PROCEDURE_CODE DESC) AS rn,*/
--ORDER BY
--	extract,
--	HSP_ACCOUNT_ID,
--	Encounter_CSN
--ORDER BY
--	enc_cte.Encounter_CSN,
--	enc_cte.PROCEDURE_CODE
ORDER BY
	enc_cte.Encounter_CSN,
	enc_cte.ENC_TIME

-- Create index for temp table #RptgTbl

--CREATE CLUSTERED INDEX IX_#RptgTbl ON #RptgTbl (Encounter_CSN, PROCEDURE_CODE)
CREATE CLUSTERED INDEX IX_#RptgTbl ON #RptgTbl (Encounter_CSN, ENC_TIME)

SELECT
	--*
	--extract,
 --   HSP_ACCOUNT_ID,
    Encounter_CSN AS UniqueID,
    Encounter_CSN AS EncounterID,
    CAST(Appt_Admsn_Time AS Date) AS AdmitDate,
    --CASE WHEN [extract] = 'ip_cte' THEN CAST(Hosp_Disch_Time AS DATE) ELSE Contact_Date END AS DischargeDate,
    CASE WHEN [extract] = 'ip_cte' THEN CAST(Hosp_Disch_Time AS DATE) ELSE CAST(Contact_Date AS DATE) END AS DischargeDate,
	Epic_Patient_Id AS UniquePatientID,
    HOSPITAL_CODE AS Facility,
    Epic_Financial_Division AS ServiceLine,
    Epic_Financial_Subdivision AS SubServiceLine,
    UOS AS ServiceType,
    PROCEDURE_CODE AS ProcedureCode,
    PROCEDURE_NAME AS ProcedureDescription,
	CASE WHEN BENEFIT_PLAN_NAME IS NULL THEN 'SELF' ELSE BENEFIT_PLAN_NAME END AS FinancialClass,
    CASE  WHEN PAYOR_NAME IS NULL THEN 'SELF' ELSE PAYOR_NAME END AS Payer,
    --TOT_CHGS AS TotalCharges,
    --ISNULL(CONVERT(NUMERIC(10,2),TOT_CHGS),'') AS TotalCharges,
    ISNULL(CAST(TOT_CHGS AS VARCHAR(10)),'') AS TotalCharges,
    --ABS(TOT_PMTS) AS TotalPayments,
    --ISNULL(CONVERT(NUMERIC(10,2),ABS(TOT_PMTS)),'') AS TotalPayments,
    ISNULL(CAST(ABS(TOT_PMTS) AS VARCHAR(10)),'') AS TotalPayments,
    Referral_NPI AS ReferringID,
    provider_NPI AS AttendingID,
    --person_id,
    --PATIENT_NAME,
    --Appointment_Time,
    --Hosp_Admsn_Time,
    --HSP_Encounter_CSN,
    --Contact_Date,
    --Referral_Entry_Date,
    --Appointment_Status,
    --Hosp_Disch_Time,
    --HSP_HOSP_ADMSN_TIME,
    --HSP_HOSP_DISCH_TIME,
    --ENC_TYPE_C,
    --Enc_Type,
    --Hosp_Admsn_Type,
    --Prov_Id,
    --Department_Id,
    --Department_Name,
    --Referral_Id,
    --Referral_NPI,
    --Provider_Name,
    --Provider_Type,
    --Provider_Resource_Type,
    --provider_NPI,
    --Provider_Specialty,
    --LOG_ID,
    --TOT_CHGS,
    --TOT_PMTS,
    CODING_STATUS_NAME,
    --PRIMARY_PAYOR_ID,
    --PRIMARY_PLAN_ID,
    --FINANCIAL_CLASS,
    --FIN_CLASS_NAME
	ROW_NUMBER() OVER(PARTITION BY Encounter_CSN ORDER BY ENC_TIME) AS enc_seq
INTO #RptgTbl2
FROM #RptgTbl
--WHERE CODING_STATUS_NAME LIKE '%Complete%'
--WHERE Encounter_CSN = 200113234196
--ORDER BY
--	Encounter_CSN,
--	PROCEDURE_CODE
--ORDER BY
--	extract,
--	Encounter_CSN,
--	PROCEDURE_CODE
ORDER BY
	Encounter_CSN

SELECT
	UniqueID,
    EncounterID,
    AdmitDate,
    DischargeDate,
    UniquePatientID,
    Facility,
    ServiceLine,
    SubServiceLine,
    ServiceType,
    ProcedureCode,
    ProcedureDescription,
    FinancialClass,
    Payer,
    TotalCharges,
    TotalPayments,
    ReferringID,
    AttendingID--,
    --CODING_STATUS_NAME,
    --enc_seq
FROM #RptgTbl2
WHERE enc_seq = 1
--ORDER BY
--	EncounterID--,
--	--enc_seq
ORDER BY
	UniquePatientID,
	EncounterID
*/
/*
SELECT
    Encounter_CSN AS EncounterCSN,
    CAST(Appt_Admsn_Time AS DATE) AS AdmitDate,
    CASE WHEN [extract] = 'ophov_cte' THEN CAST(Appt_Admsn_Time AS DATE) ELSE CAST(Hosp_Disch_Time AS DATE) END AS DischargeDate,
	Epic_Patient_Id AS PatientID,
	person_id AS PatientMRN,
    HOSPITAL_CODE AS FacilityCode,
    Epic_Financial_Division AS EpicFinancialDivision,
    Epic_Financial_Subdivision AS EpicFinancialSubdivision,
    UOS AS ServiceType,
    --CASE WHEN CPT1 IS NULL THEN CURRENT_ICD10_LIST ELSE CPT1 END AS ProcedureCode,
    --CASE WHEN CPT1 IS NOT NULL THEN 'CPTPROC' WHEN CURRENT_ICD10_LIST IS NOT NULL THEN 'ICD10DIAG' ELSE NULL END AS ProcedureCodeType,
    --CASE WHEN CPT1 IS NULL THEN DX_NAME  ELSE CPT_CODE_DESC END AS ProcedureCodeDescription,
	CASE WHEN BENEFIT_PLAN_NAME IS NULL THEN 'SELF' ELSE BENEFIT_PLAN_NAME END AS FinancialClass,
    CASE  WHEN PAYOR_NAME IS NULL THEN 'SELF' ELSE PAYOR_NAME END AS Payer,
    TOT_CHGS AS TotalCharges,
    ABS(TOT_PMTS) AS TotalPayments,
	Referral_Id AS IncomingReferralId,
	Referral_Entry_Date AS ReferralEntryDate,
    Referral_NPI AS ReferringProviderNPI,
    provider_NPI AS EncounterProviderNPI,
    [extract],
    --PATIENT_NAME,
    --Appointment_Time,
    --Hosp_Admsn_Time,
    --HSP_Encounter_CSN,
    --Contact_Date,
    --Appointment_Status,
    --HSP_HOSP_ADMSN_TIME,
    --HSP_HOSP_DISCH_TIME,
	ENC_TYPE_C,
    Enc_Type,
    --Hosp_Admsn_Type,
    --Prov_Id,
    --Department_Id,
    --Department_Name,
    --Referral_Id,
    --Provider_Name,
    --Provider_Type,
    --Provider_Resource_Type,
    --Provider_Specialty,
    --LOG_ID,
    --PRIMARY_PROCEDURE_ID,
    --PRIMARY_PROCEDURE_NM,
    HSP_ACCOUNT_ID,
    --CPT_QUANTITY,
    --CURRENT_ICD10_LIST,
    --DX_NAME,
    --dx_DX_ID,
    --dx_DX_ID_NAME,
    --REF_BILL_CODE_SET_NAME,
    --dx_REF_BILL_CODE,
    --cpt_NAME,
    --cpt_REF_BILL_CODE,
    CODING_STATUS_NAME,
    PRIMARY_PAYOR_ID,
    PRIMARY_PLAN_ID,
    FINANCIAL_CLASS,
    FIN_CLASS_NAME
INTO #RptgTbl2
FROM #RptgTbl
--ORDER BY
--	Epic_Patient_Id,
--	Appt_Admsn_Time
--ORDER BY
--	PatientID,
--	AdmitDate,
--	EncounterCSN
--ORDER BY
--	EncounterCSN,
--	PatientID,
--	AdmitDate
--ORDER BY
--	[extract],
--	EncounterCSN,
--	PatientID,
--	AdmitDate

--SELECT DISTINCT
--	Department_Id,
--	Department_Name
--FROM #RptgTbl
--WHERE extract = 'or_cte'
--ORDER BY
--	Department_Id
/*
ORDER BY
	enc.DEPARTMENT_ID,
	enc.DEPARTMENT_NAME
*/
SELECT
	HA.EncounterCSN,
    HA.AdmitDate,
    HA.DischargeDate,
    HA.PatientID,
    HA.PatientMRN,
    HA.FacilityCode,
    HA.EpicFinancialDivision,
    HA.EpicFinancialSubdivision,
    HA.ServiceType,
    HA.FinancialClass,
    HA.Payer,
    HA.TotalCharges,
    HA.TotalPayments,
    HA.IncomingReferralId,
    HA.ReferralEntryDate,
    HA.ReferringProviderNPI,
    HA.EncounterProviderNPI,
    HA.extract,
	HA.ENC_TYPE_C,
	HA.Enc_Type,
    HA.HSP_ACCOUNT_ID,
    HA.CODING_STATUS_NAME,
    HA.PRIMARY_PAYOR_ID,
    HA.PRIMARY_PLAN_ID,
    HA.FINANCIAL_CLASS,
    HA.FIN_CLASS_NAME
FROM #RptgTbl2 HA
--ORDER BY
--	[extract],
--	EncounterCSN,
--	PatientID,
--	AdmitDate
ORDER BY
	[extract],
	HA.Enc_Type,
	EncounterCSN,
	ROW_NUMBER() OVER(PARTITION BY HA.EncounterCSN ORDER BY ORDER_PROC.ORDER_INST),
	PatientID,
	AdmitDate
*/
GO