USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-------------------------------------------------------------------------------
/**********************************************************************************************************************
WHAT: Data Portal Daily Metric for Discharge Lounge Activity
WHO : Tom Burgan
WHEN: 2025/12/16
WHY : Used on the Patient Journey Dashboard to track patient location and user activity in the Discharge Lounge
-----------------------------------------------------------------------------------------------------------------------
INFO: 

      INPUTS:	              
-----------------------------------------------------------------------------------------------------------------------
MODS: 	
	2025/12/16 - TMB - Initial Creation

**********************************************************************************************************************/

--CREATE PROCEDURE [ETL].[uspSrc_Patient_Journey_Discharge_Lounge]
--AS 

SET NOCOUNT ON;

DECLARE @startdate		SMALLDATETIME 
DECLARE @enddate		SMALLDATETIME  

IF			@startdate 	IS NULL 
    	AND @enddate 	IS NULL 
BEGIN
SET	@startdate	= CAST(DATEADD(DAY, -62,GETDATE())AS DATE)
SET	@enddate	= CAST(DATEADD(DAY, -1,GETDATE())AS DATE)

END

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#PLC ') IS NOT NULL
DROP TABLE #PLC

IF OBJECT_ID('tempdb..#PLC_PLUS ') IS NOT NULL
DROP TABLE #PLC_PLUS

IF OBJECT_ID('tempdb..#dl_tms_w_enc ') IS NOT NULL
DROP TABLE #dl_tms_w_enc

IF OBJECT_ID('tempdb..#dl_tms_wo_enc ') IS NOT NULL
DROP TABLE #dl_tms_wo_enc

IF OBJECT_ID('tempdb..#dl_tms ') IS NOT NULL
DROP TABLE #dl_tms

IF OBJECT_ID('tempdb..#plc_csns ') IS NOT NULL
DROP TABLE #plc_csns

IF OBJECT_ID('tempdb..#PAT_ENC_HSP ') IS NOT NULL
DROP TABLE #PAT_ENC_HSP

IF OBJECT_ID('tempdb..#hsp_csns ') IS NOT NULL
DROP TABLE #hsp_csns

IF OBJECT_ID('tempdb..#ADT ') IS NOT NULL
DROP TABLE #ADT

IF OBJECT_ID('tempdb..#PAT_ENC ') IS NOT NULL
DROP TABLE #PAT_ENC

IF OBJECT_ID('tempdb..#ORD ') IS NOT NULL
DROP TABLE #ORD

IF OBJECT_ID('tempdb..#HL_ASGN_INFO_AUDIT ') IS NOT NULL
DROP TABLE #HL_ASGN_INFO_AUDIT

IF OBJECT_ID('tempdb..#HL_REQ_STATUS_MOD_AUDIT ') IS NOT NULL
DROP TABLE #HL_REQ_STATUS_MOD_AUDIT

IF OBJECT_ID('tempdb..#transport ') IS NOT NULL
DROP TABLE #transport

IF OBJECT_ID('tempdb..#dltx ') IS NOT NULL
DROP TABLE #dltx

IF OBJECT_ID('tempdb..#dltxp') IS NOT NULL
DROP TABLE #dltxp

IF OBJECT_ID('tempdb..#ENC ') IS NOT NULL
DROP TABLE #ENC

IF OBJECT_ID('tempdb..#RptgTmp') IS NOT NULL
DROP TABLE #RptgTmp

IF OBJECT_ID('tempdb..#pt_classes_w_activity') IS NOT NULL
DROP TABLE #pt_classes_w_activity

SELECT DISTINCT
	  -- plc.[PAT_ID]
	  --,pt.PAT_MRN_ID
       plc.[PAT_ENC_CSN_ID] -- Inpatient Admission
	  ,CASE WHEN plc.PAT_ENC_CSN_ID IS NULL THEN plc.PAT_ID ELSE CAST(plc.PAT_ENC_CSN_ID AS VARCHAR(18)) END AS ENC_SEQ_ID
      ,plc.[START_TIME]
      ,plc.[CANCELED_TIME]
      ,plc.[END_TIME]
      ,plc.[LOCATION_EVNT_ID]
      ,plc.[STATUS_C]
	  ,zps.NAME AS STATUS_NAME
      ,plc.[CASE_TRACK_EVENT_C]
      ,plc.[PRE_CANCEL_STS_C]
      ,plc.[PRIVATE_YN]
      ,plc.[SOURCE_ORC_ID]
      ,plc.[SOURCE_ORL_ID]
      ,plc.[LOCATION_RECORD_ID]
      ,plc.[USER_ID]
      ,plc.[COMMENTS]
      ,plc.[RTLS_TAGID]
      ,plc.[EVENT_TYPE_C]
	  ,zpet.NAME AS EVENT_TYPE_NAME
  INTO #PLC
  FROM [CLARITY].[dbo].[CL_PLC] plc
 -- LEFT OUTER JOIN CLARITY.dbo.PATIENT pt
	--ON pt.PAT_ID = plc.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_EVENT_TYPE zpet
	ON zpet.PLC_EVENT_TYPE_C = plc.EVENT_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_STATUS zps
	ON zps.STATUS_C = plc.STATUS_C
  WHERE plc.LOCATION_RECORD_ID = 4204
  AND CAST(plc.START_TIME AS SMALLDATETIME) BETWEEN @locstartdate AND @locenddate
ORDER BY plc.USER_ID, plc.START_TIME, plc.LOCATION_EVNT_ID

  -- Create index for temp table #PLC
  CREATE UNIQUE CLUSTERED INDEX IX_PLC ON #PLC (USER_ID, START_TIME, LOCATION_EVNT_ID)

SELECT --DISTINCT
       --plc.PAT_ID,
       --plc.PAT_MRN_ID,
       plc.PAT_ENC_CSN_ID,
       plc.ENC_SEQ_ID,
       plc.START_TIME,
       plc.CANCELED_TIME,
       plc.END_TIME,
       plc.LOCATION_EVNT_ID,
       plc.STATUS_C,
       plc.STATUS_NAME,
       plc.CASE_TRACK_EVENT_C,
       plc.PRE_CANCEL_STS_C,
       plc.PRIVATE_YN,
       plc.SOURCE_ORC_ID,
       plc.SOURCE_ORL_ID,
       plc.LOCATION_RECORD_ID,
       plc.USER_ID,
       plc.COMMENTS,
       plc.RTLS_TAGID,
       plc.EVENT_TYPE_C,
       plc.EVENT_TYPE_NAME
	  ,ROW_NUMBER() OVER(PARTITION BY plc.ENC_SEQ_ID ORDER BY plc.START_TIME) AS START_TIME_SEQ
	  ,ROW_NUMBER() OVER(PARTITION BY plc.ENC_SEQ_ID ORDER BY plc.END_TIME DESC) AS END_TIME_SEQ
	  ,emp.NAME AS USER_NAME
	  ,LOWER(emp.SYSTEM_LOGIN) AS Computer_Login_Id
	  ,wd.wd_Job_Posting_Title
  INTO #PLC_PLUS
  FROM #PLC plc
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP emp
	ON emp.USER_ID = plc.USER_ID
  LEFT OUTER JOIN
  (
  SELECT
	UVA_Computing_ID,
	wd_Job_Posting_Title
  FROM CLARITY_App.Rptg.vwRef_Crosswalk_All_Workers
  WHERE wd_Is_Primary_Job = 1 AND wd_IS_Position_Active = 1-- AND wd_Is_Active = 1
  ) wd
	ON LOWER(wd.UVA_Computing_ID) = LOWER(emp.SYSTEM_LOGIN)
ORDER BY plc.PAT_ENC_CSN_ID, plc.ENC_SEQ_ID, START_TIME_SEQ, END_TIME_SEQ

  -- Create index for temp table #PLC_PLUS
  CREATE UNIQUE CLUSTERED INDEX IX_PLC_PLUS ON #PLC_PLUS (PAT_ENC_CSN_ID, ENC_SEQ_ID, START_TIME_SEQ, END_TIME_SEQ)

  SELECT
 --   plc_start.PAT_ID,
	--plc_start.PAT_MRN_ID,
	plc_start.PAT_ENC_CSN_ID,
    plc_start.ENC_SEQ_ID,
    plc_start.START_TIME,
	plc_start.START_LOCATION_EVNT_ID,
    plc_start.START_USER_ID,
    plc_start.START_USER_NAME,
    plc_start.START_Computer_Login_Id,
    plc_start.START_wd_Job_Posting_Title,
    plc_end.END_TIME,
	plc_end.END_LOCATION_EVNT_ID,
    plc_end.END_USER_ID,
    plc_end.END_USER_NAME,
    plc_end.END_Computer_Login_Id,
    plc_end.END_wd_Job_Posting_Title
  INTO #dl_tms_w_enc
  FROM
  (
  SELECT
 --   plc.PAT_ID,
	--plc.PAT_MRN_ID,
	plc.PAT_ENC_CSN_ID,
	plc.ENC_SEQ_ID,
	plc.START_TIME,
	plc.LOCATION_EVNT_ID AS START_LOCATION_EVNT_ID,
	plc.USER_ID AS START_USER_ID,
	plc.USER_NAME AS START_USER_NAME,
	plc.Computer_Login_Id AS START_Computer_Login_Id,
	plc.wd_Job_Posting_Title AS START_wd_Job_Posting_Title
  FROM #PLC_PLUS plc
  WHERE plc.PAT_ENC_CSN_ID IS NOT NULL
  AND plc.START_TIME_SEQ = 1
  ) plc_start
  LEFT OUTER JOIN
  (
  SELECT
 --   plc.PAT_ID,
	--plc.PAT_MRN_ID,
	plc.PAT_ENC_CSN_ID,
	plc.ENC_SEQ_ID,
	plc.END_TIME,
	plc.LOCATION_EVNT_ID AS END_LOCATION_EVNT_ID,
	plc.USER_ID AS END_USER_ID,
	plc.USER_NAME AS END_USER_NAME,
	plc.Computer_Login_Id AS END_Computer_Login_Id,
	plc.wd_Job_Posting_Title AS END_wd_Job_Posting_Title
  FROM #PLC_PLUS plc
  WHERE plc.PAT_ENC_CSN_ID IS NOT NULL
  AND plc.END_TIME_SEQ = 1
  ) plc_end
  ON plc_end.PAT_ENC_CSN_ID = plc_start.PAT_ENC_CSN_ID
  AND plc_end.ENC_SEQ_ID = plc_start.ENC_SEQ_ID

  SELECT
 --   plc_start.PAT_ID,
	--plc_start.PAT_MRN_ID,
	plc_start.PAT_ENC_CSN_ID,
    plc_start.ENC_SEQ_ID,
    plc_start.START_TIME,
	plc_start.START_LOCATION_EVNT_ID,
    plc_start.START_USER_ID,
    plc_start.START_USER_NAME,
    plc_start.START_Computer_Login_Id,
    plc_start.START_wd_Job_Posting_Title,
    plc_end.END_TIME,
	plc_end.END_LOCATION_EVNT_ID,
    plc_end.END_USER_ID,
    plc_end.END_USER_NAME,
    plc_end.END_Computer_Login_Id,
    plc_end.END_wd_Job_Posting_Title
  INTO #dl_tms_wo_enc
  FROM
  (
  SELECT
 --   plc.PAT_ID,
	--plc.PAT_MRN_ID,
	plc.PAT_ENC_CSN_ID,
	plc.ENC_SEQ_ID,
	plc.START_TIME,
	plc.LOCATION_EVNT_ID AS START_LOCATION_EVNT_ID,
	plc.USER_ID AS START_USER_ID,
	plc.USER_NAME AS START_USER_NAME,
	plc.Computer_Login_Id AS START_Computer_Login_Id,
	plc.wd_Job_Posting_Title AS START_wd_Job_Posting_Title
  FROM #PLC_PLUS plc
  WHERE plc.PAT_ENC_CSN_ID IS NULL
  AND plc.START_TIME_SEQ = 1
  ) plc_start
  LEFT OUTER JOIN
  (
  SELECT
 --   plc.PAT_ID,
	--plc.PAT_MRN_ID,
	plc.PAT_ENC_CSN_ID,
	plc.ENC_SEQ_ID,
	plc.END_TIME,
	plc.LOCATION_EVNT_ID AS END_LOCATION_EVNT_ID,
	plc.USER_ID AS END_USER_ID,
	plc.USER_NAME AS END_USER_NAME,
	plc.Computer_Login_Id AS END_Computer_Login_Id,
	plc.wd_Job_Posting_Title AS END_wd_Job_Posting_Title
  FROM #PLC_PLUS plc
  WHERE plc.PAT_ENC_CSN_ID IS NULL
  AND plc.END_TIME_SEQ = 1
  ) plc_end
  ON plc_end.ENC_SEQ_ID = plc_start.ENC_SEQ_ID

   SELECT
		--dltms.PAT_ID,
  --      dltms.PAT_MRN_ID,
        dltms.PAT_ENC_CSN_ID,
        dltms.ENC_SEQ_ID,
        dltms.START_TIME,
        dltms.START_LOCATION_EVNT_ID,
        dltms.START_USER_ID,
        dltms.START_USER_NAME,
        dltms.START_Computer_Login_Id,
        dltms.START_wd_Job_Posting_Title,
        dltms.END_TIME,
        dltms.END_LOCATION_EVNT_ID,
        dltms.END_USER_ID,
        dltms.END_USER_NAME,
        dltms.END_Computer_Login_Id,
        dltms.END_wd_Job_Posting_Title
   INTO #dl_tms
   FROM
   (
   SELECT
	--wenc.PAT_ID,
 --   wenc.PAT_MRN_ID,
    wenc.PAT_ENC_CSN_ID,
    wenc.ENC_SEQ_ID,
    wenc.START_TIME,
    wenc.START_LOCATION_EVNT_ID,
    wenc.START_USER_ID,
    wenc.START_USER_NAME,
    wenc.START_Computer_Login_Id,
    wenc.START_wd_Job_Posting_Title,
    wenc.END_TIME,
    wenc.END_LOCATION_EVNT_ID,
    wenc.END_USER_ID,
    wenc.END_USER_NAME,
    wenc.END_Computer_Login_Id,
    wenc.END_wd_Job_Posting_Title
   FROM #dl_tms_w_enc wenc
   UNION ALL
   SELECT
	--woenc.PAT_ID,
 --   woenc.PAT_MRN_ID,
    woenc.PAT_ENC_CSN_ID,
    woenc.ENC_SEQ_ID,
    woenc.START_TIME,
    woenc.START_LOCATION_EVNT_ID,
    woenc.START_USER_ID,
    woenc.START_USER_NAME,
    woenc.START_Computer_Login_Id,
    woenc.START_wd_Job_Posting_Title,
    woenc.END_TIME,
    woenc.END_LOCATION_EVNT_ID,
    woenc.END_USER_ID,
    woenc.END_USER_NAME,
    woenc.END_Computer_Login_Id,
    woenc.END_wd_Job_Posting_Title
   FROM #dl_tms_wo_enc woenc
   ) dltms

  SELECT DISTINCT
	PAT_ENC_CSN_ID
  INTO #plc_csns
  FROM #dl_tms
  WHERE PAT_ENC_CSN_ID IS NOT NULL
ORDER BY PAT_ENC_CSN_ID

  -- Create index for temp table #plc_csns
  CREATE UNIQUE CLUSTERED INDEX IX_plc_csns ON #plc_csns (PAT_ENC_CSN_ID)

SELECT
    enc.PAT_ID,
	pt.PAT_MRN_ID,
    enc.PAT_ENC_CSN_ID,
    enc.LEVEL_OF_CARE_C,
	zloc.NAME AS LEVEL_OF_CARE_NAME,
    enc.HOSP_ADMSN_TIME,
    enc.HOSP_DISCH_TIME,
    enc.DISCHARGE_PROV_ID,
	ser.PROV_NAME AS DISCHARGE_PROV_NAME,
    enc.DEPARTMENT_ID,
	dep.DEPARTMENT_NAME,
    enc.DISCH_DEST_C,
	zdd.NAME AS DISCH_DEST_NAME,
    enc.ACUITY_LEVEL_C,
	zal.NAME AS ACUITY_LEVEL_NAME--,
	--CASE WHEN plc.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS plc_dschlng_event
  INTO #PAT_ENC_HSP
  FROM CLARITY.dbo.V_PAT_ENC_HSP enc
 -- INNER JOIN #plc_csns plc
	--ON enc.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
 -- LEFT OUTER JOIN #plc_csns plc
	--ON plc.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.PATIENT pt
	ON pt.PAT_ID = enc.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_LVL_OF_CARE zloc
   ON zloc.LEVEL_OF_CARE_C = enc.LEVEL_OF_CARE_C
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
	ON ser.PROV_ID = enc.DISCHARGE_PROV_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_DISCH_DEST zdd
	ON zdd.DISCH_DEST_C = enc.DISCH_DEST_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_ACUITY_LEVEL zal
	ON zal.ACUITY_LEVEL_C = enc.ACUITY_LEVEL_C
  WHERE CAST(enc.HOSP_DISCH_TIME AS SMALLDATETIME) BETWEEN @locstartdate AND @locenddate 
ORDER BY enc.PAT_ENC_CSN_ID

  -- Create index for temp table #PAT_ENC_HSP
  CREATE UNIQUE CLUSTERED INDEX IX_PAT_ENC_HSP ON #PAT_ENC_HSP (PAT_ENC_CSN_ID)

  SELECT DISTINCT
	PAT_ENC_CSN_ID
  INTO #hsp_csns
  FROM #PAT_ENC_HSP
ORDER BY PAT_ENC_CSN_ID

  -- Create index for temp table #hsp_csns
  CREATE UNIQUE CLUSTERED INDEX IX_hsp_csns ON #hsp_csns (PAT_ENC_CSN_ID)

SELECT
	adt.PAT_ENC_CSN_ID
   ,adt.EVENT_TYPE_NAME
   ,adt.PAT_CLASS_NAME
   ,adt.EFFECTIVE_TIME
INTO #ADT
FROM
(
SELECT
	   adt.PAT_ENC_CSN_ID
	  ,adt.SEQ_NUM_IN_ENC
	  ,adt.EVENT_ID
	  ,adt.EVENT_TYPE_C
	  ,zet.NAME AS EVENT_TYPE_NAME  
	  ,adt.PAT_CLASS_C
	  ,zpc.NAME AS PAT_CLASS_NAME
	  ,ROW_NUMBER() OVER(PARTITION BY adt.PAT_ENC_CSN_ID ORDER BY adt.SEQ_NUM_IN_ENC DESC) AS adtseq
	  ,adt.EFFECTIVE_TIME
  FROM CLARITY.dbo.CLARITY_ADT adt
 -- INNER JOIN #plc_csns plc
	--ON adt.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  INNER JOIN #hsp_csns hsp
	ON adt.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_EVENT_TYPE zet
	ON zet.EVENT_TYPE_C = adt.EVENT_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_CLASS zpc
	ON zpc.ADT_PAT_CLASS_C = adt.PAT_CLASS_C
  WHERE CAST(adt.EFFECTIVE_TIME AS SMALLDATETIME) BETWEEN @locstartdate AND @locenddate 
) adt
WHERE adt.adtseq = 1
ORDER BY adt.PAT_ENC_CSN_ID

  -- Create index for temp table #ADT
  CREATE UNIQUE CLUSTERED INDEX IX_ADT ON #ADT (PAT_ENC_CSN_ID)

SELECT
    enc.PAT_ID,
	pt.PAT_MRN_ID,
    enc.PAT_ENC_CSN_ID,
    enc.ENC_TYPE_C,
	zdet.NAME AS ENC_TYPE_NAME,
    enc.VISIT_PROV_ID,
	ser.PROV_NAME,
    enc.DEPARTMENT_ID,
	dep.DEPARTMENT_NAME,
    enc.APPT_TIME,
    enc.CHECKIN_TIME,
    enc.CHECKOUT_TIME,
    enc.HOSP_ADMSN_TIME,
    enc.HOSP_DISCHRG_TIME,
    enc.APPT_PRC_ID,
	prc.PRC_NAME
  INTO #PAT_ENC
  FROM CLARITY.dbo.V_PAT_ENC enc
  INNER JOIN #plc_csns plc
	ON enc.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN
  (
  SELECT DISTINCT
	PAT_ENC_CSN_ID
  FROM #PAT_ENC_HSP
  ) enc_hsp
  ON enc_hsp.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.PATIENT pt
	ON pt.PAT_ID = enc.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_DISP_ENC_TYPE zdet
	ON zdet.DISP_ENC_TYPE_C = enc.ENC_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
	ON ser.PROV_ID = enc.VISIT_PROV_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_PRC prc
	ON prc.PRC_ID = enc.APPT_PRC_ID
  WHERE CAST(enc.CHECKOUT_TIME AS SMALLDATETIME) BETWEEN @locstartdate AND @locenddate 
  AND enc_hsp.PAT_ENC_CSN_ID IS NULL
ORDER BY enc.PAT_ENC_CSN_ID

  -- Create index for temp table #PAT_ENC
  CREATE UNIQUE CLUSTERED INDEX IX_PAT_ENC ON #PAT_ENC (PAT_ENC_CSN_ID)

SELECT
	dschord.PAT_ENC_CSN_ID,
    dschord.ORDERING_PROV_ID,
    dschord.ORDERING_PROV_NAME,
    dschord.ORDER_DTTM,
    dschord.ORDER_STATUS_NAME,
    dschord.ORDER_TYPE_C,
    dschord.PAT_LOC_ID,
    dschord.PAT_LOC_NAME--,
	--dschord.plc_dschlng_event
INTO #ORD
FROM
(
SELECT
    ord.PAT_ENC_CSN_ID,
	ser.PROV_ID AS ORDERING_PROV_ID,
	ser.PROV_NAME AS ORDERING_PROV_NAME,
    ord.ORDER_DTTM,
	zos.NAME AS ORDER_STATUS_NAME,
    ord.ORDER_TYPE_C,
    ord.PAT_LOC_ID,
	dep.DEPARTMENT_NAME AS PAT_LOC_NAME,
	--CASE WHEN plc.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS plc_dschlng_event,
	--enc.plc_dschlng_event,
	ROW_NUMBER() OVER(PARTITION BY ord.PAT_ENC_CSN_ID ORDER BY ord.ORDER_DTTM DESC) AS ordseq
 FROM CLARITY.dbo.ORDER_METRICS ord -- select latest discharge order for a CSN
 -- INNER JOIN #plc_csns plc
	--ON ord.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
 -- LEFT OUTER JOIN #plc_csns plc
	--ON plc.PAT_ENC_CSN_ID = ord.PAT_ENC_CSN_ID
 -- INNER JOIN #PAT_ENC_HSP enc
	--ON ord.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  INNER JOIN #hsp_csns hsp
	ON ord.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_TYPE zot
	ON zot.ORDER_TYPE_C = ord.ORDER_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
	ON ser.PROV_ID = ord.AUTH_PROV_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser2
	ON ser2.PROV_ID = ord.ORDERING_PROV_ID
  LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	ON dep.DEPARTMENT_ID = ord.PAT_LOC_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_STATUS zos
	ON zos.ORDER_STATUS_C = ord.ORDER_STATUS_C
  WHERE zot.NAME LIKE '%disch%' -- ORDER_TYPE_C = 49 Discharge
  AND ord.ORDER_STATUS_C  = 5 -- Completed
  ) dschord
  WHERE dschord.ordseq = 1
ORDER BY dschord.PAT_ENC_CSN_ID

  -- Create index for temp table #ORD
  CREATE UNIQUE CLUSTERED INDEX IX_ORD ON #ORD (PAT_ENC_CSN_ID)

  -----------------------------------------------------------------------------------------------------------------------------

  SELECT DISTINCT
       haia.[HLR_ID]
      ,haia.[LINE]
      ,haia.[EVENT_LOCAL_DTTM]
      ,haia.[STATUS_C]
	  ,zhrs.NAME AS STATUS_NAME
	  ,zhrcr.NAME AS CANCEL_RSN_NAME
      ,[STATUS_IS_SKIP_YN]
      ,haia.[ASSIGNED_TECH_ID]
      ,[GROUP_HLR_ID]
	  ,EVENT_USER_ID
	  ,hri.REQ_HOSP_LOC_ID
	  ,hri.REQ_TASK_SUBTYPE_C
	  ,hri.REQ_TECHS_NUM
	  ,hri.REQ_REGION_SEC_ID
	  ,hri.REQ_ACTIVATION_LOCAL_DTTM
	  ,hri.REQ_START_PLF_ID
	  ,hri.REQ_END_PLF_ID
	  ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
	  ,hri.REQ_PEND_ID
	  ,hri.REQ_PAT_ID
	  ,hri.REQ_CREATE_DEPARTMENT_ID
	  ,hri.REQ_BED_ID
  INTO  #HL_ASGN_INFO_AUDIT
  FROM [CLARITY].[dbo].[HL_ASGN_INFO_AUDIT] haia
  LEFT OUTER JOIN CLARITY.dbo.HL_REQ_INFO hri
  ON hri.HLR_ID = haia.HLR_ID
--INNER JOIN #plc_csns plc
--	ON hri.REQ_ADMISSION_PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  INNER JOIN #hsp_csns hsp
	ON hri.REQ_ADMISSION_PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_CANCEL_RSN zhrcr
  ON zhrcr.HL_REQ_CANCEL_RSN_C = haia.CANCEL_RSN_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS zhrs
  ON zhrs.HL_REQ_STATUS_C = haia.STATUS_C
  WHERE
  haia.STATUS_IS_SKIP_YN <> 'Y'
  AND hri.REQ_TASK_SUBTYPE_C IN ('1', '99') -- Patient Transport, Other
  AND CAST(haia.[EVENT_LOCAL_DTTM] AS SMALLDATETIME) BETWEEN @locstartdate AND @locenddate 
ORDER BY haia.HLR_ID, haia.LINE, haia.GROUP_HLR_ID

  -- Create index for temp table #HL_ASGN_INFO_AUDIT
  CREATE UNIQUE CLUSTERED INDEX IX_HL_ASGN_INFO_AUDIT ON #HL_ASGN_INFO_AUDIT (HLR_ID, LINE, GROUP_HLR_ID)

  SELECT
       haia.GROUP_HLR_ID
	  ,haia.EVENT_USER_ID
      ,hrsma.[HLR_ID]
      ,[STATUS_LINE_NUM]
      ,[STATUS_MODIFIER_C]
	  ,zhrsm.NAME AS STATUS_MODIFIER_NAME
	  ,zhht.NAME AS HOLD_TYPE_NAME
	  ,zhrpr.NAME AS POSTPONE_RSN_NAME
      ,[START_LOCAL_DTTM]
      ,[END_LOCAL_DTTM]
      ,[HOLD_UNTIL_LOCAL_DTTM]
	  ,haia.ASSIGNED_TECH_ID
	  ,haia.REQ_HOSP_LOC_ID
	  ,haia.REQ_TASK_SUBTYPE_C
	  ,haia.REQ_ACTIVATION_LOCAL_DTTM
	  ,haia.REQ_START_PLF_ID
	  ,haia.REQ_END_PLF_ID
	  ,haia.REQ_ADMISSION_PAT_ENC_CSN_ID
	  ,haia.REQ_PEND_ID
	  ,haia.REQ_PAT_ID
	  ,haia.REQ_CREATE_DEPARTMENT_ID
	  ,haia.REQ_BED_ID
  INTO  #HL_REQ_STATUS_MOD_AUDIT
  FROM [CLARITY].[dbo].[HL_REQ_STATUS_MOD_AUDIT] hrsma
  INNER JOIN
  (
  SELECT
    haia.GROUP_HLR_ID,
	haia.EVENT_USER_ID,
	haia.HLR_ID,
    haia.LINE,
    haia.ASSIGNED_TECH_ID,
	haia.REQ_HOSP_LOC_ID,
	haia.REQ_TASK_SUBTYPE_C,
	haia.REQ_ACTIVATION_LOCAL_DTTM,
	haia.REQ_START_PLF_ID,
	haia.REQ_END_PLF_ID,
	haia.REQ_ADMISSION_PAT_ENC_CSN_ID,
	haia.REQ_PEND_ID,
	haia.REQ_PAT_ID,
	haia.REQ_CREATE_DEPARTMENT_ID,
	haia.REQ_BED_ID
  FROM #HL_ASGN_INFO_AUDIT haia
  ) haia
  ON hrsma.HLR_ID = haia.HLR_ID
  AND hrsma.STATUS_LINE_NUM = haia.LINE
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS_MOD zhrsm
  ON zhrsm.HL_REQ_STATUS_MOD_C = hrsma.STATUS_MODIFIER_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_HOLD_TYPE zhht
  ON zhht.HL_REQ_HOLD_TYPE_C = hrsma.HOLD_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_POSTPONE_RSN zhrpr
  ON zhrpr.HL_REQ_POSTPONE_RSN_C = hrsma.POSTPONE_RSN_C

SELECT DISTINCT
	hlr.GROUP_HLR_ID,
	hlr.EVENT_USER_ID,
    hlr.HLR_ID,
    hlr.LINE,
    hlr.EVENT_LOCAL_DTTM,
    hlr.END_LOCAL_DTTM,
	hlr.STATUS_C,
    hlr.STATUS_NAME,
    hlr.ASSIGNED_TECH_ID,
	hlr.REQ_HOSP_LOC_ID,
    hlr.HOLD_TYPE_NAME,
	hlr.REQ_TASK_SUBTYPE_C,
	hlr.REQ_ACTIVATION_LOCAL_DTTM,
	hlr.REQ_START_PLF_ID,
	hlr.REQ_END_PLF_ID,
	hlr.REQ_ADMISSION_PAT_ENC_CSN_ID,
	hlr.REQ_PEND_ID,
	hlr.REQ_PAT_ID,
	hlr.REQ_CREATE_DEPARTMENT_ID,
	hlr.REQ_BED_ID
INTO #transport
FROM
(
SELECT
    haia.GROUP_HLR_ID,
	haia.EVENT_USER_ID,
	haia.HLR_ID,
    haia.LINE,
    haia.EVENT_LOCAL_DTTM,
	NULL AS END_LOCAL_DTTM,
	haia.STATUS_C,
    haia.STATUS_NAME,
    haia.ASSIGNED_TECH_ID,
	haia.REQ_HOSP_LOC_ID,
	NULL AS HOLD_TYPE_NAME,
	haia.REQ_TASK_SUBTYPE_C,
	haia.REQ_ACTIVATION_LOCAL_DTTM,
	haia.REQ_START_PLF_ID,
	haia.REQ_END_PLF_ID,
	haia.REQ_ADMISSION_PAT_ENC_CSN_ID,
	haia.REQ_PEND_ID,
	haia.REQ_PAT_ID,
	haia.REQ_CREATE_DEPARTMENT_ID,
	haia.REQ_BED_ID
FROM #HL_ASGN_INFO_AUDIT haia
UNION ALL
SELECT
	hrsma.GROUP_HLR_ID,
	hrsma.EVENT_USER_ID,
	hrsma.HLR_ID,
    hrsma.STATUS_LINE_NUM AS LINE,
    hrsma.START_LOCAL_DTTM AS EVENT_LOCAL_DTTM,
	hrsma.END_LOCAL_DTTM,
	hrsma.STATUS_MODIFIER_C AS STATUS_C,
    hrsma.STATUS_MODIFIER_NAME AS STATUS_NAME,
    hrsma.ASSIGNED_TECH_ID,
	hrsma.REQ_HOSP_LOC_ID,
	hrsma.HOLD_TYPE_NAME,
	hrsma.REQ_TASK_SUBTYPE_C,
	hrsma.REQ_ACTIVATION_LOCAL_DTTM,
	hrsma.REQ_START_PLF_ID,
	hrsma.REQ_END_PLF_ID,
	hrsma.REQ_ADMISSION_PAT_ENC_CSN_ID,
	hrsma.REQ_PEND_ID,
	hrsma.REQ_PAT_ID,
	hrsma.REQ_CREATE_DEPARTMENT_ID,
	hrsma.REQ_BED_ID
FROM #HL_REQ_STATUS_MOD_AUDIT hrsma
) hlr
ORDER BY hlr.REQ_START_PLF_ID, hlr.REQ_END_PLF_ID, hlr.EVENT_USER_ID, hlr.LINE, hlr.GROUP_HLR_ID, hlr.STATUS_C, hlr.EVENT_LOCAL_DTTM, hlr.END_LOCAL_DTTM

  -- Create index for temp table #transport
  CREATE UNIQUE CLUSTERED INDEX IX_transport ON #transport(REQ_START_PLF_ID, REQ_END_PLF_ID, EVENT_USER_ID, LINE, GROUP_HLR_ID, STATUS_C, EVENT_LOCAL_DTTM, END_LOCAL_DTTM)

-- SELECT
-- 	ROW_NUMBER() OVER(PARTITION BY hlr.REQ_START_PLF_ID, hlr.REQ_END_PLF_ID, hlr.EVENT_USER_ID, hlr.LINE, hlr.GROUP_HLR_ID, hlr.STATUS_C, hlr.EVENT_LOCAL_DTTM, hlr.END_LOCAL_DTTM ORDER BY hlr.REQ_START_PLF_ID, hlr.REQ_END_PLF_ID, hlr.EVENT_USER_ID, hlr.LINE, hlr.GROUP_HLR_ID, hlr.STATUS_C, hlr.EVENT_LOCAL_DTTM, hlr.END_LOCAL_DTTM) AS ROW_NUMBER,
--   hlr.*
-- FROM #transport hlr
-- ORDER BY ROW_NUMBER() OVER(PARTITION BY hlr.REQ_START_PLF_ID, hlr.REQ_END_PLF_ID, hlr.EVENT_USER_ID, hlr.LINE, hlr.GROUP_HLR_ID, hlr.STATUS_C, hlr.EVENT_LOCAL_DTTM, hlr.END_LOCAL_DTTM ORDER BY hlr.REQ_START_PLF_ID, hlr.REQ_END_PLF_ID, hlr.EVENT_USER_ID, hlr.LINE, hlr.GROUP_HLR_ID, hlr.STATUS_C, hlr.EVENT_LOCAL_DTTM, hlr.END_LOCAL_DTTM) DESC, hlr.REQ_START_PLF_ID, hlr.REQ_END_PLF_ID, hlr.EVENT_USER_ID, hlr.LINE, hlr.GROUP_HLR_ID, hlr.STATUS_C, hlr.EVENT_LOCAL_DTTM, hlr.END_LOCAL_DTTM

SELECT
    tx.REQ_ADMISSION_PAT_ENC_CSN_ID,
	tx.GROUP_HLR_ID,
	tx.EVENT_USER_ID,
	emp.NAME AS EVENT_USER_NAME,
    tx.HLR_ID,
    tx.LINE,
    tx.EVENT_LOCAL_DTTM,
    tx.END_LOCAL_DTTM,
    tx.STATUS_C,
    tx.STATUS_NAME,
    tx.ASSIGNED_TECH_ID,
    tx.REQ_HOSP_LOC_ID,
    tx.HOLD_TYPE_NAME,
    tx.REQ_TASK_SUBTYPE_C,
    tx.REQ_ACTIVATION_LOCAL_DTTM,
    tx.REQ_START_PLF_ID,
    tx.REQ_END_PLF_ID,
    tx.REQ_PEND_ID,
    tx.REQ_PAT_ID,
    tx.REQ_CREATE_DEPARTMENT_ID,
    tx.REQ_BED_ID,
	plf_from.RECORD_NAME AS plf_from_name,
	plf_to.RECORD_NAME AS plf_to_name,
	ROW_NUMBER() OVER(PARTITION BY tx.REQ_ADMISSION_PAT_ENC_CSN_ID ORDER BY LINE DESC) AS evtseq
INTO #dltx
FROM #transport tx
LEFT OUTER JOIN CLARITY.dbo.CL_PLF plf_from
	ON plf_from.RECORD_ID = tx.REQ_START_PLF_ID
LEFT OUTER JOIN CLARITY.dbo.CL_PLF plf_to
	ON plf_to.RECORD_ID = tx.REQ_END_PLF_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP emp
	ON emp.USER_ID = tx.EVENT_USER_ID
--WHERE REQ_START_PLF_ID = 4204 OR REQ_END_PLF_ID = 4204

SELECT
	REQ_ADMISSION_PAT_ENC_CSN_ID,
    GROUP_HLR_ID,
    EVENT_USER_ID,
	EVENT_USER_NAME,
    HLR_ID,
    EVENT_LOCAL_DTTM,
    END_LOCAL_DTTM,
    STATUS_C,
    STATUS_NAME,
    ASSIGNED_TECH_ID,
    REQ_HOSP_LOC_ID,
    HOLD_TYPE_NAME,
    REQ_TASK_SUBTYPE_C,
    REQ_ACTIVATION_LOCAL_DTTM,
    REQ_START_PLF_ID,
    REQ_END_PLF_ID,
    REQ_PEND_ID,
    REQ_PAT_ID,
    REQ_CREATE_DEPARTMENT_ID,
    REQ_BED_ID,
    plf_from_name,
    plf_to_name
INTO #dltxp
FROM #dltx
WHERE evtseq = 1
ORDER BY REQ_ADMISSION_PAT_ENC_CSN_ID

  -- Create index for temp table #dltxp
  CREATE UNIQUE CLUSTERED INDEX IX_dltxp ON #dltxp(REQ_ADMISSION_PAT_ENC_CSN_ID)

  SELECT
	enc.PAT_ID,
    enc.PAT_MRN_ID,
    enc.PAT_ENC_CSN_ID,
    enc.HOSP_ADMSN_TIME,
    enc.HOSP_DISCH_TIME,
    enc.DEPARTMENT_ID,
    enc.DEPARTMENT_NAME,
    enc.PROV_ID,
    enc.PROV_NAME,
    enc.LEVEL_OF_CARE_C,
    enc.LEVEL_OF_CARE_NAME,
    enc.DISCH_DEST_C,
    enc.DISCH_DEST_NAME,
    enc.ACUITY_LEVEL_C,
    enc.ACUITY_LEVEL_NAME,
    enc.ENC_TYPE_C,
    enc.ENC_TYPE_NAME,
    enc.APPT_TIME,
    enc.CHECKIN_TIME,
    enc.CHECKOUT_TIME,
    enc.APPT_PRC_ID,
    enc.PRC_NAME
  INTO #ENC
  FROM
  (
  SELECT 
    PAT_ID,
	PAT_MRN_ID,
	PAT_ENC_CSN_ID,
    HOSP_ADMSN_TIME,
    HOSP_DISCH_TIME,
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    DISCHARGE_PROV_ID AS PROV_ID,
    DISCHARGE_PROV_NAME AS PROV_NAME,
    LEVEL_OF_CARE_C,
    LEVEL_OF_CARE_NAME,
    DISCH_DEST_C,
    DISCH_DEST_NAME,
    ACUITY_LEVEL_C,
    ACUITY_LEVEL_NAME,
    CAST(NULL AS VARCHAR(66)) AS ENC_TYPE_C,
    CAST(NULL AS VARCHAR(254)) AS ENC_TYPE_NAME,
    CAST(NULL AS DATETIME) AS APPT_TIME,
    CAST(NULL AS DATETIME) AS CHECKIN_TIME,
    CAST(NULL AS DATETIME) AS CHECKOUT_TIME,
    CAST(NULL AS VARCHAR(18)) AS APPT_PRC_ID,
    CAST(NULL AS VARCHAR(200)) AS PRC_NAME
  FROM #PAT_ENC_HSP
  UNION ALL
  SELECT
    PAT_ID,
	PAT_MRN_ID,
	PAT_ENC_CSN_ID,
    HOSP_ADMSN_TIME,
    HOSP_DISCHRG_TIME AS HOSP_DISCH_TIME,
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    VISIT_PROV_ID AS PROV_ID,
    PROV_NAME,
    CAST(NULL AS VARCHAR(66)) AS LEVEL_OF_CARE_C,
    CAST(NULL AS VARCHAR(254)) AS LEVEL_OF_CARE_NAME,
    CAST(NULL AS VARCHAR(66)) AS DISCH_DEST_C,
    CAST(NULL AS VARCHAR(254)) AS DISCH_DEST_NAME,
    CAST(NULL AS VARCHAR(66)) AS ACUITY_LEVEL_C,
    CAST(NULL AS VARCHAR(254)) AS ACUITY_LEVEL_NAME,
    ENC_TYPE_C,
    ENC_TYPE_NAME,
    APPT_TIME,
    CHECKIN_TIME,
    CHECKOUT_TIME,
    APPT_PRC_ID,
    PRC_NAME
  FROM #PAT_ENC
  ) enc
ORDER BY enc.PAT_ENC_CSN_ID

  -- Create index for temp table #ENC
  CREATE UNIQUE CLUSTERED INDEX IX_ENC ON #ENC (PAT_ENC_CSN_ID)
  
  SELECT DISTINCT
    enc.PAT_ID,
    enc.PAT_MRN_ID,
    enc.PAT_ENC_CSN_ID,
    enc.HOSP_ADMSN_TIME,
    enc.HOSP_DISCH_TIME,
    enc.DEPARTMENT_ID,
    enc.DEPARTMENT_NAME,
    enc.PROV_ID,
    enc.PROV_NAME,
    enc.LEVEL_OF_CARE_C,
    enc.LEVEL_OF_CARE_NAME,
    enc.DISCH_DEST_C,
    enc.DISCH_DEST_NAME,
    enc.ACUITY_LEVEL_C,
    enc.ACUITY_LEVEL_NAME,
    enc.ENC_TYPE_C,
    enc.ENC_TYPE_NAME,
    enc.APPT_TIME,
    enc.CHECKIN_TIME,
    enc.CHECKOUT_TIME,
    enc.APPT_PRC_ID,
    enc.PRC_NAME,
	adt.PAT_CLASS_NAME,
    ord.ORDERING_PROV_ID,
    ord.ORDERING_PROV_NAME,
    ord.ORDER_DTTM,
    ord.ORDER_STATUS_NAME,
    ord.PAT_LOC_ID,
    ord.PAT_LOC_NAME,
	dltx.EVENT_LOCAL_DTTM,
	dltx.EVENT_USER_ID,
	dltx.EVENT_USER_NAME,
    dltx.STATUS_NAME AS TX_STATUS_NAME,
    dltx.REQ_ACTIVATION_LOCAL_DTTM,
    dltx.REQ_BED_ID,
    dltx.REQ_START_PLF_ID,
    dltx.REQ_END_PLF_ID,
    dltx.plf_from_name,
    dltx.plf_to_name,
    plc.START_LOCATION_EVNT_ID,
	plc.END_LOCATION_EVNT_ID,
    plc.START_TIME,
    plc.END_TIME,
	plc.START_USER_ID,
	plc.START_USER_NAME,
	plc.START_Computer_Login_Id,
	plc.START_wd_Job_Posting_Title,
	plc.END_USER_ID,
	plc.END_USER_NAME,
	plc.END_Computer_Login_Id,
	plc.END_wd_Job_Posting_Title
  INTO #RptgTmp
  FROM #ENC enc
  LEFT OUTER JOIN #ADT adt
	ON adt.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN #ORD ord
	ON ord.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN #dltxp dltx
	ON dltx.REQ_ADMISSION_PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN
  (
  SELECT
	*
  FROM #dl_tms
  WHERE PAT_ENC_CSN_ID IS NOT NULL
  ) plc
  ON plc.PAT_ENC_CSN_ID = enc.PAT_ENC_CSN_ID

  SELECT DISTINCT
    PAT_CLASS_NAME,
    COUNT(*) AS Cnt
  INTO #pt_classes_w_activity
  FROM #RptgTmp dlrptg
  WHERE REQ_START_PLF_ID = 4204 OR REQ_END_PLF_ID = 4204
  GROUP BY
  dlrptg.PAT_CLASS_NAME

  SELECT
	*
  FROM #pt_classes_w_activity dlrptg
  ORDER BY
	dlrptg.PAT_CLASS_NAME 

  -- SELECT
	-- *
  -- FROM #RptgTmp dlrptg
  -- ORDER BY
	-- dlrptg.START_TIME, dlrptg.PAT_ENC_CSN_ID

/*  
  SELECT DISTINCT
	plc.PAT_ID,
    plc.PAT_MRN_ID,
    plc.PAT_ENC_CSN_ID,
    plc.START_LOCATION_EVNT_ID,
	plc.END_LOCATION_EVNT_ID,
    plc.START_TIME,
    plc.END_TIME,
	plc.START_USER_ID,
	plc.START_USER_NAME,
	plc.START_Computer_Login_Id,
	plc.START_wd_Job_Posting_Title,
	plc.END_USER_ID,
	plc.END_USER_NAME,
	plc.END_Computer_Login_Id,
	plc.END_wd_Job_Posting_Title,
	adt.PAT_CLASS_NAME,
	enc.ENC_TYPE_NAME AS OPHOV_ENC_TYPE_NAME,
	enc.VISIT_PROV_ID AS OPHOV_VISIT_PROV_ID,
	enc.PROV_NAME AS OPHOV_VISIT_PROV_NAME,
	enc.DEPARTMENT_ID AS OPHOV_DEPARTMENT_ID,
	enc.DEPARTMENT_NAME AS OPHOV_DEPARTMENT_NAME,
	enc.APPT_TIME AS OPHOV_APPT_TIME,
	enc.CHECKIN_TIME AS OPHOV_CHECKIN_TIME,
	enc.CHECKOUT_TIME AS OPHOV_CHECKOUT_TIME,
	enc.PRC_NAME AS OPHOV_PRC_NAME,
    enc_hsp.LEVEL_OF_CARE_NAME AS IP_LEVEL_OF_CARE_NAME,
    enc_hsp.HOSP_ADMSN_TIME AS IP_HOSP_ADMSN_TIME,
    enc_hsp.HOSP_DISCH_TIME AS IP_HOSP_DISCH_TIME,
    enc_hsp.DISCHARGE_PROV_ID AS IP_DISCHARGE_PROV_ID,
    enc_hsp.DISCHARGE_PROV_NAME AS IP_DISCHARGE_PROV_NAME,
    enc_hsp.DEPARTMENT_ID AS IP_DEPARTMENT_ID,
    enc_hsp.DEPARTMENT_NAME AS IP_DEPARTMENT_NAME,
    enc_hsp.DISCH_DEST_NAME AS IP_DISCH_DEST_NAME,
    enc_hsp.ACUITY_LEVEL_NAME AS IP_ACUITY_LEVEL_NAME,
    ord.ORDERING_PROV_ID,
    ord.ORDERING_PROV_NAME,
    ord.ORDER_DTTM,
    ord.ORDER_STATUS_NAME,
    ord.PAT_LOC_ID,
    ord.PAT_LOC_NAME,
	dltx.EVENT_LOCAL_DTTM,
	dltx.EVENT_USER_ID,
	dltx.EVENT_USER_NAME,
    dltx.STATUS_NAME AS TX_STATUS_NAME,
    dltx.REQ_ACTIVATION_LOCAL_DTTM,
    dltx.REQ_BED_ID,
    dltx.plf_from_name,
    dltx.plf_to_name
  INTO #RptgTmp
  FROM #dl_tms plc
  LEFT OUTER JOIN #ADT adt
	ON adt.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN #PAT_ENC enc
	ON enc.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN #PAT_ENC_HSP enc_hsp
	ON enc_hsp.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN #ORD ord
	ON ord.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN #dltxp dltx
	ON dltx.REQ_ADMISSION_PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID

  SELECT
	1 AS event_count,
	'Discharge Lounge' AS 'event_category',	
	CAST(dlrptg.START_TIME AS DATE)  AS event_date,
    dlrptg.START_TIME AS PLC_Start_Event_Dttm,
	dlrptg.START_USER_ID AS PLC_Start_Event_User_Id,
    dlrptg.START_USER_NAME AS PLC_Start_Event_User_Name,
	dlrptg.START_wd_Job_Posting_Title AS PLC_Start_Event_User_Job_Title,
	dlrptg.START_Computer_Login_Id AS PLC_Start_Event_User_UVA_Computing_ID,
    dlrptg.END_TIME AS PLC_End_Event_Dttm,
	dlrptg.END_USER_ID AS PLC_End_Event_User_Id,
    dlrptg.END_USER_NAME AS PLC_End_Event_User_Name,
	dlrptg.END_wd_Job_Posting_Title AS PLC_End_Event_User_Job_Title,
	dlrptg.END_Computer_Login_Id AS PLC_End_Event_User_UVA_Computing_ID,
    dlrptg.PAT_ENC_CSN_ID AS Discharge_Encounter,
    dlrptg.PAT_CLASS_NAME AS Discharge_Patient_Class,
    CASE WHEN dlrptg.IP_DISCHARGE_PROV_ID IS NOT NULL THEN dlrptg.IP_DISCHARGE_PROV_ID ELSE dlrptg.OPHOV_VISIT_PROV_ID END AS Discharge_Encounter_Provider_Id,
    CASE WHEN dlrptg.IP_DISCHARGE_PROV_NAME IS NOT NULL THEN dlrptg.IP_DISCHARGE_PROV_NAME ELSE dlrptg.OPHOV_VISIT_PROV_NAME END AS Dischage_Encounter_Provider_Name,
    CASE WHEN dlrptg.IP_DEPARTMENT_ID IS NOT NULL THEN dlrptg.IP_DEPARTMENT_ID ELSE dlrptg.OPHOV_DEPARTMENT_ID END AS Dischatge_Encounter_Department_Id,
    CASE WHEN dlrptg.IP_DEPARTMENT_NAME IS NOT NULL THEN dlrptg.IP_DEPARTMENT_NAME ELSE dlrptg.OPHOV_DEPARTMENT_NAME END AS Discharge_Encounter_Department_Name,
	CASE WHEN dlrptg.IP_HOSP_DISCH_TIME IS NOT NULL THEN dlrptg.IP_HOSP_DISCH_TIME ELSE dlrptg.OPHOV_CHECKOUT_TIME END AS Source_Encounter_End_Dttm,
    dlrptg.IP_LEVEL_OF_CARE_NAME AS Discharge_IP_Level_Of_Care,	
    dlrptg.ORDER_DTTM AS Discharge_IP_Order_Dttm,	
    dlrptg.ORDERING_PROV_ID AS Discharge_IP_Order_Provider_Id,
    dlrptg.ORDERING_PROV_NAME AS Discharge_IP_Order_Provider_Name,
    dlrptg.REQ_ACTIVATION_LOCAL_DTTM AS Transport_Request_Activation_Dttm,
    dlrptg.EVENT_USER_ID AS Transport_Request_Assigned_User_Id,
    dlrptg.EVENT_USER_NAME AS Transport_Request_Assigned_User_Name,
    dlrptg.EVENT_LOCAL_DTTM AS Transport_Request_Completed_Dttm,
    dlrptg.plf_from_name AS Discharge_Bed_Label,
		
/* Standard Fields */
	/* Date/times */
	dd.Fmonth_num AS 'Fmonth_num'	,
	dd.Fyear_num AS 'Fyear_num',
	dd.Fyear_name AS 'Fyear_name',

/* Provider info */
	dlrptg.ORDERING_PROV_ID		AS 'provider_id',
	mdmprov.Prov_Nme	AS 'provider_name',
	CAST(NULL AS INT)	AS 'prov_service_line_id'	,
	mdmprov.Service_Line	AS 'prov_service_line',
	CAST(CASE WHEN mdmprov.Financial_Division = 'na'THEN NULL ELSE mdmprov.Financial_Division END  AS INT) AS 'financial_division_id',
	CAST(mdmprov.Financial_Division_Name AS VARCHAR(150))		AS 'financial_division_name',
	CAST(CASE WHEN mdmprov.Financial_SubDivision ='na'THEN NULL ELSE mdmprov.Financial_SubDivision END AS INT) AS 'financial_sub_division_id',
	CAST(mdmprov.Financial_SubDivision_Name AS VARCHAR(150))	AS 'financial_sub_division_name',
	
/* Fac/Org info */
	CASE WHEN dlrptg.IP_DEPARTMENT_ID IS NOT NULL THEN dlrptg.IP_DEPARTMENT_ID ELSE dlrptg.OPHOV_DEPARTMENT_ID END AS 'epic_department_id',
	mdmdept.epic_department_name		AS 'epic_department_name',
	mdmdept.epic_department_name_external	AS 'epic_department_name_external'	,
	mdmdept.LOC_ID		AS 'rev_location_id',
	mdmdept.REV_LOC_NAME		AS 'rev_location',
	CAST(mdmdept.POD_ID	AS VARCHAR(66))	AS 'pod_id',
	mdmdept.PFA_POD	AS 'pod_name',
	CAST(mdmdept.HUB_ID AS VARCHAR(66))	AS 'hub_id',
	mdmdept.HUB				AS 'hub_name',

/* Service line info */
	mdmdept.hs_area_id AS 'hs_area_id'	,
	mdmdept.hs_area_name AS 'hs_area_name',
	--mdmdept.practice_group_id AS 'practice_group_id',
	--mdmdept.practice_group_name	AS 'practice_group_name',
	
/* UPG Practice Info */
	--CAST(vwdep.UPG_PRACTICE_REGION_ID	AS INT)  AS 'upg_practice_region_id',
	--CAST(vwdep.UPG_PRACTICE_REGION_NAME AS VARCHAR(150))  AS 'upg_practice_region_name',
	--CAST(vwdep.UPG_PRACTICE_ID			AS INT)		AS 'upg_practice_id',
	--CAST(vwdep.UPG_PRACTICE_NAME		AS VARCHAR(150))		AS 'upg_practice_name',
	--CAST(vwdep.UPG_PRACTICE_FLAG		AS INT)			AS 'upg_practice_flag',
	
/* SOM info */
	orgmap.som_hs_area_id  AS 'som_hs_area_id'	,
	CAST(orgmap.som_hs_area_name		AS VARCHAR(150)) AS 'som_hs_area_name',
	orgmap.som_group_id	 AS 'som_group_id',
	CAST(orgmap.som_group_name			AS VARCHAR(150))  AS 'som_group_name',
	orgmap.department_id  AS 'som_department_id',
	CAST(orgmap.department				AS VARCHAR(150))  AS 'som_department_name'	,
	orgmap.Org_Number  'som_division_id',
	CAST(orgmap.Organization			AS VARCHAR(150))	 AS 'som_division_name',

/*Patient Info*/
	vwpat.MRN_Clrt AS person_id,
	vwpat.PAT_NAME AS person_name,
	vwpat.BIRTH_DATE AS person_birth_date,
	vwpat.Gender_Identity_Name AS person_gender,

/* Others */
	CAST(' Patient Journey Discharge Lounge Location Activity' AS VARCHAR(50))  AS 'event_type',
	vwpat.sk_Dim_Clrt_Pt AS sk_Dim_Pt	,
	g.[childrens_flag] AS peds,
	mdmprov.sk_Dim_Physcn AS sk_dim_physcn,
	o.organization_id,
	COALESCE(o.[organization_name], 'No Organization Assigned') organization_name,
	s.service_id,
	COALESCE(s.[service_name], 'No Service Assigned') service_name,
	c.clinical_area_id,
	COALESCE(c.[clinical_area_name], 'No Clinical Area Assigned') clinical_area_name

  FROM #RptgTmp dlrptg
  LEFT JOIN CLARITY_App.dbo.Dim_Date dd			ON	CAST(dlrptg.START_TIME AS DATE) = dd.day_date
  LEFT JOIN	 CLARITY_App.Rptg.vwDim_Clrt_SERsrc						mdmprov		ON	dlrptg.ORDERING_PROV_ID = mdmprov.PROV_ID
  LEFT JOIN	 CLARITY_App.Rptg.vwRef_MDM_Location_Master_EpicSvc		mdmdept		ON	CASE WHEN dlrptg.IP_DEPARTMENT_ID IS NOT NULL THEN dlrptg.IP_DEPARTMENT_ID ELSE dlrptg.OPHOV_DEPARTMENT_ID END	= mdmdept.epic_department_id
  LEFT JOIN CLARITY_App.Rptg.vwCLARITY_DEP							vwdep		ON	CASE WHEN dlrptg.IP_DEPARTMENT_ID IS NOT NULL THEN dlrptg.IP_DEPARTMENT_ID ELSE dlrptg.OPHOV_DEPARTMENT_ID END = vwdep.DEPARTMENT_ID
  LEFT JOIN CLARITY_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv	orgmap		ON  mdmprov.Financial_SubDivision = orgmap.Epic_Financial_Subdivision_Code
  LEFT JOIN CLARITY_App.rptg.vwDim_Clrt_Pt					     	vwpat		ON  vwpat.Clrt_PAT_ID =	dlrptg.PAT_ID
  LEFT JOIN CLARITY_App.[Mapping].Epic_Dept_Groupers				g			ON g.epic_department_id =	CASE WHEN dlrptg.IP_DEPARTMENT_ID IS NOT NULL THEN dlrptg.IP_DEPARTMENT_ID ELSE dlrptg.OPHOV_DEPARTMENT_ID END
  LEFT JOIN CLARITY_App.[Mapping].Ref_Clinical_Area_Map				c			ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
  LEFT JOIN CLARITY_App.[Mapping].Ref_Service_Map					s			ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
  LEFT JOIN CLARITY_App.[Mapping].Ref_Organization_Map				o			ON o.organization_id = s.organization_id
  ORDER BY
	dlrptg.START_TIME, dlrptg.PAT_ENC_CSN_ID
*/
GO


