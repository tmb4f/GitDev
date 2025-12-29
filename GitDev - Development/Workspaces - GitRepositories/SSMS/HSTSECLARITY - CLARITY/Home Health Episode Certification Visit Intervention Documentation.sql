USE [CLARITY]
GO

DECLARE @startdate AS SMALLDATETIME;
DECLARE @enddate AS SMALLDATETIME;
SET @startdate = NULL
SET @enddate = NULL

SET NOCOUNT ON 

DECLARE @strCertificationPeriodBegindt AS VARCHAR(19)


    DECLARE @currdate SMALLDATETIME;

    SET @currdate=CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME);


    IF @startdate IS NULL AND @enddate IS NULL
      EXEC CLARITY_App.etl.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT

 -------------------------------------

-- 30-days prior to reporting period begin date
SET @strCertificationPeriodBegindt = DATEADD(DAY, -30, DATEADD(MONTH, DATEDIFF(MONTH,0,@startdate), 0))

if OBJECT_ID('tempdb..#hh_certifications') is not NULL
DROP TABLE #hh_certifications

if OBJECT_ID('tempdb..#hh_interventions') is not NULL
DROP TABLE #hh_interventions

SELECT CASE
         WHEN hpe.Pat_ID IS NOT NULL THEN hpe.PAT_ID
         WHEN patepi.PAT_ID IS NOT NULL THEN patepi.PAT_ID
         ELSE NULL
       END AS Pat_ID
	  ,epi.EPISODE_ID
      ,epl.PAT_ENC_CSN_ID
      ,epi.[START_DATE] AS EPISODE_START_DATE
	  ,epi.END_DATE AS EPISODE_END_DATE
	  ,epist.NAME AS EPISODE_STATUS_NAME
	  ,CAST(hpe.[CERT_PER_STRT_DT] AS SMALLDATETIME) as CERT_PER_STRT_DT
	  ,CAST(hpe.cert_period_end_dt AS SMALLDATETIME) as CERT_PERIOD_END_DT
	  ,hpe.VISIT_START_DTTM
	  ,hpe.VISIT_END_DTTM
	  ,hpc.CARE_PLAN_ID
	  ,hpc.CERT_PERIOD
	  ,hpc.POC_ENC_NUM
	  ,hpc.POC_ENC_DEL_YN
	  ,Dep.Department_Name
	  ,Dep.Department_ID
	  ,pat.PAT_NAME
	  ,CAST(pat.PAT_MRN_ID AS INTEGER) AS MRN_Clrt
	  ,convert(varchar,pat.BIRTH_DATE,101) DOB
	  ,epm.[PAYOR_NAME]
	  ,det.NAME Contact_type
	  ,enc.appt_prc_id
	  ,prc.prc_name Descript
	  ,svc.NAME Service_type
	  ,svc.title Service_title
	  ,enc.ENC_TYPE_C
	  ,hei.hh_episode_type_c
	  ,het.NAME
  INTO #hh_certifications
  FROM CLARITY.dbo.EPISODE epi
  LEFT join CLARITY.dbo.episode_link epl on epi.episode_id=epl.episode_id
  LEFT OUTER JOIN CLARITY.dbo.ZC_EPISODE_STATUS epist ON epist.EPISODE_STATUS_C = epi.STATUS_C
  left join CLARITY.dbo.hh_pat_enc hpe on epl.[PAT_ENC_CSN_ID]=hpe.[PAT_ENC_CSN_ID]
  left join CLARITY.dbo.pat_enc enc on hpe.[PAT_ENC_CSN_ID]=enc.[PAT_ENC_CSN_ID]
  LEFT OUTER JOIN (SELECT 
                       -- [PAT_ID]
                       --,[CONTACT_DATE_REAL]
                       --,[CARE_PLAN_ID]
                        [CARE_PLAN_ID]
                       ,[CERT_PERIOD]
                       --,[VISIT_SET]
                       ,[PAT_ENC_CSN_ID]
                       --,[CONTACT_DATE]
                       --,[CM_PHY_OWNER_ID]
                       --,[CM_LOG_OWNER_ID]
                       ,[POC_ENC_NUM]
                       --,[CM_CT_OWNER_ID]
                       --,[AUTH_PROVIDER_ID]
                       --,[REPORTING_DISC_C]
                       ,[POC_ENC_DEL_YN]
                 FROM CLARITY.dbo.[HH_PAT_CERT_PERIOD]
                 WHERE POC_ENC_NUM IS NOT NULL) hpc ON hpc.PAT_ENC_CSN_ID = hpe.PAT_ENC_CSN_ID
  left join CLARITY.dbo.hh_epsd_info hei on epi.episode_id=hei.summary_block_id
  left join CLARITY.dbo.clarity_dep dep on enc.effective_dept_id=dep.department_id
  left join CLARITY.dbo.HH_PAT_CHARGE chg on epl.PAT_ENC_CSN_ID=chg.PAT_ENC_CSN_ID
  left join CLARITY.dbo.Clarity_EPM EPM on enc.VISIT_EPM_ID = epm.PAYOR_ID
  LEFT join CLARITY.dbo.Clarity_FC fc on epm.financial_Class=fc.financial_class
  LEFT OUTER JOIN CLARITY.dbo.PAT_EPISODE patepi ON patepi.EPISODE_ID = epi.EPISODE_ID
  LEFT join CLARITY.dbo.Patient Pat on CASE WHEN hpe.Pat_ID IS NOT NULL THEN hpe.PAT_ID WHEN patepi.PAT_ID IS NOT NULL THEN patepi.PAT_ID ELSE NULL END=pat.pat_id
  left join CLARITY.dbo.ZC_HH_TYPE_OF_SVC svc on hpe.HH_type_of_svc_c=svc.hh_type_of_svc_c
  left join CLARITY.dbo.ZC_APPT_STATUS apt on enc.APPT_STATUS_C=apt.APPT_STATUS_C
  left join CLARITY.dbo.ZC_DISP_ENC_TYPE det on enc.enc_type_c=det.disp_enc_type_c
  left join CLARITY.dbo.CLARITY_PRC prc on enc.APPT_PRC_ID=prc.PRC_ID
  join CLARITY.dbo.ZC_HH_EPS_TYPE het on hei.hh_episode_type_c=het.HH_EPISODE_TYPE
  WHERE 1 = 1
  --AND epi.START_DATE between @strCertificationPeriodBegindt and @enddate
  AND hei.hh_episode_type_c in ('1') --1 = HH and 1090000000 = Hospice and 1090000002 = Birth to 3

  AND epi.EPISODE_ID = 90930153

  --AND hpc.POC_ENC_NUM IS NOT NULL
  --AND det.NAME = 'Home Care Visit'
  GROUP BY
       epi.EPISODE_ID
	  ,epl.PAT_ENC_CSN_ID
      ,Dep.Department_Name
      ,Dep.Department_ID
	  ,hpe.[CERT_PER_STRT_DT]
	  ,CASE WHEN hpe.Pat_ID IS NOT NULL THEN hpe.PAT_ID WHEN patepi.PAT_ID IS NOT NULL THEN patepi.PAT_ID ELSE NULL END
	  ,epi.[START_DATE]
	  ,epi.END_DATE
	  ,epist.NAME
	  ,hpe.cert_period_end_dt
	  ,pat.PAT_NAME
	  ,pat.PAT_MRN_ID
	  ,convert(varchar,pat.BIRTH_DATE,101)
	  ,epm.[PAYOR_NAME]
	  ,epm.financial_class
	  ,visit_start_dttm
	  ,VISIT_END_DTTM
	  ,hpc.CARE_PLAN_ID
	  ,hpc.CERT_PERIOD
	  ,hpc.POC_ENC_NUM
	  ,hpc.POC_ENC_DEL_YN
	  ,det.NAME
	  ,enc.appt_prc_id
	  ,prc.prc_name
	  ,svc.NAME
	  ,svc.title
	  ,enc.ENC_TYPE_C
	  ,hei.hh_episode_type_c
	  ,het.NAME

SELECT DISTINCT
       info.[INTERVENTION_ID]
      --,info.[CM_LOG_OWNER_ID]
      --,info.[CM_PHY_OWNER_ID]
      --,[INTRVNTION_TYPE_ID]
	  ,type.INTRVNTN_TYPE_NAME
      ,[CREATE_DATE]
      --,[DELETE_DATE]
      ,[INITIAL_NOTES_ID]
	  --,hno.NOTE_DESC AS hno_NOTE_DESC
	  --,hnotxt.LINE AS hnotxt_LINE
	  ,hnotxt.NOTE_TEXT AS hnotxt_NOTE_TEXT
	  --,hno2.NOTE_DESC AS hno2_NOTE_DESC
	  --,hnotxt2.LINE AS hnotxt2_LINE
	  ,hnotxt2.NOTE_TEXT AS hnotxt2_NOTE_TEXT
      ,[INCLUDE_NOTES]
      --,[SMART_TEXT_ID]
      --,[IP_TASK_TEMP_ID]
      --,[REQ_ORDER_UPDATE_C]
      ,[HH_EPISODE_ID]
  INTO #hh_interventions
  FROM [CLARITY].[dbo].[HH_INTVTN_INFO] info
  INNER JOIN
  (
  SELECT DISTINCT
		EPISODE_ID
  FROM #hh_certifications
  ) certs
  ON info.HH_EPISODE_ID = certs.EPISODE_ID
  LEFT OUTER JOIN CLARITY.dbo.INTRVTN_TYPE type
  ON type.INTRVNTN_TYPE_ID = info.INTRVNTION_TYPE_ID
  INNER JOIN CLARITY.dbo.HNO_INFO hno
  ON info.INITIAL_NOTES_ID = hno.NOTE_ID
  LEFT OUTER JOIN CLARITY.dbo.HNO_NOTE_TEXT hnotxt
  ON hnotxt.NOTE_ID = hno.NOTE_ID
  LEFT OUTER JOIN CLARITY.dbo.HH_INTVTN_CONTACT cnt
  ON cnt.INTERVENTION_ID = info.INTERVENTION_ID
  INNER JOIN CLARITY.dbo.HNO_INFO hno2
  ON cnt.CONTACT_NOTES_ID = hno2.NOTE_ID
  LEFT OUTER JOIN CLARITY.dbo.HNO_NOTE_TEXT hnotxt2
  ON hnotxt2.NOTE_ID = hno2.NOTE_ID
  WHERE HH_EPISODE_ID = 90930153
  AND hnotxt2.NOTE_TEXT IS NOT NULL
  ORDER BY info.INTERVENTION_ID, info.INITIAL_NOTES_ID--, hnotxt.LINE

SELECT *
FROM #hh_certifications
WHERE Pat_ID = 'Z228495'
ORDER BY MRN_Clrt
       , EPISODE_ID
	   , PAT_ENC_CSN_ID

SELECT *
FROM #hh_interventions
ORDER BY HH_EPISODE_ID
	   , INTERVENTION_ID

GO