USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [Rptg].[uspSrc_HL_Transport_Request_Summary]
--(
--    @startdate AS VARCHAR(19) = NULL
--   ,@enddate AS VARCHAR(19) = NULL
--   ,@de_control VARCHAR(255) = 'UVA-MC'
--)
--AS
/**********************************************************************************************************************
WHAT: Transport statistics for patient and non-patient transport
WHO : Tom Burgan
WHEN: 05/07/2024
WHY : SSRS report 'TXPORT_SUMMARY'
-----------------------------------------------------------------------------------------------------------------------
INFO: 
      INPUTS:	CLARITY.dbo.HL_ASGN_INFO_AUDIT
						CLARITY.dbo.HL_REQ_INFO
						CLARITY.dbo.ZC_HL_REQ_CANCEL_RSN
						CLARITY.dbo.ZC_HL_REQ_STATUS
						CLARITY.dbo.HL_REQ_STATUS_MOD_AUDIT
						CLARITY.dbo.ZC_HL_REQ_STATUS_MOD
						CLARITY.dbo.ZC_HL_REQ_HOLD_TYPE
						CLARITY.dbo.ZC_HL_REQ_POSTPONE
						CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group
      OUTPUTS:  

------------------------------------------------------------------------------------------------------------------------
MODS:				05/08/2024 -- TMB Edit logic to include all hospital codes and an "ALL" parameter value
						05/09/2024 -- TMB Edit logic for identifying transport requests in Hospital Logistics 	
************************************************************************************************************************/

SET NOCOUNT ON;

DECLARE @de_control VARCHAR(255)  = 'UVA-MC';
--DECLARE @startdate datetime = '3/31/2025';
--DECLARE @enddate datetime = '4/6/2025';

--DECLARE @startdate datetime = '3/31/2025';
--DECLARE @enddate datetime = '04/09/2025';

DECLARE @startdate datetime = '7/1/2025';
DECLARE @enddate datetime = '09/23/2025';

IF OBJECT_ID('tempdb..#HL_ASGN_INFO_AUDIT ') IS NOT NULL
DROP TABLE #HL_ASGN_INFO_AUDIT

IF OBJECT_ID('tempdb..#HL_REQ_STATUS_MOD_AUDIT ') IS NOT NULL
DROP TABLE #HL_REQ_STATUS_MOD_AUDIT

IF OBJECT_ID('tempdb..#transport ') IS NOT NULL
DROP TABLE #transport

IF OBJECT_ID('tempdb..#dltx ') IS NOT NULL
DROP TABLE #dltx

IF OBJECT_ID('tempdb..#planned ') IS NOT NULL
DROP TABLE #planned

IF OBJECT_ID('tempdb..#completed ') IS NOT NULL
DROP TABLE #completed

IF OBJECT_ID('tempdb..#dlact') IS NOT NULL
DROP TABLE #dlact

IF OBJECT_ID('tempdb..#dlactwotxp') IS NOT NULL
DROP TABLE #dlactwotxp
  
  SELECT DISTINCT
       haia.[HLR_ID]
      ,haia.[LINE]
      ,haia.[EVENT_LOCAL_DTTM]
      ,haia.[STATUS_C]
	  ,zhrs.NAME AS STATUS_NAME
	  ,zhrcr.NAME AS CANCEL_RSN_NAME
      ,[STATUS_IS_SKIP_YN]
      ,[ASSIGNED_TECH_ID]
      ,[GROUP_HLR_ID]
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
  INNER JOIN CLARITY.dbo.HL_REQ_INFO hri
  ON haia.HLR_ID = hri.HLR_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_CANCEL_RSN zhrcr
  ON zhrcr.HL_REQ_CANCEL_RSN_C = haia.CANCEL_RSN_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS zhrs
  ON zhrs.HL_REQ_STATUS_C = haia.STATUS_C
  WHERE
  haia.STATUS_IS_SKIP_YN <> 'Y'
  AND CAST(haia.EVENT_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
  --AND CAST(hri.REQ_ACTIVATION_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
  AND hri.REQ_TASK_SUBTYPE_C IN ('1', '99') -- Patient Transport, Other
  AND hri.REQ_REGION_SEC_ID IN
  (3100000086, -- General - UVA GRAND CENTRAL UVA HOSPITAL TRANSPORT
   3100000108, -- General - UVA GRAND CENTRAL CULPEPER HOSPITAL TRANSPORT
   3100000113  -- General - UVA GRAND CENTRAL PRINCE WILLIAM MEDICAL CENTER TRANSPORT
   )

  SELECT
       haia.GROUP_HLR_ID
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

SELECT
	hlr.GROUP_HLR_ID,
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
--WHERE hlr.ASSIGNED_TECH_ID = 1646

SELECT
	tx.GROUP_HLR_ID,
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
    tx.REQ_ADMISSION_PAT_ENC_CSN_ID,
    tx.REQ_PEND_ID,
    tx.REQ_PAT_ID,
    tx.REQ_CREATE_DEPARTMENT_ID,
    tx.REQ_BED_ID,
	plf_from.RECORD_NAME AS plf_from_name,
	plf_to.RECORD_NAME AS plf_to_name
INTO #dltx
FROM #transport tx
LEFT OUTER JOIN CLARITY.dbo.CL_PLF plf_from
	ON plf_from.RECORD_ID = tx.REQ_START_PLF_ID
LEFT OUTER JOIN CLARITY.dbo.CL_PLF plf_to
	ON plf_to.RECORD_ID = tx.REQ_END_PLF_ID
--where STATUS_C = 35
--AND CAST(EVENT_LOCAL_DTTM AS DATE) = '3/31/2025'
WHERE REQ_START_PLF_ID = 4204 OR REQ_END_PLF_ID = 4204
--ORDER BY
--	ASSIGNED_TECH_ID,
--	GROUP_HLR_ID,
--	HLR_ID,
--	EVENT_LOCAL_DTTM
--ORDER BY
--	GROUP_HLR_ID,
--	HLR_ID,
--	LINE,
--	EVENT_LOCAL_DTTM

SELECT
	*
FROM #dltx
ORDER BY
	HLR_ID,
	LINE,
	EVENT_LOCAL_DTTM

SELECT DISTINCT
	HLR_ID
INTO #planned
FROM #dltx
WHERE STATUS_C = 5 --	Planned

SELECT
	*
FROM #planned
ORDER BY
	HLR_ID

SELECT DISTINCT
	HLR_ID
INTO #completed
FROM #dltx
WHERE STATUS_C = 35 --	Completed

SELECT
	*
FROM #completed
ORDER BY
	HLR_ID
/*
;WITH MDM_REV_LOC_ID AS 
(	
	SELECT 
		DISTINCT 	
		t1.REV_LOC_ID
		,t1.HOSPITAL_CODE
		,t1.DE_HOSPITAL_CODE
		,t1.HOSPITAL_GROUP
	FROM	
		[CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group] t1
	WHERE	
		(t1.REV_LOC_ID IS NOT NULL)
)	

,txp_src AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID, txp.EVENT_LOCAL_DTTM, txp.REQ_HOSP_LOC_ID
FROM #transport txp
WHERE
		txp.STATUS_C IN (35,40) --Completed/Canceled cases
)

,txp_pnd AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID, txp.ASSIGNED_TECH_ID, txp.EVENT_LOCAL_DTTM
FROM #transport txp
WHERE  txp.STATUS_C = 5 --Planned
		AND txp.EVENT_LOCAL_DTTM =(SELECT MIN(EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
		AND txp.LINE=(SELECT MIN(txpmx.LINE) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=5)									AND txpmx.STATUS_C=5)
)	

,txp_asgn AS  
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID, txp.ASSIGNED_TECH_ID, txp.EVENT_LOCAL_DTTM
FROM #transport txp
WHERE  txp.STATUS_C=10 --Assigned
		AND txp.EVENT_LOCAL_DTTM =(SELECT MAX (EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=10)
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=10)
)

,txp_inpgr AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID, txp.ASSIGNED_TECH_ID, txp.EVENT_LOCAL_DTTM
FROM #transport txp
WHERE txp.STATUS_C=25 --In Progress
		AND txp.EVENT_LOCAL_DTTM =(SELECT MAX (EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=25)
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=25)
)

,txp_dlyd AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID, txp.EVENT_LOCAL_DTTM
FROM #transport txp
WHERE 	txp.STATUS_C=1 --Delay
		AND txp.EVENT_LOCAL_DTTM =(SELECT MIN (EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=1)
)

,txp_cmplt AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID, txp.ASSIGNED_TECH_ID, txp.EVENT_LOCAL_DTTM
FROM #transport txp
WHERE 	txp.STATUS_C=35 --Completed
		AND txp.EVENT_LOCAL_DTTM =(SELECT MAX (EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=35)
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=35)
)

,txp_cncl AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID
FROM #transport txp
WHERE txp.STATUS_C=40 --Canceled
		AND txp.EVENT_LOCAL_DTTM =(SELECT MAX (EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=40)
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=40)	
)

,txp_pstpn AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID
FROM #transport txp
WHERE txp.EVENT_LOCAL_DTTM =(SELECT MAX (EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=2)
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=2)
		AND txp.STATUS_C=2 --Hold
)

,txp_acknw AS 
(
SELECT txp.GROUP_HLR_ID, txp.HLR_ID, txp.EVENT_LOCAL_DTTM
FROM #transport txp
WHERE txp.EVENT_LOCAL_DTTM =(SELECT MIN (EVENT_LOCAL_DTTM) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=15)
		AND txp.LINE=(SELECT MIN(txpmx.LINE) 
											FROM #transport txpmx
											WHERE txp.GROUP_HLR_ID = txpmx.GROUP_HLR_ID
											AND txp.HLR_ID = txpmx.HLR_ID
											AND txpmx.STATUS_C=15)
		AND txp.STATUS_C=15 --Acknowledged
)

,txp AS 
(
SELECT DISTINCT
		txp_src.GROUP_HLR_ID, txp_src.HLR_ID, txp_src.REQ_HOSP_LOC_ID
		,txprqst.REQ_TASK_SUBTYPE_C --1: Patient Transport, 2:	Bed Clean, 3: Maintenance Clean, 99: Other

		,txp_src.EVENT_LOCAL_DTTM

		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '1'
				THEN	1
				ELSE	0
				END																												'Pt_Txp_Rqst'
		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '99'
				THEN	1
				ELSE	0
				END																												'Non-Pt_Txp_Rqst'
		,CASE WHEN txp_cncl.HLR_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'cancelled_event'
		,CASE WHEN txp_cmplt.HLR_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'Cmplted_Event'
		,CASE WHEN txp_pstpn.HLR_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'pstpn_Event'
		,CASE WHEN txp_dlyd.HLR_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'delayed_Event'
		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '1'
						AND txp_cmplt.HLR_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Pt_Cmplt_Txp_Rqst'
		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '99'
						AND txp_cmplt.HLR_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Non-Pt_Cmplt_Txp_Rqst'
		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '1'
						AND txp_cncl.HLR_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Pt_Cncld_Txp_Rqst'
		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '99'
						AND txp_cncl.HLR_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Non-Pt_Cncld_Txp_Rqst'
		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '1'
						AND txp_dlyd.HLR_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Pt_dlyd_Txp_Rqst'
		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '99'
						AND txp_dlyd.HLR_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Non-Pt_dlyd_Txp_Rqst'
		,dte.fmonth_num
		,dte.month_name	
		,txp_pnd.EVENT_LOCAL_DTTM																						'Request_Pend_Time'
		,txp_asgn.EVENT_LOCAL_DTTM																						'Rqst_Assigned_Time'
		,txp_inpgr.EVENT_LOCAL_DTTM																						'Rqst_Inprgs_Time'
		,txp_cmplt.EVENT_LOCAL_DTTM																						'Rqst_Completed_Time'
		,DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_asgn.EVENT_LOCAL_DTTM)									'Time to Assign'
		,DATEDIFF(MINUTE,txp_asgn.EVENT_LOCAL_DTTM,txp_inpgr.EVENT_LOCAL_DTTM)									'Time to in progress'
		,DATEDIFF(MINUTE, txp_inpgr.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)								'Time to complete'
		,DATEDIFF(MINUTE,txp_asgn.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)									'Assign_cmplt'
		,DATEDIFF(MINUTE,txp_dlyd.EVENT_LOCAL_DTTM,txp_asgn.EVENT_LOCAL_DTTM)									'Delay_Assign'
		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '1'
						AND txp_dlyd.HLR_ID IS NOT NULL)
				THEN DATEDIFF(MINUTE,txp_dlyd.EVENT_LOCAL_DTTM,txp_inpgr.EVENT_LOCAL_DTTM)
				END																												'Pt_Delay'

		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '1'
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_inpgr.EVENT_LOCAL_DTTM)
				END																												'Pt_Time Pend to inpgr'
		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '1'
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)
				END																												'Pt_Time Pend to Cmplt'
		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '1'
				THEN DATEDIFF(MINUTE,txp_acknw.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)
				END																												'Pt_Time Acknw_cmplt'

		,CASE WHEN (txprqst.REQ_TASK_SUBTYPE_C = '99' AND txp_dlyd.HLR_ID IS NOT NULL)
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_dlyd.EVENT_LOCAL_DTTM)
				END																												'Non-Pt_Delay'
		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '99'
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_inpgr.EVENT_LOCAL_DTTM)
				END																												'Non-Pt_Time Pend to inpgr'
		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '99'
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)
				END																												'Non-Pt_Time Pend to Cmplt'
		,CASE WHEN txprqst.REQ_TASK_SUBTYPE_C = '99'
				THEN DATEDIFF(MINUTE,txp_acknw.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)
				END																												'Non-Pt_Time Acknw_cmplt'

		,CASE WHEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=30
				THEN 1
				ELSE 0
				END																												'compliance_30'
		,CASE WHEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=45
				THEN 1
				ELSE 0
				END																												'compliance_45'
		,CASE WHEN DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=60
				THEN 1
				ELSE 0
				END																												'compliance_60'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=30
						AND txprqst.REQ_TASK_SUBTYPE_C = '1')
				THEN 1
				ELSE 0
				END																												'Pt_compliance_30'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=45
						AND txprqst.REQ_TASK_SUBTYPE_C = '1')
				THEN 1
				ELSE 0
				END																												'Pt_compliance_45'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=60
						AND txprqst.REQ_TASK_SUBTYPE_C = '1')
				THEN 1
				ELSE 0
				END																												'Pt_compliance_60'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=30
						AND txprqst.REQ_TASK_SUBTYPE_C = '99')
				THEN 1
				ELSE 0
				END																												'non-Pt_compliance_30'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=45
						AND txprqst.REQ_TASK_SUBTYPE_C = '99')
				THEN 1
				ELSE 0
				END																												'Non-Pt_compliance_45'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_LOCAL_DTTM,txp_cmplt.EVENT_LOCAL_DTTM)<=60
						AND txprqst.REQ_TASK_SUBTYPE_C = '99')
				THEN 1
				ELSE 0
				END																												'Non-Pt_compliance_60'
		,txp_pnd.ASSIGNED_TECH_ID																										'pnd_wrk'
		,txp_asgn.ASSIGNED_TECH_ID																										'Asgn_wrk'
		,txp_inpgr.ASSIGNED_TECH_ID																									'Inpr_wrk'
		,txp_cmplt.ASSIGNED_TECH_ID																									'Cmplt_wrk'
FROM txp_src
LEFT OUTER JOIN txp_asgn						ON txp_src.GROUP_HLR_ID=txp_asgn.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_asgn.HLR_ID
LEFT OUTER JOIN txp_inpgr							ON txp_src.GROUP_HLR_ID=txp_inpgr.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_inpgr.HLR_ID
LEFT OUTER JOIN txp_pnd							ON txp_src.GROUP_HLR_ID=txp_pnd.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_pnd.HLR_ID
LEFT OUTER JOIN txp_cncl						ON txp_src.GROUP_HLR_ID=txp_cncl.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_cncl.HLR_ID
LEFT OUTER JOIN txp_pstpn						ON txp_src.GROUP_HLR_ID=txp_pstpn.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_pstpn.HLR_ID
LEFT OUTER JOIN txp_dlyd							ON txp_src.GROUP_HLR_ID=txp_dlyd.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_dlyd.HLR_ID
LEFT OUTER JOIN txp_acknw						ON txp_src.GROUP_HLR_ID=txp_acknw.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_acknw.HLR_ID
LEFT OUTER JOIN txp_cmplt							ON txp_src.GROUP_HLR_ID=txp_cmplt.GROUP_HLR_ID
																		AND txp_src.HLR_ID=txp_cmplt.HLR_ID
INNER JOIN #transport txprqst			ON txprqst.GROUP_HLR_ID = txp_src.GROUP_HLR_ID
															AND txprqst.HLR_ID=txp_src.HLR_ID
INNER JOIN CLARITY_App.dbo.Dim_Date dte				ON CAST(dte.day_date AS DATE) =CAST(txp_src.EVENT_LOCAL_DTTM AS DATE)

)

SELECT
	*
FROM txp
ORDER BY
	GROUP_HLR_ID,
	HLR_ID,
	REQ_HOSP_LOC_ID,
	REQ_TASK_SUBTYPE_C,
	EVENT_LOCAL_DTTM
*/
/*
SELECT 
		 --loc_id
		MAX(txp.month_name)							'Month'
		,txp.fmonth_num									'MnthNum'
		,mloc.DE_HOSPITAL_CODE				'Hospital_Code'
		,COUNT(txp.HLR_ID)							'Total_Requests'
		,SUM(txp.Cmplted_Event)							'Total_Cmpt_Rqst'
		,SUM(txp.Pt_Txp_Rqst)							'Tot_Pt_Rqst'
		,SUM(txp.[Non-Pt_Txp_Rqst])						'Non-Patient_Rqst'
		,SUM(txp.Pt_Cmplt_Txp_Rqst)						'Pt_Cmpt_Txp_Rqst'
		,SUM(txp.[Non-Pt_Cmplt_Txp_Rqst])				'Non-Pt_Cmpt_txp_Rqst'
		,SUM(txp.cancelled_event)						'Cancelled_Txp_Rqst'
		,SUM(txp.Pt_Cncld_Txp_Rqst)						'Cancelled_Pt_Txp_Rqst'
		,SUM(txp.[Non-Pt_Cncld_Txp_Rqst])				'Cancelled_NonPt_Txp_Rqst'
		,SUM(txp.delayed_Event)							'Delayed_Txp_Rqst'
		,SUM(txp.Pt_dlyd_Txp_Rqst)						'Delayed_Pt_Txp_Rqst'
		,SUM(txp.[Non-Pt_dlyd_Txp_Rqst])				'Delayed_Non-Pt_Txp_Rqst'
		,SUM(txp.Pt_compliance_30)						'Pt_compliance_30'
		,SUM(txp.Pt_compliance_45)						'Pt_compliance_45'
		,SUM(txp.Pt_compliance_60)						'Pt_compliance_60'
		,SUM(txp.[non-Pt_compliance_30])				'non-Pt_compliance_30'
		,SUM(txp.[Non-Pt_compliance_45])				'non-Pt_compliance_45'
		,SUM(txp.[Non-Pt_compliance_60])				'non-Pt_compliance_60'
		,AVG(txp.Pt_Delay)								'Average_Pt_Delay_min'
		,AVG(txp.[Pt_Time Pend to inpgr])				'Avg_Pt_Pend_Inpgr'
		,AVG(txp.[Pt_Time Pend to Cmplt])				'Avg_Pt_Pend_Cmplt'
		,AVG(txp.[Pt_Time Acknw_cmplt])					'Avg_Pt_Ackg_cmplt'
		,AVG(txp.[Non-Pt_Delay])						'Average_Non-Pt_Delay_min'
		,AVG(txp.[non-Pt_Time Pend to inpgr])			'Avg_Non-Pt_Pend_Inpgr'
		,AVG(txp.[Non-Pt_Time Pend to Cmplt])			'Avg_Non-Pt_Pend_Cmplt'
		,AVG(txp.[Non-Pt_Time Acknw_cmplt])				'Avg_Non-Pt_Ackg_cmplt'
		,CASE WHEN SUM(txp.Pt_Cmplt_Txp_Rqst) > 0
		THEN
		    CAST(ROUND(SUM(txp.Pt_compliance_30)*100.00 /SUM(txp.Pt_Cmplt_Txp_Rqst),2)	AS FLOAT)					
		ELSE 0       END    '%Pt_compliance_30'
		,CASE WHEN SUM(txp.Pt_Cmplt_Txp_Rqst) > 0
		THEN
		    CAST(ROUND(SUM(txp.Pt_compliance_45)*100.00 /SUM(txp.Pt_Cmplt_Txp_Rqst),2)	AS FLOAT)					
		ELSE 0       END    '%Pt_compliance_45'
		,CASE WHEN SUM(txp.Pt_Cmplt_Txp_Rqst) > 0
		THEN
		    CAST(ROUND(SUM(txp.Pt_compliance_60)*100.00 /SUM(txp.Pt_Cmplt_Txp_Rqst),2)	AS FLOAT)					
		ELSE 0       END    '%Pt_compliance_60'
		,CASE WHEN SUM(txp.[Non-Pt_Cmplt_Txp_Rqst]) > 0
		THEN
		    CAST(ROUND(SUM(txp.[non-Pt_compliance_30])*100.00 /SUM(txp.[Non-Pt_Cmplt_Txp_Rqst]),2)	AS FLOAT)					
		ELSE 0       END    '%non-Pt_compliance_30'
		,CASE WHEN SUM(txp.[Non-Pt_Cmplt_Txp_Rqst]) > 0
		THEN
		    CAST(ROUND(SUM(txp.[non-Pt_compliance_45])*100.00 /SUM(txp.[Non-Pt_Cmplt_Txp_Rqst]),2)	AS FLOAT)					
		ELSE 0       END    '%non-Pt_compliance_45'
        ,CASE WHEN SUM(txp.[Non-Pt_Cmplt_Txp_Rqst]) > 0
		THEN
		    CAST(ROUND(SUM(txp.[non-Pt_compliance_60])*100.00 /SUM(txp.[Non-Pt_Cmplt_Txp_Rqst]),2)	AS FLOAT)					
		ELSE 0       END    '%non-Pt_compliance_60'

FROM txp
LEFT JOIN  MDM_REV_LOC_ID AS mloc					ON mloc.REV_LOC_ID = txp.REQ_HOSP_LOC_ID	
WHERE
		COALESCE(mloc.DE_HOSPITAL_CODE,'UVA-MC') IN
		(
			SELECT VALUE 
			FROM 
			STRING_SPLIT (@de_control,',')
		)

GROUP BY fmonth_num, mloc.DE_HOSPITAL_CODE
*/
/*

                       SELECT DISTINCT
                               hrsa.HLR_ID
                              ,hrsa.EVENT_LOCAL_DTTM
                              ,cd.DEPARTMENT_ID
							  ,cd2.DEPARTMENT_ID
							  ,parent_dep.DEPARTMENT_ID
							  ,cp.RECORD_ID
                              ,cd.DEPARTMENT_NAME
                              ,cd2.DEPARTMENT_NAME
                              ,parent_dep.DEPARTMENT_NAME
                              ,cp.RECORD_NAME
                              ,dep_crt.DEPARTMENT_NAME                                                                    AS creating_department_name
                              ,dep_crt.DEPARTMENT_ID                                                                      AS creating_department_id
                              ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
                              ,hri.REQ_PAT_ID
                              ,peh.INPATIENT_DATA_ID
                              ,peh.HOSP_ADMSN_TIME
                              ,peh.HOSP_DISCH_TIME
                              ,CASE
                                   WHEN hrr.HLR_ID IS NOT NULL THEN
                                       'hlr-RN'
                                   ELSE
                                       'hlr'
                               END                                                                                        AS loc_source --set loc_source to lhr-RN to indicate Nurse assist transport
                        FROM CLARITY.dbo.HL_REQ_STATUS_AUDIT                  AS hrsa
                            INNER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS           AS zhrs
                                ON hrsa.STATUS_C = zhrs.HL_REQ_STATUS_C
                            INNER JOIN CLARITY.dbo.HL_REQ_INFO                AS hri
                                ON hri.HLR_ID = hrsa.HLR_ID
                            INNER JOIN CLARITY.dbo.ZC_HLR_TYPE                AS zht
                                ON zht.HLR_TYPE_C = hri.HLR_TYPE_C
                            INNER JOIN CLARITY.dbo.CLARITY_DEP                AS dep
                                ON hri.REQ_DEPARTMENT_ID = dep.DEPARTMENT_ID
                            INNER JOIN CLARITY.dbo.CL_PLF                     AS cp
                                ON hri.REQ_END_PLF_ID = cp.RECORD_ID
                            --INNER JOIN #base_encounters                       AS li4
                            --    ON li4.PAT_ID = hri.REQ_PAT_ID
                            INNER JOIN CLARITY.dbo.PAT_ENC_HSP                AS peh
                                ON hri.REQ_ADMISSION_PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
                            LEFT OUTER JOIN CLARITY.dbo.F_SCHED_APPT          AS hlr_appt
                                ON hri.REQ_END_APPT_PAT_ENC_CSN_ID = hlr_appt.PAT_ENC_CSN_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_PRC           AS hlr_prc
                                ON hlr_prc.PRC_ID = hlr_appt.PRC_ID
                            LEFT OUTER JOIN CLARITY.dbo.CL_PLF                AS parent_plf
                                ON cp.PARENT_LOCATION_ID = parent_plf.RECORD_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS parent_dep
                                ON parent_plf.DEPARTMENT_ID = parent_dep.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_BED           AS cb
                                ON cb.BED_ID = cp.BED_ID
                            LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_MODE        AS zhrm
                                ON hri.REQ_MODE_C = zhrm.HL_REQ_MODE_C
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_ROM           AS cr
                                ON cb.ROOM_ID = cr.ROOM_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS cd2
                                ON cr.DEPARTMENT_ID = cd2.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.HL_REQ_REQUIREMENTS   AS hrr
                                ON hrr.HLR_ID = hrsa.HLR_ID
                                   AND hrr.REQUIREMENT_C = 103 --Nurse Assist
                            LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_REQUIREMENT AS zhrr
                                ON hrr.REQUIREMENT_C = zhrr.HL_REQ_REQUIREMENT_C
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS cd
                                ON cp.DEPARTMENT_ID = cd.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS dep_crt
                                ON hri.REQ_CREATE_DEPARTMENT_ID = dep_crt.DEPARTMENT_ID
                        WHERE (
                                  hrsa.STATUS_C = 35 --completed
                                  OR
                                  (
                                      hrsa.STATUS_C = 40 --cancelled
                                      AND hri.REQ_CANCEL_RSN_C = 119
                                  )
                              ) --unit transported
							  AND hrsa.HLR_ID = 2547746
                              AND zht.NAME = 'job'
                              AND hri.HLR_NAME = 'Patient Transport'
                              --AND cp.RECORD_NAME <> 'DISCHARGE SUITE'
                              AND cp.RECORD_NAME = 'DISCHARGE SUITE'
*/
--/*

                       SELECT DISTINCT
                               hrsa.HLR_ID
							  ,hrsa.LINE
                              ,hrsa.EVENT_LOCAL_DTTM
                              ,cd.DEPARTMENT_ID
							  ,cd2.DEPARTMENT_ID AS cd2_DEPARTMENT_ID
							  ,parent_dep.DEPARTMENT_ID AS parent_dep_DEPARTMENT_ID
							  ,cp.RECORD_ID
                              ,cd.DEPARTMENT_NAME
                              ,cd2.DEPARTMENT_NAME AS cd2_DEPARTMENT_NAME
                              ,parent_dep.DEPARTMENT_NAME AS parent_dep_DEPARTMENT_NAME
                              ,cp.RECORD_NAME
                              ,dep_crt.DEPARTMENT_NAME                                                                    AS creating_department_name
                              ,dep_crt.DEPARTMENT_ID                                                                      AS creating_department_id
                              ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
                              ,hri.REQ_PAT_ID
                              ,peh.INPATIENT_DATA_ID
                              ,peh.HOSP_ADMSN_TIME
                              ,peh.HOSP_DISCH_TIME
                              ,CASE
                                   WHEN hrr.HLR_ID IS NOT NULL THEN
                                       'hlr-RN'
                                   ELSE
                                       'hlr'
                               END                                                                                        AS loc_source --set loc_source to lhr-RN to indicate Nurse assist transport
							  ,plc.START_TIME
							  ,plc.END_TIME
							  ,plc.CANCELED_TIME
							  ,plc.EVENT_TYPE_C
							  ,zpet.NAME AS EVENT_TYPE_NAME
						INTO #dlact
                        FROM CLARITY.dbo.HL_REQ_STATUS_AUDIT                  AS hrsa
							INNER JOIN #completed txp
								ON txp.HLR_ID = hrsa.HLR_ID
                            INNER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS           AS zhrs
                                ON hrsa.STATUS_C = zhrs.HL_REQ_STATUS_C
                            INNER JOIN CLARITY.dbo.HL_REQ_INFO                AS hri
                                ON hri.HLR_ID = hrsa.HLR_ID
                            INNER JOIN CLARITY.dbo.ZC_HLR_TYPE                AS zht
                                ON zht.HLR_TYPE_C = hri.HLR_TYPE_C
                            INNER JOIN CLARITY.dbo.CLARITY_DEP                AS dep
                                ON hri.REQ_DEPARTMENT_ID = dep.DEPARTMENT_ID
                            INNER JOIN CLARITY.dbo.CL_PLF                     AS cp
                                ON hri.REQ_END_PLF_ID = cp.RECORD_ID
                            --INNER JOIN #base_encounters                       AS li4
                            --    ON li4.PAT_ID = hri.REQ_PAT_ID
                            INNER JOIN CLARITY.dbo.PAT_ENC_HSP                AS peh
                                ON hri.REQ_ADMISSION_PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
                            LEFT OUTER JOIN CLARITY.dbo.F_SCHED_APPT          AS hlr_appt
                                ON hri.REQ_END_APPT_PAT_ENC_CSN_ID = hlr_appt.PAT_ENC_CSN_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_PRC           AS hlr_prc
                                ON hlr_prc.PRC_ID = hlr_appt.PRC_ID
                            LEFT OUTER JOIN CLARITY.dbo.CL_PLF                AS parent_plf
                                ON cp.PARENT_LOCATION_ID = parent_plf.RECORD_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS parent_dep
                                ON parent_plf.DEPARTMENT_ID = parent_dep.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_BED           AS cb
                                ON cb.BED_ID = cp.BED_ID
                            LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_MODE        AS zhrm
                                ON hri.REQ_MODE_C = zhrm.HL_REQ_MODE_C
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_ROM           AS cr
                                ON cb.ROOM_ID = cr.ROOM_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS cd2
                                ON cr.DEPARTMENT_ID = cd2.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.HL_REQ_REQUIREMENTS   AS hrr
                                ON hrr.HLR_ID = hrsa.HLR_ID
                                   AND hrr.REQUIREMENT_C = 103 --Nurse Assist
                            LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_REQUIREMENT AS zhrr
                                ON hrr.REQUIREMENT_C = zhrr.HL_REQ_REQUIREMENT_C
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS cd
                                ON cp.DEPARTMENT_ID = cd.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS dep_crt
                                ON hri.REQ_CREATE_DEPARTMENT_ID = dep_crt.DEPARTMENT_ID
							LEFT OUTER JOIN CLARITY.dbo.CL_PLC plc
								ON plc.PAT_ENC_CSN_ID = hri.REQ_ADMISSION_PAT_ENC_CSN_ID
								AND plc.LOCATION_RECORD_ID = 4204
							LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_EVENT_TYPE zpet
								ON zpet.PLC_EVENT_TYPE_C = plc.EVENT_TYPE_C
                        WHERE 1 =1
						AND (
                                  hrsa.STATUS_C = 35 --completed
                                  OR
                                  (
                                      hrsa.STATUS_C = 40 --cancelled
                                      AND hri.REQ_CANCEL_RSN_C = 119
                                  )
                              ) --unit transported
							  --AND hrsa.HLR_ID = 2547746
                              AND zht.NAME = 'job'
                              AND hri.HLR_NAME = 'Patient Transport'
                              --AND cp.RECORD_NAME <> 'DISCHARGE SUITE'
                              AND cp.RECORD_NAME = 'DISCHARGE SUITE'
							  --AND plc.EVENT_TYPE_C =  1 -- Manually Created
                              --AND hrsa.EVENT_LOCAL_DTTM
                              --BETWEEN COALESCE(li4.ADT_ARRIVAL_DTTM, li4.HOSP_ADMSN_TIME) AND COALESCE(
                              --                                                                            li4.HOSP_DISCH_TIME
                              --                                                                           ,GETDATE()
                              --                                                                        )
                              --AND
                              --(
                              --    hlr_prc.PRC_NAME NOT IN ( 'XR 15 MIN', 'UVA IR GENERIC/HYBRID APPT'
                              --                             ,'UVA OR29 240 APPT', 'UVA OR29 HYBRID APPT'
                              --                             ,'UVA OR29 VC GENERIC APPT'
                              --                            ) --exclude 15 min X-ray appts and IR appts in the OR
                              --    OR hlr_prc.PRC_NAME IS NULL
                              --)
						AND CAST(hrsa.EVENT_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
                        --GROUP BY hrsa.HLR_ID
                        --        ,hrsa.EVENT_LOCAL_DTTM
                        --        ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
                        --        ,hri.REQ_PAT_ID
                        --        ,peh.INPATIENT_DATA_ID
                        --        ,peh.HOSP_ADMSN_TIME
                        --        ,peh.HOSP_DISCH_TIME
                        --        ,dep_crt.DEPARTMENT_NAME
                        --        ,dep_crt.DEPARTMENT_ID
                        --        ,CASE
                        --             WHEN hrr.HLR_ID IS NOT NULL THEN
                        --                 'hlr-RN'
                        --             ELSE
                        --                 'hlr'
                        --         END

						SELECT
							*
						FROM #dlact
						ORDER BY
							HLR_ID, LINE

						SELECT DISTINCT
							HLR_ID
						FROM #dlact
						ORDER BY
							HLR_ID

                       SELECT DISTINCT
                               hrsa.HLR_ID
							  ,hrsa.LINE
                              ,hrsa.EVENT_LOCAL_DTTM
                              ,cd.DEPARTMENT_ID
							  ,cd2.DEPARTMENT_ID AS cd2_DEPARTMENT_ID
							  ,parent_dep.DEPARTMENT_ID AS parent_dep_DEPARTMENT_ID
							  ,cp.RECORD_ID
                              ,cd.DEPARTMENT_NAME
                              ,cd2.DEPARTMENT_NAME AS cd2_DEPARTMENT_NAME
                              ,parent_dep.DEPARTMENT_NAME AS parent_dep_DEPARTMENT_NAME
                              ,cp.RECORD_NAME
                              ,dep_crt.DEPARTMENT_NAME                                                                    AS creating_department_name
                              ,dep_crt.DEPARTMENT_ID                                                                      AS creating_department_id
                              ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
                              ,hri.REQ_PAT_ID
                              ,peh.INPATIENT_DATA_ID
                              ,peh.HOSP_ADMSN_TIME
                              ,peh.HOSP_DISCH_TIME
                              ,CASE
                                   WHEN hrr.HLR_ID IS NOT NULL THEN
                                       'hlr-RN'
                                   ELSE
                                       'hlr'
                               END                                                                                        AS loc_source --set loc_source to lhr-RN to indicate Nurse assist transport
							  ,plc.START_TIME
							  ,plc.END_TIME
							  ,plc.CANCELED_TIME
							  ,plc.EVENT_TYPE_C
							  ,zpet.NAME AS EVENT_TYPE_NAME
						INTO #dlactwotxp
                        FROM CLARITY.dbo.HL_REQ_STATUS_AUDIT                  AS hrsa
							--INNER JOIN #completed txp
							--	ON txp.HLR_ID = hrsa.HLR_ID
                            INNER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS           AS zhrs
                                ON hrsa.STATUS_C = zhrs.HL_REQ_STATUS_C
                            INNER JOIN CLARITY.dbo.HL_REQ_INFO                AS hri
                                ON hri.HLR_ID = hrsa.HLR_ID
                            INNER JOIN CLARITY.dbo.ZC_HLR_TYPE                AS zht
                                ON zht.HLR_TYPE_C = hri.HLR_TYPE_C
                            INNER JOIN CLARITY.dbo.CLARITY_DEP                AS dep
                                ON hri.REQ_DEPARTMENT_ID = dep.DEPARTMENT_ID
                            INNER JOIN CLARITY.dbo.CL_PLF                     AS cp
                                ON hri.REQ_END_PLF_ID = cp.RECORD_ID
                            --INNER JOIN #base_encounters                       AS li4
                            --    ON li4.PAT_ID = hri.REQ_PAT_ID
                            INNER JOIN CLARITY.dbo.PAT_ENC_HSP                AS peh
                                ON hri.REQ_ADMISSION_PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
                            LEFT OUTER JOIN CLARITY.dbo.F_SCHED_APPT          AS hlr_appt
                                ON hri.REQ_END_APPT_PAT_ENC_CSN_ID = hlr_appt.PAT_ENC_CSN_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_PRC           AS hlr_prc
                                ON hlr_prc.PRC_ID = hlr_appt.PRC_ID
                            LEFT OUTER JOIN CLARITY.dbo.CL_PLF                AS parent_plf
                                ON cp.PARENT_LOCATION_ID = parent_plf.RECORD_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS parent_dep
                                ON parent_plf.DEPARTMENT_ID = parent_dep.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_BED           AS cb
                                ON cb.BED_ID = cp.BED_ID
                            LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_MODE        AS zhrm
                                ON hri.REQ_MODE_C = zhrm.HL_REQ_MODE_C
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_ROM           AS cr
                                ON cb.ROOM_ID = cr.ROOM_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS cd2
                                ON cr.DEPARTMENT_ID = cd2.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.HL_REQ_REQUIREMENTS   AS hrr
                                ON hrr.HLR_ID = hrsa.HLR_ID
                                   AND hrr.REQUIREMENT_C = 103 --Nurse Assist
                            LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_REQUIREMENT AS zhrr
                                ON hrr.REQUIREMENT_C = zhrr.HL_REQ_REQUIREMENT_C
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS cd
                                ON cp.DEPARTMENT_ID = cd.DEPARTMENT_ID
                            LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP           AS dep_crt
                                ON hri.REQ_CREATE_DEPARTMENT_ID = dep_crt.DEPARTMENT_ID
							LEFT OUTER JOIN CLARITY.dbo.CL_PLC plc
								ON plc.PAT_ENC_CSN_ID = hri.REQ_ADMISSION_PAT_ENC_CSN_ID
								AND plc.LOCATION_RECORD_ID = 4204
							LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_EVENT_TYPE zpet
								ON zpet.PLC_EVENT_TYPE_C = plc.EVENT_TYPE_C
                        WHERE 1 =1
						AND (
                                  hrsa.STATUS_C = 35 --completed
                                  OR
                                  (
                                      hrsa.STATUS_C = 40 --cancelled
                                      AND hri.REQ_CANCEL_RSN_C = 119
                                  )
                              ) --unit transported
							  --AND hrsa.HLR_ID = 2547746
                              AND zht.NAME = 'job'
                              AND hri.HLR_NAME = 'Patient Transport'
                              --AND cp.RECORD_NAME <> 'DISCHARGE SUITE'
                              AND cp.RECORD_NAME = 'DISCHARGE SUITE'
							  --AND plc.EVENT_TYPE_C =  1 -- Manually Created
                              --AND hrsa.EVENT_LOCAL_DTTM
                              --BETWEEN COALESCE(li4.ADT_ARRIVAL_DTTM, li4.HOSP_ADMSN_TIME) AND COALESCE(
                              --                                                                            li4.HOSP_DISCH_TIME
                              --                                                                           ,GETDATE()
                              --                                                                        )
                              --AND
                              --(
                              --    hlr_prc.PRC_NAME NOT IN ( 'XR 15 MIN', 'UVA IR GENERIC/HYBRID APPT'
                              --                             ,'UVA OR29 240 APPT', 'UVA OR29 HYBRID APPT'
                              --                             ,'UVA OR29 VC GENERIC APPT'
                              --                            ) --exclude 15 min X-ray appts and IR appts in the OR
                              --    OR hlr_prc.PRC_NAME IS NULL
                              --)
						AND CAST(hrsa.EVENT_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
                        --GROUP BY hrsa.HLR_ID
                        --        ,hrsa.EVENT_LOCAL_DTTM
                        --        ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
                        --        ,hri.REQ_PAT_ID
                        --        ,peh.INPATIENT_DATA_ID
                        --        ,peh.HOSP_ADMSN_TIME
                        --        ,peh.HOSP_DISCH_TIME
                        --        ,dep_crt.DEPARTMENT_NAME
                        --        ,dep_crt.DEPARTMENT_ID
                        --        ,CASE
                        --             WHEN hrr.HLR_ID IS NOT NULL THEN
                        --                 'hlr-RN'
                        --             ELSE
                        --                 'hlr'
                        --         END

						SELECT
							*
						FROM #dlactwotxp
						ORDER BY
							HLR_ID, LINE

						SELECT DISTINCT
							HLR_ID
						FROM #dlactwotxp
						ORDER BY
							HLR_ID

						SELECT DISTINCT
							HLR_ID
						FROM #dlactwotxp
						WHERE END_TIME > START_TIME
						ORDER BY
							HLR_ID
--*/
GO


