USE CLARITY

DECLARE @startdate datetime = '7/1/2025';
DECLARE @enddate datetime = '10/15/2025';

--DECLARE @startdate datetime = '7/1/2024';
--DECLARE @enddate datetime = '09/23/2025';

IF OBJECT_ID('tempdb..#HL_ASGN_INFO_AUDIT ') IS NOT NULL
DROP TABLE #HL_ASGN_INFO_AUDIT

IF OBJECT_ID('tempdb..#HL_REQ_STATUS_MOD_AUDIT ') IS NOT NULL
DROP TABLE #HL_REQ_STATUS_MOD_AUDIT

IF OBJECT_ID('tempdb..#transport ') IS NOT NULL
DROP TABLE #transport

IF OBJECT_ID('tempdb..#dltx ') IS NOT NULL
DROP TABLE #dltx

--IF OBJECT_ID('tempdb..#planned ') IS NOT NULL
--DROP TABLE #planned

IF OBJECT_ID('tempdb..#completed ') IS NOT NULL
DROP TABLE #completed

IF OBJECT_ID('tempdb..#completed_canceled ') IS NOT NULL
DROP TABLE #completed_canceled

IF OBJECT_ID('tempdb..#dltxp') IS NOT NULL
DROP TABLE #dltxp

IF OBJECT_ID('tempdb..#dlevt') IS NOT NULL
DROP TABLE #dlevt

IF OBJECT_ID('tempdb..#dlact') IS NOT NULL
DROP TABLE #dlact

IF OBJECT_ID('tempdb..#dlactwotxp') IS NOT NULL
DROP TABLE #dlactwotxp

/*
CL_PLC_ADT_INFO
CL_PLC_ORD_INFO
*/
  
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

--SELECT
--	*
--FROM #dltx
--ORDER BY
--	HLR_ID,
--	LINE,
--	EVENT_LOCAL_DTTM

--SELECT DISTINCT
--	HLR_ID
--INTO #planned
--FROM #dltx
--WHERE STATUS_C = 5 --	Planned

--SELECT
--	*
--FROM #planned
--ORDER BY
--	HLR_ID

SELECT DISTINCT
	HLR_ID,
	EVENT_LOCAL_DTTM
INTO #completed
FROM #dltx
WHERE STATUS_C = 35 --	Completed

SELECT DISTINCT
	HLR_ID,
	EVENT_LOCAL_DTTM
INTO #completed_canceled
FROM #dltx
WHERE STATUS_C IN (35,40) --	Completed,Canceled

                       SELECT DISTINCT
                               hrsa.HLR_ID
							  ,hrsa.LINE
                              ,hrsa.EVENT_LOCAL_DTTM
							  ,txp.EVENT_LOCAL_DTTM AS Completed_LOCAL_DTTM
                              ,peh.HOSP_ADMSN_TIME
                              ,peh.HOSP_DISCH_TIME
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
                              ,CASE
                                   WHEN hrr.HLR_ID IS NOT NULL THEN
                                       'hlr-RN'
                                   ELSE
                                       'hlr'
                               END                                                                                        AS loc_source --set loc_source to lhr-RN to indicate Nurse assist transport
							  --,plc.START_TIME
							  --,plc.END_TIME
							  --,plc.CANCELED_TIME
							  --,plc.EVENT_TYPE_C
							  --,zpet.NAME AS EVENT_TYPE_NAME
						INTO #dltxp
                        FROM CLARITY.dbo.HL_REQ_STATUS_AUDIT                  AS hrsa
							--INNER JOIN #completed txp
							INNER JOIN #completed_canceled txp
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
							--LEFT OUTER JOIN CLARITY.dbo.CL_PLC plc
							--	ON plc.PAT_ENC_CSN_ID = hri.REQ_ADMISSION_PAT_ENC_CSN_ID
							--	AND plc.LOCATION_RECORD_ID = 4204
							--LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_EVENT_TYPE zpet
							--	ON zpet.PLC_EVENT_TYPE_C = plc.EVENT_TYPE_C
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

						--SELECT
						--	*
						--FROM #dltxp
						----ORDER BY
						----	HLR_ID, LINE
						--ORDER BY
						--	REQ_ADMISSION_PAT_ENC_CSN_ID, HLR_ID, LINE

SELECT DISTINCT
	   hri2.REQ_ADMISSION_PAT_ENC_CSN_ID -- same as PAT_ENC_CSN_ID
	  ,hri2.REQ_END_APPT_PAT_ENC_CSN_ID
      ,hri2.HLR_ID
	  ,hri2.HLR_NAME
	  ,hri2.HLR_TYPE_C
	  ,hri2.HLR_TYPE_NAME
	  ,hri2.RECORD_ID
	  ,hri2.RECORD_NAME
      ,peh.HOSP_ADMSN_TIME
      ,peh.HOSP_DISCH_TIME
      ,plc.[PAT_ID]
	  ,pt.PAT_MRN_ID
      ,plc.[PAT_ENC_CSN_ID] -- Inpatient Admission
      ,[START_TIME]
      ,[CANCELED_TIME]
      ,[END_TIME]
      ,[LOCATION_EVNT_ID]
      ,plc.[CM_PHY_OWNER_ID]
      ,plc.[CM_LOG_OWNER_ID]
      ,plc.[STATUS_C]
	  ,zps.NAME AS STATUS_NAME
      ,[CASE_TRACK_EVENT_C]
      ,[PRE_CANCEL_STS_C]
      ,[PRIVATE_YN]
      ,[SOURCE_ORC_ID]
      ,[SOURCE_ORL_ID]
      ,[LOCATION_RECORD_ID]
      ,[USER_ID]
      ,[COMMENTS]
      ,[RTLS_TAGID]
      ,[EVENT_TYPE_C]
	  ,zpet.NAME AS EVENT_TYPE_NAME
  INTO #dlevt
  FROM [CLARITY].[dbo].[CL_PLC] plc
  LEFT OUTER JOIN CLARITY.dbo.PATIENT pt
	ON pt.PAT_ID = plc.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_EVENT_TYPE zpet
	ON zpet.PLC_EVENT_TYPE_C = plc.EVENT_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_STATUS zps
	ON zps.STATUS_C = plc.STATUS_C
/*
  LEFT OUTER JOIN CLARITY.dbo.HL_REQ_INFO hri
    ON hri.REQ_ADMISSION_PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
  LEFT OUTER JOIN CLARITY.dbo.CL_PLF cp
	ON hri.REQ_END_PLF_ID = cp.RECORD_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_HLR_TYPE zht
	ON zht.HLR_TYPE_C = hri.HLR_TYPE_C
*/
LEFT OUTER JOIN
(
SELECT
	hri.HLR_ID,
    hri.HLR_NAME,
    hri.HLR_TYPE_C,
    hri.REQ_END_PLF_ID,
    hri.REQ_ADMISSION_PAT_ENC_CSN_ID,
	hri.REQ_END_APPT_PAT_ENC_CSN_ID,
    hri.HLR_TYPE_NAME,
    hri.RECORD_ID,
    hri.RECORD_NAME
FROM
(
SELECT DISTINCT
	   hri.HLR_ID
	  ,hri.HLR_NAME
	  ,hri.HLR_TYPE_C
	  ,hri.REQ_END_PLF_ID
	  ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
	  ,hri.REQ_END_APPT_PAT_ENC_CSN_ID
	  ,zht.NAME AS HLR_TYPE_NAME
	  ,cp.RECORD_ID
	  ,cp.RECORD_NAME
FROM CLARITY.dbo.HL_REQ_INFO hri
LEFT OUTER JOIN CLARITY.dbo.CL_PLF cp
	ON hri.REQ_END_PLF_ID = cp.RECORD_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_HLR_TYPE zht
	ON zht.HLR_TYPE_C = hri.HLR_TYPE_C
WHERE 1 = 1
AND hri.HLR_NAME = 'Patient Transport'
AND zht.NAME = 'Job'
AND cp.RECORD_NAME = 'DISCHARGE SUITE'
) hri
) hri2
ON hri2.REQ_ADMISSION_PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.PAT_ENC_HSP                AS peh
	ON hri2.REQ_ADMISSION_PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.F_SCHED_APPT          AS hlr_appt
	ON hri2.REQ_END_APPT_PAT_ENC_CSN_ID = hlr_appt.PAT_ENC_CSN_ID
  WHERE plc.LOCATION_RECORD_ID = 4204
  AND CAST(plc.START_TIME AS DATE) BETWEEN @startdate AND @enddate
  --AND hri.HLR_NAME = 'Patient Transport'
  --AND zht.NAME = 'Job'
  --AND cp.RECORD_NAME = 'DISCHARGE SUITE'

SELECT 
	*
 FROM #dlevt plc
  --ORDER BY plc.START_TIME
  --ORDER BY plc.PAT_ID, plc.PAT_ENC_CSN_ID, plc.LOCATION_EVNT_ID
  --ORDER BY plc.START_TIME, plc.PAT_ENC_CSN_ID, plc.LOCATION_EVNT_ID
  ORDER BY plc.PAT_ENC_CSN_ID, plc.START_TIME, plc.LOCATION_EVNT_ID

--SELECT DISTINCT
--	plc.PAT_ID,
--	plc.PAT_ENC_CSN_ID
-- FROM #dlevt plc
--  ORDER BY plc.PAT_ID, plc.PAT_ENC_CSN_ID

--SELECT 
--	*
-- FROM #dlevt plc
--WHERE END_TIME > START_TIME
--  --ORDER BY plc.START_TIME
--  --ORDER BY plc.PAT_ID, plc.PAT_ENC_CSN_ID, plc.LOCATION_EVNT_ID
--  --ORDER BY plc.START_TIME, plc.PAT_ENC_CSN_ID, plc.LOCATION_EVNT_ID
--  ORDER BY plc.PAT_ENC_CSN_ID, plc.START_TIME, plc.LOCATION_EVNT_ID

SELECT
	plc.*,
	adt.*
FROM CLARITY.dbo.CL_PLC_ADT_INFO plc
INNER JOIN CLARITY.dbo.CLARITY_ADT adt
	ON plc.ADT_EVENT_ID = adt.EVENT_ID
WHERE LOCATION_EVNT_ID = 11148435 -- ADT_EVENT_ID is the ADT discharge event

SELECT
	plc.*,
	ord.*,
	zot.NAME AS ORDER_TYPE_NAME
FROM 
(
SELECT DISTINCT
plc.LOCATION_EVNT_ID,
plc.PAT_ENC_CSN_ID
FROM #dlevt plc
) plc
LEFT OUTER JOIN CLARITY.dbo.ORDER_METRICS ord -- select latest discharge order for a CSN
	ON ord.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_TYPE zot
	ON zot.ORDER_TYPE_C = ord.ORDER_TYPE_C
WHERE zot.NAME LIKE '%disch%' -- ORDER_TYPE_C = 49 Discharge
ORDER BY
	plc.LOCATION_EVNT_ID,
	plc.PAT_ENC_CSN_ID

SELECT DISTINCT
	   hri2.REQ_ADMISSION_PAT_ENC_CSN_ID -- same as PAT_ENC_CSN_ID
	  ,hri2.REQ_END_APPT_PAT_ENC_CSN_ID
      ,hri2.HLR_ID
	  ,hri2.HLR_NAME
	  ,hri2.HLR_TYPE_C
	  ,hri2.HLR_TYPE_NAME
	  ,hri2.RECORD_ID
	  ,hri2.RECORD_NAME
      ,peh.HOSP_ADMSN_TIME
      ,peh.HOSP_DISCH_TIME
	  ,hlr_appt.CHECKIN_DTTM
	  ,hlr_appt.CHECKOUT_DTTM
      ,plc.[PAT_ID]
	  ,pt.PAT_MRN_ID
      ,plc.[PAT_ENC_CSN_ID] -- Inpatient Admission
      ,[START_TIME]
      ,[CANCELED_TIME]
      ,[END_TIME]
      ,[LOCATION_EVNT_ID]
      --,plc.[CM_PHY_OWNER_ID]
      --,plc.[CM_LOG_OWNER_ID]
      ,plc.[STATUS_C]
	  ,zps.NAME AS STATUS_NAME
      ,[CASE_TRACK_EVENT_C]
      ,[PRE_CANCEL_STS_C]
      ,[PRIVATE_YN]
      ,[SOURCE_ORC_ID]
      ,[SOURCE_ORL_ID]
      ,[LOCATION_RECORD_ID]
      ,[USER_ID]
      ,[COMMENTS]
      ,[RTLS_TAGID]
      ,[EVENT_TYPE_C]
	  ,zpet.NAME AS EVENT_TYPE_NAME
	  , ord.ORDER_ID,
       ord.AUTH_PROV_ID,
       ord.ORDERING_PROV_ID,
       ord.ORDERING_USER_ID,
       ord.ORDER_DTTM,
       ord.ORDER_DESC,
       ord.DISPLAY_NAME,
       ord.ORDER_STATUS_C,
       ord.ORDER_TYPE_C,
       ord.PAT_LOC_ID,
       ord.ORIG_AUTH_PROV_ID,
       ord.ORIG_ORD_PROV_ID
  FROM [CLARITY].[dbo].[CL_PLC] plc
  LEFT OUTER JOIN CLARITY.dbo.PATIENT pt
	ON pt.PAT_ID = plc.PAT_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_EVENT_TYPE zpet
	ON zpet.PLC_EVENT_TYPE_C = plc.EVENT_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_PLC_STATUS zps
	ON zps.STATUS_C = plc.STATUS_C
  LEFT OUTER JOIN
  (
  SELECT
	ord.ORDER_ID,
    ord.AUTH_PROV_ID,
    ord.ORDERING_PROV_ID,
    ord.ORDERING_USER_ID,
    --ord.CPOE_YN,
    --ord.LGQ_ORDERSET_ID,
    --ord.USER_OVERRIDE_YN,
    --ord.REORDERED_YN,
    --ord.MODIFIED_YN,
    --ord.ORDER_MODE,
    --ord.ORD_VRB_MSGSENT_YN,
    --ord.ORD_COS_MSGSENT_YN,
    --ord.DISCONTINUE_MODE,
    --ord.DSC_VRB_MSGSENT_YN,
    --ord.DSC_COS_MSGSENT_YN,
    --ord.ORDER_SOURCE_C,
    --ord.PRL_ORDERSET_ID,
    --ord.FIRST_VERIFY_CDR,
    --ord.FIRST_DISPENSE_CDR,
    --ord.ORD_WORKSTATION_ID,
    --ord.CM_PHY_OWNER_ID,
    --ord.CM_LOG_OWNER_ID,
    ord.ORDER_DTTM,
    --ord.ACKNOWLEDGE_DTTM,
    --ord.SESSION_KEY,
    --ord.MU_CPOE_YN,
    --ord.CSGN_TURNAROUND_SEC,
    ord.ORDER_DESC,
    ord.DISPLAY_NAME,
    ord.ORDER_STATUS_C,
    --ord.PAT_ID,
    ord.PAT_ENC_CSN_ID,
    --ord.ACTIVE_ORDER_C,
    ord.ORDER_TYPE_C,
    --ord.ORIGINAL_SESSIONKEY,
    ord.PAT_LOC_ID,
    --ord.DEST_DEPT_OVRIDE_YN,
    --ord.CANC_DEPT_OVRIDE_YN,
    ord.ORIG_AUTH_PROV_ID,
    ord.ORIG_ORD_PROV_ID--,
    --ord.PREFERENCE_LIST_TYPE_C,
    --ord.DISCON_LOC_DTTM,
    --ord.SPECIMEN_RECV_DATE,
    --ord.FIRST_FINAL_LOC_DTTM,
    --ord.PARENT_CE_ORDER_ID
  FROM CLARITY.dbo.ORDER_METRICS ord -- select latest discharge order for a CSN
  LEFT OUTER JOIN CLARITY.dbo.ZC_ORDER_TYPE zot
	ON zot.ORDER_TYPE_C = ord.ORDER_TYPE_C
  WHERE zot.NAME LIKE '%disch%' -- ORDER_TYPE_C = 49 Discharge
  ) ord
	ON ord.PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
LEFT OUTER JOIN
(
SELECT
	hri.HLR_ID,
    hri.HLR_NAME,
    hri.HLR_TYPE_C,
    hri.REQ_END_PLF_ID,
    hri.REQ_ADMISSION_PAT_ENC_CSN_ID,
	hri.REQ_END_APPT_PAT_ENC_CSN_ID,
    hri.HLR_TYPE_NAME,
    hri.RECORD_ID,
    hri.RECORD_NAME
FROM
(
SELECT DISTINCT
	   hri.HLR_ID
	  ,hri.HLR_NAME
	  ,hri.HLR_TYPE_C
	  ,hri.REQ_END_PLF_ID
	  ,hri.REQ_ADMISSION_PAT_ENC_CSN_ID
	  ,hri.REQ_END_APPT_PAT_ENC_CSN_ID
	  ,zht.NAME AS HLR_TYPE_NAME
	  ,cp.RECORD_ID
	  ,cp.RECORD_NAME
FROM CLARITY.dbo.HL_REQ_INFO hri
LEFT OUTER JOIN CLARITY.dbo.CL_PLF cp
	ON hri.REQ_END_PLF_ID = cp.RECORD_ID
LEFT OUTER JOIN CLARITY.dbo.ZC_HLR_TYPE zht
	ON zht.HLR_TYPE_C = hri.HLR_TYPE_C
WHERE 1 = 1
AND hri.HLR_NAME = 'Patient Transport'
AND zht.NAME = 'Job'
AND cp.RECORD_NAME = 'DISCHARGE SUITE'
) hri
) hri2
ON hri2.REQ_ADMISSION_PAT_ENC_CSN_ID = plc.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.PAT_ENC_HSP                AS peh
	ON hri2.REQ_ADMISSION_PAT_ENC_CSN_ID = peh.PAT_ENC_CSN_ID
LEFT OUTER JOIN CLARITY.dbo.F_SCHED_APPT          AS hlr_appt
	ON hri2.REQ_END_APPT_PAT_ENC_CSN_ID = hlr_appt.PAT_ENC_CSN_ID
  WHERE plc.LOCATION_RECORD_ID = 4204
  AND CAST(plc.START_TIME AS DATE) BETWEEN @startdate AND @enddate
  ORDER BY plc.PAT_ENC_CSN_ID, plc.START_TIME, plc.LOCATION_EVNT_ID

/*SELECT PAT_ENC_CSN_ID,  --- Patient admitted to Neurosurgery and discharged to home/home health
		PAT_ID,
		HOSP_ADM_DATE,
		HOSP_ADM_DTTM,
		HOSP_DISCH_DATE,
		HOSP_DISCH_DTTM,
		ADM_DEPT_ID,
		ADM_PROV_ID,
		ADM_ATND_PROV_ID,
		DISCH_DEPT_ID,
		DISCH_PROV_ID,
		DISCH_ATND_PROV_ID,
		FINAL_DRG_TYPE_ID,
		FINAL_DRG_ID,
		HOSPITAL_SERVICE_C,  --- Reference ZC_PAT_SERVICE for service list
		DISCHARGE_DISPOSITION_C -- Reference ZC_DISCH_DISP for discharge disposition
FROM CLARITY..F_IP_HSP_ADMISSION
*/
/*
SELECT  niph.PAT_ENC_CSN_ID,  --- Neurosurgery patient discharge orders placed by APPs 
		niph.PAT_ID,
		niph.HOSP_DISCH_DATE,
		niph.HOSP_DISCH_DTTM,
		om.ORDER_ID,
		om.ORDERING_PROV_ID,
		ser.PROV_NAME AS ORDERING_PROV_NM,
		ser.PROV_TYPE,
		om.AUTH_PROV_ID,
		om.ORIG_AUTH_PROV_ID,
		om.ORIG_ORD_PROV_ID,
		om.ORDER_DTTM,
		CAST (om.ORDER_DTTM as TIME) AS ORDER_TIME,
		om.ORDER_TYPE_C,
		om.ORDER_DESC
		INTO #NS_PT_DC_ORDERS
FROM NSURG_IP_PT_HH niph
INNER JOIN CLARITY..ORDER_METRICS om
ON om.PAT_ENC_CSN_ID = niph.PAT_ENC_CSN_ID 
INNER JOIN [Rptg].[vwAHP_PPES_Reporting_Provider_List] ser
ON ser.PROV_ID = om.ORDERING_PROV_ID
WHERE om.ORDER_TYPE_C = '49' --- Discharge orders
--AND ser.PROV_TYPE IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') --- Joined to APP view instead
AND ser.RPT_GRP_FIVE = 'Neurosciences & Behavioral Health'
*/