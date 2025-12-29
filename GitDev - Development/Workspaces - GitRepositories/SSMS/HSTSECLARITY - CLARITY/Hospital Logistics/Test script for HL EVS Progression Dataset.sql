USE CLARITY

DECLARE @StartDate AS DATE, @EndDate AS DATE

--SET @StartDate = '2/1/2024'
--SET @EndDate = '3/31/2024'
--27,814 (05/20)
--16,460 (05/21)
SET @StartDate = '4/1/2024'
SET @EndDate = '4/30/2024'

DECLARE @de_control AS VARCHAR(10), @DeptName AS VARCHAR(50)

SET @de_control = 'UVA-MC'
--SET @DeptName = 'UVHE 5 SOUTH'
-- 203 (05/23)
SET @DeptName = 'UVHE 5 EAST,UVHE 6 NORTH'
-- 192 (79, 113, 05/28, April 2024)

IF OBJECT_ID('tempdb..#MDM_REV_LOC_ID ') IS NOT NULL -- 2
DROP TABLE #MDM_REV_LOC_ID

IF OBJECT_ID('tempdb..#HL_ASGN_INFO_AUDIT ') IS NOT NULL -- 2
DROP TABLE #HL_ASGN_INFO_AUDIT

IF OBJECT_ID('tempdb..#HL_REQ_STATUS_MOD_AUDIT ') IS NOT NULL -- 1
DROP TABLE #HL_REQ_STATUS_MOD_AUDIT

IF OBJECT_ID('tempdb..#evs ') IS NOT NULL -- 2
DROP TABLE #evs

SELECT DISTINCT
           t1.REV_LOC_ID
          ,t1.HOSPITAL_CODE
          ,t1.DE_HOSPITAL_CODE
          ,t1.HOSPITAL_GROUP
	INTO #MDM_REV_LOC_ID
    FROM [CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group] t1
    WHERE (t1.REV_LOC_ID IS NOT NULL)

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
  AND REQ_TASK_SUBTYPE_C = 2 -- Bed Clean
	AND hri.REQ_REGION_SEC_ID IN
  (3100000057, -- General - UVA GRAND CENTRAL Culpeper EVS
   3100000000, -- General - UVA GRAND CENTRAL UVHE EVS
   3100000072  -- General - UVA GRAND CENTRAL Prince William EVS
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
	haia.REQ_ACTIVATION_LOCAL_DTTM
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
	hlr.POSTPONE_RSN_NAME,
	hlr.REQ_TASK_SUBTYPE_C,
	hlr.REQ_ACTIVATION_LOCAL_DTTM
INTO #evs
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
	NULL AS POSTPONE_RSN_NAME,
	haia.REQ_TASK_SUBTYPE_C,
	haia.REQ_ACTIVATION_LOCAL_DTTM
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
	hrsma.POSTPONE_RSN_NAME,
	hrsma.REQ_TASK_SUBTYPE_C,
	hrsma.REQ_ACTIVATION_LOCAL_DTTM
FROM #HL_REQ_STATUS_MOD_AUDIT hrsma
) hlr

;WITH hlbev_cte AS
(
SELECT ClnSts.*
    FROM
    (
        SELECT evs.GROUP_HLR_ID
		      ,evs.HLR_ID AS RECORD_ID
              ,evs.STATUS_NAME AS 'CleanSts'
              ,EVENT_LOCAL_DTTM AS INSTANT_TM
        FROM #evs evs
    ) AS event
    PIVOT
    (
        MAX(INSTANT_TM)
        FOR CleanSts IN ([Unplanned], [Planned], [Assigned], [Acknowledged], [At Start Location], [In Progress], [At End Location], [Completed], [Canceled], [Hold])
    ) AS ClnSts
	INNER JOIN CLARITY..HL_REQ_INFO cba
	ON ClnSts.RECORD_ID = cba.HLR_ID
	WHERE
	(REQ_STAGE_NUM IS NULL
	OR REQ_STAGE_NUM = '1')
	),

    --get all Bed clean requests	
bev_cte AS
(
 SELECT ClnSts.*
    FROM
    (
        SELECT RECORD_ID
              ,CASE
                   WHEN STATUS_C = 1
                   THEN 'Unplanned'
                   WHEN STATUS_C = 2
                   THEN 'Assigned'
                   WHEN STATUS_C = 3
                   THEN 'In Progress'
                   WHEN STATUS_C = 4
                   THEN 'Hold'
                   WHEN STATUS_C = 5
                   THEN 'Completed'
               END       'CleanSts'
              ,INSTANT_TM
        FROM CLARITY.dbo.CL_BEV_EVENTS_ALL
    ) AS event
    PIVOT
    (
        MAX(INSTANT_TM)
        FOR CleanSts IN ([Unplanned], [Planned], [Assigned], [Acknowledged], [At Start Location], [In Progress], [At End Location], [Completed], [Canceled], [Hold])
    ) AS ClnSts
	INNER JOIN CLARITY..CL_BEV_ALL cba
	ON ClnSts.RECORD_ID = cba.RECORD_ID
	WHERE 	EVENT_TYPE_C = '0'
	AND (STAGE_NUMBER IS NULL
	OR STAGE_NUMBER = '1')
	),

hlhkr_cte AS
(
SELECT ClnSts.*
    FROM
    (
        SELECT evs.GROUP_HLR_ID
		      ,evs.HLR_ID AS RECORD_ID
              ,evs.STATUS_NAME AS 'CleanSts'
              ,ASSIGNED_TECH_ID AS HKR_ID
        FROM #evs evs
    ) AS event
    PIVOT
    (
        MAX(HKR_ID)
        FOR CleanSts IN ([Unplanned], [Planned], [Assigned], [Acknowledged], [At Start Location], [In Progress], [At End Location], [Completed], [Canceled], [Hold])
    ) AS ClnSts
	),

    --GET all bed clean responsible employees
hkr_cte AS
(
SELECT *
    FROM
    (
        SELECT RECORD_ID
              ,CASE
                   WHEN STATUS_C = 1
                   THEN 'Unplanned'
                   WHEN STATUS_C = 2
                   THEN 'Assigned'
                   WHEN STATUS_C = 3
                   THEN 'In Progress'
                   WHEN STATUS_C = 4
                   THEN 'Hold'
                   WHEN STATUS_C = 5
                   THEN 'Completed'
               END      'CleanSts'
              ,HKR_ID
        FROM CLARITY.dbo.CL_BEV_EVENTS_ALL
    ) AS emp
    PIVOT
    (
        MAX(HKR_ID)
        FOR CleanSts IN ([Unplanned], [Planned], [Assigned], [Acknowledged], [At Start Location], [In Progress], [At End Location], [Completed], [Canceled], [Hold])
    ) AS ClnSts
	),

    --get all hours of day
HR_CTE AS
(
SELECT TOP 24
           NUM - 1 'HR'
    FROM CLARITY.dbo.D_NUMBERS_10000
	),

    --get all months between star and end time 
Mnth_cte AS
(
SELECT DISTINCT
           MONTH_NUMBER
          ,YEAR
          ,MONTH_NAME
    FROM CLARITY.dbo.DATE_DIMENSION
    WHERE CALENDAR_DT >= @StartDate
          AND CALENDAR_DT <= @EndDate
		  ),
		  		   
    --get all hours 
tm_cte AS
(
SELECT *
    FROM Mnth_cte
        CROSS JOIN HR_CTE
		),

--/*
    --get all bed clean events and responsible employees 
allevs_cte AS
(
SELECT bev_cte.RECORD_ID
          ,bev_cte.Unplanned                                           'TimeDirty'
          ,bev_cte.Assigned                                        'TimeAssgn'
          ,bev_cte.[In Progress]                                      'TimeInPgr'
          ,bev_cte.Hold                                          'TimeOnHld'
          ,bev_cte.Completed                                       'TimeCmplt'
          ,dept.DEPARTMENT_NAME
          ,dept.DEPARTMENT_ID
          ,bevall.BED_ID
          ,mloc.*
          ,sts.NAME                                                'Priority'
          ,esc.NAME                                                'EscReason'
          ,src.NAME                                                'EventSource'
          ,hkr_cte.Unplanned                                           'empDirty'
          ,hkr_cte.Assigned                                        'EmpAssigned'
          ,hkr_cte.[In Progress]                                      'EmpInpgr'
          ,hkr_cte.Hold                                          'EmpOnHold'
          ,hkr_cte.Completed                                       'EmpComplt'
          ,emp.RECORD_NAME                                         'UserName'
          ,emp.EMP_ID
          ,DATEPART(hh, bev_cte.Unplanned)                             'RequestHr'
          ,DATEPART(dw, bev_cte.Unplanned)                             'RequestDw'
          ,DATENAME(dw, bev_cte.Unplanned)                             'RequestDy'
          ,MONTH(bev_cte.Unplanned)                                    'RequestMonth'
          ,DATEDIFF(MINUTE, bev_cte.Unplanned, bev_cte.Assigned)       'GapRqstAssng'
          ,DATEDIFF(MINUTE, bev_cte.Assigned, bev_cte.[In Progress])  'GapAssInPgr'
          ,DATEDIFF(MINUTE, bev_cte.[In Progress], bev_cte.Completed) 'GapInpgrCmplt'
          ,DATEDIFF(MINUTE, bev_cte.Unplanned, bev_cte.[In Progress])     'GapRqstInpgr'
          ,DATEDIFF(MINUTE, bev_cte.Unplanned, bev_cte.Completed)      'TAT'
          ,CASE
               WHEN DATEPART(hh, bev_cte.Unplanned) < 7
               THEN '00:00-06:59'
               WHEN DATEPART(hh, bev_cte.Unplanned) < 15
               THEN '07:00-14:59'
               ELSE '15:00-23:59'
           END                                                     'HrGrp'
    FROM bev_cte bev_cte
        INNER JOIN CLARITY.dbo.CL_BEV_ALL           bevall
            ON bevall.RECORD_ID = bev_cte.RECORD_ID
        LEFT OUTER JOIN hkr_cte hkr_cte
            ON hkr_cte.RECORD_ID = bev_cte.RECORD_ID
        INNER JOIN CLARITY.dbo.CLARITY_DEP          dept
            ON bevall.DEP_ID = dept.DEPARTMENT_ID
        INNER JOIN #MDM_REV_LOC_ID                   AS mloc
            ON mloc.REV_LOC_ID = dept.REV_LOC_ID
        INNER JOIN
		(
		SELECT
			BED_ID,
			ROW_NUMBER() OVER(PARTITION BY BED_ID ORDER BY BED_CONT_DATE_REAL DESC) AS bed_seq
		FROM CLARITY.dbo.CLARITY_BED
		) bed
            ON bevall.BED_ID = bed.BED_ID
        LEFT OUTER JOIN CLARITY.dbo.ZC_PRIORITY_2   sts
            ON bevall.PRIORITY_C = sts.PRIORITY_2_C
        LEFT OUTER JOIN CLARITY.dbo.ZC_ESC_REASON   esc
            ON bevall.ESC_REASON_C = esc.ESC_REASON_C
        LEFT OUTER JOIN CLARITY.dbo.ZC_EVENT_SOURCE src
            ON bevall.EVENT_SOURCE_C = src.EVENT_SOURCE_C
        INNER JOIN CLARITY.dbo.CL_HKR               emp
            ON emp.RECORD_ID = hkr_cte.Assigned
    WHERE bevall.EVENT_TYPE_C = 0
          AND
          (
              bev_cte.Unplanned >= @StartDate
              AND bev_cte.Unplanned <= @EndDate
          )
		  AND bed.bed_seq = 1

	UNION ALL	

       SELECT bev_cte.RECORD_ID
          ,bev_cte.Unplanned												    'TimeDirty'
          ,bev_cte.Assigned                                         'TimeAssgn'
          ,bev_cte.[In Progress]                                      'TimeInPgr'
          ,bev_cte.Hold                                          'TimeOnHld'
          --,NULL															'TimeOnHld'
          ,bev_cte.Completed                                      'TimeCmplt'
          ,dept.DEPARTMENT_NAME
          ,dept.DEPARTMENT_ID
          ,bevall.REQ_BED_ID
          ,mloc.*
          ,sts.NAME                                                'Priority'
          ,zesc.NAME                                                'EscReason'
		  ,zevnt.NAME												 'EventSource'
          ,hkr_cte.Unplanned                                           'empDirty'
          ,hkr_cte.Assigned                                        'EmpAssigned'
          ,hkr_cte.[In Progress]                                      'EmpInpgr'
          ,hkr_cte.Hold                                          'EmpOnHold'
          ,hkr_cte.Completed                                       'EmpComplt'
          ,emp.RECORD_NAME                                         'UserName'
          ,emp.EMP_ID
          ,DATEPART(hh, bev_cte.Unplanned)                             'RequestHr'
          ,DATEPART(dw, bev_cte.Unplanned)                             'RequestDw'
          ,DATENAME(dw, bev_cte.Unplanned)                             'RequestDy'
          ,MONTH(bev_cte.Unplanned)                                    'RequestMonth'
          ,DATEDIFF(MINUTE, bev_cte.Unplanned, bev_cte.Assigned)       'GapRqstAssng'
          ,DATEDIFF(MINUTE, bev_cte.Assigned, bev_cte.[In Progress])  'GapAssInPgr'
          ,DATEDIFF(MINUTE, bev_cte.[In Progress], bev_cte.Completed) 'GapInpgrCmplt'
          ,DATEDIFF(MINUTE, bev_cte.Unplanned, bev_cte.[In Progress])     'GapRqstInpgr'
          ,DATEDIFF(MINUTE, bev_cte.Unplanned, bev_cte.Completed)      'TAT'
          ,CASE
               WHEN DATEPART(hh, bev_cte.Unplanned) < 7
               THEN '00:00-06:59'
               WHEN DATEPART(hh, bev_cte.Unplanned) < 15
               THEN '07:00-14:59'
               ELSE '15:00-23:59'
           END                                                     'HrGrp'
    FROM hlbev_cte bev_cte
        INNER JOIN CLARITY.dbo.HL_REQ_INFO bevall
            ON bevall.HLR_ID = bev_cte.RECORD_ID
        LEFT OUTER JOIN hlhkr_cte hkr_cte
            ON hkr_cte.RECORD_ID = bev_cte.RECORD_ID
        INNER JOIN CLARITY.dbo.CLARITY_DEP          dept
            ON bevall. REQ_DEPARTMENT_ID = dept.DEPARTMENT_ID
        INNER JOIN #MDM_REV_LOC_ID                   AS mloc
            ON mloc.REV_LOC_ID = dept.REV_LOC_ID
        INNER JOIN
		(
		SELECT
			BED_ID,
			ROW_NUMBER() OVER(PARTITION BY BED_ID ORDER BY BED_CONT_DATE_REAL DESC) AS bed_seq
		FROM CLARITY.dbo.CLARITY_BED
		) bed
            ON bevall.REQ_BED_ID = bed.BED_ID
        LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_PRIORITY  sts
            ON bevall.REQ_PRIORITY_C = sts.HL_REQ_PRIORITY_C
		LEFT OUTER JOIN
		(
		SELECT
			HLR_ID,
			LINE,
			REASON_C,
			ROW_NUMBER() OVER(PARTITION BY esc.HLR_ID ORDER BY LINE DESC) AS seq
		FROM CLARITY.dbo.HL_ASGN_ESCL_AUDIT esc
		) esc
			ON esc.HLR_ID = bevall.HLR_ID
			AND esc.seq = 1
        LEFT OUTER JOIN CLARITY.dbo.ZC_HL_ASGN_ESCL_REASON   zesc
            ON esc.REASON_C = zesc.HL_ASGN_ESCL_REASON_C
		LEFT OUTER JOIN
		(
		SELECT
			evnts.HLR_ID,
			evnts.LINE,
			evnts.LINKED_EVENT_C,
			ROW_NUMBER() OVER(PARTITION BY evnts.HLR_ID ORDER BY LINE DESC) AS seq2
		FROM CLARITY.dbo.HL_REQ_LINKED_EVENTS evnts
		WHERE evnts.LINKED_EVENT_C NOT IN (1,2)
		) evnts
			ON evnts.HLR_ID = bevall.HLR_ID
			AND seq2 = 1
        LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_LINKED_EVENT zevnt
            ON evnts.LINKED_EVENT_C = zevnt.HL_REQ_LINKED_EVENT_C
        INNER JOIN CLARITY.dbo.CL_HKR               emp
            ON emp.RECORD_ID = hkr_cte.Assigned
    WHERE
          (
              bev_cte.Unplanned >= @StartDate
              AND bev_cte.Unplanned <= @EndDate
          )
		  AND bed.bed_seq = 1
	)

--connect all
SELECT DISTINCT
       evs_cte.HOSPITAL_CODE,
       evs_cte.DEPARTMENT_NAME,
       evs_cte.TimeDirty,
       tm_cte.HR,
       tm_cte.MONTH_NUMBER,
       tm_cte.YEAR,
       tm_cte.MONTH_NAME,
       evs_cte.RECORD_ID,
       evs_cte.TimeAssgn,
       evs_cte.TimeInPgr,
       evs_cte.TimeOnHld,
       evs_cte.TimeCmplt,
       evs_cte.DEPARTMENT_ID,
       evs_cte.BED_ID,
       evs_cte.REV_LOC_ID,
       evs_cte.DE_HOSPITAL_CODE,
       evs_cte.HOSPITAL_GROUP,
       evs_cte.Priority,
       evs_cte.EscReason,
       evs_cte.EventSource,
       evs_cte.empDirty,
       evs_cte.EmpAssigned,
       evs_cte.EmpInpgr,
       evs_cte.EmpOnHold,
       evs_cte.EmpComplt,
       evs_cte.UserName,
       evs_cte.EMP_ID,
       evs_cte.RequestHr,
       evs_cte.RequestDw,
       evs_cte.RequestDy,
       evs_cte.RequestMonth,
       evs_cte.GapRqstAssng,
       evs_cte.GapAssInPgr,
       evs_cte.GapInpgrCmplt,
       evs_cte.GapRqstInpgr,
       evs_cte.TAT,
       evs_cte.HrGrp
FROM tm_cte
    LEFT OUTER JOIN allevs_cte evs_cte
        ON tm_cte.MONTH_NUMBER = evs_cte.RequestMonth
           AND tm_cte.HR = evs_cte.RequestHr
WHERE ( (evs_cte.de_hospital_code IN (select value from string_split(@de_control,',')) OR evs_cte.hospital_group IN (select value from string_split(@de_control,',')))
AND ( evs_cte.DEPARTMENT_NAME IN (select value from string_split(@DeptName,','))) )

ORDER BY HOSPITAL_CODE;