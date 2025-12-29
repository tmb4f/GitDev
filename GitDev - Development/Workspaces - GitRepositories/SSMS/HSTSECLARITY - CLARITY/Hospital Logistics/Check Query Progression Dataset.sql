USE CLARITY

--DECLARE @de_control VARCHAR(255)  = 'UVA-MC';
--DECLARE @startdate DATETIME = '03/01/2024';
--DECLARE @enddate DATETIME = '04/30/2024';
--27,890 (05/20)
DECLARE @startdate DATETIME = '02/01/2024';
DECLARE @enddate DATETIME = '03/31/2024';
--27,814 (05/20)
--16,460 (05/21)
--DECLARE @startdate DATETIME = '04/22/2024';
--DECLARE @enddate DATETIME = '04/22/2024';

IF OBJECT_ID('tempdb..#MDM_REV_LOC_ID ') IS NOT NULL -- 2
DROP TABLE #MDM_REV_LOC_ID

IF OBJECT_ID('tempdb..#HL_ASGN_INFO_AUDIT ') IS NOT NULL -- 2
DROP TABLE #HL_ASGN_INFO_AUDIT

IF OBJECT_ID('tempdb..#HL_REQ_STATUS_MOD_AUDIT ') IS NOT NULL -- 1
DROP TABLE #HL_REQ_STATUS_MOD_AUDIT

IF OBJECT_ID('tempdb..#evs ') IS NOT NULL -- 2
DROP TABLE #evs

IF OBJECT_ID('tempdb..#hlbev_cte ') IS NOT NULL -- 1
DROP TABLE #hlbev_cte

IF OBJECT_ID('tempdb..#bev_cte ') IS NOT NULL -- 1
DROP TABLE #bev_cte

IF OBJECT_ID('tempdb..#hlhkr_cte ') IS NOT NULL -- 1
DROP TABLE #hlhkr_cte

IF OBJECT_ID('tempdb..#hkr_cte ') IS NOT NULL -- 1
DROP TABLE #hkr_cte

IF OBJECT_ID('tempdb..#HR_CTE ') IS NOT NULL -- 1
DROP TABLE #HR_CTE

IF OBJECT_ID('tempdb..#Mnth_cte ') IS NOT NULL -- 1
DROP TABLE #Mnth_cte

IF OBJECT_ID('tempdb..#tm_cte ') IS NOT NULL -- 1
DROP TABLE #tm_cte

IF OBJECT_ID('tempdb..#allevs_cte ') IS NOT NULL -- 1
DROP TABLE #allevs_cte

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
  --AND CAST(hri.REQ_ACTIVATION_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
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

--SELECT
--	*
--FROM #evs
--ORDER BY GROUP_HLR_ID, HLR_ID, EVENT_LOCAL_DTTM
    --get all Bed clean requests	

--    ,hlbev_cte
--AS (

SELECT ClnSts.*
    INTO #hlbev_cte
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
	--ORDER BY
	--	ClnSts.GROUP_HLR_ID, ClnSts.RECORD_ID

	--SELECT
	--	*
	--FROM #hlbev_cte
	--ORDER BY
	--	GROUP_HLR_ID, RECORD_ID

    --get all Bed clean requests		
 SELECT ClnSts.*
    INTO #bev_cte
    FROM
    (
        SELECT RECORD_ID
              ,CASE
                   --WHEN STATUS_C = 1
                   --THEN 'Dirty'
                   WHEN STATUS_C = 1
                   THEN 'Unplanned'
                   WHEN STATUS_C = 2
                   THEN 'Assigned'
                   --WHEN STATUS_C = 3
                   --THEN 'InProgress'
                   WHEN STATUS_C = 3
                   THEN 'In Progress'
                   --WHEN STATUS_C = 4
                   --THEN 'OnHold'
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
        --FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
        FOR CleanSts IN ([Unplanned], [Planned], [Assigned], [Acknowledged], [At Start Location], [In Progress], [At End Location], [Completed], [Canceled], [Hold])
    ) AS ClnSts
	INNER JOIN CLARITY..CL_BEV_ALL cba
	ON ClnSts.RECORD_ID = cba.RECORD_ID
	WHERE 	EVENT_TYPE_C = '0'
	AND (STAGE_NUMBER IS NULL
	OR STAGE_NUMBER = '1')

	--SELECT
	--	*
	--FROM #bev_cte
	--ORDER BY
	--	RECORD_ID

SELECT ClnSts.*
    INTO #hlhkr_cte
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
	--ORDER BY
	--	ClnSts.GROUP_HLR_ID, ClnSts.RECORD_ID

	--SELECT
	--	*
	--FROM #hlhkr_cte
	--ORDER BY
	--	GROUP_HLR_ID, RECORD_ID

    --GET all bed clean responsible employees
SELECT *
INTO #hkr_cte
    FROM
    (
        SELECT RECORD_ID
              ,CASE
                   --WHEN STATUS_C = 1
                   --THEN 'Dirty'
                   WHEN STATUS_C = 1
                   THEN 'Unplanned'
                   WHEN STATUS_C = 2
                   THEN 'Assigned'
                   --WHEN STATUS_C = 3
                   --THEN 'InProgress'
                   WHEN STATUS_C = 3
                   THEN 'In Progress'
                   --WHEN STATUS_C = 4
                   --THEN 'OnHold'
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
        --FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
        FOR CleanSts IN ([Unplanned], [Planned], [Assigned], [Acknowledged], [At Start Location], [In Progress], [At End Location], [Completed], [Canceled], [Hold])
    ) AS ClnSts

	--SELECT
	--	*
	--FROM #hkr_cte
	--ORDER BY
	--	RECORD_ID

    --get all hours of day
SELECT TOP 24
           NUM - 1 'HR'
INTO #HR_CTE
    FROM CLARITY.dbo.D_NUMBERS_10000

    --get all months between star and end time 
SELECT DISTINCT
           MONTH_NUMBER
          ,YEAR
          ,MONTH_NAME
INTO #Mnth_cte
    FROM CLARITY.dbo.DATE_DIMENSION
    WHERE CALENDAR_DT >= @StartDate
          AND CALENDAR_DT <= @EndDate
		  		   
    --get all hours 
SELECT *
INTO #tm_cte
    FROM #Mnth_cte
        CROSS JOIN #HR_CTE

--/*
    --get all bed clean events and responsible employees 
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
	INTO #allevs_cte
    FROM #bev_cte bev_cte
        INNER JOIN CLARITY.dbo.CL_BEV_ALL           bevall
            ON bevall.RECORD_ID = bev_cte.RECORD_ID
        LEFT OUTER JOIN #hkr_cte hkr_cte
            ON hkr_cte.RECORD_ID = bev_cte.RECORD_ID
        INNER JOIN CLARITY.dbo.CLARITY_DEP          dept
            ON bevall.DEP_ID = dept.DEPARTMENT_ID
        INNER JOIN #MDM_REV_LOC_ID                   AS mloc
            ON mloc.REV_LOC_ID = dept.REV_LOC_ID
        --INNER JOIN CLARITY.dbo.CLARITY_BED          bed
        --    ON bevall.BED_ID = bed.BED_ID
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
    --INNER JOIN  MDM_DEP_ID t_mdm ON (t_mdm.EPIC_DEPARTMENT_ID = dept.DEPARTMENT_ID) 
    WHERE bevall.EVENT_TYPE_C = 0
          AND
          (
              bev_cte.Unplanned >= @StartDate
              AND bev_cte.Unplanned <= @EndDate
          )
		  AND bed.bed_seq = 1
    --AND DATENAME(dw,bev_cte.Unplanned)=@DayofWeek						
    --)
    -----location filtering for CH go live 09/2022----
    --AND 
    --(
    --	(UPPER(@de_control)=UPPER(mloc.de_hospital_code))
    --	OR (UPPER(@de_control)=UPPER(mloc.hospital_group))
    --	OR (UPPER(@de_control)='ALL')
    --)

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
		  --,esc.LINE
		  --,esc.START_LOCAL_DTTM
		  --,esc.END_LOCAL_DTTM
		  --,esc.REASON_C
          ,zesc.NAME                                                'EscReason'
          --,src.NAME                                                'EventSource'
		  --,evnts.LINE
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
    FROM #hlbev_cte bev_cte
        INNER JOIN CLARITY.dbo.HL_REQ_INFO bevall
            ON bevall.HLR_ID = bev_cte.RECORD_ID
        LEFT OUTER JOIN #hlhkr_cte hkr_cte
            ON hkr_cte.RECORD_ID = bev_cte.RECORD_ID
        INNER JOIN CLARITY.dbo.CLARITY_DEP          dept
            ON bevall. REQ_DEPARTMENT_ID = dept.DEPARTMENT_ID
        INNER JOIN #MDM_REV_LOC_ID                   AS mloc
            ON mloc.REV_LOC_ID = dept.REV_LOC_ID
        --INNER JOIN CLARITY.dbo.CLARITY_BED          bed
        --    ON bevall.REQ_BED_ID = bed.BED_ID
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
    --INNER JOIN  MDM_DEP_ID t_mdm ON (t_mdm.EPIC_DEPARTMENT_ID = dept.DEPARTMENT_ID) 
    WHERE
		  --bevall.EVENT_TYPE_C = 0
    --      AND
          (
              bev_cte.Unplanned >= @StartDate
              AND bev_cte.Unplanned <= @EndDate
          )
		  AND bed.bed_seq = 1
    --AND DATENAME(dw,bev_cte.Unplanned)=@DayofWeek						
    --)
    -----location filtering for CH go live 09/2022----
    --AND 
    --(
    --	(UPPER(@de_control)=UPPER(mloc.de_hospital_code))
    --	OR (UPPER(@de_control)=UPPER(mloc.hospital_group))
    --	OR (UPPER(@de_control)='ALL')
    --)

	SELECT
		*
	FROM #allevs_cte
	--ORDER BY
	--	RECORD_ID
	ORDER BY
		TimeDirty, RECORD_ID
--*/
/*
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
  
HL_TASK_SUBTYPE_C	 NAME
1	Patient Transport
2	Bed Clean
3	Maintenance Clean
99	Other
  AND REQ_TASK_SUBTYPE_C IN ('1', '99') -- Patient Transport, Other
  AND hri.REQ_REGION_SEC_ID =  3100000086 -- UVA GRAND CENTRAL UVA HOSPITAL TRANSPORT
  5/14/2024
  AND REQ_TASK_SUBTYPE_C IN ('1', '99') -- Patient Transport, Other
  AND hri.REQ_REGION_SEC_ID IN
  (3100000086, -- General - UVA GRAND CENTRAL UVA HOSPITAL TRANSPORT
   3100000108, -- General - UVA GRAND CENTRAL CULPEPER HOSPITAL TRANSPORT
   3100000113  -- General - UVA GRAND CENTRAL PRINCE WILLIAM MEDICAL CENTER TRANSPORT
   )
   3100000057, 3100000000, 3100000072 are EVS
   
   SELECT S.HL_PARENT_REGION_ID
,S.HL_GENERAL_SECTOR_DISPLAY_NAME

FROM CLARITY.dbo.CL_SEC S
WHERE 1=1
AND S.HL_IS_GENERAL_SECTOR_YN ='Y'
*/
/*
SELECT ClnSts.*,
               cba.REQ_TASK_SUBTYPE_C,
			   cba.REQ_REGION_SEC_ID,
			   sec.HL_GENERAL_SECTOR_DISPLAY_NAME
    FROM
    (
        SELECT HLR_ID AS RECORD_ID
              ,CASE
                   WHEN STATUS_C = 0 -- Unplanned
                   THEN 'Dirty'
                   WHEN STATUS_C = 10 -- Assigned
                   THEN 'Assigned'
                   WHEN STATUS_C = 25 -- In Progress
                   THEN 'InProgress'
                   WHEN STATUS_C = 35 -- Completed
                   THEN 'Completed'
               END       'CleanSts'
              ,EVENT_LOCAL_DTTM AS INSTANT_TM
        FROM CLARITY.dbo.HL_ASGN_INFO_AUDIT
		WHERE CAST(EVENT_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
    ) AS event
    PIVOT
    (
        MAX(INSTANT_TM)
        FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
    ) AS ClnSts
	INNER JOIN CLARITY..HL_REQ_INFO cba
	ON ClnSts.RECORD_ID = cba.HLR_ID
	LEFT OUTER JOIN
	(
	SELECT S.HL_PARENT_REGION_ID
,S.HL_GENERAL_SECTOR_DISPLAY_NAME

FROM CLARITY.dbo.CL_SEC S
WHERE 1=1
AND S.HL_IS_GENERAL_SECTOR_YN ='Y'
) sec
ON sec.HL_PARENT_REGION_ID = cba.REQ_REGION_SEC_ID
	WHERE
	(REQ_STAGE_NUM IS NULL
	OR REQ_STAGE_NUM = '1')
	AND cba.REQ_TASK_SUBTYPE_C = 2 -- Bed Clean
	AND cba.REQ_REGION_SEC_ID IN
  (3100000057, -- General - UVA GRAND CENTRAL Culpeper EVS
   3100000000, -- General - UVA GRAND CENTRAL UVHE EVS
   3100000072  -- General - UVA GRAND CENTRAL Prince William EVS
   )
	ORDER BY
		ClnSts.RECORD_ID
*/
/*
--SELECT haia.HLR_ID AS RECORD_ID,
--		zhrs.NAME AS  'CleanSts'
--              ,EVENT_LOCAL_DTTM AS INSTANT_TM
SELECT DISTINCT
	haia.STATUS_C,
	zhrs.NAME AS  'CleanSts'
        FROM CLARITY.dbo.HL_ASGN_INFO_AUDIT haia
	INNER JOIN CLARITY..HL_REQ_INFO cba
	ON haia.HLR_ID = cba.HLR_ID
	LEFT OUTER JOIN
	(
	SELECT S.HL_PARENT_REGION_ID
,S.HL_GENERAL_SECTOR_DISPLAY_NAME

FROM CLARITY.dbo.CL_SEC S
WHERE 1=1
AND S.HL_IS_GENERAL_SECTOR_YN ='Y'
) sec
ON sec.HL_PARENT_REGION_ID = cba.REQ_REGION_SEC_ID
        LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS zhrs
        ON zhrs.HL_REQ_STATUS_C = haia.STATUS_C
		WHERE CAST(EVENT_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
AND cba.REQ_TASK_SUBTYPE_C = 2 -- Bed Clean
	AND cba.REQ_REGION_SEC_ID IN
  (3100000057,
   3100000000,
   3100000072
   )
		--ORDER BY RECORD_ID, INSTANT_TM
		--ORDER BY zhrs.NAME
		ORDER BY haia.STATUS_C
*/
/*
SELECT ClnSts.*,
               cba.REQ_TASK_SUBTYPE_C,
			   cba.REQ_REGION_SEC_ID,
			   sec.HL_GENERAL_SECTOR_DISPLAY_NAME
    FROM
    (
        SELECT HLR_ID AS RECORD_ID,
		zhrs.NAME AS  'CleanSts'
              ,EVENT_LOCAL_DTTM AS INSTANT_TM
        FROM CLARITY.dbo.HL_ASGN_INFO_AUDIT
        LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS zhrs
        ON zhrs.HL_REQ_STATUS_C = HL_ASGN_INFO_AUDIT.STATUS_C
		WHERE CAST(EVENT_LOCAL_DTTM AS DATE) BETWEEN @startdate AND @enddate
    ) AS event
    PIVOT
    (
        MAX(INSTANT_TM)
        FOR CleanSts IN ([Unplanned], [Planned], [Assigned], [Acknowledged], [At Start Location], [In Progress], [At End Location], [Completed], [Canceled])
    ) AS ClnSts
	INNER JOIN CLARITY..HL_REQ_INFO cba
	ON ClnSts.RECORD_ID = cba.HLR_ID
	LEFT OUTER JOIN
	(
	SELECT S.HL_PARENT_REGION_ID
,S.HL_GENERAL_SECTOR_DISPLAY_NAME

FROM CLARITY.dbo.CL_SEC S
WHERE 1=1
AND S.HL_IS_GENERAL_SECTOR_YN ='Y'
) sec
ON sec.HL_PARENT_REGION_ID = cba.REQ_REGION_SEC_ID
	WHERE
	(REQ_STAGE_NUM IS NULL
	OR REQ_STAGE_NUM = '1')
	AND cba.REQ_TASK_SUBTYPE_C = 2 -- Bed Clean
	AND cba.REQ_REGION_SEC_ID IN
  (3100000057,
   3100000000,
   3100000072
   )
	ORDER BY
		ClnSts.RECORD_ID
*/
GO