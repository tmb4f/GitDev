USE CLARITY

DECLARE @StartDate AS DATE, @EndDate AS DATE
SET @StartDate = '2/1/2024'
SET @EndDate = '3/30/2024'

DECLARE @de_control AS VARCHAR(10), @DeptName AS VARCHAR(50)

SET @de_control = 'UVA-MC'
SET @DeptName = 'UVHE 5 SOUTH'

;
WITH MDM_REV_LOC_ID
AS (SELECT DISTINCT
           t1.REV_LOC_ID
          ,t1.HOSPITAL_CODE
          ,t1.DE_HOSPITAL_CODE
          ,t1.HOSPITAL_GROUP
    FROM [CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group] t1
    WHERE (t1.REV_LOC_ID IS NOT NULL))

    -- Insert statements for procedure here
    --get all Bed clean requests		
    ,bev_cte
AS (SELECT ClnSts.*
    FROM
    (
        SELECT RECORD_ID
              ,CASE
                   WHEN STATUS_C = 1
                   THEN 'Dirty'
                   WHEN STATUS_C = 2
                   THEN 'Assigned'
                   WHEN STATUS_C = 3
                   THEN 'InProgress'
                   WHEN STATUS_C = 4
                   THEN 'OnHold'
                   WHEN STATUS_C = 5
                   THEN 'Completed'
               END       'CleanSts'
              ,INSTANT_TM
        FROM CLARITY.dbo.CL_BEV_EVENTS_ALL
    ) AS event
    PIVOT
    (
        MAX(INSTANT_TM)
        FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
    ) AS ClnSts
	INNER JOIN CLARITY..CL_BEV_ALL cba
	ON ClnSts.RECORD_ID = cba.RECORD_ID
	WHERE 	EVENT_TYPE_C = '0'
	AND (STAGE_NUMBER IS NULL
	OR STAGE_NUMBER = '1'))

    ,hlbev_cte
AS (SELECT ClnSts.*
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
    ) AS event
    PIVOT
    (
        MAX(INSTANT_TM)
        FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
    ) AS ClnSts
	INNER JOIN CLARITY..HL_REQ_INFO cba
	ON ClnSts.RECORD_ID = cba.HLR_ID
	WHERE
	(REQ_STAGE_NUM IS NULL
	OR REQ_STAGE_NUM = '1')
	)

    --GET all bed clean responsible employees
    ,hkr_cte
AS (SELECT *
    FROM
    (
        SELECT RECORD_ID
              ,CASE
                   WHEN STATUS_C = 1
                   THEN 'Dirty'
                   WHEN STATUS_C = 2
                   THEN 'Assigned'
                   WHEN STATUS_C = 3
                   THEN 'InProgress'
                   WHEN STATUS_C = 4
                   THEN 'OnHold'
                   WHEN STATUS_C = 5
                   THEN 'Completed'
               END      'CleanSts'
              ,HKR_ID
        FROM CLARITY.dbo.CL_BEV_EVENTS_ALL
    ) AS emp
    PIVOT
    (
        MAX(HKR_ID)
        FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
    ) AS ClnSts)
	
    ,hlhkr_cte
AS (SELECT *
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
              ,ASSIGNED_TECH_ID AS HKR_ID
        --FROM CLARITY.dbo.CL_BEV_EVENTS_ALL
        FROM CLARITY.dbo.HL_ASGN_INFO_AUDIT
    ) AS emp
    PIVOT
    (
        MAX(HKR_ID)
        FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
    ) AS ClnSts)

    --get all hours of day
    ,HR_CTE
AS (SELECT TOP 24
           NUM - 1 'HR'
    FROM CLARITY.dbo.D_NUMBERS_10000)

    --get all months between star and end time 
    ,Mnth_cte
AS (SELECT DISTINCT
           MONTH_NUMBER
          ,YEAR
          ,MONTH_NAME
    FROM CLARITY.dbo.DATE_DIMENSION
    WHERE CALENDAR_DT >= @StartDate
          AND CALENDAR_DT <= @EndDate)
		  		   
    --get all hours 
    ,tm_cte
AS (SELECT *
    FROM Mnth_cte
        CROSS JOIN HR_CTE)

    --get all bed clean events and responsible employees 
    ,allevs_cte
AS (SELECT bev_cte.RECORD_ID
          ,bev_cte.Dirty                                           'TimeDirty'
          ,bev_cte.Assigned                                        'TimeAssgn'
          ,bev_cte.InProgress                                      'TimeInPgr'
          ,bev_cte.OnHold                                          'TimeOnHld'
          ,bev_cte.Completed                                       'TimeCmplt'
          ,dept.DEPARTMENT_NAME
          ,dept.DEPARTMENT_ID
          ,bevall.BED_ID
          ,mloc.*
          ,sts.NAME                                                'Priority'
          ,esc.NAME                                                'EscReason'
          ,src.NAME                                                'EventSource'
          ,hkr_cte.Dirty                                           'empDirty'
          ,hkr_cte.Assigned                                        'EmpAssigned'
          ,hkr_cte.InProgress                                      'EmpInpgr'
          ,hkr_cte.OnHold                                          'EmpOnHold'
          ,hkr_cte.Completed                                       'EmpComplt'
          ,emp.RECORD_NAME                                         'UserName'
          ,emp.EMP_ID
          ,DATEPART(hh, bev_cte.Dirty)                             'RequestHr'
          ,DATEPART(dw, bev_cte.Dirty)                             'RequestDw'
          ,DATENAME(dw, bev_cte.Dirty)                             'RequestDy'
          ,MONTH(bev_cte.Dirty)                                    'RequestMonth'
          ,DATEDIFF(MINUTE, bev_cte.Dirty, bev_cte.Assigned)       'GapRqstAssng'
          ,DATEDIFF(MINUTE, bev_cte.Assigned, bev_cte.InProgress)  'GapAssInPgr'
          ,DATEDIFF(MINUTE, bev_cte.InProgress, bev_cte.Completed) 'GapInpgrCmplt'
          ,DATEDIFF(MINUTE, bev_cte.Dirty, bev_cte.InProgress)     'GapRqstInpgr'
          ,DATEDIFF(MINUTE, bev_cte.Dirty, bev_cte.Completed)      'TAT'
          ,CASE
               WHEN DATEPART(hh, bev_cte.Dirty) < 7
               THEN '00:00-06:59'
               WHEN DATEPART(hh, bev_cte.Dirty) < 15
               THEN '07:00-14:59'
               ELSE '15:00-23:59'
           END                                                     'HrGrp'
    FROM bev_cte
        INNER JOIN CLARITY.dbo.CL_BEV_ALL           bevall
            ON bevall.RECORD_ID = bev_cte.RECORD_ID
        LEFT OUTER JOIN hkr_cte
            ON hkr_cte.RECORD_ID = bev_cte.RECORD_ID
        INNER JOIN CLARITY.dbo.CLARITY_DEP          dept
            ON bevall.DEP_ID = dept.DEPARTMENT_ID
        INNER JOIN MDM_REV_LOC_ID                   AS mloc
            ON mloc.REV_LOC_ID = dept.REV_LOC_ID
        INNER JOIN CLARITY.dbo.CLARITY_BED          bed
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
              bev_cte.Dirty >= @StartDate
              AND bev_cte.Dirty <= @EndDate
          )
    --AND DATENAME(dw,bev_cte.Dirty)=@DayofWeek						
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
          ,bev_cte.Dirty												    'TimeDirty'
          ,bev_cte.Assigned                                         'TimeAssgn'
          ,bev_cte.InProgress                                      'TimeInPgr'
          ,bev_cte.OnHold                                          'TimeOnHld'
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
          ,hkr_cte.Dirty                                           'empDirty'
          ,hkr_cte.Assigned                                        'EmpAssigned'
          ,hkr_cte.InProgress                                      'EmpInpgr'
          ,hkr_cte.OnHold                                          'EmpOnHold'
          ,hkr_cte.Completed                                       'EmpComplt'
          ,emp.RECORD_NAME                                         'UserName'
          ,emp.EMP_ID
          ,DATEPART(hh, bev_cte.Dirty)                             'RequestHr'
          ,DATEPART(dw, bev_cte.Dirty)                             'RequestDw'
          ,DATENAME(dw, bev_cte.Dirty)                             'RequestDy'
          ,MONTH(bev_cte.Dirty)                                    'RequestMonth'
          ,DATEDIFF(MINUTE, bev_cte.Dirty, bev_cte.Assigned)       'GapRqstAssng'
          ,DATEDIFF(MINUTE, bev_cte.Assigned, bev_cte.InProgress)  'GapAssInPgr'
          ,DATEDIFF(MINUTE, bev_cte.InProgress, bev_cte.Completed) 'GapInpgrCmplt'
          ,DATEDIFF(MINUTE, bev_cte.Dirty, bev_cte.InProgress)     'GapRqstInpgr'
          ,DATEDIFF(MINUTE, bev_cte.Dirty, bev_cte.Completed)      'TAT'
          ,CASE
               WHEN DATEPART(hh, bev_cte.Dirty) < 7
               THEN '00:00-06:59'
               WHEN DATEPART(hh, bev_cte.Dirty) < 15
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
        INNER JOIN MDM_REV_LOC_ID                   AS mloc
            ON mloc.REV_LOC_ID = dept.REV_LOC_ID
        INNER JOIN CLARITY.dbo.CLARITY_BED          bed
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
              bev_cte.Dirty >= @StartDate
              AND bev_cte.Dirty <= @EndDate
          )
    --AND DATENAME(dw,bev_cte.Dirty)=@DayofWeek						
    --)
    -----location filtering for CH go live 09/2022----
    --AND 
    --(
    --	(UPPER(@de_control)=UPPER(mloc.de_hospital_code))
    --	OR (UPPER(@de_control)=UPPER(mloc.hospital_group))
    --	OR (UPPER(@de_control)='ALL')
    --)
    )

--select *
--from
--evs_cte
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
     -- ,'Rptg. UspSrc_Environmental_Services' 'ETL_Guid'
     -- ,GETDATE()                             'Load_Dtm'
FROM tm_cte
    LEFT OUTER JOIN allevs_cte evs_cte
        ON tm_cte.MONTH_NUMBER = evs_cte.RequestMonth
           AND tm_cte.HR = evs_cte.RequestHr
WHERE ( (evs_cte.de_hospital_code IN (SELECT value FROM STRING_SPLIT(@de_control,',')) OR evs_cte.hospital_group IN (SELECT value FROM STRING_SPLIT(@de_control,',')))
AND ( evs_cte.DEPARTMENT_NAME IN (SELECT value FROM STRING_SPLIT(@DeptName,','))) )
--(
 --         (UPPER(@de_control) = UPPER(evs_cte.de_hospital_code))
  --        OR (UPPER(@de_control) = UPPER(evs_cte.hospital_group))
  --        OR (UPPER(@de_control) = 'ALL')
  --    )
	  ----- Added by of7nq on 04/25/2023
--AND (
--(UPPER(@DeptName) = UPPER(evs_cte.DEPARTMENT_NAME))

--OR (UPPER(@DeptName) = 'ALL')

--)

--ORDER BY HOSPITAL_CODE;
ORDER BY HOSPITAL_CODE, evs_cte.DEPARTMENT_NAME, evs_cte.TimeDirty;