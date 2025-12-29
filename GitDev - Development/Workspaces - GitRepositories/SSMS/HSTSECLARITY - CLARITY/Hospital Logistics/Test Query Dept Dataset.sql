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
               END       'CleanSts'
              ,INSTANT_TM
        FROM CLARITY.dbo.CL_BEV_EVENTS_ALL
    ) AS event
    PIVOT
    (
        MAX(INSTANT_TM)
        FOR CleanSts IN ([Dirty], [Assigned], [InProgress], [OnHold], [Completed])
    ) AS ClnSts)

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
    WHERE bevall.EVENT_TYPE_C = 0
          AND
          (
              bev_cte.Dirty >= @StartDate
              AND bev_cte.Dirty <= @EndDate
          )

	UNION ALL	

       SELECT bev_cte.RECORD_ID
          ,bev_cte.Dirty												    'TimeDirty'
          ,bev_cte.Assigned                                         'TimeAssgn'
          ,bev_cte.InProgress                                      'TimeInPgr'
          ,bev_cte.OnHold                                          'TimeOnHld'
          ,bev_cte.Completed                                      'TimeCmplt'
          ,dept.DEPARTMENT_NAME
          ,dept.DEPARTMENT_ID
          ,bevall.REQ_BED_ID
          ,mloc.*
          ,sts.NAME                                                'Priority'
          ,zesc.NAME                                                'EscReason'
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
          (
              bev_cte.Dirty >= @StartDate
              AND bev_cte.Dirty <= @EndDate
          )
    AND 
    ( UPPER(mloc.de_hospital_code) IN ( SELECT UPPER(value) FROM string_split(@de_control,',') ) OR UPPER(mloc.hospital_group) IN ( SELECT UPPER(value) FROM string_split(@de_control,',') ) )
    )

--connect all
SELECT DISTINCT
       DEPARTMENT_NAME
	,HOSPITAL_CODE
FROM tm_cte
    LEFT OUTER JOIN allevs_cte evs_cte
        ON tm_cte.MONTH_NUMBER = evs_cte.RequestMonth
           AND tm_cte.HR = evs_cte.RequestHr
ORDER BY HOSPITAL_CODE