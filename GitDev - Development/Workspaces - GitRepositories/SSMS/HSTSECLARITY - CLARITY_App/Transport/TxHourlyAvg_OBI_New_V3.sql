USE CLARITY_App;

DECLARE @startdate datetime = '3/31/2025';
DECLARE @enddate datetime = '04/09/2025';

IF OBJECT_ID('tempdb..#HL_ASGN_INFO_AUDIT ') IS NOT NULL
DROP TABLE #HL_ASGN_INFO_AUDIT

IF OBJECT_ID('tempdb..#HL_REQ_STATUS_MOD_AUDIT ') IS NOT NULL
DROP TABLE #HL_REQ_STATUS_MOD_AUDIT

IF OBJECT_ID('tempdb..#transport ') IS NOT NULL
DROP TABLE #transport
  
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
  
  FROM [CLARITY].[dbo].[HL_ASGN_INFO_AUDIT]             AS haia
  INNER JOIN CLARITY.dbo.HL_REQ_INFO                    AS hri               ON haia.HLR_ID = hri.HLR_ID
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_CANCEL_RSN      AS zhrcr             ON zhrcr.HL_REQ_CANCEL_RSN_C = haia.CANCEL_RSN_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS          AS zhrs              ON zhrs.HL_REQ_STATUS_C = haia.STATUS_C
  
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
  
  INTO  #HL_REQ_STATUS_MOD_AUDIT
  FROM [CLARITY].[dbo].[HL_REQ_STATUS_MOD_AUDIT] AS hrsma
  
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
  ON hrsma.HLR_ID = haia.HLR_ID AND hrsma.STATUS_LINE_NUM = haia.LINE
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_STATUS_MOD              AS zhrsm ON zhrsm.HL_REQ_STATUS_MOD_C = hrsma.STATUS_MODIFIER_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_HOLD_TYPE               AS zhht  ON zhht.HL_REQ_HOLD_TYPE_C = hrsma.HOLD_TYPE_C
  LEFT OUTER JOIN CLARITY.dbo.ZC_HL_REQ_POSTPONE_RSN            AS zhrpr ON zhrpr.HL_REQ_POSTPONE_RSN_C = hrsma.POSTPONE_RSN_C

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
	hlr.REQ_ACTIVATION_LOCAL_DTTM
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
	    hrsma.REQ_TASK_SUBTYPE_C,
	    hrsma.REQ_ACTIVATION_LOCAL_DTTM
    
    FROM #HL_REQ_STATUS_MOD_AUDIT AS hrsma
    ) hlr
-- Testing with this ID
 --  WHERE hlr.ASSIGNED_TECH_ID = 1646

--select * from #transport where ASSIGNED_TECH_ID = 1646


;WITH 
txp_nxt AS 
	(
	SELECT
		 hrsa.HLR_ID                   AS [transport_id]
		,hai.ASGN_TECH_ID
		--,hri.REQ_START_UTC_DTTM        AS [txprtNxt] -- Start time
		,CLARITY.EPIC_UTIL.EFN_UTC_TO_LOCAL(hri.REQ_START_UTC_DTTM)        AS [txprtNxt] -- Start time												20250522
		
		,hrsa.STATUS_C                 AS [status]
	FROM CLARITY..HL_REQ_STATUS_AUDIT  AS hrsa 
		LEFT JOIN CLARITY..HL_REQ_INFO AS hri ON hrsa.HLR_ID = hri.HLR_ID
		LEFT JOIN CLARITY..HL_ASGN_INFO AS hai ON hrsa.HLR_ID = hai.HLR_ID
	WHERE 1=1
		--AND hrsa.HLR_ID = 1879395 -- Testing
		AND hri.REQ_TASK_SUBTYPE_C IN (1,99) --Patient Transport, Other
		AND hrsa.STATUS_C = 10 -- Assigned
		--AND hri.REQ_START_UTC_DTTM BETWEEN @startdate AND @enddate
		AND CLARITY.EPIC_UTIL.EFN_UTC_TO_LOCAL(hri.REQ_START_UTC_DTTM) BETWEEN @startdate AND @enddate --					20250522
	)

-- Get shift information. SHIFT_ID is removed as it was causing duplicate rows in the final result set.
,txphkr AS
	(
	SELECT *
	FROM
		(
		SELECT 
		     --hse.SHIFT_ID AS SHIFT_ID
			 --CAST(hse.SHIFT_EVENT_INST_UTC_DTTM AS DATE) AS [contact_date]
			 CAST(CLARITY.EPIC_UTIL.EFN_UTC_TO_LOCAL(hse.SHIFT_EVENT_INST_UTC_DTTM) AS DATE) AS [contact_date] --				20250522
			--,hse.SHIFT_EVENT_INST_UTC_DTTM
			,CLARITY.EPIC_UTIL.EFN_UTC_TO_LOCAL(hse.SHIFT_EVENT_INST_UTC_DTTM) AS SHIFT_EVENT_INST_LOCAL_DTTM --	20250522
			,hkr.HL_USER_ID                    -- EMP .1 records
			,et.NAME AS [txpter]               -- Event type: log in, log out, break, etc.
			,hkr.EMP_TYPE_C 
			,hkr.RECORD_ID

		FROM CLARITY..HL_SHIFT_EVENTS             AS hse
		LEFT JOIN CLARITY..CL_HKR                 AS hkr               ON hse.SHIFT_EVENT_TECHNICIAN_ID = hkr.RECORD_ID
		LEFT JOIN CLARITY..ZC_HL_SHIFT_EVENT_TYPE AS et                ON hse.SHIFT_EVENT_TYPE_C = et.HL_SHIFT_EVENT_TYPE_C

		WHERE 1=1
		AND hkr.EMP_TYPE_C IN (3, 4, 10) -- Transporter (3), Transport Manager(4), Logistics Technician(10)
		) AS txpter

		PIVOT
		(
		--MAX(SHIFT_EVENT_INST_UTC_DTTM)
		MAX(SHIFT_EVENT_INST_LOCAL_DTTM) --																																				20250522
		FOR txpter IN ([Sign In], [Sign Out], [Break Start], [Break End])
		) AS txpprd
	)

--select * from txphkr where HL_USER_ID =46297 
/*
-- Testing
SELECT * FROM txphkr WHERE SHIFT_ID = 9881
*/

,tot_Wrk AS 
	(
	SELECT
		 txphkr.HL_USER_ID
		,txphkr.RECORD_ID 'Worker_ID'
		,txphkr.contact_date
		,SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Sign In],txphkr.[Sign Out]), 0)) 'LogInOut'
		,SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Break Start],txphkr.[Break End]), 0)) 'TotBrk'
		,(SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Sign In],txphkr.[Sign Out]) ,0))) -(SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Break Start],txphkr.[Break End]), 0))) 'TotWorkTime'

	FROM txphkr
	WHERE 1=1 		AND txphkr.contact_date BETWEEN @startdate AND @enddate
	GROUP BY 
		 txphkr.RECORD_ID
		,txphkr.HL_USER_ID
		,txphkr.contact_date
	)

,txp_cte AS (
                SELECT *
                FROM (
                      SELECT
                          t.HLR_ID
                         ,t.ASSIGNED_TECH_ID
                         ,t.EVENT_LOCAL_DTTM
                         ,t.STATUS_NAME
                         ,t.REQ_TASK_SUBTYPE_C
                         --,t.STATUS_C																																												20250522
                      FROM #transport AS t
                      WHERE 1 = 1
                     -- AND t.STATUS_C = 35 -- Completed
                      AND t.REQ_TASK_SUBTYPE_C IN (1, 99) -- Patient Transport, Other
                      AND t.REQ_ACTIVATION_LOCAL_DTTM BETWEEN @startdate AND @enddate
                        ) AS txprt
                             PIVOT (
                                    MAX (EVENT_LOCAL_DTTM)
                                    FOR STATUS_NAME IN ([Delay], [Hold], [Planned], [Assigned], [Acknowledged], [In_Progress], [Completed], [Canceled])
                                    ) AS txpstatus 
                            )
--select * from txp_cte where ASSIGNED_TECH_ID = 1646

,txp_fin AS (
                SELECT
                     txp_cte.HLR_ID
                    ,txp_cte.ASSIGNED_TECH_ID
                  --  ,txp_cte.EVENT_LOCAL_DTTM
                    ,dte.day_date AS [EventTime]
                    ,dte.month_num
                    ,dte.month_name
                    ,dte.Fyear_num
                    ,dte.week_num
                    ,CASE WHEN txp_cte.REQ_TASK_SUBTYPE_C = 1 -- Patient Transport
                        THEN 1
                        ELSE 0
                        END [Patient_Txp]
                    ,CASE WHEN txp_cte.REQ_TASK_SUBTYPE_C = 2 -- Bed Clean
                        THEN 1
                        ELSE 0
                        END [Bed_Clean]
                    ,CASE WHEN txp_cte.REQ_TASK_SUBTYPE_C = 3 -- Maintenance Clean
                        THEN 1
                        ELSE 0
                        END [Maintenance_Clean]
                    ,CASE WHEN txp_cte.REQ_TASK_SUBTYPE_C = 99 -- Other
                        THEN 1
                        ELSE 0
                        END [Other]
                    ,txp_cte.Delay
                    ,txp_cte.Hold
                    ,txp_cte.Planned
                    ,txp_cte.Assigned
                    ,txp_cte.Acknowledged
                    ,txp_cte.In_Progress
                    ,txp_cte.Completed
                    ,txp_cte.Canceled
                    ,DATEDIFF(MINUTE, txp_cte.Assigned, txp_cte.Acknowledged)  AS [Time_AssignToAcknldg]
                    ,DATEDIFF(MINUTE, txp_cte.Acknowledged, txp_cte.Completed) AS [Time_AcknldgToCmplt]
                    ,DATEDIFF(MINUTE, txp_cte.Completed, txp_nxt.txprtNxt)     AS [Time_CmpltToNext]
                    ,ROW_NUMBER()OVER (PARTITION BY txp_cte.HLR_ID ORDER BY DATEDIFF(MINUTE, txp_cte.completed, txp_nxt.txprtNxt)) AS [RNo]
                    
                FROM txp_cte
                INNER JOIN txp_nxt                           ON txp_nxt.ASGN_TECH_ID = txp_cte.ASSIGNED_TECH_ID AND txp_cte.Completed <= txp_nxt.txprtNxt
               -- INNER JOIN CLARITY..HL_REQ_INFO    AS hri    ON txp_cte.HLR_ID = hri.HLR_ID
                INNER JOIN CLARITY_App..Dim_Date   AS dte    ON CAST(txp_cte.Completed AS DATE) = CAST(dte.day_date AS DATE)
                WHERE 1 = 1
                --AND txp_cte.STATUS_C = 35 -- Completed
                AND txp_cte.Completed IS NOT NULL  -- 																																				20250522
               -- AND hri.REQ_STATUS_C = 35 -- Completed
            )
/*
-- Testing 
select * 
from txp_fin 
LEFT JOIN tot_Wrk                      ON txp_fin.ASSIGNED_TECH_ID = tot_Wrk.Worker_ID 
                                            AND CAST(tot_Wrk.contact_date AS DATE) = CAST(txp_fin.EventTime AS DATE)
LEFT JOIN CLARITY..CLARITY_EMP AS emp  ON tot_Wrk.HL_USER_ID = emp.USER_ID
where ASSIGNED_TECH_ID = 1646

SELECT DISTINCT
     txp.HLR_ID
    ,txp.ASSIGNED_TECH_ID
    ,emp.SYSTEM_LOGIN AS [UserID]
    ,emp.NAME         AS [EmployeeName]
    ,txp.Fyear_num
    ,txp.month_name
	,txp.week_num
	,txp.EventTime
    ,txp.Patient_Txp AS [Patient_Txp]
    ,txp.Bed_Clean   AS [Bed_Clean]
    ,txp.Maintenance_Clean AS [Maintenance_Clean]
    ,txp.Other            AS [Other]
    ,txp.Time_AssignToAcknldg AS [Time_AssignToAcknldg]
    ,txp.Time_AcknldgToCmplt AS [Time_AcknldgToCmplt]
    ,txp.Time_CmpltToNext     AS [Time_CmpltToNext]
    ,tot_Wrk.TotWorkTime      AS [TotalWorkTime]

FROM (
        SELECT *
        FROM txp_fin
        WHERE 1 = 1
        AND txp_fin.RNo = 1
    ) txp

LEFT JOIN tot_Wrk                      ON txp.ASSIGNED_TECH_ID = tot_Wrk.Worker_ID 
                                            AND CAST(tot_Wrk.contact_date AS DATE) = CAST(txp.EventTime AS DATE)
LEFT JOIN CLARITY..CLARITY_EMP AS emp  ON tot_Wrk.HL_USER_ID = emp.USER_ID

WHERE 1 = 1
AND txp.ASSIGNED_TECH_ID = 1646
*/

SELECT
     txp.ASSIGNED_TECH_ID
    ,MAX(emp.SYSTEM_LOGIN) AS [UserID]
    ,MAX(emp.NAME)         AS [EmployeeName]
    ,txp.Fyear_num
    ,txp.month_name
	,txp.week_num
	,txp.EventTime
    ,SUM(txp.Patient_Txp) AS [Patient_Txp]
    ,SUM(txp.Bed_Clean)   AS [Bed_Clean]
    ,SUM(txp.Maintenance_Clean) AS [Maintenance_Clean]
    ,SUM(txp.Other)             AS [Other]
    ,AVG(txp.Time_AssignToAcknldg) AS [Time_AssignToAcknldg]
    ,AVG(txp.Time_AcknldgToCmplt)  AS [Time_AcknldgToCmplt]
    ,AVG(txp.Time_CmpltToNext)     AS [Time_CmpltToNext]
    ,MAX(tot_Wrk.TotWorkTime)      AS [TotalWorkTime]

FROM (
        SELECT *
        FROM txp_fin
        WHERE 1 = 1
        AND txp_fin.RNo = 1
    ) txp

LEFT JOIN tot_Wrk                      ON txp.ASSIGNED_TECH_ID = tot_Wrk.Worker_ID 
                                           AND CAST(tot_Wrk.contact_date AS DATE) = CAST(txp.EventTime AS DATE)
LEFT JOIN CLARITY..CLARITY_EMP AS emp  ON tot_Wrk.HL_USER_ID = emp.USER_ID

WHERE 1 = 1
AND txp.ASSIGNED_TECH_ID = 1646

GROUP BY
     txp.ASSIGNED_TECH_ID
    ,txp.Fyear_num
    ,txp.month_name
    ,txp.week_num
    ,txp.EventTime

ORDER BY
     txp.EventTime
