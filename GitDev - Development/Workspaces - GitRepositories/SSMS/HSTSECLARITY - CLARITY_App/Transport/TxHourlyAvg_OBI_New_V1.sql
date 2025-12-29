USE CLARITY_App;


/*
Update the existing TxHourlyAvg_OBI report with the new tables for Hospital Logistics.
Link to existing SSRS report: https://hstsbissrs.hscs.virginia.edu/reports/report/Clarity/ADT%20Events/Transport/TxpHourlyAvg_OBI  
*/


DECLARE @p_SlotStart AS DATE = '2025-03-31'
DECLARE @p_SlotEnd AS DATE = '2025-04-06'


;WITH 
txp_nxt AS 
	(
	SELECT
		 hrsa.HLR_ID                   AS [transport_id]
		,hai.ASGN_TECH_ID
		,hri.REQ_START_UTC_DTTM        AS [txprtNxt] -- Start time
		,hrsa.STATUS_C                 AS [status]
	FROM CLARITY..HL_REQ_STATUS_AUDIT  AS hrsa 
		LEFT JOIN CLARITY..HL_REQ_INFO AS hri ON hrsa.HLR_ID = hri.HLR_ID
		LEFT JOIN CLARITY..HL_ASGN_INFO AS hai ON hrsa.HLR_ID = hai.HLR_ID
	WHERE 1=1
		--AND hrsa.HLR_ID = 1879395 -- Testing
		AND hri.REQ_TASK_SUBTYPE_C IN (1,99) --Patient Transport, Other
		AND hrsa.STATUS_C = 10 -- Assigned
		AND hri.REQ_START_UTC_DTTM BETWEEN @p_SlotStart AND @p_SlotEnd
	)

/* 
-- Testing 
SELECT * FROM txp_nxt
*/

,txphkr AS
	(
	SELECT *
	FROM
		(
		SELECT 
			 hse.SHIFT_ID
			,CAST(hse.SHIFT_EVENT_INST_UTC_DTTM AS DATE) AS [contact_date]
			,hse.SHIFT_EVENT_INST_UTC_DTTM
			,hkr.HL_USER_ID                    -- EMP .1 records
			,et.NAME AS [txpter]               -- Event type: log in, log out, break, etc.
			,hkr.EMP_TYPE_C 
			,hkr.RECORD_ID

		FROM CLARITY..HL_SHIFT_EVENTS             AS hse
		LEFT JOIN CLARITY..CL_HKR                 AS hkr               ON hse.ENTRY_TECHNICIAN_ID = hkr.RECORD_ID
		LEFT JOIN CLARITY..ZC_HL_SHIFT_EVENT_TYPE AS et                ON hse.SHIFT_EVENT_TYPE_C = et.HL_SHIFT_EVENT_TYPE_C

		WHERE 1=1
		AND hkr.EMP_TYPE_C IN (3, 4, 10) -- Transporter, Transport Manager, Logistics Technician
		AND hse.SHIFT_EVENT_INST_UTC_DTTM >= @p_SlotStart AND hse.SHIFT_EVENT_INST_UTC_DTTM < @p_SlotEnd

 
		-- Don't really need this part. Just using for testing.
		GROUP BY
			 hkr.HL_USER_ID 
			,hse.SHIFT_ID
			,hse.SHIFT_EVENT_INST_UTC_DTTM
			,et.NAME
			,hkr.EMP_TYPE_C 
			,hkr.RECORD_ID

		ORDER BY
			 hse.SHIFT_ID
			,hkr.HL_USER_ID
			,hse.SHIFT_EVENT_INST_UTC_DTTM
			 OFFSET 0 ROWS
		) AS txpter

		PIVOT
		(
		MAX(SHIFT_EVENT_INST_UTC_DTTM)
		FOR txpter IN ([Sign In], [Sign Out], [Break Start], [Break End])
		) AS txpprd
	)


-- Testing
--SELECT * FROM txphkr WHERE SHIFT_ID = 9881


,tot_Wrk AS 
	(
	SELECT --txphkr.UserID
		 txphkr.HL_USER_ID
		,txphkr.RECORD_ID 'Worker_ID'
		,txphkr.contact_date
		,SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Sign In],txphkr.[Sign Out]), 0)) 'LogInOut'
		,SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Break Start],txphkr.[Break End]), 0)) 'TotBrk'
		,(SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Sign In],txphkr.[Sign Out]) ,0))) -(SUM(ISNULL(DATEDIFF(HOUR,txphkr.[Break Start],txphkr.[Break End]), 0))) 'TotWorkTime'
		,txphkr.SHIFT_ID
	FROM txphkr
	--WHERE 1=1 		AND txphkr.CONTACT_DATE >=@p_SlotStart AND txphkr.CONTACT_DATE<@p_SlotEnd
	GROUP BY 
		 txphkr.RECORD_ID
		,txphkr.HL_USER_ID
		,txphkr.contact_date
		,txphkr.SHIFT_ID
	)


-- Testing
--SELECT * FROM tot_Wrk WHERE SHIFT_ID = 9881


,txp_cte AS 
	(
	SELECT *
	FROM 
			(  -- Couldn't find pending, delayed, future, or postponed
				SELECT
					 hrsa.HLR_ID
					,hai.ASGN_TECH_ID     -- HKR .1 records
					,CAST(hri.REQ_START_UTC_DTTM AS DATE) AS [TXPORT_DATE]
					,hri.REQ_START_UTC_DTTM
					,CASE WHEN hrsa.STATUS_C = 10 -- Assigned
						   THEN 'Assigned'
					      WHEN hrsa.STATUS_C = 15 -- Acknowledged
						   THEN 'Acknowledged'
						  WHEN hrsa.STATUS_C = 25 -- In Progress
						   THEN 'In_Progress'
						  WHEN hrsa.STATUS_C = 35  -- Completed
						   THEN 'Completed'
					 END 'txprt'
					,hrsa.EVENT_LOCAL_DTTM
				FROM CLARITY..HL_REQ_STATUS_AUDIT AS hrsa
					INNER JOIN CLARITY..HL_REQ_INFO AS hri ON hrsa.HLR_ID = hri.HLR_ID
					INNER JOIN CLARITY..HL_ASGN_INFO AS hai ON hrsa.HLR_ID = hai.HLR_ID
				
				WHERE 1=1
					AND hri.REQ_STATUS_C = 35 -- Completed
					AND hri.REQ_TASK_SUBTYPE_C IN (1, 99) -- Patient Transport, Other
					AND hri.REQ_START_UTC_DTTM >= @p_SlotStart AND hri.REQ_START_UTC_DTTM < @p_SlotEnd
			) AS txprt
				PIVOT (
						MAX (EVENT_LOCAL_DTTM)
						FOR txprt IN ([Assigned],[Acknowledged], [InProgress], [Completed])
						) AS txpstatus
	)


,txp_fin AS 
	(
	SELECT 
		 txp_cte.HLR_ID
		,txp_cte.ASGN_TECH_ID
		,txp_cte.TXPORT_DATE
		,dte.day_date  'EventTime'
	--,txp_cte.RNo
		,dte.month_num
		,dte.month_name
		,dte.Fyear_num
		,dte.week_num
		,CASE WHEN hri.REQ_TASK_SUBTYPE_C = 1 -- Patient
			THEN 1
			ELSE 0
			END		'Patient_Txp'
		,CASE WHEN hri.HL_FUNC_TYPE_C = 1  --Non-patient transport
				THEN 1
				ELSE 0
				END		'Non_Patient_Txp'
		,CASE WHEN hri.REQ_TASK_SUBTYPE_C = 3 -- Maintenance Clean
				THEN 1
				ELSE 0
				END		'Maintenance_Clean'
		,CASE WHEN hri.REQ_TASK_SUBTYPE_C = 99 -- Other
				THEN 1
				ELSE 0
				END		'Other'
		,txp_cte.Assigned
		,txp_cte.InProgress
		,txp_cte.Completed
		,txp_nxt.txprtNxt
		,DATEDIFF(MINUTE,txp_cte.Assigned,txp_cte.Acknowledged) 'Time_AssignToAcknldg'
		,DATEDIFF(MINUTE,txp_cte.Acknowledged,txp_cte.Completed) 'Time_AcknldgToCmplt'
		,DATEDIFF(MINUTE,txp_cte.Completed,txp_nxt.txprtNxt) 'Time_CmpltToNext'
		,ROW_NUMBER()OVER (PARTITION BY txp_cte.HLR_ID ORDER BY DATEDIFF(MINUTE,txp_cte.Completed,txp_nxt.txprtNxt)) 'RNo'

FROM txp_cte
INNER JOIN txp_nxt                           ON txp_nxt.ASGN_TECH_ID = txp_cte.ASGN_TECH_ID AND txp_cte.Completed <= txp_nxt.txprtNxt
INNER JOIN CLARITY..HL_REQ_INFO	    AS hri   ON hri.HLR_ID=txp_cte.HLR_ID
INNER JOIN CLARITY_App.dbo.Dim_Date AS dte   ON CAST(txp_cte.Completed AS DATE)=CAST(dte.day_date AS DATE)

WHERE hri.REQ_STATUS_C= 35 -- Completed


)

SELECT 
		txp.ASGN_TECH_ID
		,MAX(emp.SYSTEM_LOGIN) 'UserID'
		,MAX(emp.NAME) 'EmployeeName'
		,txp.Fyear_num
		,txp.month_name
		,txp.week_num
		,txp.EventTime
		,SUM(txp.Patient_Txp) 'Patient_Txp'
		,SUM(txp.Non_Patient_Txp) 'Non_Patient_Txp'
		,SUM(txp.Other) 'Non_Patient_Other'
		,AVG(txp.Time_AssignToAcknldg) 'Time_AssignToAcknldg'
		,AVG(txp.Time_AcknldgToCmplt) 'Time_AcknldgToCmplt'
		,AVG(txp.Time_CmpltToNext) 'Time_CmpltToNext'
		,MAX(tot_Wrk.TotWorkTime) 'TotalWorkTime'
	--	,*
FROM (SELECT *
		FROM txp_fin
		WHERE 1=1
			AND txp_fin.RNo='1'
	) txp
	INNER JOIN tot_Wrk				ON tot_Wrk.Worker_ID=txp.ASGN_TECH_ID
										AND CAST(tot_Wrk.CONTACT_DATE AS DATE)=CAST(txp.EventTime AS DATE)
	INNER JOIN CLARITY.dbo.CLARITY_EMP emp		ON tot_Wrk.HL_USER_ID=emp.USER_ID
WHERE 1=1
--AND txp_fin.TXPORT_ID='413271'
AND txp.ASGN_TECH_ID = 1646

GROUP BY txp.ASGN_TECH_ID
		,txp.Fyear_num
		,txp.month_name
		,txp.week_num
		,txp.EventTime
ORDER BY txp.ASGN_TECH_ID
		,txp.Fyear_num
		,txp.month_name
		,txp.week_num
		,txp.EventTime