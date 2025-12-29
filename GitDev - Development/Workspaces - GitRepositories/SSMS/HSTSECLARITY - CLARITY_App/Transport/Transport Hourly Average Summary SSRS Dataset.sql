USE CLARITY

DECLARE @p_SlotStart AS DATETIME
DECLARE @p_SlotEnd AS DATETIME

;WITH txp_nxt AS 
			(
				SELECT 
					tx.TXPORT_ID
					,tx.HKR_TXPORT_ID
					,tx.EVENT_INSTANT_LOCAL_DTTM 'txprtNxt'
				FROM dbo.TXPORT_EVENTS tx
						INNER JOIN dbo.TXPORT_REQ_INFO inf	ON inf.TRANSPORT_ID=tx.TXPORT_ID
				WHERE tx.ASGN_STATUS_C=2 --assigned
						AND tx.EVENT_INSTANT_LOCAL_DTTM>=@p_SlotStart AND tx.EVENT_INSTANT_LOCAL_DTTM<@p_SlotEnd
						
)


, txphkr AS 
(
SELECT *
FROM 
(
SELECT --TOP 500 
	hkrev.RECORD_ID
	,hkrev.CONTACT_DATE
	,hkr.EMP_ID
	--,emp.SYSTEM_LOGIN 'UserID'
	--,emp.NAME 'EmployeeName'
	,txptyp.NAME 'txpter'
	,hkrev.EVENT_INST
	
FROM CLARITY.dbo.CL_HKR_EVENTS hkrev
INNER JOIN CLARITY.dbo.CL_HKR hkr					ON hkr.RECORD_ID = hkrev.RECORD_ID
INNER JOIN CLARITY.dbo.ZC_EVENT_TYPE_3 txptyp		ON hkrev.EVENT_TYPE_C=txptyp.EVENT_TYPE_3_C
--INNER JOIN CLARITY.dbo.CLARITY_EMP emp		ON hkr.EMP_ID=emp.USER_ID
WHERE 1=1
	AND hkr.EMP_TYPE_C IN ('3','4') --transporter/Transport manager
	AND hkrev.EVENT_INST>=@p_SlotStart AND hkrev.EVENT_INST<@p_SlotEnd
) AS txpter

PIVOT
(
MAX(EVENT_INST)
FOR txpter IN ([Log In],[Log Out],[On Break],[Off Break])
) AS txpprd
)


,tot_Wrk AS 
(
SELECT --txphkr.UserID
		txphkr.EMP_ID
		 ,txphkr.RECORD_ID 'Worker_ID'
		 ,txphkr.CONTACT_DATE
		,SUM(DATEDIFF(HOUR,txphkr.[Log In],txphkr.[Log Out])) 'LogInOut'
		,SUM(DATEDIFF(HOUR,txphkr.[On Break],txphkr.[Off Break])) 'TotBrk'
		,(SUM(DATEDIFF(HOUR,txphkr.[Log In],txphkr.[Log Out]))) -(SUM(DATEDIFF(HOUR,txphkr.[On Break],txphkr.[Off Break]))) 'TotWorkTime'
FROM txphkr
--WHERE 1=1 		AND txphkr.CONTACT_DATE >=@p_SlotStart AND txphkr.CONTACT_DATE<@p_SlotEnd
GROUP BY txphkr.RECORD_ID
	,txphkr.EMP_ID
	,txphkr.CONTACT_DATE
)

,txp_cte AS 
(
	SELECT *
	FROM 
			(
				SELECT 
					tx.TXPORT_ID
					,tx.WORKER_ID
					,inf.TXPORT_DATE
					,CASE WHEN tx.ASGN_STATUS_C=1 --pending 
							THEN 'Pending'
						WHEN tx.ASGN_STATUS_C=2 --assigned
							THEN 'Assigned'
						WHEN tx.ASGN_STATUS_C=3 --Inprogess
							THEN 'InProgress'
						WHEN tx.ASGN_STATUS_C=4 --Delayed
								THEN 'Delayed'
						WHEN tx.ASGN_STATUS_C=5 --Completed
								THEN 'Completed'
						WHEN tx.ASGN_STATUS_C=7 --Future
								THEN 'Future'
						WHEN tx.ASGN_STATUS_C=8 --postponed
								THEN 'Postponed'
						WHEN tx.ASGN_STATUS_C=9  --Acknowledge
								THEN 'Acknowledge'
								END 'txprt'
					,tx.EVENT_INSTANT_LOCAL_DTTM
				FROM dbo.TXPORT_EVENTS tx
					INNER JOIN dbo.TXPORT_REQ_INFO inf	ON inf.TRANSPORT_ID=tx.TXPORT_ID
				
				WHERE inf.CURRENT_STATUS_C='5' --completed
						AND tx.EVENT_INSTANT_LOCAL_DTTM>=@p_SlotStart AND tx.EVENT_INSTANT_LOCAL_DTTM<@p_SlotEnd
			) AS txprt
		PIVOT (
				MAX (EVENT_INSTANT_LOCAL_DTTM)
				FOR txprt IN ([Pending],[Assigned],[InProgree],[Delayed],[Completed],[Future],[Postponed],[Acknowledge])
				) AS txpstatus
)


,txp_fin AS 
(
SELECT txp_cte.TXPORT_ID
		,txp_cte.WORKER_ID
		
		--,txp_cte.TXPORT_DATE
		,dte.day_date  'EventTime'
	--,txp_cte.RNo
		,dte.month_num
		,dte.month_name
		,dte.Fyear_num
		,dte.week_num
		,CASE WHEN info.TXPORT_TYPE_C = '1' --Patient
			THEN 1
			ELSE 0
			END		'Patient_Txp'
		,CASE WHEN info.TXPORT_TYPE_C = '2' --Non-patient
				THEN 1
				ELSE 0
				END		'Non-patient_Txp'
		,CASE WHEN info.TXPORT_TYPE_C = '3' --Cleaning Inspection
				THEN 1
				ELSE 0
				END		'Cleaning Inspection_Txp'
		,CASE WHEN info.TXPORT_TYPE_C = '4' --Non-patient Bundle
				THEN 1
				ELSE 0
				END		'Non-patient Bundle_Txp'
		,txp_cte.Pending
		,txp_cte.Assigned
		,txp_cte.InProgree
		,txp_cte.Completed
		,txp_nxt.txprtNxt
		,DATEDIFF(MINUTE,txp_cte.Assigned,txp_cte.Acknowledge) 'Time_AssignToAcknldg'
		,DATEDIFF(MINUTE,txp_cte.Acknowledge,txp_cte.Completed) 'Time_AcknldgToCmplt'
		,DATEDIFF(MINUTE,txp_cte.Completed,txp_nxt.txprtNxt) 'Time_CmpltToNext'
		,ROW_NUMBER()OVER (PARTITION BY txp_cte.TXPORT_ID ORDER BY DATEDIFF(MINUTE,txp_cte.Completed,txp_nxt.txprtNxt)) 'RNo'
FROM txp_cte
INNER JOIN txp_nxt			ON txp_nxt.HKR_TXPORT_ID=txp_cte.WORKER_ID
									AND txp_cte.Completed<=txp_nxt.txprtNxt
INNER JOIN CLARITY.dbo.TXPORT_REQ_INFO info						ON info.TRANSPORT_ID=txp_cte.TXPORT_ID
INNER JOIN CLARITY_App.dbo.Dim_Date dte							ON CAST(txp_cte.Completed AS DATE)=CAST(dte.day_date AS DATE)

WHERE info.CURRENT_STATUS_C='5' --completed


)

SELECT 
		txp.WORKER_ID
		,MAX(emp.SYSTEM_LOGIN) 'UserID'
		,MAX(emp.NAME) 'EmployeeName'
		,txp.Fyear_num
		,txp.month_name
		,txp.week_num
		,txp.EventTime
		,SUM(txp.Patient_Txp) 'Patient_Txp'
		,SUM(txp.[Non-patient_Txp]) 'Non-patient_Txp'
		,SUM(txp.[Non-patient Bundle_Txp]) 'Non-patient Bundle_Txp'
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
	INNER JOIN tot_Wrk				ON tot_Wrk.Worker_ID=txp.WORKER_ID
										AND CAST(tot_Wrk.CONTACT_DATE AS DATE)=CAST(txp.EventTime AS DATE)
	INNER JOIN CLARITY.dbo.CLARITY_EMP emp		ON tot_Wrk.EMP_ID=emp.USER_ID
WHERE 1=1
--AND txp_fin.TXPORT_ID='413271'

GROUP BY txp.WORKER_ID
		,txp.Fyear_num
		,txp.month_name
		,txp.week_num
		,txp.EventTime