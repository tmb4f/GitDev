USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [Rptg].[uspSrc_Transport_Summary]
--(
--    @startdate AS VARCHAR(19) = NULL
--   ,@enddate AS VARCHAR(19) = NULL
--   ,@de_control VARCHAR(255) = 'UVA-MC'
--)
--AS
/**********************************************************************************************************************
WHAT: Transport statistics for patient and non-patient transport
WHO : Kat Mayfield
WHEN: 04/04/2023
WHY : SSRS report 'TXPORT_SUMMARY'
-----------------------------------------------------------------------------------------------------------------------
INFO: 
      INPUTS:	CLARITY.dbo.TXPORT_EVENTS
				CLARITY.dbo.TXPORT_REQ_INFO
      OUTPUTS:  

------------------------------------------------------------------------------------------------------------------------
MODS: 	
************************************************************************************************************************/

SET NOCOUNT ON;

DECLARE @de_control VARCHAR(255)  = 'UVA-MC';
--DECLARE @startdate DATETIME = '7/01/2023';
--DECLARE @enddate DATETIME = '07/31/2023';
DECLARE @startdate DATETIME = '2/01/2024';
DECLARE @enddate DATETIME = '02/29/2024';

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
--SELECT txp. *,tsk.LOC_ID
SELECT txp.TXPORT_ID, txp.EVENT_INSTANT_LOCAL_DTTM,tsk.LOC_ID
FROM CLARITY.dbo.TXPORT_EVENTS txp
INNER JOIN CLARITY.dbo.TXPORT_REQ_INFO tsk				ON tsk.TRANSPORT_ID=txp.TXPORT_ID
WHERE txp.EVENT_INSTANT_LOCAL_DTTM>=@startdate
		AND txp.EVENT_INSTANT_LOCAL_DTTM<=@EndDate
		AND txp.ASGN_STATUS_C IN ('5','6')  --Completed/cancelled cases
		AND COALESCE(tsk.TXPORT_TASK_ID,'11111') NOT IN ('100053', '100191')--EQP Txport, Depot Restock 
)

,txp_pnd AS 
(
--SELECT *
SELECT txp.TXPORT_ID, txp.WORKER_ID, txp.EVENT_INSTANT_LOCAL_DTTM
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE  txp.ASGN_STATUS_C='1' --Pending
		AND txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MIN(EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
		AND txp.LINE=(SELECT MIN(txpmx.LINE) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='1')									AND txpmx.ASGN_STATUS_C='1')
)	

,txp_asgn AS 
(
--SELECT DISTINCT *
SELECT txp.TXPORT_ID, txp.WORKER_ID, txp.EVENT_INSTANT_LOCAL_DTTM
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE  txp.ASGN_STATUS_C='2' --Assigned
		AND txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MAX (EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='2')
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='2')
)

,txp_inpgr AS 
(
--SELECT  *
SELECT txp.TXPORT_ID, txp.WORKER_ID, txp.EVENT_INSTANT_LOCAL_DTTM
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE txp.ASGN_STATUS_C='3' --Inprogress
		AND txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MAX (EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='3')
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='3')
)

,txp_dlyd AS 
(
--SELECT *
SELECT txp.TXPORT_ID, txp.EVENT_INSTANT_LOCAL_DTTM
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE 	txp.ASGN_STATUS_C='4' --delayed
		AND txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MIN (EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='4')
)

,txp_cmplt AS 
(
--SELECT  *
SELECT txp.TXPORT_ID, txp.WORKER_ID, txp.EVENT_INSTANT_LOCAL_DTTM
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE 	txp.ASGN_STATUS_C='5' --Completed
		AND txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MAX (EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='5')
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='5')
)

,txp_cncl AS 
(
--SELECT  *
SELECT txp.TXPORT_ID
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE txp.ASGN_STATUS_C='6' --Cancelled
		AND txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MAX (EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='6')
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='6')	
)

,txp_pstpn AS 
(
--SELECT  *
SELECT txp.TXPORT_ID
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MAX (EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='8')
		AND txp.LINE=(SELECT MAX(txpmx.LINE) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='8')
		AND txp.ASGN_STATUS_C='8' --Postponed
)

,txp_acknw AS 
(
--SELECT  *
SELECT txp.TXPORT_ID, txp.EVENT_INSTANT_LOCAL_DTTM
FROM CLARITY.dbo.TXPORT_EVENTS txp
WHERE txp.EVENT_INSTANT_LOCAL_DTTM =(SELECT MIN (EVENT_INSTANT_LOCAL_DTTM) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='9')
		AND txp.LINE=(SELECT MIN(txpmx.LINE) 
											FROM CLARITY.dbo.TXPORT_EVENTS txpmx
											WHERE txp.TXPORT_ID=txpmx.TXPORT_ID
											AND txpmx.ASGN_STATUS_C='9')
		AND txp.ASGN_STATUS_C='9' --Acknowledge
)

,txp AS 
(
SELECT DISTINCT
		txp_src.TXPORT_ID, txp_src.LOC_ID
		,txprqst.TXPORT_TYPE_C --1: Patient; 2: Non-Patient 3:Cleaning Inspection 4: Non-patient Bundle

		--,txprqst.TXPORT_PAT_CSN
		--,txprqst.PAT_ID
		--,txprqst.TXPORT_DATE

		,CASE WHEN txprqst.TXPORT_TYPE_C='1'
				THEN	1
				ELSE	0
				END																												'Pt_Txp_Rqst'
		,CASE WHEN txprqst.TXPORT_TYPE_C IN ('2', '3', '4')
				THEN	1
				ELSE	0
				END																												'Non-Pt_Txp_Rqst'
		,CASE WHEN txp_cncl.TXPORT_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'cancelled_event'
		,CASE WHEN txp_cmplt.TXPORT_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'Cmplted_Event'
		,CASE WHEN txp_pstpn.TXPORT_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'pstpn_Event'
		,CASE WHEN txp_dlyd.TXPORT_ID IS NOT NULL
				THEN 1
				ELSE 0
				END																												'delayed_Event'
		,CASE WHEN (txprqst.TXPORT_TYPE_C='1'
						AND txp_cmplt.TXPORT_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Pt_Cmplt_Txp_Rqst'
		,CASE WHEN (txprqst.TXPORT_TYPE_C IN ('2', '3', '4')
						AND txp_cmplt.TXPORT_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Non-Pt_Cmplt_Txp_Rqst'
		,CASE WHEN (txprqst.TXPORT_TYPE_C='1'
						AND txp_cncl.TXPORT_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Pt_Cncld_Txp_Rqst'
		,CASE WHEN (txprqst.TXPORT_TYPE_C IN ('2', '3', '4')
						AND txp_cncl.TXPORT_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Non-Pt_Cncld_Txp_Rqst'
		,CASE WHEN (txprqst.TXPORT_TYPE_C='1'
						AND txp_dlyd.TXPORT_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Pt_dlyd_Txp_Rqst'
		,CASE WHEN (txprqst.TXPORT_TYPE_C IN ('2', '3', '4')
						AND txp_dlyd.TXPORT_ID IS NOT NULL)
				THEN	1
				ELSE	0
				END																												'Non-Pt_dlyd_Txp_Rqst'
		,dte.fmonth_num
		,dte.month_name	
		,txp_pnd.EVENT_INSTANT_LOCAL_DTTM																						'Request_Pend_Time'
		,txp_asgn.EVENT_INSTANT_LOCAL_DTTM																						'Rqst_Assigned_Time'
		,txp_inpgr.EVENT_INSTANT_LOCAL_DTTM																						'Rqst_Inprgs_Time'
		,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM																						'Rqst_Completed_Time'
		,DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_asgn.EVENT_INSTANT_LOCAL_DTTM)									'Time to Assign'
		,DATEDIFF(MINUTE,txp_asgn.EVENT_INSTANT_LOCAL_DTTM,txp_inpgr.EVENT_INSTANT_LOCAL_DTTM)									'Time to in progress'
		,DATEDIFF(MINUTE, txp_inpgr.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)								'Time to complete'
		,DATEDIFF(MINUTE,txp_asgn.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)									'Assign_cmplt'
		,DATEDIFF(MINUTE,txp_dlyd.EVENT_INSTANT_LOCAL_DTTM,txp_asgn.EVENT_INSTANT_LOCAL_DTTM)									'Delay_Assign'
		,CASE WHEN (txprqst.TXPORT_TYPE_C='1'
						AND txp_dlyd.TXPORT_ID IS NOT NULL)
				THEN DATEDIFF(MINUTE,txp_dlyd.EVENT_INSTANT_LOCAL_DTTM,txp_inpgr.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Pt_Delay'

		,CASE WHEN txprqst.TXPORT_TYPE_C='1'
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_inpgr.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Pt_Time Pend to inpgr'
		,CASE WHEN txprqst.TXPORT_TYPE_C='1'
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Pt_Time Pend to Cmplt'
		,CASE WHEN txprqst.TXPORT_TYPE_C='1'
				THEN DATEDIFF(MINUTE,txp_acknw.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Pt_Time Acknw_cmplt'

		,CASE WHEN (txprqst.TXPORT_TYPE_C IN ('2', '3', '4') AND txp_dlyd.TXPORT_ID IS NOT NULL)
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_dlyd.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Non-Pt_Delay'
		,CASE WHEN txprqst.TXPORT_TYPE_C IN ('2', '3', '4')
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_inpgr.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Non-Pt_Time Pend to inpgr'
		,CASE WHEN txprqst.TXPORT_TYPE_C IN ('2', '3', '4')
				THEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Non-Pt_Time Pend to Cmplt'
		,CASE WHEN txprqst.TXPORT_TYPE_C IN ('2', '3', '4')
				THEN DATEDIFF(MINUTE,txp_acknw.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)
				END																												'Non-Pt_Time Acknw_cmplt'

		,CASE WHEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=30
				THEN 1
				ELSE 0
				END																												'compliance_30'
		,CASE WHEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=45
				THEN 1
				ELSE 0
				END																												'compliance_45'
		,CASE WHEN DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=60
				THEN 1
				ELSE 0
				END																												'compliance_60'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=30
						AND txprqst.TXPORT_TYPE_C='1')
				THEN 1
				ELSE 0
				END																												'Pt_compliance_30'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=45
						AND txprqst.TXPORT_TYPE_C='1')
				THEN 1
				ELSE 0
				END																												'Pt_compliance_45'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=60
						AND txprqst.TXPORT_TYPE_C='1')
				THEN 1
				ELSE 0
				END																												'Pt_compliance_60'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=30
						AND txprqst.TXPORT_TYPE_C IN ('2', '3', '4'))
				THEN 1
				ELSE 0
				END																												'non-Pt_compliance_30'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=45
						AND txprqst.TXPORT_TYPE_C IN ('2', '3', '4'))
				THEN 1
				ELSE 0
				END																												'Non-Pt_compliance_45'
		,CASE WHEN (DATEDIFF(MINUTE,txp_pnd.EVENT_INSTANT_LOCAL_DTTM,txp_cmplt.EVENT_INSTANT_LOCAL_DTTM)<=60
						AND txprqst.TXPORT_TYPE_C IN ('2', '3', '4'))
				THEN 1
				ELSE 0
				END																												'Non-Pt_compliance_60'
		,txp_pnd.WORKER_ID																										'pnd_wrk'
		,txp_asgn.WORKER_ID																										'Asgn_wrk'
		,txp_inpgr.WORKER_ID																									'Inpr_wrk'
		,txp_cmplt.WORKER_ID																									'Cmplt_wrk'
FROM txp_src
LEFT OUTER JOIN txp_asgn							ON txp_src.TXPORT_ID=txp_asgn.TXPORT_ID
LEFT OUTER JOIN txp_inpgr							ON txp_src.TXPORT_ID=txp_inpgr.TXPORT_ID
LEFT OUTER JOIN txp_pnd								ON txp_src.TXPORT_ID=txp_pnd.TXPORT_ID
LEFT OUTER JOIN txp_cncl							ON txp_src.TXPORT_ID=txp_cncl.TXPORT_ID
LEFT OUTER JOIN txp_pstpn							ON txp_src.TXPORT_ID=txp_pstpn.TXPORT_ID
LEFT OUTER JOIN txp_dlyd							ON txp_src.TXPORT_ID=txp_dlyd.TXPORT_ID
LEFT OUTER JOIN txp_acknw							ON txp_src.TXPORT_ID=txp_acknw.TXPORT_ID
LEFT OUTER JOIN txp_cmplt							ON txp_src.TXPORT_ID=txp_cmplt.TXPORT_ID
INNER JOIN CLARITY.dbo.TXPORT_REQ_INFO txprqst			ON txprqst.TRANSPORT_ID=txp_src.TXPORT_ID
INNER JOIN CLARITY_App.dbo.Dim_Date dte				ON CAST(dte.day_date AS DATE) =CAST(txp_src.EVENT_INSTANT_LOCAL_DTTM AS DATE)


)

--SELECT
--	*
--FROM txp
--WHERE txp.TXPORT_TYPE_C = 1
----ORDER BY
----	txp.TXPORT_ID
--ORDER BY
--    txp.PAT_ID,
--	txp.TXPORT_ID

SELECT 
		 --loc_id
		MAX(txp.month_name)							'Month'
		,txp.fmonth_num									'MnthNum'
		,COUNT(txp.TXPORT_ID)							'Total_Requests'
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
LEFT JOIN  MDM_REV_LOC_ID AS mloc					ON mloc.REV_LOC_ID = txp.LOC_ID	
WHERE
			(
				(UPPER(@de_control)=UPPER(mloc.de_hospital_code))
				OR (UPPER(@de_control)=UPPER(mloc.hospital_group))
				OR (UPPER(@de_control)='UVA-MC')
			)

GROUP BY fmonth_num

GO


