USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @StartDate SMALLDATETIME = NULL, @EndDate SMALLDATETIME = NULL

SET @StartDate = '12/15/2024 00:00'
--SET @EndDate = CAST(CAST(DATEADD(DAY, -1, GETDATE()) AS DATE) AS SMALLDATETIME);
SET @EndDate = '1/19/2025 00:00'

  SET NOCOUNT ON;

DECLARE @locstartdate DATETIME,
        @locenddate DATETIME

SET @locstartdate = CAST(@StartDate AS DATETIME)
SET @locenddate   = CAST(@EndDate AS DATETIME)

SELECT
	   zcsh.NAME AS CODING_STATUS_NAME, -- 1 Not Started, 2 In Progress, 3 Waiting, 4 Completed, 5 Ready To Start, 6 On Hold
	   zocr.NAME AS OR_CANCEL_RSN_NAME,
	   lgb.PRIMARY_PROCEDURE_NM,
	   lgb.PROC_DATE AS CONTACT_DATE,
	   COALESCE(lgb.IN_OR_DTTM, vsurg.CASE_BEGIN_INSTANT) AS IN_OR_DTTM,
       COALESCE(lgb.OUT_OR_DTTM, vsurg.CASE_END_INSTANT) AS OUT_OR_DTTM,
	   har.TOT_CHGS,
	   har.TOT_PMTS,
	   vsurg.[PAT_ID],
	   lnk.PAT_ENC_CSN_ID AS PAT_ENC_CSN_ID,
	   lnk.OR_LINK_CSN AS HSP_PAT_ENC_CSN_ID,
	   hsp.HOSP_ADMSN_TIME AS HSP_HOSP_ADMSN_TIME,
	   hsp.HOSP_DISCH_TIME AS HSP_HOSP_DISCH_TIME,
	   lgb.PATIENT_CLASS_NM AS Enc_Type,
	   zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
       lgb.PRIMARY_PHYSICIAN_ID AS Prov_Id,
	   lgb.LOCATION_ID AS DEPARTMENT_ID,
	   lgb.LOCATION_NM AS DEPARTMENT_NAME,
	   CASE WHEN orl.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS,
	   lgb.PROC_NOT_PERF_C,
	   lgb.PROC_NOT_PERF_NM,

	   orl.LOG_ID,
	   lgb.PRIMARY_PROCEDURE_ID,
	   hsp.HSP_ACCOUNT_ID,
       cpt1.CPT_CODE [CPT1],
	   cpt1.CPT_CODE_DESC,
	   cpt1.CPT_QUANTITY,
	   icd10.CURRENT_ICD10_LIST,
	   icd10.DX_NAME
	
		FROM CLARITY.dbo.OR_CASE vsurg
		LEFT OUTER JOIN CLARITY.dbo.OR_LOG orl											ON orl.CASE_ID = vsurg.OR_CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb							ON lgb.CASE_ID=vsurg.OR_CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.PAT_OR_ADM_LINK lnk						ON lnk.CASE_ID = vsurg.OR_CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.	PAT_ENC_HSP	hsp						ON lnk.OR_LINK_CSN = hsp.PAT_ENC_CSN_ID
		LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT har							ON har.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
	    LEFT OUTER JOIN CLARITY.dbo.OR_CASE_2 cs2									ON vsurg.OR_CASE_ID=cs2.CASE_ID
		LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C

		LEFT OUTER JOIN 
				(SELECT cptt.HSP_ACCOUNT_ID,
                        cptt.LINE,
                        cptt.CPT_CODE,
                        cptt.CPT_CODE_DESC,
                        cptt.CPT_QUANTITY
				FROM CLARITY.dbo.HSP_ACCT_CPT_CODES cptt	
				WHERE cptt.LINE='1') cpt1									ON cpt1.HSP_ACCOUNT_ID=har.HSP_ACCOUNT_ID

		LEFT OUTER JOIN 
				(SELECT cpt.*,eap.CPT_CODE, eap.NAME_HISTORY
					FROM CLARITY.dbo.OR_CASE_ALL_PROC cpt							
						LEFT OUTER JOIN CLARITY.dbo.CLARITY_EAP_OT eap								ON cpt.ALL_PROC_CODE_ID=eap.PROC_ID
					--	LEFT OUTER JOIN dbo.OR_PROC_EXT_ID prc								ON prc.OR_PROC_ID = cpt.OR_PROC_ID
					WHERE cpt.LINE='1'
				) schcpt1														ON schcpt1.OR_CASE_ID=vsurg.OR_CASE_ID
		
		LEFT OUTER JOIN 
				(SELECT *
				FROM CLARITY.dbo.HSP_ACCT_DX_LIST opdx
				WHERE opdx.LINE='1'	) opdx										ON opdx.HSP_ACCOUNT_ID=har.HSP_ACCOUNT_ID
	
		LEFT OUTER JOIN 
				(SELECT *
				FROM CLARITY.dbo.CLARITY_EDG edg
				WHERE edg.REF_BILL_CODE_SET_C='2') icd10							ON icd10.DX_ID=opdx.DX_ID

		LEFT OUTER JOIN 
				(SELECT orlp.*,eap.PROC_NAME
					FROM CLARITY.dbo.X_ORL_PANEL_1_PROC orlp							
						LEFT OUTER JOIN CLARITY.dbo.CLARITY_EAP eap								ON orlp.PANEL1_PROC_ID=eap.PROC_ID
					WHERE orlp.LINE='1'
				) orlcpt1														ON orlcpt1.LOG_ID=orl.LOG_ID
				/*CPT_LOG*/

		LEFT JOIN CLARITY.dbo.ZC_CODING_STS_HA zcsh ON zcsh.CODING_STATUS_C = har.CODING_STATUS_C

		LEFT JOIN CLARITY.dbo.ZC_OR_CANCEL_RSN zocr ON zocr.CANCEL_REASON_C = vsurg.CANCEL_REASON_C

	WHERE 1=1
		AND lgb.PROC_DATE BETWEEN @locstartdate AND @locenddate
		AND vsurg.SCHED_STATUS_C NOT IN ('2','5') /* Not  Canceled or Voided */
		AND lgb.LOG_STATUS_C NOT IN ('4','6') /* Not Voided or Canceled */
		AND har.TOT_CHGS > 0
		AND vsurg.CANCEL_REASON_C IS NOT NULL /* Has a Cancel Reason */

ORDER BY
	zcsh.NAME,
	zocr.NAME,
	lgb.PROC_NOT_PERF_NM,
	lgb.PROC_DATE


GO


