USE CLARITY

DECLARE @StartDate SMALLDATETIME = NULL, @EndDate SMALLDATETIME = NULL

--SET @StartDate = '10/1/2024 00:00'
--SET @StartDate = '1/1/2025 00:00'
--SET @StartDate = '7/1/2025 00:00'
SET @StartDate = '8/24/2025 00:00'
--SET @EndDate = CAST(CAST(DATEADD(DAY, -1, GETDATE()) AS DATE) AS SMALLDATETIME);
--SET @EndDate = '12/31/2024 00:00'
--SET @EndDate = '4/30/2025 00:00'
SET @EndDate = '8/31/2025 00:00'

SELECT
            vsurg.[PAT_ID],
            --cte.person_id,
            --cte.PATIENT_NAME,
            lnk.PAT_ENC_CSN_ID AS PAT_ENC_CSN_ID,
            lnk.OR_LINK_CSN AS HSP_PAT_ENC_CSN_ID,
            lgb.PROC_DATE AS CONTACT_DATE,
            --NULL AS APPT_TIME,
            COALESCE(lgb.IN_OR_DTTM, vsurg.CASE_BEGIN_INSTANT) AS APPT_TIME,
            NULL AS APPT_STATUS_NAME,
            COALESCE(lgb.IN_OR_DTTM, vsurg.CASE_BEGIN_INSTANT) AS HOSP_ADMSN_TIME,
            COALESCE(lgb.OUT_OR_DTTM, vsurg.CASE_END_INSTANT) AS HOSP_DISCH_TIME,
            hsp.HOSP_ADMSN_TIME AS HSP_HOSP_ADMSN_TIME,
            hsp.HOSP_DISCH_TIME AS HSP_HOSP_DISCH_TIME,
            lgb.PATIENT_CLASS_C AS ENC_TYPE_C,
            lgb.PATIENT_CLASS_NM AS Enc_Type,
            zhat.NAME AS HOSP_ADMSN_TYPE_NAME,
            lgb.PRIMARY_PHYSICIAN_ID AS Prov_Id,
            lgb.LOCATION_ID AS DEPARTMENT_ID,
            lgb.LOCATION_NM AS DEPARTMENT_NAME,
            NULL AS REFERRAL_ID,
            --NULL AS Referral_Entry_Date,
            CASE WHEN orl.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS,
            'or_cte' AS [extract]	,

            orl.LOG_ID,
            lgb.PRIMARY_PROCEDURE_ID,
            lgb.PRIMARY_PROCEDURE_NM,
            hsp.HSP_ACCOUNT_ID,
            har.TOT_CHGS,
            har.TOT_PMTS,
            zcsh.NAME AS CODING_STATUS_NAME, -- 1 Not Started, 2 In Progress, 3 Waiting, 4 Completed, 5 Ready To Start, 6 On Hold
            har.PRIMARY_PAYOR_ID,
            har.PRIMARY_PLAN_ID

        FROM CLARITY.dbo.OR_CASE vsurg
            --INNER JOIN #rflpts cte ON vsurg.PAT_ID = cte.PAT_ID
            LEFT OUTER JOIN CLARITY.dbo.OR_LOG orl ON orl.CASE_ID = vsurg.OR_CASE_ID
            LEFT OUTER JOIN CLARITY.dbo.V_LOG_BASED lgb ON lgb.CASE_ID=vsurg.OR_CASE_ID
            LEFT OUTER JOIN CLARITY.dbo.PAT_OR_ADM_LINK lnk ON lnk.CASE_ID = vsurg.OR_CASE_ID
            LEFT OUTER JOIN CLARITY.dbo.	PAT_ENC_HSP	hsp ON lnk.OR_LINK_CSN = hsp.PAT_ENC_CSN_ID
            LEFT OUTER JOIN CLARITY.dbo.HSP_ACCOUNT har ON har.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
            LEFT OUTER JOIN CLARITY.dbo.OR_CASE_2 cs2 ON vsurg.OR_CASE_ID=cs2.CASE_ID
            LEFT OUTER JOIN CLARITY.dbo.ZC_HOSP_ADMSN_TYPE zhat ON zhat.HOSP_ADMSN_TYPE_C = hsp.HOSP_ADMSN_TYPE_C

            LEFT JOIN CLARITY.dbo.ZC_CODING_STS_HA zcsh ON zcsh.CODING_STATUS_C = har.CODING_STATUS_C

        WHERE 1=1
            AND lgb.PROC_DATE >=@StartDate
			AND lgb.PROC_DATE <= @EndDate

            AND vsurg.SCHED_STATUS_C NOT IN ('2','5') /* Not  Canceled or Voided */
            AND lgb.LOG_STATUS_C NOT IN ('4','6') /* Not Voided or Canceled */
            ----AND vsurg.CANCEL_REASON_C IS NULL /* No Cancel Reason */
            --AND (vsurg.CANCEL_REASON_C IS NULL /* No Cancel Reason */
            --	OR (vsurg.CANCEL_REASON_C = 999 AND zcsh.NAME = 'Completed'))
            AND lgb.PROC_NOT_PERF_C IS NULL
			AND zcsh.NAME LIKE '%Complete%'
			ORDER BY
				lgb.LOG_ID