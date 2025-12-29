USE CLARITY

SELECT
 [ext_pat_id]				= ISNULL(CONVERT(VARCHAR(255),TBL.[ext_pat_id]),'')				
,[int_pat_id]				= ISNULL(CONVERT(VARCHAR(255),TBL.[int_pat_id]),'')				
,[int_pat_id_assign_auth]	= ISNULL(CONVERT(VARCHAR(255),TBL.[int_pat_id_assign_auth]),'')	
,[int_pat_id_type]			= ISNULL(CONVERT(VARCHAR(255),TBL.[int_pat_id_type]),'')			
,[pat_acct_num]				= ISNULL(CONVERT(VARCHAR(255),TBL.[pat_acct_num]),'')				
,[visit_num]				= ISNULL(CONVERT(VARCHAR(255),TBL.[visit_num]),'')				
,[encounter_class]			= ISNULL(CONVERT(VARCHAR(255),TBL.[encounter_class]),'')			
,[start_datetime]			= ISNULL(CONVERT(VARCHAR(255),TBL.[start_datetime]),'')			
,[end_datetime]				= ISNULL(CONVERT(VARCHAR(255),TBL.[end_datetime]),'')				
,[diagnosis_code]			= ISNULL(CONVERT(VARCHAR(255),TBL.[diagnosis_code]),'')			
,[diag_code_sys]			= ISNULL(CONVERT(VARCHAR(255),TBL.[diag_code_sys]),'')			
,[is_primary]				= ISNULL(CONVERT(VARCHAR(255),TBL.[is_primary]),'')				
,[line_num]					= ISNULL(CONVERT(VARCHAR(255),TBL.[line_num]),'')					
,[diag_poa]					= ISNULL(CONVERT(VARCHAR(255),TBL.[diag_poa]),'')	
,TBL.CODING_STATUS_C
,TBL.CODING_STATUS_NAME
,TBL.PRIMARY_PAYOR_ID
,TBL.PAYOR_NAME
,TBL.FINANCIAL_CLASS_NAME
FROM (
SELECT 
 [ext_pat_id]				 = PAT_ENC.PAT_ID				-- 1 External Patient ID							string(25) A unique identifier for this patient across all facilities, such as an EMPI ID. Ifunavailable, then this field should be left blank.
,[int_pat_id]				 = IDENTITY_ID.IDENTITY_ID		-- 2 Internal Patient ID							string(25) A unique identifier for the patient within the facility where the encounter occurred,such as MRN.
,[int_pat_id_assign_auth]	 = 'UVA'						-- 3 Internal Patient IDAssigning Authority			string(15) A unique name for the system that generated the Internal Patient ID.
,[int_pat_id_type]			 = 'MRN'						-- 4 Internal Patient IDType Code					string(15) A code corresponding to the type of the Internal Patient ID, such as MRN. Ifunavailable, then this field should be left blank.
,[pat_acct_num]				 = HSP_ACCOUNT.HSP_ACCOUNT_ID   -- 5 Patient Account Number							string(25) A unique identifier for the patient account for which this encounter is a part, such asHAR.
,[visit_num]				 = HSP_ACCOUNT.PRIM_ENC_CSN_ID		-- 6 Visit Number									string(25) A unique identifier for the encounter, such as CSN.
,[encounter_class]			 = ZC_ACCT_CLASS_HA.NAME		-- 7 Encounter Class								string(254) The classification of the encounter.
,[start_datetime]			 = CONVERT(VARCHAR(19),COALESCE(PAT_ENC_HSP.HOSP_ADMSN_TIME,PAT_ENC.EFFECTIVE_DATE_DTTM),120)  -- 8 Start Date/TIME		datetime YYYY-MM-DDHH24:MM:SSThe date and time for the start of the encounter.
,[end_datetime]				 = CONVERT(VARCHAR(19),COALESCE(PAT_ENC_HSP.HOSP_DISCH_TIME,PAT_ENC.EFFECTIVE_DATE_DTTM),120)  -- 9 End Date/TIME			datetime YYYY-MM-DDHH24:MM:SSThe date and time for the end of the encounter.
,[diagnosis_code]			 = CLARITY_EDG.REF_BILL_CODE										--10 Diagnosis Code			string(100) The diagnosis code. This can be a string separated by commas
,[diag_code_sys]			 = ZC_EDG_CODE_SET.NAME												--11 Diagnosis Code SYSTEM	string(20) ICD9ICD10etc.An identifier for the diagnosis code system.
,[is_primary]				 = CASE WHEN HSP_ACCT_DX_LIST.LINE = 1 THEN 'Y' ELSE 'N'  END		--12 Is Primary Diagnosis	enumerated YN Indicates whether this is the primary diagnosis. Each encounter can have only one primary diagnosis.
,[line_num]					 = HSP_ACCT_DX_LIST.LINE											--13 Diagnosis Line Number	integer The line number or sequence for this diagnosis from 1 to N.
,[diag_poa]					 = ZC_DX_POA.NAME													--14 Diagnosis POA			string(100) 1 Diagnosis present on arrival flag. If unavailable, then this field should be left blank.
,HSP_ACCOUNT.CODING_STATUS_C
,zcs.NAME AS CODING_STATUS_NAME
,HSP_ACCOUNT.PRIMARY_PAYOR_ID
,epm.PAYOR_NAME
,FIN_CLASS.NAME AS FINANCIAL_CLASS_NAME

FROM			
						

				CLARITY.dbo.HSP_ACCOUNT			HSP_ACCOUNT			
--INNER JOIN		CLARITY.dbo.PAT_ENC				PAT_ENC				ON PAT_ENC.HSP_ACCOUNT_ID			= HSP_ACCOUNT.HSP_ACCOUNT_ID AND 
--																	 PAT_ENC.PAT_ENC_CSN_ID			= HSP_ACCOUNT.PRIM_ENC_CSN_ID  -- pull har AND only pat enc prim csn.  	
LEFT OUTER JOIN		CLARITY.dbo.PAT_ENC				PAT_ENC				ON PAT_ENC.HSP_ACCOUNT_ID			= HSP_ACCOUNT.HSP_ACCOUNT_ID AND 
																	 PAT_ENC.PAT_ENC_CSN_ID			= HSP_ACCOUNT.PRIM_ENC_CSN_ID  -- pull har AND only pat enc prim csn.  
--INNER JOIN      CLARITY.dbo.HSP_ACCT_DX_LIST    HSP_ACCT_DX_LIST    ON HSP_ACCT_DX_LIST.HSP_ACCOUNT_ID	= HSP_ACCOUNT.HSP_ACCOUNT_ID 
LEFT OUTER JOIN      CLARITY.dbo.HSP_ACCT_DX_LIST    HSP_ACCT_DX_LIST    ON HSP_ACCT_DX_LIST.HSP_ACCOUNT_ID	= HSP_ACCOUNT.HSP_ACCOUNT_ID
--INNER JOIN      CLARITY.dbo.CLARITY_EDG			CLARITY_EDG			ON CLARITY_EDG.DX_ID				= HSP_ACCT_DX_LIST.DX_ID
LEFT OUTER JOIN      CLARITY.dbo.CLARITY_EDG			CLARITY_EDG			ON CLARITY_EDG.DX_ID				= HSP_ACCT_DX_LIST.DX_ID
--INNER JOIN		CLARITY.dbo. ZC_EDG_CODE_SET    ZC_EDG_CODE_SET     ON CLARITY_EDG.REF_BILL_CODE_SET_C	= ZC_EDG_CODE_SET.EDG_CODE_SET_C
LEFT OUTER JOIN		CLARITY.dbo. ZC_EDG_CODE_SET    ZC_EDG_CODE_SET     ON CLARITY_EDG.REF_BILL_CODE_SET_C	= ZC_EDG_CODE_SET.EDG_CODE_SET_C
INNER JOIN		CLARITY.dbo.PATIENT				PATIENT				ON PATIENT.PAT_ID					= PAT_ENC.PAT_ID
INNER JOIN		CLARITY.dbo.IDENTITY_ID			IDENTITY_ID			ON PAT_ENC.PAT_ID					= IDENTITY_ID.PAT_ID   AND IDENTITY_ID.IDENTITY_TYPE_ID = '14'
--INNER JOIN		CLARITY.dbo.PAT_ENC_2           PAT_ENC_2			ON PAT_ENC.PAT_ENC_CSN_ID			= PAT_ENC_2.PAT_ENC_CSN_ID
LEFT OUTER JOIN		CLARITY.dbo.PAT_ENC_2           PAT_ENC_2			ON PAT_ENC.PAT_ENC_CSN_ID			= PAT_ENC_2.PAT_ENC_CSN_ID
INNER JOIN		CLARITY.dbo.ZC_ACCT_CLASS_HA	ZC_ACCT_CLASS_HA    ON PAT_ENC_2.ADT_PAT_CLASS_C		= ZC_ACCT_CLASS_HA.ACCT_CLASS_HA_C
LEFT JOIN		CLARITY.dbo.PAT_ENC_HSP			PAT_ENC_HSP			ON PAT_ENC.PAT_ENC_CSN_ID			= PAT_ENC_HSP.PAT_ENC_CSN_ID
LEFT JOIN		CLARITY.dbo.ZC_DX_POA			ZC_DX_POA			ON HSP_ACCT_DX_LIST.FINAL_DX_POA_C  = ZC_DX_POA.DX_POA_C
LEFT JOIN CLARITY.dbo.ZC_CODING_STS_HA zcs ON zcs.CODING_STATUS_C = HSP_ACCOUNT.CODING_STATUS_C
LEFT JOIN CLARITY.dbo.CLARITY_EPM epm ON epm.PAYOR_ID = HSP_ACCOUNT.PRIMARY_PAYOR_ID
LEFT JOIN CLARITY..ZC_FINANCIAL_CLASS FIN_CLASS ON FIN_CLASS.FINANCIAL_CLASS = epm.FINANCIAL_CLASS


WHERE 1=1

--AND	CAST(COALESCE(PAT_ENC.HOSP_DISCHRG_TIME,PAT_ENC.EFFECTIVE_DATE_DTTM) AS DATE)> @StartDate366 -- Discharged/Visit Date  DC within last 366 days of today
--AND CAST(COALESCE(PAT_ENC.HOSP_DISCHRG_TIME,PAT_ENC.EFFECTIVE_DATE_DTTM) AS DATE)<= @EndDate -- Discharged/Visit Date
--AND HSP_ACCOUNT.TOT_CHGS > 0
--AND HSP_ACCOUNT.CODING_STATUS_C = 4 -- CODING COMPLETE -- ADDED 2019.11.13
AND HSP_ACCOUNT.HSP_ACCOUNT_ID = 13022721273
--AND PAT_ENC.PAT_ID = 'Z1191932'
)TBL

ORDER BY 
ext_pat_id, TBL.pat_acct_num,TBL.visit_num,TBL.line_num
