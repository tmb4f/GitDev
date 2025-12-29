
-- Using the TxportDelayRsn SSRS reportas a template to recreate using the Hospital Logistics tables.

USE CLARITY_App;

 
--Testing
DECLARE @start_date DATETIME;
DECLARE @end_date DATETIME;

SET @start_date = '2025-01-01';  
SET @end_date = '2025-01-09';


SELECT
hasma.HLR_ID,
hasma.ASSIGNMENT_DATE_REAL,
hasma.LINE,
hasma.STATUS_LINE_NUM,
--haea.LINE,
--hli.HLR_NAME,
--asgn.DELAY_RSN_C,
zrsn.NAME                                         AS       [delay_reason],
--haea.START_UTC_DTTM                             AS       [delay_time],
hasma.START_UTC_DTTM,
--hai.ASGN_TECH_ID                                AS       [tech_id],
hkr.RECORD_NAME                                   AS       [tech_name],
hkr2.RECORD_NAME                                  AS       [transporter_name_who_changed_req_status],
--ce.USER_ID,
ce.NAME                                           AS       [user_name_who_changed_req_status],
hasma.START_LOCAL_DTTM

FROM  CLARITY..HL_ASGN_STATUS_MOD_AUDIT           AS hasma
LEFT JOIN CLARITY..HL_REQ_INFO                    AS hli                ON hasma.HLR_ID = hli.HLR_ID
LEFT JOIN CLARITY..ZC_HL_ASGN_DELAY_RSN           AS zrsn               ON hasma.DELAY_RSN_C = zrsn.HL_ASGN_DELAY_RSN_C 
LEFT JOIN CLARITY..HL_ASGN_INFO                   AS hai                ON hasma.HLR_ID = hai.HLR_ID AND hasma.ASSIGNMENT_DATE_REAL = hai.ASSIGNMENT_DATE_REAL
--LEFT JOIN CLARITY..HL_ASGN_ESCL_AUDIT             AS haea               ON hasma.HLR_ID = haea.HLR_ID AND hasma.ASSIGNMENT_DATE_REAL = haea.ASSIGNMENT_DATE_REAL AND hasma.LINE = haea.LINE
LEFT JOIN CLARITY..CL_HKR                         AS hkr                ON hai.ASGN_TECH_ID = hkr.RECORD_ID
LEFT JOIN CLARITY..CL_HKR                         AS hkr2               ON hasma.START_TECH_ID = hkr2.RECORD_ID
LEFT JOIN CLARITY..CLARITY_EMP                    AS ce                 ON hasma.START_USER_ID = ce.USER_ID

WHERE 1 = 1
--AND hli.REQ_TASK_SUBTYPE_C = 1 --Patient Transport
AND hli.REQ_TASK_SUBTYPE_C IN (1,99) --Patient Transport, Other									******* Tom's edits
AND hasma.STATUS_MOD_C = 1  -- Delay																				******* Tom's edits
AND hai.ASGN_STATUS_C <> 40 --Canceled
AND ce.USER_ID NOT LIKE '%[^0-9]%' -- Filter out 'SYSUSER' USER_IDs 

-- Testing
--AND hasma.HLR_ID = 1370889
AND hli.REQ_ACTIVATION_LOCAL_DTTM BETWEEN @start_date AND @end_date

ORDER BY
hasma.HLR_ID,
hasma.ASSIGNMENT_DATE_REAL,
hasma.LINE 
