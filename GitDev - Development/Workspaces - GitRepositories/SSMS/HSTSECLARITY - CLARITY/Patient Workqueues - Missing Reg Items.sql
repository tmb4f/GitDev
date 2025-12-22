USE CLARITY

SELECT *
FROM dbo.workqueue_info wi
WHERE (wi.RECORD_STATE_C IS NULL OR wi.RECORD_STATE_C = 0) 
AND (wi.ACTIVE_YN IS NULL OR wi.ACTIVE_YN = 'Y')
--AND wi.WORKQUEUE_NAME LIKE '%missing%'
AND wi.WORKQUEUE_NAME LIKE '%missing reg items%'
--AND wi.DESCRIPTION LIKE '%missing reg items for appointments%'
AND wi.WORKQUEUE_TYPE_C = '3' -- Patient
ORDER BY wi.WORKQUEUE_NAME
