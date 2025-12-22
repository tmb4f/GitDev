USE CLARITY

DECLARE @StartDate AS DATETIME
DECLARE @EndDate AS DATETIME

--WITH dates AS(
--select '9/1/2022 00:00 AM' AS StartDate, 
--         '2/28/2023 11:59 PM' as EndDate,
--         MIN(ddend.tomorrow_dt) as EndDateExcl
--		 from clarity..DATE_DIMENSION ddend
--  where ddend.CALENDAR_DT = '2/28/2023 11:59 PM'
--  )

--SET @StartDate = '9/1/2022 00:00 AM'
--SET @EndDate = '2/28/2023 11:59 PM'
--SET @StartDate = '6/1/2024 00:00 AM'
--SET @EndDate = '8/31/2024 11:59 PM'

  --SELECT *
  --FROM dates

IF OBJECT_ID('tempdb..#avail_slot ') IS NOT NULL
DROP TABLE #avail_slot

IF OBJECT_ID('tempdb..#booked_by_block ') IS NOT NULL
DROP TABLE #booked_by_block

IF OBJECT_ID('tempdb..#booked_by_block_summary ') IS NOT NULL
DROP TABLE #booked_by_block_summary

IF OBJECT_ID('tempdb..#avail_slot_summary ') IS NOT NULL
DROP TABLE #avail_slot_summary

IF OBJECT_ID('tempdb..#avail_block ') IS NOT NULL
DROP TABLE #avail_block

IF OBJECT_ID('tempdb..#avail_by_block ') IS NOT NULL
DROP TABLE #avail_by_block

IF OBJECT_ID('tempdb..#org_avail_blocks ') IS NOT NULL
DROP TABLE #org_avail_blocks

IF OBJECT_ID('tempdb..#org_avail_blocks_summary ') IS NOT NULL
DROP TABLE #org_avail_blocks_summary

IF OBJECT_ID('tempdb..#booked_by_block_plus ') IS NOT NULL
DROP TABLE #booked_by_block_plus

IF OBJECT_ID('tempdb..#unique_booked_available ') IS NOT NULL
DROP TABLE #unique_booked_available

IF OBJECT_ID('tempdb..#booked_available_by_block ') IS NOT NULL
DROP TABLE #booked_available_by_block

IF OBJECT_ID('tempdb..#booked_available_by_block_agg ') IS NOT NULL
DROP TABLE #booked_available_by_block_agg

IF OBJECT_ID('tempdb..#booked_available_summary ') IS NOT NULL
DROP TABLE #booked_available_summary

IF OBJECT_ID('tempdb..#usage_by_block ') IS NOT NULL
DROP TABLE #usage_by_block

IF OBJECT_ID('tempdb..#blocks_used_summary ') IS NOT NULL
DROP TABLE #blocks_used_summary

IF OBJECT_ID('tempdb..#blocks_openings_summary ') IS NOT NULL
DROP TABLE #blocks_openings_summary

IF OBJECT_ID('tempdb..#avail_by_block ') IS NOT NULL
DROP TABLE #avail_by_block

--; WITH
--availbase as (
   SELECT 
          avail.department_id, 
		  department_name = avail.DEPARTMENT_NAME,
		  department_service_line = CLARITY_DEP.RPT_GRP_THIRTY ,
		   pod_name = ZC_DEP_RPT_GRP_6.name ,
		  --avail.DEPT_SPECIALTY_NAME,
          		  avail.prov_id, 
		  provider = PROV_NM_WID,
		  provider_type = ZC_PROV_TYPE.NAME,
          person_or_resource = avail.PROV_SCHED_TYPE_NAME,
		  dd.day_of_week,
          slot_date,
		  slot_begin_time, 
		  slot_length,
		  booked_length=V_SCHED_APPT.APPT_LENGTH,
		  appt_slot_number = appt_number, 
          num_apts_scheduled, 
		  regular_openings = org_reg_openings,
		  overbook_openings = ORG_OVBK_OPENINGS, 
		  openings = COALESCE(org_reg_openings,0) + COALESCE(ORG_OVBK_OPENINGS,0),
		  --template_block_name = avail.APPT_BLOCK_NAME, 
		  template_block_name = COALESCE(avail.APPT_BLOCK_NAME, 'Unknown'),
		  unavailable_reason = dbo.ZC_UNAVAIL_REASON.NAME,
          overbook_yn = COALESCE(appt_overbook_yn, 'N'), 
          outside_template_yn = COALESCE(outside_template_yn, 'N'), 
		  held_yn = CASE WHEN COALESCE(avail.day_held_rsn_c, avail.time_held_rsn_c) IS NULL
							THEN 'N'
							ELSE 'Y'
					 END,
		  CASE WHEN avail.APPT_NUMBER > 0 AND COALESCE(appt_overbook_yn, 'N') = 'N' AND COALESCE(outside_template_yn, 'N') = 'N' THEN 'Y' ELSE 'N' END AS regular_opening_yn,
  
            MRN= IDENTITY_ID.IDENTITY_ID ,	
           visit_type = V_SCHED_APPT.PRC_NAME,
           appt_status = V_SCHED_APPT.APPT_STATUS_NAME
            --V_SCHED_APPT.COMPLETED_STATUS_YN ,
            --V_SCHED_APPT.SAME_DAY_YN ,
            --V_SCHED_APPT.JOINT_APPT_YN 
			
   INTO #avail_slot

    from v_availability avail 
	--INNER join dates on avail.SLOT_BEGIN_TIME >= dates.StartDate and 
 --                                     avail.SLOT_BEGIN_TIME < dates.EndDateExcl

	INNER join CLARITY_App.Rptg.vwDim_Date dd				ON avail.SLOT_DATE = dd.day_date
	LEFT OUTER JOIN CLARITY..CLARITY_SER						ON avail.PROV_ID = CLARITY_SER.PROV_ID
	LEFT OUTER JOIN clarity..CLARITY_DEP						ON clarity_dep.DEPARTMENT_ID = avail.DEPARTMENT_ID              
	LEFT OUTER JOIN clarity..ZC_DEP_RPT_GRP_6					ON CLARITY_DEP.RPT_GRP_SIX=ZC_DEP_RPT_GRP_6.RPT_GRP_SIX
	LEFT OUTER JOIN clarity..ZC_DEP_RPT_GRP_7					ON CLARITY_DEP.RPT_GRP_seven=ZC_DEP_RPT_GRP_7.RPT_GRP_seven

	LEFT OUTER JOIN clarity..V_SCHED_APPT						ON v_sched_appt.PAT_ENC_CSN_ID = avail.PAT_ENC_CSN_ID
	LEFT outer JOIN CLARITY..PATIENT PATIENT					ON V_SCHED_APPT.PAT_ID = PATIENT.PAT_ID
            
	LEFT OUTER JOIN CLARITY..IDENTITY_ID						ON IDENTITY_ID.PAT_ID = V_SCHED_APPT.PAT_ID AND IDENTITY_ID.IDENTITY_TYPE_ID = 14
	LEFT OUTER JOIN clarity..ZC_UNAVAIL_REASON					ON zc_unavail_reason.UNAVAILABLE_RSN_C = avail.UNAVAILABLE_RSN_C
	LEFT OUTER JOIN clarity..ZC_PROV_TYPE					    ON zc_prov_type.PROV_TYPE_C = clarity_ser.PROVider_TYPE_C

   where 1=1
   --AND slot_begin_time >=  dates.StartDate  
   --AND  SLOT_BEGIN_TIME < dates.EndDateExcl
   --AND slot_begin_time >=  @StartDate  
   --AND  SLOT_BEGIN_TIME <= @EndDate
   /* Test 09/04/2024 10295006	1301224	2024-09-19 11:30:00.000 */
   /*
   AND avail.DEPARTMENT_ID = '10295006'
   AND avail.PROV_ID = '1301224'
   AND avail.SLOT_BEGIN_TIME = '9/19/2024 11:30:00'
   */
   --AND avail.DEPARTMENT_ID = '10242005' -- UVPC DERMATOLOGY
   --AND avail.DEPARTMENT_ID = '10419012' -- OCIR PROSTHETICS/ORTCS
   --AND avail.DEPARTMENT_ID = '10341001' -- CVPE UVA RHEU PANTOPS
   --AND avail.DEPARTMENT_ID = '10419010' --	OCIR HAND CENTER
   --AND avail.DEPARTMENT_ID = '10212016' -- 	F500 CARDIOLOGY
   --AND avail.DEPARTMENT_ID = '10239015' --	UVMS SURGERY
   --AND avail.DEPARTMENT_ID = '10381004'
   --AND avail.PROV_ID = '28931' -- RUSSELL, MARK [28931]
   --AND avail.PROV_ID = '133761'
   --AND (avail.SLOT_BEGIN_TIME = '10/15/2024 13:00:00'
   --OR avail.SLOT_BEGIN_TIME = '10/15/2024 11:45:00')
   --AND avail.PROV_ID = '120025' -- JOWDY, PETER [120025]
   --AND (avail.SLOT_BEGIN_TIME = '8/6/2024 9:45:00')
   --AND avail.DEPARTMENT_ID = '10443001'
   --AND avail.PROV_ID = '1569997'
   --AND avail.SLOT_DATE = '9/26/2024'
   --AND avail.DEPARTMENT_ID = '10244004' -- UVWC OPHTHALMOLOGY
   AND avail.DEPARTMENT_ID = '10419014' -- OCIR SPORTS MED
   --AND avail.PROV_ID = '1300188' -- RETINA
   --AND avail.SLOT_DATE = '10/24/2024'


   AND dd.day_date > CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME)
   --AND dd.day_date <= DATEADD(DAY, 13, CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME))

   --AND dd.day_date > '8/2/2024'
   --AND dd.day_date <= '10/15/2024'

   --AND avail.PROV_SCHED_TYPE_NAME = 'Person'

--SELECT * FROM #avail_slot availbase
----where  department_id in (
----10242051, -- UVPC DIGESTIVE HEALTH
----10243003, -- UVHE DIGESTIVE HEALTH
----10246004, -- WALL MED DH FAM MED
----10210028) -- ECCC MED DHC EAST CL
--WHERE 1 = 1
----AND availbase.DEPARTMENT_ID = 10212016
----AND availbase.PROV_ID = '1300588'
--ORDER BY
--	availbase.DEPARTMENT_ID,
--	availbase.PROV_ID,
--	availbase.SLOT_DATE,
--	availbase.SLOT_BEGIN_TIME,
--	availbase.appt_slot_number

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name,
	COUNT(*) AS booked
INTO #booked_by_block
FROM #avail_slot
WHERE appt_slot_number > 0 -- booked records
GROUP BY
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name

--SELECT
--	*
--FROM #booked_by_block
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time,
--	template_block_name

SELECT DISTINCT
	booked.DEPARTMENT_ID,
	booked.PROV_ID,
	booked.SLOT_BEGIN_TIME,
   (SELECT bookedt.template_block_name + ' (' + CONVERT(VARCHAR(3),bookedt.booked) + '),' AS [text()]
	FROM #booked_by_block bookedt
	WHERE bookedt.DEPARTMENT_ID = booked.DEPARTMENT_ID
	AND bookedt.PROV_ID = booked.PROV_ID
	AND bookedt.SLOT_BEGIN_TIME = booked.SLOT_BEGIN_TIME
	ORDER BY bookedt.template_block_name DESC
	FOR XML PATH ('')) AS booked_by_block_string
INTO #booked_by_block_summary
FROM #booked_by_block booked

--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    LEFT(booked_by_block_string, LEN(booked_by_block_string) -  1) AS booked_by_block_string
--FROM #booked_by_block_summary
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	MAX(regular_openings) AS regular_openings,
	MAX(overbook_openings) AS overbook_openings,
	MAX(openings) AS openings,
	SUM(CASE WHEN appt_slot_number > 0 AND outside_template_yn = 'Y' THEN 1 ELSE 0 END) AS outside_template_openings,
	MAX(NUM_APTS_SCHEDULED) AS num_apts_scheduled,
	SUM(CASE WHEN appt_slot_number > 0 AND regular_opening_yn = 'Y' THEN 1 ELSE 0 END) AS num_regular_opening_apts_scheduled,
	SUM(CASE WHEN appt_slot_number > 0 AND overbook_yn = 'Y' THEN 1 ELSE 0 END) AS num_overbook_apts_scheduled,
	SUM(CASE WHEN appt_slot_number > 0 AND outside_template_yn = 'Y' THEN 1 ELSE 0 END) AS num_outside_template_apts_scheduled
INTO #avail_slot_summary
FROM #avail_slot
--WHERE appt_slot_number > 0 -- booked records
GROUP BY
	department_id,
	prov_id,
	slot_begin_time

--SELECT
--	*
--FROM #avail_slot_summary
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time
/*
SELECT
	summary.DEPARTMENT_ID,
    summary.PROV_ID,
    summary.SLOT_BEGIN_TIME,
    summary.regular_openings,
    summary.overbook_openings,
    summary.num_apts_scheduled,
    (summary.num_apts_scheduled - (summary.num_overbook_apts_scheduled + summary.num_outside_template_apts_scheduled)) AS num_regular_opening_apts_scheduled,
    summary.num_overbook_apts_scheduled,
    summary.num_outside_template_apts_scheduled,
	LEFT(booked.booked_by_block_string, LEN(booked.booked_by_block_string) -  1) AS scheduled_by_block
FROM #avail_slot_summary summary
LEFT OUTER JOIN #booked_by_block_summary booked
ON booked.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND booked.PROV_ID = summary.PROV_ID
AND booked.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
ORDER BY
	department_id,
	prov_id,
	slot_begin_time
*/
--/*
/*====================================================================================================================*/

SELECT blk.DEPARTMENT_ID
	 , blk.PROV_ID
	 , CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SLOT_DATE
	 --, CAST(blk.SLOT_BEGIN_TIME AS TIME) AS SLOT_BEGIN_TIME
	 , blk.SLOT_BEGIN_TIME
	 , blk.LINE
	 --, zab.NAME AS BLOCK_NAME
	 , COALESCE(zab.NAME, 'Unknown') AS BLOCK_NAME
	 --, blk.ORG_AVAIL_BLOCKS
	 , COALESCE(blk.ORG_AVAIL_BLOCKS, 999) AS ORG_AVAIL_BLOCKS
	 --, blk.BLOCKS_USED
	 , COALESCE(blk.BLOCKS_USED,0) AS BLOCKS_USED
	 ----, CASE WHEN slot.regular_openings IS NULL THEN 0 ELSE slot.regular_openings END AS regular_openings
	 --, COALESCE(slot.regular_openings, 0) AS regular_openings
	 --, COALESCE(slot.overbook_openings, 0) AS overbook_openings
	 --, COALESCE(slot.outside_template_openings,0) AS outside_template_openings
	 --, COALESCE(slot.num_apts_scheduled, 0) AS num_apts_scheduled
INTO #avail_block
FROM dbo.AVAIL_BLOCK blk
INNER JOIN CLARITY_App.Rptg.vwDim_Date dd				ON CAST(CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SMALLDATETIME)  = dd.day_date

--LEFT OUTER JOIN #avail_slot_summary slot
----INNER JOIN #avail_slot_summary slot
--ON slot.DEPARTMENT_ID = blk.DEPARTMENT_ID
--AND slot.PROV_ID = blk.PROV_ID
--AND slot.SLOT_BEGIN_TIME = blk.SLOT_BEGIN_TIME

LEFT OUTER JOIN dbo.ZC_APPT_BLOCK zab
ON zab.APPT_BLOCK_C = blk.BLOCK_C
WHERE 1=1
   --AND slot_begin_time >=  dates.StartDate  
   --AND  SLOT_BEGIN_TIME < dates.EndDateExcl
   --AND slot_begin_time >=  @StartDate  
   --AND  SLOT_BEGIN_TIME <= @EndDate
/*
   AND blk.DEPARTMENT_ID = '10295006'
   AND blk.PROV_ID = '1301224'
   AND blk.SLOT_BEGIN_TIME = '9/19/2024 11:30:00'
*/
   --AND blk.DEPARTMENT_ID = '10242005' -- UVPC DERMATOLOGY
   --AND blk.DEPARTMENT_ID = '10419012' -- OCIR PROSTHETICS/ORTCS
   --AND blk.DEPARTMENT_ID = '10341001' -- CVPE UVA RHEU PANTOPS
   --AND blk.DEPARTMENT_ID = '10419010' --	OCIR HAND CENTER
   --AND blk.DEPARTMENT_ID = '10212016' -- 	F500 CARDIOLOGY
   --AND blk.DEPARTMENT_ID = '10239015' --	UVMS SURGERY
   --AND blk.DEPARTMENT_ID = '10381004'
   --AND blk.PROV_ID = '28931' -- RUSSELL, MARK [28931]
   --AND blk.PROV_ID = '133761'
   --AND blk.PROV_ID = '120025' -- JOWDY, PETER [120025]
   --AND (blk.SLOT_BEGIN_TIME = '10/15/2024 13:00:00'
   --OR blk.SLOT_BEGIN_TIME = '10/15/2024 11:45:00')
   --AND (blk.SLOT_BEGIN_TIME = '8/6/2024 9:45:00')
   --AND blk.DEPARTMENT_ID = '10443001'
   --AND blk.PROV_ID = '1569997'
   --AND CAST(blk.SLOT_BEGIN_TIME AS DATE) = '9/26/2024'
   --AND blk.DEPARTMENT_ID = '10244004' -- UVWC OPHTHALMOLOGY
   AND blk.DEPARTMENT_ID = '10419014' -- OCIR SPORTS MED
   --AND blk.PROV_ID = '1300188' -- RETINA
   --AND CAST(blk.SLOT_BEGIN_TIME AS DATE) = '10/24/2024'

   AND dd.day_date > CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME)
   --AND dd.day_date <= DATEADD(DAY, 13, CAST(CAST(GETDATE() AS DATE) AS SMALLDATETIME))

   --AND dd.day_date > '8/2/2024'
   --AND dd.day_date <= '10/15/2024'

--blk.DEPARTMENT_ID = 10354008 -- UVBB PEDS DENTISTRY
--blk.DEPARTMENT_ID = 10419014 -- OCIR SPORTS MED
--blk.DEPARTMENT_ID = 10210002
--blk.DEPARTMENT_ID = 10243087
--blk.DEPARTMENT_ID IN (10210002,10210030,10243003,10243087,10244023)
--CAST(blk.SLOT_BEGIN_TIME AS DATE) >= '8/3/2024' /*****/
--CAST(blk.SLOT_BEGIN_TIME AS DATE) >= CAST(@StartDate AS DATE)
--AND CAST(blk.SLOT_BEGIN_TIME AS DATE) <= CAST(@EndDate AS DATE)
--AND blk.PROV_ID = '150459' -- HUGHES, BENJAMIN J [150459]
--AND blk.PROV_ID = '137167' --	HIGGINBOTHAM, KIMBERLY
--AND blk.PROV_ID = '84374' -- TUCKER, SHANNON
--AND blk.PROV_ID = '92145' -- YOUNGBERG, HEATHER
--AND blk.PROV_ID = '30288' -- FRIEL, CHARLES
--AND avail.PROV_ID = '1300382'
--AND CAST(blk.SLOT_BEGIN_TIME AS DATE) = '1/28/2019'
--AND CAST(blk.SLOT_BEGIN_TIME AS DATE) = '10/8/2018'
--AND CAST(SLOT_DATE AS DATE) >= CAST(@StartDate AS DATE)
--AND CAST(blk.SLOT_BEGIN_TIME AS DATE) >= '7/1/2017' /*****/
--AND CAST(blk.SLOT_BEGIN_TIME AS DATE) >= '1/28/2019'
--AND blk.SLOT_BEGIN_TIME = '7/5/2024 9:15:00'
--AND blk.BLOCK_C IS NOT NULL
ORDER BY blk.DEPARTMENT_ID
       , blk.PROV_ID
	   , CAST(blk.SLOT_BEGIN_TIME AS DATE)
	   , CAST(blk.SLOT_BEGIN_TIME AS TIME)

  -- Create index for temp table #avail_block
  CREATE UNIQUE CLUSTERED INDEX IX_avail_block ON #avail_block (DEPARTMENT_ID, PROV_ID, SLOT_DATE, SLOT_BEGIN_TIME, LINE, BLOCK_NAME)

--SELECT *
--FROM #avail_block
--WHERE 1 = 1
--   --AND DEPARTMENT_ID = '10212016' -- 	F500 CARDIOLOGY
--   --AND PROV_ID = '105518'
--ORDER BY DEPARTMENT_ID
--       , PROV_ID
--	   , SLOT_DATE
--	   , SLOT_BEGIN_TIME
--	   , BLOCK_NAME

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	block_name,
	SUM(ORG_AVAIL_BLOCKS) AS available
INTO #avail_by_block
FROM #avail_block
GROUP BY
	department_id,
	prov_id,
	slot_begin_time,
	block_name

--SELECT
--	*
--FROM #avail_by_block
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time,
--	block_name

SELECT DISTINCT
	blk.DEPARTMENT_ID,
	blk.PROV_ID,
	blk.SLOT_BEGIN_TIME,
   (SELECT blkt.BLOCK_NAME + ' (' + CONVERT(VARCHAR(3),blkt.ORG_AVAIL_BLOCKS) + '),' AS [text()]
	FROM #avail_block blkt
	WHERE blkt.DEPARTMENT_ID = blk.DEPARTMENT_ID
	AND blkt.PROV_ID = blk.PROV_ID
	AND blkt.SLOT_BEGIN_TIME = blk.SLOT_BEGIN_TIME
	ORDER BY blkt.BLOCK_NAME DESC
	FOR XML PATH ('')) AS org_avail_blocks_string
INTO #org_avail_blocks
FROM #avail_block blk

--SELECT
--	*
--FROM #org_avail_blocks
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time

SELECT DISTINCT
	blk.DEPARTMENT_ID,
	blk.PROV_ID,
	blk.SLOT_BEGIN_TIME,
	COUNT(*) AS block_count,
	MAX(blk.BLOCK_NAME) AS block_name
INTO #org_avail_blocks_summary
FROM #avail_block blk
GROUP BY
	department_id,
	prov_id,
	slot_begin_time

--SELECT
--	*
--FROM #org_avail_blocks_summary
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time

SELECT
	bbb.DEPARTMENT_ID,
    bbb.PROV_ID,
    bbb.SLOT_BEGIN_TIME,
	oab.org_avail_blocks_string,
    CASE WHEN oabs.block_name IS NOT NULL THEN oabs.block_name ELSE bbb.template_block_name END AS template_block_name,
	ass.openings,
    bbb.booked
INTO #booked_by_block_plus
FROM #booked_by_block bbb
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    org_avail_blocks_string
FROM #org_avail_blocks
) oab
ON oab.DEPARTMENT_ID = bbb.DEPARTMENT_ID
AND oab.PROV_ID = bbb.PROV_ID
AND  oab.SLOT_BEGIN_TIME = bbb.SLOT_BEGIN_TIME
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    block_name
FROM #org_avail_blocks_summary
WHERE block_count = 1
) oabs
ON oabs.DEPARTMENT_ID = bbb.DEPARTMENT_ID
AND oabs.PROV_ID = bbb.PROV_ID
AND  oabs.SLOT_BEGIN_TIME = bbb.SLOT_BEGIN_TIME
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    regular_openings,
    overbook_openings,
    openings,
    outside_template_openings,
    num_apts_scheduled,
	num_regular_opening_apts_scheduled,
    num_overbook_apts_scheduled,
    num_outside_template_apts_scheduled
FROM #avail_slot_summary
) ass
ON ass.DEPARTMENT_ID = bbb.DEPARTMENT_ID
AND ass.PROV_ID = bbb.PROV_ID
AND ass.SLOT_BEGIN_TIME = bbb.SLOT_BEGIN_TIME

--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    org_avail_blocks_string,
--    template_block_name,
--	openings,
--    booked
--FROM #booked_by_block_plus
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time,
--	template_block_name

SELECT
	unique_booked_available.DEPARTMENT_ID,
    unique_booked_available.PROV_ID,
    unique_booked_available.SLOT_BEGIN_TIME,
    unique_booked_available.block_name
INTO #unique_booked_available
FROM
(
SELECT
	booked_available.DEPARTMENT_ID,
    booked_available.PROV_ID,
    booked_available.SLOT_BEGIN_TIME,
    booked_available.block_name,
	ROW_NUMBER() OVER(PARTITION BY booked_available.DEPARTMENT_ID, booked_available.PROV_ID, booked_available.SLOT_BEGIN_TIME, booked_available.block_name ORDER BY booked_available.block_name) AS seq
FROM  
(
SELECT
	booked.DEPARTMENT_ID,
    booked.PROV_ID,
    booked.SLOT_BEGIN_TIME,
    booked.template_block_name AS block_name
--FROM #booked_by_block booked
FROM #booked_by_block_plus booked
UNION ALL
SELECT
	available.DEPARTMENT_ID,
    available.PROV_ID,
    available.SLOT_BEGIN_TIME,
    available.BLOCK_NAME AS block_name
FROM #avail_by_block available
) booked_available
) unique_booked_available
WHERE unique_booked_available.seq = 1

--SELECT
--	*
--FROM #unique_booked_available
--ORDER BY
--	DEPARTMENT_ID,
--	PROV_ID,
--	SLOT_BEGIN_TIME

SELECT
	summary.DEPARTMENT_ID,
    summary.PROV_ID,
    summary.SLOT_BEGIN_TIME,
    summary.regular_openings,
    summary.overbook_openings,
    summary.openings,
    summary.outside_template_openings,
    summary.num_apts_scheduled,
	summary.num_regular_opening_apts_scheduled,
    summary.num_overbook_apts_scheduled,
    summary.num_outside_template_apts_scheduled,
    --summary.block_name,
    COALESCE(summary.block_name,'Unknown') AS block_name,
    --COALESCE(booked_by_block.booked,0) AS booked,
    --booked_by_block.booked,
    CASE WHEN summary.block_name IS NULL AND booked_by_block.booked IS NULL THEN 0
			   WHEN summary.block_name IS NOT NULL AND booked_by_block.booked IS NULL THEN 0
			   ELSE booked_by_block.booked
	END AS booked,
	--COALESCE(avail_by_block.available,0) AS org_available_block_openings
	avail_by_block.available AS org_available_block_openings
INTO #booked_available_by_block
FROM
(
SELECT
	avail_slot.DEPARTMENT_ID,
    avail_slot.PROV_ID,
    avail_slot.SLOT_BEGIN_TIME,
    avail_slot.regular_openings,
    avail_slot.overbook_openings,
    avail_slot.openings,
    avail_slot.outside_template_openings,
    avail_slot.num_apts_scheduled,
	avail_slot.num_regular_opening_apts_scheduled,
    avail_slot.num_overbook_apts_scheduled,
    avail_slot.num_outside_template_apts_scheduled,
    booked_available.block_name
FROM #avail_slot_summary avail_slot
LEFT OUTER JOIN #unique_booked_available booked_available
ON booked_available.DEPARTMENT_ID = avail_slot.DEPARTMENT_ID
AND booked_available.PROV_ID = avail_slot.PROV_ID
AND booked_available.SLOT_BEGIN_TIME = avail_slot.SLOT_BEGIN_TIME
) summary
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    template_block_name,
    booked
--FROM #booked_by_block
FROM #booked_by_block_plus
) booked_by_block
ON booked_by_block.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND booked_by_block.PROV_ID = summary.PROV_ID
AND booked_by_block.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
AND booked_by_block.template_block_name = summary.block_name
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    BLOCK_NAME,
    available
FROM #avail_by_block
) avail_by_block
ON avail_by_block.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND avail_by_block.PROV_ID = summary.PROV_ID
AND avail_by_block.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
AND avail_by_block.BLOCK_NAME = summary.block_name

--SELECT 
--	*
--FROM #booked_available_by_block
--ORDER BY
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME

SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    MAX(regular_openings) AS regular_openings,
    MAX(openings) AS openings,
    SUM(booked) AS booked,
	--COUNT(*) AS blocks
	SUM(CASE WHEN block_name IS NOT NULL THEN 1 ELSE 0 END) AS blocks
INTO #booked_available_by_block_agg
FROM #booked_available_by_block
GROUP BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME

--SELECT
--	*
--FROM #booked_available_by_block_agg
--ORDER BY
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME

SELECT
	booked_available_by_block.DEPARTMENT_ID,
    booked_available_by_block.PROV_ID,
    booked_available_by_block.SLOT_BEGIN_TIME,
    booked_available_by_block.regular_openings,
    booked_available_by_block.openings,
    overbook_openings,
    outside_template_openings,
    num_apts_scheduled,
	num_regular_opening_apts_scheduled,
    num_overbook_apts_scheduled,
    num_outside_template_apts_scheduled,
    block_name,
    booked_available_by_block.booked,
    org_available_block_openings,
	booked_available_by_block_agg.booked AS booked_total,
	booked_available_by_block_agg.blocks AS blocks_total
INTO #booked_available_summary
FROM #booked_available_by_block booked_available_by_block

LEFT OUTER JOIN #booked_available_by_block_agg booked_available_by_block_agg
ON booked_available_by_block_agg.DEPARTMENT_ID = booked_available_by_block.DEPARTMENT_ID
AND booked_available_by_block_agg.PROV_ID = booked_available_by_block.PROV_ID
AND booked_available_by_block_agg.SLOT_BEGIN_TIME = booked_available_by_block.SLOT_BEGIN_TIME

--SELECT
--	*
--FROM #booked_available_summary
--ORDER BY
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    block_name

SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    regular_openings,
    openings,
    block_name,
    booked,
    org_available_block_openings,
    booked_total,
    blocks_total,
	CASE
		--WHEN ((regular_openings = booked) AND (booked_total = booked)) THEN 'Y'
		WHEN ((openings = booked) AND (booked_total = booked)) THEN 'Y'
		WHEN ((openings > booked) AND (booked_total = booked)) THEN 'Y'
		WHEN (blocks_total =1 AND booked = 0) THEN 'Y'
		WHEN (blocks_total >1 AND (booked_total < openings)) THEN 'Y'
		WHEN (openings > 0 AND booked_total = 0) THEN 'Y'
		WHEN ((openings = booked) AND (booked_total > booked)) THEN 'Y'
		WHEN ((booked > openings) AND (booked_total = booked)) THEN 'Y'
		ELSE NULL
	END AS 'Keep?',
    overbook_openings,
    outside_template_openings,
    num_apts_scheduled,
	num_regular_opening_apts_scheduled,
    num_overbook_apts_scheduled,
    num_outside_template_apts_scheduled
FROM #booked_available_summary
ORDER BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    block_name

/*
SELECT
	booked.DEPARTMENT_ID,
    booked.PROV_ID,
    booked.SLOT_BEGIN_TIME,
    booked.template_block_name,
    booked.booked,
    blk.DEPARTMENT_ID,
    blk.PROV_ID,
    blk.SLOT_BEGIN_TIME,
    blk.BLOCK_NAME,
    blk.ORG_AVAIL_BLOCKS,
    blk.BLOCKS_USED,
    blk.regular_openings,
    blk.overbook_openings,
    blk.outside_template_openings,
    blk.num_apts_scheduled
FROM #booked_by_block booked
LEFT OUTER JOIN
(	   
SELECT
	department_id,
	prov_id,
	slot_begin_time,
	BLOCK_NAME,
	SUM(ORG_AVAIL_BLOCKS) AS ORG_AVAIL_BLOCKS,
	SUM(BLOCKS_USED) AS BLOCKS_USED,
	MAX(regular_openings) AS regular_openings,
	MAX(overbook_openings) AS overbook_openings,
	MAX(outside_template_openings) AS outside_template_openings,
	MAX(num_apts_scheduled) AS num_apts_scheduled
--INTO #usage_by_block
FROM #avail_block
GROUP BY
	department_id,
	prov_id,
	slot_begin_time,
	BLOCK_NAME
) blk
ON blk.DEPARTMENT_ID = booked.DEPARTMENT_ID
AND blk.PROV_ID = booked.PROV_ID
AND blk.SLOT_BEGIN_TIME = booked.SLOT_BEGIN_TIME
AND blk.BLOCK_NAME = booked.template_block_name
ORDER BY
	booked.DEPARTMENT_ID
   ,booked.PROV_ID
   ,booked.SLOT_BEGIN_TIME
   ,booked.template_block_name

SELECT
	blk.DEPARTMENT_ID,
    blk.PROV_ID,
    blk.SLOT_BEGIN_TIME,
    blk.BLOCK_NAME,
    blk.ORG_AVAIL_BLOCKS,
    blk.BLOCKS_USED,
	allblk.TOTAL_BLOCKS_USED,
	CASE WHEN blk.ORG_AVAIL_BLOCKS = 999 THEN 999 WHEN blk.ORG_AVAIL_BLOCKS = 0 THEN 0 ELSE (blk.ORG_AVAIL_BLOCKS - blk.BLOCKS_USED) END AS BLOCK_OPENINGS,
	CASE WHEN blk.num_apts_scheduled = (blk.regular_openings + blk.overbook_openings) THEN 0
	           WHEN blk.BLOCKS_USED = 0 AND allblk.TOTAL_BLOCKS_USED = 0 THEN blk.regular_openings + blk.overbook_openings
	           WHEN blk.ORG_AVAIL_BLOCKS = 999 AND (blk.BLOCKS_USED = allblk.TOTAL_BLOCKS_USED) THEN 0
	           WHEN blk.ORG_AVAIL_BLOCKS = 999 AND blk.BLOCKS_USED = 0 AND (allblk.TOTAL_BLOCKS_USED = blk.regular_openings) THEN 0
			   WHEN (blk.BLOCKS_USED = allblk.TOTAL_BLOCKS_USED) AND (allblk.TOTAL_BLOCKS_USED = blk.regular_openings) THEN 0
			   ELSE (blk.ORG_AVAIL_BLOCKS - blk.BLOCKS_USED)
    END AS BLOCK_OPENINGS_CALC,
	blk.regular_openings,
	blk.overbook_openings,
	blk.outside_template_openings,
	blk.num_apts_scheduled
INTO #usage_by_block
FROM
(
SELECT
	department_id,
	prov_id,
	slot_begin_time,
	BLOCK_NAME,
	SUM(ORG_AVAIL_BLOCKS) AS ORG_AVAIL_BLOCKS,
	SUM(BLOCKS_USED) AS BLOCKS_USED,
	MAX(regular_openings) AS regular_openings,
	MAX(overbook_openings) AS overbook_openings,
	MAX(outside_template_openings) AS outside_template_openings,
	MAX(num_apts_scheduled) AS num_apts_scheduled
--INTO #usage_by_block
FROM #avail_block
GROUP BY
	department_id,
	prov_id,
	slot_begin_time,
	BLOCK_NAME
) blk
LEFT OUTER JOIN
(
SELECT
	department_id,
	prov_id,
	slot_begin_time,
	--BLOCK_NAME,
	--SUM(ORG_AVAIL_BLOCKS) AS ORG_AVAIL_BLOCKS,
	SUM(BLOCKS_USED) AS TOTAL_BLOCKS_USED--,
	--MAX(regular_openings) AS regular_opneings
--INTO #usage_by_block
FROM #avail_block
GROUP BY
	department_id,
	prov_id,
	slot_begin_time--,
	--BLOCK_NAME
) allblk
ON allblk.DEPARTMENT_ID = blk.DEPARTMENT_ID
AND allblk.SLOT_BEGIN_TIME = blk.SLOT_BEGIN_TIME
AND allblk.PROV_ID = blk.PROV_ID

SELECT
	*
FROM #usage_by_block
ORDER BY
	department_id,
	prov_id,
	slot_begin_time,
	BLOCK_NAME

SELECT DISTINCT
	blk.DEPARTMENT_ID,
	blk.PROV_ID,
	blk.SLOT_BEGIN_TIME,
   (SELECT blkt.BLOCK_NAME + ' (' + CONVERT(VARCHAR(3),blkt.ORG_AVAIL_BLOCKS) + '),' AS [text()]
	FROM #usage_by_block blkt
	WHERE blkt.DEPARTMENT_ID = blk.DEPARTMENT_ID
	AND blkt.PROV_ID = blk.PROV_ID
	AND blkt.SLOT_BEGIN_TIME = blk.SLOT_BEGIN_TIME
	ORDER BY blkt.BLOCK_NAME DESC
	FOR XML PATH ('')) AS org_avail_blocks_string
INTO #org_avail_blocks_summary
FROM #usage_by_block blk

--SELECT
--	*
--FROM #org_avail_blocks_summary
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time

SELECT DISTINCT
	blk.DEPARTMENT_ID,
	blk.PROV_ID,
	blk.SLOT_BEGIN_TIME,
   (SELECT blkt.BLOCK_NAME + ' (' + CONVERT(VARCHAR(3),blkt.BLOCKS_USED) + '),' AS [text()]
	FROM #usage_by_block blkt
	WHERE blkt.DEPARTMENT_ID = blk.DEPARTMENT_ID
	AND blkt.PROV_ID = blk.PROV_ID
	AND blkt.SLOT_BEGIN_TIME = blk.SLOT_BEGIN_TIME
	ORDER BY blkt.BLOCK_NAME DESC
	FOR XML PATH ('')) AS blocks_used_string
INTO #blocks_used_summary
FROM #usage_by_block blk

--SELECT
--	*
--FROM #blocks_used_summary
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time

SELECT DISTINCT
	blk.DEPARTMENT_ID,
	blk.PROV_ID,
	blk.SLOT_BEGIN_TIME,
   --(SELECT blkt.BLOCK_NAME + ' (' + CONVERT(VARCHAR(3),blkt.BLOCK_OPENINGS) + '),' AS [text()]
   (SELECT blkt.BLOCK_NAME + ' (' + CONVERT(VARCHAR(3),blkt.BLOCK_OPENINGS_CALC) + '),' AS [text()]
	FROM #usage_by_block blkt
	WHERE blkt.DEPARTMENT_ID = blk.DEPARTMENT_ID
	AND blkt.PROV_ID = blk.PROV_ID
	AND blkt.SLOT_BEGIN_TIME = blk.SLOT_BEGIN_TIME
	ORDER BY blkt.BLOCK_NAME DESC
	FOR XML PATH ('')) AS blocks_openings_string
INTO #blocks_openings_summary
FROM #usage_by_block blk

--SELECT
--	*
--FROM #blocks_used_summary
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time
	
SELECT
	summary.DEPARTMENT_ID,
	dep.DEPARTMENT_NAME,
    summary.PROV_ID,
    summary.SLOT_BEGIN_TIME,
    summary.regular_openings,
    summary.overbook_openings,
    summary.num_apts_scheduled,
    (summary.num_apts_scheduled - (summary.num_overbook_apts_scheduled + summary.num_outside_template_apts_scheduled)) AS num_regular_opening_apts_scheduled,
    summary.num_overbook_apts_scheduled,
    summary.num_outside_template_apts_scheduled,
	CASE WHEN LEN(org_avail.org_avail_blocks_string) >= 2 THEN LEFT(org_avail.org_avail_blocks_string, LEN(org_avail.org_avail_blocks_string) -  1) ELSE org_avail.org_avail_blocks_string END AS originally_available_block_openings,
	CASE WHEN LEN(booked.booked_by_block_string) >= 2 THEN LEFT(booked.booked_by_block_string, LEN(booked.booked_by_block_string) -  1) ELSE booked.booked_by_block_string END AS blocks_with_scheduled_appts,
	CASE WHEN LEN(usage.blocks_used_string) >= 2 THEN LEFT(usage.blocks_used_string, LEN(usage.blocks_used_string) -  1) ELSE usage.blocks_used_string END AS originally_available_block_usage,
	CASE WHEN LEN(openings.blocks_openings_string) >= 2 THEN LEFT(openings.blocks_openings_string, LEN(openings.blocks_openings_string) -  1) ELSE openings.blocks_openings_string END AS current_available_block_openings
FROM #avail_slot_summary summary
LEFT OUTER JOIN #booked_by_block_summary booked
ON booked.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND booked.PROV_ID = summary.PROV_ID
AND booked.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
LEFT OUTER JOIN #org_avail_blocks_summary org_avail
ON org_avail.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND org_avail.PROV_ID = summary.PROV_ID
AND org_avail.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
LEFT OUTER JOIN #blocks_used_summary usage
ON usage.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND usage.PROV_ID = summary.PROV_ID
AND usage.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
LEFT OUTER JOIN #blocks_openings_summary openings
ON openings.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND openings.PROV_ID = summary.PROV_ID
AND openings.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
ON dep.DEPARTMENT_ID = summary.DEPARTMENT_ID
WHERE 1 = 1
--AND summary.DEPARTMENT_ID = 10212016
--AND summary.PROV_ID = '105518'
AND org_avail.DEPARTMENT_ID IS NOT NULL
--ORDER BY
--	summary.department_id,
--	summary.prov_id,
--	summary.slot_begin_time
ORDER BY
	dep.DEPARTMENT_NAME,
	summary.prov_id,
	summary.slot_begin_time
*/
--*/

GO