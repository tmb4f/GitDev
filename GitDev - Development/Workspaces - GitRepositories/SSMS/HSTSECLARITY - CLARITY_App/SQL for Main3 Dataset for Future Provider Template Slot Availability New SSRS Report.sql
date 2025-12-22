SELECT 
          avail.department_id, 
		  department_name = avail.DEPARTMENT_NAME,
		  department_service_line = dep.RPT_GRP_THIRTY ,
		  pod_name = zdrg6.name ,
          avail.prov_id, 
		  provider = PROV_NM_WID,
		  provider_type = zpt.NAME,
          person_or_resource = avail.PROV_SCHED_TYPE_NAME,
		  dd.day_of_week,
          slot_date,
		  slot_begin_time, 
		  slot_length,
		  booked_length=appt.APPT_LENGTH,
		  appt_slot_number = appt_number, 
          num_apts_scheduled, 
		  regular_openings = org_reg_openings,
		  overbook_openings = ORG_OVBK_OPENINGS, 
		  openings = COALESCE(org_reg_openings,0) + COALESCE(ORG_OVBK_OPENINGS,0),
		  template_block_name = COALESCE(avail.APPT_BLOCK_NAME, 'Unknown'),
		  unavailable_reason = rsn.NAME,
          overbook_yn = COALESCE(appt_overbook_yn, 'N'), 
          outside_template_yn = COALESCE(outside_template_yn, 'N'), 
		  held_yn = case when coalesce(avail.day_held_rsn_c, avail.time_held_rsn_c) is null
							THEN 'N'
							ELSE 'Y'
					 END,
		  CASE WHEN avail.APPT_NUMBER > 0 AND COALESCE(appt_overbook_yn, 'N') = 'N' AND COALESCE(outside_template_yn, 'N') = 'N' THEN 'Y' ELSE 'N' END AS regular_opening_yn,
  
            MRN= IDENTITY_ID.IDENTITY_ID ,	
           visit_type = appt.PRC_NAME,
           appt_status = appt.APPT_STATUS_NAME,
           avail.UNAVAILABLE_RSN_NAME
			
   INTO #avail_slot

    from dbo.V_AVAILABILITY avail 

	INNER join dbo.DATE_DIMENSION dd				ON avail.SLOT_DATE = dd.CALENDAR_DT
	LEFT OUTER JOIN dbo.CLARITY_SER ser			ON avail.PROV_ID = ser.PROV_ID
	LEFT OUTER JOIN dbo.CLARITY_DEP dep						ON dep.DEPARTMENT_ID = avail.DEPARTMENT_ID              
	LEFT OUTER JOIN dbo.ZC_DEP_RPT_GRP_6 zdrg6					ON dep.RPT_GRP_SIX=zdrg6.RPT_GRP_SIX
	LEFT OUTER JOIN dbo.ZC_DEP_RPT_GRP_7					ON dep.RPT_GRP_seven=ZC_DEP_RPT_GRP_7.RPT_GRP_seven

	LEFT OUTER JOIN dbo.V_SCHED_APPT	 appt					ON appt.PAT_ENC_CSN_ID = avail.PAT_ENC_CSN_ID
	LEFT outer JOIN dbo.PATIENT PATIENT					ON appt.PAT_ID = PATIENT.PAT_ID
            
	LEFT OUTER JOIN dbo.IDENTITY_ID						ON IDENTITY_ID.PAT_ID = appt.PAT_ID AND IDENTITY_ID.IDENTITY_TYPE_ID = 14
	LEFT OUTER JOIN dbo.ZC_UNAVAIL_REASON rsn	ON rsn.UNAVAILABLE_RSN_C = avail.UNAVAILABLE_RSN_C
	LEFT OUTER JOIN dbo.ZC_PROV_TYPE	 zpt				    ON zpt.PROV_TYPE_C = ser.PROVider_TYPE_C

   WHERE 1=1
	AND ( 
			(@HierarchyLookup <> 8 AND avail.DEPARTMENT_ID IN (SELECT value FROM STRING_SPLIT(@Departments,',')))
			OR
			(@HierarchyLookup = 8 AND COALESCE(ser.RPT_GRP_EIGHT,'0')	IN (SELECT value FROM STRING_SPLIT(@Departments,',')) ) -- says department but the department parameter does department and financial subdivision
			 )
	AND avail.PROV_ID IN (SELECT value FROM STRING_SPLIT(@Providers,','))
    AND dd.CALENDAR_DT >= @StartDate
	AND dd.CALENDAR_DT <= @EndDate
	AND avail.UNAVAILABLE_RSN_NAME IS NULL

	ORDER BY 
          avail.department_id, 
		  avail.prov_id, 
		  avail.slot_begin_time,
		  avail.appt_number

  CREATE UNIQUE CLUSTERED INDEX IX_avail_slot ON #avail_slot (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, appt_slot_number)

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name,
	COUNT(*) AS booked
INTO #booked_by_block
FROM #avail_slot p
WHERE appt_slot_number > 0 -- booked records
GROUP BY
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name
ORDER BY
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name

  CREATE UNIQUE CLUSTERED INDEX IX_booked_by_block ON #booked_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, template_block_name)

 SELECT
	sum1.DEPARTMENT_ID,
    sum1.PROV_ID,
    sum1.SLOT_BEGIN_TIME,
    sum1.regular_openings,
    sum1.overbook_openings,
    sum1.openings,
    sum1.outside_template_openings,
    sum1.num_apts_scheduled,
    sum1.num_regular_opening_apts_scheduled,
    sum1.num_overbook_apts_scheduled,
    sum1.num_outside_template_apts_scheduled,
    sum1.provider_type,
    sum2.visit_types
INTO #avail_slot_summary
FROM
(
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
	SUM(CASE WHEN appt_slot_number > 0 AND outside_template_yn = 'Y' THEN 1 ELSE 0 END) AS num_outside_template_apts_scheduled,
	MAX(provider_type) AS provider_type
FROM #avail_slot
GROUP BY
	department_id,
	prov_id,
	slot_begin_time
) sum1
LEFT OUTER JOIN
(
SELECT
	department_id,
	prov_id,
	slot_begin_time,
	STUFF((
	SELECT ',' + CAST(innerTable.visit_type AS varchar(30)) + ' ' + CAST(COUNT(innerTable.visit_type) AS VARCHAR(5))
	FROM #avail_slot AS innerTable
	WHERE innerTable.department_id = p.department_id
	AND innerTable.prov_id = p.prov_id
	AND innerTable.slot_begin_time = p.slot_begin_time
	GROUP BY
		innerTable.department_id,
		innerTable.prov_id,
		innerTable.slot_begin_time,
		innerTable.visit_type
	FOR XML PATH('')
	),1,1,'') AS visit_types
FROM #avail_slot p
WHERE p.appt_slot_number > 0
GROUP BY
	department_id,
	prov_id,
	slot_begin_time
) sum2
ON sum2.department_id = sum1.department_id
AND sum2.prov_id = sum1.prov_id
AND sum2.slot_begin_time = sum1.slot_begin_time
ORDER BY
	sum1.DEPARTMENT_ID,
	sum1.PROV_ID,
	sum1.SLOT_BEGIN_TIME

 CREATE UNIQUE CLUSTERED INDEX IX_avail_slot_summary ON #avail_slot_summary (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)

/*====================================================================================================================*/

SELECT blk.DEPARTMENT_ID
	 , blk.PROV_ID
	 , CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SLOT_DATE
	 , blk.SLOT_BEGIN_TIME
	 , blk.LINE
	 , COALESCE(zab.NAME, 'Unknown') AS BLOCK_NAME
	 , COALESCE(blk.ORG_AVAIL_BLOCKS, 999) AS ORG_AVAIL_BLOCKS
	 , COALESCE(blk.BLOCKS_USED,0) AS BLOCKS_USED
INTO #avail_block
FROM dbo.AVAIL_BLOCK blk
INNER join dbo.DATE_DIMENSION dd				ON CAST(CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SMALLDATETIME)  = dd.CALENDAR_DT
LEFT OUTER JOIN dbo.ZC_APPT_BLOCK zab
ON zab.APPT_BLOCK_C = blk.BLOCK_C
LEFT OUTER JOIN dbo.CLARITY_SER ser						ON blk.PROV_ID = ser.PROV_ID
   WHERE 1=1
	AND ( 
			(@HierarchyLookup <> 8 AND blk.DEPARTMENT_ID IN (SELECT value FROM STRING_SPLIT(@Departments,',')))
			OR
			(@HierarchyLookup = 8 AND COALESCE(ser.RPT_GRP_EIGHT,'0')	IN (SELECT value FROM STRING_SPLIT(@Departments,',')) ) -- says department but the department parameter does department and financial subdivision
			 )
	AND blk.PROV_ID IN (SELECT value FROM STRING_SPLIT(@Providers,','))
    AND dd.CALENDAR_DT >= @StartDate
	AND dd.CALENDAR_DT <= @EndDate
	

ORDER BY blk.DEPARTMENT_ID
       , blk.PROV_ID
	   , CAST(blk.SLOT_BEGIN_TIME AS DATE)
	   , CAST(blk.SLOT_BEGIN_TIME AS TIME)

  CREATE UNIQUE CLUSTERED INDEX IX_avail_block ON #avail_block (DEPARTMENT_ID, PROV_ID, SLOT_DATE, SLOT_BEGIN_TIME, LINE, BLOCK_NAME)

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
ORDER BY
	department_id,
	prov_id,
	slot_begin_time,
	block_name

  CREATE UNIQUE CLUSTERED INDEX IX_avail_by_block ON #avail_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, BLOCK_NAME)

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
ORDER BY
	department_id,
	prov_id,
	slot_begin_time

  CREATE UNIQUE CLUSTERED INDEX IX_org_avail_blocks_summary ON #org_avail_blocks_summary (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)

SELECT
	bbb.DEPARTMENT_ID,
    bbb.PROV_ID,
    bbb.SLOT_BEGIN_TIME,
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
    openings + outside_template_openings AS openings,
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
ORDER BY
	bbb.department_id,
	bbb.prov_id,
	bbb.slot_begin_time,
	CASE WHEN oabs.block_name IS NOT NULL THEN oabs.block_name ELSE bbb.template_block_name END

  CREATE NONCLUSTERED INDEX IX_booked_by_block_plus ON #booked_by_block_plus (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, template_block_name)

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
ORDER BY
	unique_booked_available.DEPARTMENT_ID,
    unique_booked_available.PROV_ID,
    unique_booked_available.SLOT_BEGIN_TIME,
    unique_booked_available.block_name

  CREATE UNIQUE CLUSTERED INDEX IX_unique_booked_available ON #unique_booked_available (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)

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
    COALESCE(summary.block_name,'Unknown') AS block_name,
    CASE WHEN summary.block_name IS NULL AND booked_by_block.booked IS NULL THEN 0
			   WHEN summary.block_name IS NOT NULL AND booked_by_block.booked IS NULL THEN 0
			   ELSE booked_by_block.booked
	END AS booked,
	avail_by_block.available AS org_available_block_openings,
	summary.provider_type,
	summary.visit_types
INTO #booked_available_by_block
FROM
(
SELECT
	avail_slot.DEPARTMENT_ID,
    avail_slot.PROV_ID,
    avail_slot.SLOT_BEGIN_TIME,
    avail_slot.regular_openings,
    avail_slot.overbook_openings,
    avail_slot.openings + avail_slot.outside_template_openings AS openings,
    avail_slot.outside_template_openings,
    avail_slot.num_apts_scheduled,
	avail_slot.num_regular_opening_apts_scheduled,
    avail_slot.num_overbook_apts_scheduled,
    avail_slot.num_outside_template_apts_scheduled,
    booked_available.block_name,
	avail_slot.provider_type,
	avail_slot.visit_types

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
ORDER BY
	summary.DEPARTMENT_ID,
    summary.PROV_ID,
    summary.SLOT_BEGIN_TIME,
    COALESCE(summary.block_name,'Unknown')

  CREATE NONCLUSTERED INDEX IX_booked_available_by_block ON #booked_available_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)

SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    MAX(regular_openings) AS regular_openings,
    MAX(openings) AS openings,
    SUM(booked) AS booked,
	SUM(CASE WHEN block_name IS NOT NULL THEN 1 ELSE 0 END) AS blocks,
	MAX(provider_type) AS provider_type,
	MAX(visit_types) AS visit_types,
	STUFF((
	SELECT ',' + CAST(innerTable.block_name AS varchar(30)) + ' ' + CAST(booked AS VARCHAR(5))
	FROM #booked_available_by_block AS innerTable
	WHERE innerTable.department_id = p.department_id
	AND innerTable.prov_id = p.prov_id
	AND innerTable.slot_begin_time = p.slot_begin_time
	FOR XML PATH('')
	),1,1,'') AS blocks_booked
INTO #booked_available_by_block_agg
FROM #booked_available_by_block p
GROUP BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME
ORDER BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME

  CREATE UNIQUE CLUSTERED INDEX IX_booked_available_by_block_agg ON #booked_available_by_block_agg (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)

SELECT
	booked_available_by_block.DEPARTMENT_ID,
    booked_available_by_block.PROV_ID,
    booked_available_by_block.SLOT_BEGIN_TIME,
    booked_available_by_block.regular_openings,
    booked_available_by_block.openings,
    booked_available_by_block.overbook_openings,
    booked_available_by_block.outside_template_openings,
    booked_available_by_block.num_apts_scheduled,
	booked_available_by_block.num_regular_opening_apts_scheduled,
    booked_available_by_block.num_overbook_apts_scheduled,
    booked_available_by_block.num_outside_template_apts_scheduled,
    booked_available_by_block.block_name,
    booked_available_by_block.booked,
    booked_available_by_block.org_available_block_openings,
	booked_available_by_block_agg.booked AS booked_total,
	booked_available_by_block_agg.openings - booked_available_by_block_agg.booked AS openings_available_total,
	booked_available_by_block_agg.blocks AS blocks_total,
	booked_available_by_block_agg.provider_type,
	booked_available_by_block_agg.visit_types,
	booked_available_by_block_agg.blocks_booked
INTO #booked_available_summary 
FROM #booked_available_by_block booked_available_by_block
LEFT OUTER JOIN #booked_available_by_block_agg booked_available_by_block_agg
ON booked_available_by_block_agg.DEPARTMENT_ID = booked_available_by_block.DEPARTMENT_ID
AND booked_available_by_block_agg.PROV_ID = booked_available_by_block.PROV_ID
AND booked_available_by_block_agg.SLOT_BEGIN_TIME = booked_available_by_block.SLOT_BEGIN_TIME
ORDER BY
	booked_available_by_block.DEPARTMENT_ID,
    booked_available_by_block.PROV_ID,
    booked_available_by_block.SLOT_BEGIN_TIME,
    booked_available_by_block.block_name

  CREATE NONCLUSTERED INDEX IX_booked_available_summary ON #booked_available_summary (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	STUFF((
	SELECT ',' + CAST(innerTable.block_name AS varchar(30)) + ' ' + CAST((innerTable.openings - innerTable.booked_total) AS VARCHAR(10))
	FROM #booked_available_summary AS innerTable
	WHERE innerTable.department_id = p.department_id
	AND innerTable.prov_id = p.prov_id
	AND innerTable.slot_begin_time = p.slot_begin_time
	FOR XML PATH('')
	),1,1,'') AS total_openings_available
INTO #booked_available_summary2
FROM #booked_available_summary p
WHERE openings > booked_total
GROUP BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME	
ORDER BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME	

  CREATE UNIQUE CLUSTERED INDEX IX_booked_available_summary2 ON #booked_available_summary2 (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)

SELECT
	s1.DEPARTMENT_ID,
	o.organization_name,
	s.service_name,
	c.clinical_area_name,
    s1.PROV_ID,
    s1.SLOT_BEGIN_TIME,
	CAST(s1.SLOT_BEGIN_TIME AS DATE) AS SLOT_BEGIN_DATE,
    s1.regular_openings,
    s1.openings,
    s1.block_name,
    s1.booked,
    s1.org_available_block_openings,
    s1.booked_total,
    s1.openings_available_total,
    s1.blocks_total,
	CASE
		WHEN ((s1.openings = s1.booked) AND (s1.booked_total = s1.booked)) THEN 'Y'
		WHEN ((s1.openings > s1.booked) AND (s1.booked_total = s1.booked)) THEN 'Y'
		WHEN (s1.blocks_total =1 AND s1.booked = 0) THEN 'Y'
		WHEN (s1.blocks_total >1 AND (s1.booked_total < s1.openings)) THEN 'Y'
		WHEN (s1.openings > 0 AND s1.booked_total = 0) THEN 'Y'
		WHEN ((s1.openings = s1.booked) AND (s1.booked_total > s1.booked)) THEN 'Y'
		WHEN ((s1.booked > s1.openings) AND (s1.booked_total = s1.booked)) THEN 'Y'
		ELSE NULL
	END AS 'Keep?',
    s1.overbook_openings,
    s1.outside_template_openings,
    s1.num_apts_scheduled,
	s1.num_regular_opening_apts_scheduled,
    s1.num_overbook_apts_scheduled,
    s1.num_outside_template_apts_scheduled,
	s1.provider_type,
	s1.visit_types,
	s1.blocks_booked,
	s2.total_openings_available AS total_openings_available_string
INTO #availability
FROM #booked_available_summary s1
LEFT OUTER JOIN #booked_available_summary2 s2
ON s2.DEPARTMENT_ID = s1.DEPARTMENT_ID
AND s2.PROV_ID = s1.PROV_ID
AND s2.SLOT_BEGIN_TIME = s1.SLOT_BEGIN_TIME
LEFT JOIN [CLARITY_App].[Mapping].[Epic_Dept_Groupers] g ON s1.DEPARTMENT_ID = g.epic_department_id
LEFT JOIN [CLARITY_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
LEFT JOIN [CLARITY_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
LEFT JOIN [CLARITY_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id

SELECT DISTINCT
	avail.DEPARTMENT_ID AS Department_Id,
	dep.DEPARTMENT_NAME AS Department_Name,
	avail.organization_name AS Organization,
	avail.service_name AS [Service],
	avail.clinical_area_name AS Clinical_Area,
    avail.PROV_ID AS Provider_Id,
	ser.PROV_NAME AS Provider_Name,
	avail.provider_type AS Provider_Type,
    avail.SLOT_BEGIN_TIME AS Slot_Begin_Time,
    avail.SLOT_BEGIN_DATE AS Slot_Begin_Date,
    avail.regular_openings AS Total_Regular_Openings,
    avail.overbook_openings AS Total_Overbook_Openings,
    avail.outside_template_openings AS Total_Outside_Template_Openings,
    avail.openings AS Total_Openings,
    avail.booked_total AS Total_Booked,
    avail.openings_available_total AS Total_Openings_Available,
    avail.blocks_total AS Blocks_Available,
	avail.blocks_booked AS Booked_Blocks,
	avail.visit_types AS Booked_Visit_Types,
	avail.total_openings_available_string AS Openings_Available
INTO #RptgTmp
FROM #availability avail
LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	ON dep.DEPARTMENT_ID = avail.DEPARTMENT_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
	ON ser.PROV_ID = avail.PROV_ID
WHERE [Keep?] = 'Y'
AND avail.openings > avail.booked

SELECT
	avail.Organization,
	avail.Service,
	avail.Clinical_Area,
	avail.Department_Name,
	avail.Provider_Name,
	avail.Slot_Begin_Date,
	avail.Slot_Begin_Time,
	avail.Openings_Available,
	SUM(avail.Total_Openings_Available) AS Total_Openings
FROM #RptgTmp avail
GROUP BY
	avail.Organization,
	avail.Service,
	avail.Clinical_Area,
	avail.Department_Name,
	avail.Provider_Name,
	avail.Slot_Begin_Date,
	avail.Slot_Begin_Time,
	avail.Openings_Available
ORDER BY
	avail.Organization,
	avail.Service,
	avail.Clinical_Area,
	avail.Department_Name,
	avail.Provider_Name,
	avail.Slot_Begin_Date,
	avail.Slot_Begin_Time,
	avail.Openings_Available