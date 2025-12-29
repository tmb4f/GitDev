USE CLARITY
/*
 ORG_AVAIL_BLOCKS
   
   The number of block openings initially available for block BLOCK_C before any appointments were booked using this block.
   A null value corresponds to an infinite number of block openings.
   Note that block openings are separate from slot openings.
   Column ORG_REG_OPENINGS  in table AVAILABILITY contains the original number of slot openings.
   For example, a slot could have two openings with 2 CONSULT blocks and 2 SAME DAY blocks.
   Since the slot is restricted to two openings it means either 2 CONSULTS, 2 SAME DAYS or one of each can be made.  However, 4 visits cannot be made.

   BLOCKS_USED

   The number of block openings used by appointments for this LINE and BLOCK_C block type.
*/

IF OBJECT_ID('tempdb..#avail_block ') IS NOT NULL
DROP TABLE #avail_block

SELECT blk.DEPARTMENT_ID
	 , blk.PROV_ID
	 , CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SLOT_DATE
	 , CAST(blk.SLOT_BEGIN_TIME AS TIME) AS SLOT_BEGIN_TIME
	 --, COALESCE(avail.APPT_BLOCK_NAME,'Unblocked') AS APPT_BLOCK_NAME
	 , blk.LINE
	 --, blk.BLOCK_C
	 , zab.NAME AS BLOCK_NAME
	 --, COALESCE(blk.ORG_AVAIL_BLOCKS,0) AS ORG_AVAIL_BLOCKS
	 , blk.ORG_AVAIL_BLOCKS
	 --, CASE WHEN COALESCE(blk.BLOCKS_USED,0) = 1 THEN 'Y' ELSE 'N' END AS BLOCKS_USED
	 , blk.BLOCKS_USED
	 --, blk.REL_BLOCK_C
	 --, zarb.NAME AS REL_BLOCK_NAME
	 --, zab.NAME + '(' + CAST(COALESCE(blk.BLOCKS_USED,0) AS VARCHAR(2)) + ')' AS BLOCK_NAME_COUNT
INTO #avail_block
FROM dbo.AVAIL_BLOCK blk
LEFT OUTER JOIN dbo.ZC_APPT_BLOCK zab
ON zab.APPT_BLOCK_C = blk.BLOCK_C
--LEFT OUTER JOIN dbo.ZC_APPT_BLOCK zarb
--ON zarb.APPT_BLOCK_C = blk.REL_BLOCK_C
--LEFT OUTER JOIN dbo.AVAIL_BLOCK blk
--ON blk.DEPARTMENT_ID = avail.DEPARTMENT_ID
--AND blk.PROV_ID = avail.PROV_ID
--AND blk.SLOT_BEGIN_TIME = avail.SLOT_BEGIN_TIME
--INNER JOIN @DevPeds DevPeds
--ON blk.PROV_ID = DevPeds.Provider_Id
WHERE 1 = 1
--blk.DEPARTMENT_ID = 10354008 -- UVBB PEDS DENTISTRY
--blk.DEPARTMENT_ID = 10419014 -- OCIR SPORTS MED
--blk.DEPARTMENT_ID = 10210002
--blk.DEPARTMENT_ID = 10243087
--blk.DEPARTMENT_ID IN (10210002,10210030,10243003,10243087,10244023)
AND zab.NAME LIKE 'follow up -%'
--AND CAST(blk.SLOT_BEGIN_TIME AS DATE) >= '8/3/2024' /*****/
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

SELECT *
FROM #avail_block
ORDER BY DEPARTMENT_ID
       , PROV_ID
	   , SLOT_DATE
	   , SLOT_BEGIN_TIME