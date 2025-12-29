USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL, 
        @enddate SMALLDATETIME = NULL

SET @startdate = '7/1/2024 00:00 AM'
SET @enddate = '1/31/2025 11:59 PM'

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
DECLARE @slotstartdate DATETIME,
        @slotenddate DATETIME
SET @slotstartdate = CAST(@startdate AS DATETIME)
SET @slotenddate   = CAST(@enddate AS DATETIME)
-------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#blkdtl') IS NOT NULL
DROP TABLE #blkdtl

SELECT blk.DEPARTMENT_ID
	 , blk.SLOT_BEGIN_TIME
	 , ser.PROV_TYPE
	 , blk.PROV_ID
	 , zab.NAME AS APPT_BLOCK_NAME
	 , COALESCE(blk.ORG_AVAIL_BLOCKS,0) AS ORG_AVAIL_BLOCKS
	 , blk.BLOCKS_USED AS BLOCKS_USED
INTO #blkdtl
FROM CLARITY.dbo.AVAIL_BLOCK blk
INNER JOIN CLARITY_App.Rptg.vwDim_Date dd				ON CAST(CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SMALLDATETIME)  = dd.day_date
LEFT OUTER JOIN CLARITY.dbo.ZC_APPT_BLOCK zab
ON zab.APPT_BLOCK_C = blk.BLOCK_C

	LEFT OUTER JOIN
	(
	SELECT
		PROV_ID,
		PROVIDER_TYPE_C,
		PROV_TYPE
	FROM CLARITY.dbo.CLARITY_SER
	) ser
	ON ser.PROV_ID = blk.PROV_ID
   WHERE 1=1
    AND CAST(CAST(blk.SLOT_BEGIN_TIME AS DATE) AS DATETIME) >= @slotstartdate
    AND CAST(CAST(blk.SLOT_BEGIN_TIME AS DATE) AS DATETIME) <  @slotenddate
	AND blk.DEPARTMENT_ID = 10212016
	AND ser.PROVIDER_TYPE_C IN ('6',  -- 	Physician Assistant
															'9')  -- 	Nurse Practitioner'
	AND NOT (
				(blk.BLOCK_C IS NULL) AND (blk.ORG_AVAIL_BLOCKS IS NULL) AND (blk.BLOCKS_USED IS NULL)
			)

ORDER BY blk.DEPARTMENT_ID
	   , blk.SLOT_BEGIN_TIME
	   , ser.PROV_TYPE
	   , blk.PROV_ID

  -- Create index for temp table #blksum

CREATE NONCLUSTERED INDEX IX_blkdtl ON #blkdtl ([DEPARTMENT_ID], APPT_BLOCK_NAME, SLOT_BEGIN_TIME, PROV_TYPE, PROV_ID)

SELECT
	blk.DEPARTMENT_ID,
    blk.APPT_BLOCK_NAME,
    blk.SLOT_BEGIN_TIME,
    blk.PROV_TYPE,
	blk.PROV_ID,
    blk.ORG_AVAIL_BLOCKS,
    blk.BLOCKS_USED
FROM #blkdtl blk
ORDER BY blk.DEPARTMENT_ID
	   , blk.APPT_BLOCK_NAME
	   , blk.SLOT_BEGIN_TIME
	   , blk.PROV_TYPE
	   , blk.PROV_ID
GO


