USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [ETL].[uspSrc_WQ_Missing_Reg_Monitoring]
--    (
--     @StartDate DATE = NULL
--    ,@EndDate DATE = NULL
--	,@WQ VARCHAR(18) = NULL
--	,@de_control VARCHAR(10) = NULL
--    )
--AS 

DECLARE @StartDate DATE
DECLARE @EndDate DATE
DECLARE @de_control VARCHAR(10)
DECLARE @WQ VARCHAR(18)

SET @StartDate = '1/1/2025'
SET @EndDate = '1/8/2025'
SET @de_control = 'UVA-MC'
SET @WQ = '2555' -- OP PRIMARY CARE POD MISSING REG ITEMS

--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_WQ_Missing_Reg_Monitoring
--WHO : Tom Burgan
--WHEN: 2/11/25
--WHY : Report workqueue details for items in missing registration items workqueues.
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	
--				CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group
--				CLARITY.dbo.REG_HX
--				CLARITY.dbo.REG_HX_AFF_PAT_RECS
--				CLARITY.dbo.IDENTITY_ID
--				CLARITY.dbo.CLARITY_DEP
--				CLARITY_App.Rptg.MDM_REV_LOC_ID
--				CLARITY.dbo.CLARITY_EMP
--                
--      OUTPUTS:  [ETL].[uspSrc_WQ_Missing_Reg_Monitoring]
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--       02/11/2025 - TMB - create stored procedure
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default date range
    IF @StartDate IS NULL
        AND @EndDate IS NULL
		BEGIN
    --    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
			SET @StartDate = dateadd(month, datediff(month, 0, getdate())-1, 0)
			SET @EndDate = dateadd(day, -1, dateadd(month, datediff(month, 0, getdate()), 0))
		END
 
	--SELECT @StartDate, @EndDate

DECLARE @locstartdate DATE,
        @locenddate DATE
SET @locstartdate = @StartDate
SET @locenddate   = @EndDate
-------------------------------------------------------------------------------

/* 8:12, 3209375 without date filter
   5:54, 15533 with date filter
   6:53, 15526*/

IF OBJECT_ID('tempdb..#wqi ') IS NOT NULL
DROP TABLE #wqi

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

SELECT
	REG_HX_AFF_WQ_ITEM_ID,
	REG_HX_EVENT_C,
	REG_HX_USER_ID,
	REG_HX_LOGIN_DEP_ID,
	CLARITY.EPIC_UTIL.EFN_UTC_TO_LOCAL(REG_HX_INST_UTC_DTTM) AS REG_HX_INST_LOCAL_DTTM,
	REG_HX_OPEN_PAT_ID,
	REG_HX_OPEN_PAT_CSN,
	IDX.IDENTITY_ID				'MRN',
	CRTDEP.DEPARTMENT_NAME,
	REGHX.REG_HX_EVENT_ID,
	mloc.DE_HOSPITAL_CODE,
	mloc.HOSPITAL_GROUP
INTO #wqi
FROM CLARITY.dbo.REG_HX REGHX
LEFT OUTER JOIN CLARITY.dbo.REG_HX_AFF_PAT_RECS REGPT					ON REGHX.REG_HX_EVENT_ID=REGPT.REG_HX_EVENT_ID 
LEFT OUTER JOIN (SELECT *
									FROM CLARITY.dbo.IDENTITY_ID
									WHERE IDENTITY_TYPE_ID='14') IDX	
																												ON REGPT.REG_HX_AFF_PAT_ID=IDX.PAT_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP CRTDEP				ON REGHX.REG_HX_LOGIN_DEP_ID=CRTDEP.DEPARTMENT_ID
LEFT OUTER JOIN MDM_REV_LOC_ID mloc			                            ON mloc.REV_LOC_ID = CRTDEP.REV_LOC_ID		--Location Update
WHERE 1=1 
AND REGHX.REG_HX_AFF_WQ_ID=@WQ
AND REGHX.REG_HX_EVENT_C IN ('71','82', '72','77','73','74')  --Workqueue contact created /73 - Workqueue rules added / 82 - Workqueue contact returned from admin WQ
AND CLARITY.EPIC_UTIL.EFN_UTC_TO_LOCAL(REGHX.REG_HX_INST_UTC_DTTM) >= @StartDate
AND CLARITY.EPIC_UTIL.EFN_UTC_TO_LOCAL(REGHX.REG_HX_INST_UTC_DTTM) <= @EndDate

CREATE NONCLUSTERED INDEX IX_wqi ON #wqi (REG_HX_EVENT_C, REG_HX_INST_LOCAL_DTTM)

SELECT 
			REGHX.REG_HX_AFF_WQ_ITEM_ID
			,REGHX.REG_HX_EVENT_C
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('71', '82','73') 
						THEN 1
						ELSE 0
						END																		'WQ CREATED'
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('77', '72','74') 
						THEN 1
						ELSE 0
						END																		'WQ RESOLVED'
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('71', '82','73') 
						THEN REGHX.REG_HX_USER_ID
						ELSE ''
						END																		'WQITM_CREATED_USER'
			
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('71', '82','73') 
						THEN CONCAT(CRTEMP.NAME, ' ','(', CRTEMP.SYSTEM_LOGIN,')')	
						ELSE ''
						END																		'WQITM_CREATED_USER_ID'
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('71', '82','73')
						THEN REGHX.REG_HX_LOGIN_DEP_ID	
						ELSE 0
						END																		'WQITM_CREATED_DEPT_ID'
	
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('71', '82','73')
						THEN REGHX.DEPARTMENT_NAME	
						ELSE ''
						END																		'WQITM_CREATED_DEPT'
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('71', '82','73')
						THEN REGHX.REG_HX_INST_LOCAL_DTTM
						ELSE NULL
						END																		'WQITM_CREATED_DTTM'
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('72', '77','74')
						THEN REGHX.REG_HX_USER_ID		
						ELSE ''
						END																		'WQITM_RESOLVED_USER'
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('72', '77','74')
						THEN CONCAT( RSVEMP.NAME , ' ','(',RSVEMP.SYSTEM_LOGIN,')'	)
						ELSE ''
						END																		'WQITM_RESOLVED_USER_ID'
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('72', '77','74') 
						THEN REGHX.REG_HX_LOGIN_DEP_ID	
						ELSE 0
						END																		'WQITM_RESOLVED_DEPT_ID'
	
			,CASE WHEN REGHX.REG_HX_EVENT_C IN ('72', '77','74')
						THEN REGHX.DEPARTMENT_NAME	
						ELSE ''
						END																		'WQITM_RESOLVED_DEPT'
	 	    ,CASE WHEN REGHX.REG_HX_EVENT_C IN ('72', '77','74')
						THEN REGHX.REG_HX_INST_LOCAL_DTTM 
						ELSE ''
						END																		'WQITM_RESOLVED_DTTM'
	
			,REGHX.REG_HX_OPEN_PAT_ID
			,REGHX.REG_HX_OPEN_PAT_CSN
			,WQ.[# CONTACTS]
			,WQ.[# USERS]
			,REGHX.MRN
FROM #wqi REGHX
		--ACTIVITY IN WQ
 LEFT OUTER JOIN (
					SELECT HX.REG_HX_AFF_WQ_ITEM_ID		'WQ_ITM_ID'
							,COUNT(DISTINCT HX.REG_HX_USER_ID) '# USERS'
							,COUNT(HX.REG_HX_EVENT_ID) '# CONTACTS'
					FROM #wqi hx							
					GROUP BY HX.REG_HX_AFF_WQ_ITEM_ID
				  ) WQ												ON REGHX.REG_HX_AFF_WQ_ITEM_ID=WQ.WQ_ITM_ID 
		--REFERENCE
LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP CRTEMP							ON REGHX.REG_HX_USER_ID=CRTEMP.USER_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_EMP RSVEMP							ON REGHX.REG_HX_USER_ID=RSVEMP.USER_ID

WHERE 1=1
	AND
    (
    (UPPER(@de_control)=coalesce(UPPER(REGHX.de_hospital_code),'UVA-MC'))
    OR (UPPER(@de_control)=coalesce(UPPER(REGHX.hospital_group),'UVA-MC'))
    OR (UPPER(@de_control)='ALL')
    )
