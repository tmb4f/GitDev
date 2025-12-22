USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-------------------------------------------------------------------------------
/**********************************************************************************************************************
WHAT: Data Portal Metric for Missing Registration Items Workqueue User Productivity (Metric ID 989)
WHO : Tom Burgan
WHEN: 2025/02/21
WHY : Monitoring user activity in the Missing Reg Items workqueues
-----------------------------------------------------------------------------------------------------------------------
INFO: 
	Metric showing daily counts of added and released workqueue items attributed to users 
	Elapsed_Time: Duration between the creation instant and the end/released time of a workqueue item.
	This is filtered specifically to user activity in the Missing Reg Items workqueues.
     
		INPUTS:	  


		OUTPUTS:
			Granularity at the access team, supervisory org, and department level, query pulls the items that have been entered into and\or released from the workqueues by each user.
			Includes items in the "Patient" workqueues that contain the phrase "MISSING REG ITEMS".
			Elapsed time is the number of minutes between the START_TIME and END_TIME for released workqueue items.
			The event_date is the ENTRY_DATE documented on a workqueue item; if NULL, it is set to the EXIT_DATE value.
-----------------------------------------------------------------------------------------------------------------------
MODS: 	
	2025/03/09 - TMB-	Initital Creation
	2025/03/19 - TMB-	Edit logic to match existing report extract
	2025/04/30 - TMB-	Edit query to generate a summary of all workqueue activity by user
	2025/05/02 - TMB-	Add PAT_ENC_CSN_ID, remove service line columns
	2025/05/14 - TMB-	Add column WQ_USER_UVAID

**********************************************************************************************************************/

--ALTER PROCEDURE [ETL].[uspSrc_Missing_Reg_Item_WQ_Monitoring]
--AS 

SET NOCOUNT ON 

DECLARE @WQs TABLE (
	[WORKQUEUE_ID] [VARCHAR](18) NULL,
    [WORKQUEUE_NAME] [VARCHAR](200) NULL
);
	
INSERT INTO @WQs
(
    WORKQUEUE_ID,
    WORKQUEUE_NAME
)
VALUES
 ('18907','OP WOMEN POD MISSING REG ITEMS')
,('18908','OP CHILDREN POD MISSING REG ITEMS ')
,('19117','OP PSYCHIATRY POD MISSING REG ITEMS')
,('19396','COMMUNITY HEALTH - SPECIALTY CARE POD MISSING REG ITEMS')
,('19399','Community Health - Primary Care Pod Missing Reg Items for Appointments')
,('19400','Community Health - Culpeper Pod Missing Reg Items for Appointments')
,('20789','OP NEUROSURGERY MISSING REG ITEMS')
,('20790','OP NEUROLOGY POD MISSING REG ITEMS')
,('2550','OP CANCER POD MISSING REG ITEMS')
,('2551','OP HEART & VASCULAR POD MISSING REG ITEMS')
,('2553','OP NEUROSCIENCES POD MISSING REG ITEMS')
,('2554','OP CPG POD MISSING REG ITEMS')
,('2555','OP PRIMARY CARE POD MISSING REG ITEMS')
,('2556','OP RADIOLOGY POD MISSING REG ITEMS')
,('2557','OP SURGICAL/PROCEDURAL SPECIALTIES POD MISSING REG ITEMS')
,('2558','OP TRANSPLANT POD MISSING REG ITEMS')
,('2560','OP MUSCULOSKELETAL POD MISSING REG ITEMS')
,('2963','OP UVA HOPE CANCER CARE POD MISSING REG ITEMS')
,('3710','OP CULPEPER HEART & VASCULAR MISSING REG ITEMS')
,('3858','OP OPHTHALMOLOGY POD MISSING REG ITEMS')
,('3877','OP DIGESTIVE HEALTH POD MISSING REG ITEMS')
,('3902','OP NRDG COMMUNITY MEDICINE POD MISSING REG ITEMS')
;

DECLARE @startdate		SMALLDATETIME 
DECLARE @enddate		SMALLDATETIME

/*	Last two months of workqueue activity	*/
SET @startdate = DATEADD(DAY,-62,CAST(GETDATE() AS DATE))
SET @enddate = DATEADD(DAY,-1,CAST(GETDATE() AS DATE))

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#item_index ') IS NOT NULL
DROP TABLE #item_index

IF OBJECT_ID('tempdb..#detail ') IS NOT NULL
DROP TABLE #detail

IF OBJECT_ID('tempdb..#entry ') IS NOT NULL
DROP TABLE #entry

IF OBJECT_ID('tempdb..#exit ') IS NOT NULL
DROP TABLE #exit

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

IF OBJECT_ID('tempdb..#user_index ') IS NOT NULL
DROP TABLE #user_index

IF OBJECT_ID('tempdb..#user ') IS NOT NULL
DROP TABLE #user
;

WITH item_index AS 
	(	SELECT	ITEM_ID
		FROM	CLARITY..PAT_WQ_ITEMS		pawi
		INNER JOIN @WQs ON [@WQs].WORKQUEUE_ID = pawi.WORKQUEUE_ID
		WHERE	1 = 1
			AND ENTRY_DATE		BETWEEN @startdate	AND @enddate

			UNION

		SELECT	ITEM_ID
		FROM	CLARITY..PAT_WQ_ITEMS		pawi			
		INNER JOIN @WQs ON [@WQs].WORKQUEUE_ID = pawi.WORKQUEUE_ID		
		WHERE	1 = 1
			AND RELEASE_DATE	BETWEEN @startdate	AND @enddate
	)

SELECT * INTO #item_index FROM item_index
CREATE CLUSTERED INDEX  items ON #item_index (ITEM_ID)
;

WITH detail AS (
SELECT
		pawi.ITEM_ID
	,   pe.PAT_ENC_CSN_ID
	,   pe.EFFECTIVE_DEPT_ID AS DEPARTMENT_ID
	,	wquh.START_TIME 'ENTRY_DATE'
	,	wqux.END_TIME 'EXIT_DATE'
	,	COALESCE(wquh.USER_ID,wqxe.USER_ID,wqxe2.USER_ID) AS  'ENTRY_USER'
	,	wqux.USER_ID 'EXIT_USER'
	,	CASE WHEN wquh.USER_ID = wqux.USER_ID THEN 'SELF' ELSE 'OTHER' END	'USER_MATCH_YN'
	,	pawr.RULE_ID
	,	ccer.RULE_NAME
	,	ROW_NUMBER() OVER (PARTITION BY pawi.ITEM_ID ORDER BY pawr.LINE) 'RN'

FROM CLARITY..PAT_WQ_ITEMS		pawi			
		INNER JOIN	#item_index			itin	ON pawi.ITEM_ID = itin.ITEM_ID
        INNER JOIN CLARITY..PAT_ENC		   pe ON pawi.PAT_ENC_CSN_ID	   = pe.PAT_ENC_CSN_ID
		LEFT JOIN	CLARITY..WQ_USR_HISTORY		wquh	ON itin.ITEM_ID = wquh.WQ_ITM_ID		AND wquh.WQ_ACTIVITY_C = 1		--1=ENTRY
		LEFT JOIN	CLARITY..WQ_USR_HISTORY		wqxe	ON itin.ITEM_ID = wqxe.WQ_ITM_ID		AND wquh.LINE + 1 = wqxe.LINE	--Get next line for entry use identification	
		LEFT JOIN	CLARITY..WQ_USR_HISTORY		wqxe2	ON itin.ITEM_ID = wqxe2.WQ_ITM_ID	AND wquh.LINE + 2 = wqxe2.LINE	--Get next line for entry use identification
		LEFT JOIN	CLARITY..WQ_USR_HISTORY		wqux	ON itin.ITEM_ID = wqux.WQ_ITM_ID		AND wqux.WQ_ACTIVITY_C IN (3,9) --3=Release, 9=Manually removed	
		LEFT JOIN	CLARITY..PAT_WQI_RULES		pawr			ON pawi.ITEM_ID = pawr.ITEM_ID			AND wquh.LINE = pawr.LINE
		LEFT JOIN	CLARITY..CL_CHRG_EDIT_RULE	ccer	ON pawr.RULE_ID = ccer.RULE_ID
)

SELECT * INTO #detail FROM detail
CREATE CLUSTERED INDEX detail_index ON #detail (RN, ENTRY_DATE, EXIT_DATE, ITEM_ID)

;
WITH entrys AS
(
SELECT
	detail.DEPARTMENT_ID,
	detail.PAT_ENC_CSN_ID,
    detail.ENTRY_USER AS WQ_USER,
	CAST(CAST(detail.ENTRY_DATE AS DATE) AS SMALLDATETIME) AS event_date,
    detail.RULE_ID,
    detail.RULE_NAME
FROM #detail detail
WHERE detail.RN = 1
AND detail.ENTRY_DATE IS NOT NULL
)

SELECT
	detail.DEPARTMENT_ID,
	detail.PAT_ENC_CSN_ID,
    detail.WQ_USER,
    detail.event_date,
    detail.RULE_ID,
    detail.RULE_NAME,
	COUNT(*) AS Entry_Count
INTO #entry
FROM entrys detail
GROUP BY
	detail.DEPARTMENT_ID,
	detail.PAT_ENC_CSN_ID,
    detail.WQ_USER,
	detail.event_date,
    detail.RULE_ID,
    detail.RULE_NAME
	
CREATE CLUSTERED INDEX entry_index ON #entry (DEPARTMENT_ID, PAT_ENC_CSN_ID, WQ_USER, event_date, RULE_ID)
;
WITH exits AS
(
SELECT
	detail.DEPARTMENT_ID,
	detail.PAT_ENC_CSN_ID,
    detail.EXIT_USER AS WQ_USER,
	CAST(CAST(detail.EXIT_DATE AS DATE) AS SMALLDATETIME) AS event_date,
    detail.RULE_ID,
    detail.RULE_NAME
FROM #detail detail
WHERE detail.RN = 1
AND detail.EXIT_DATE IS NOT NULL
)

SELECT
	detail.DEPARTMENT_ID,
	detail.PAT_ENC_CSN_ID,
    detail.WQ_USER,
    detail.event_date,
    detail.RULE_ID,
    detail.RULE_NAME,
	COUNT(*) AS Exit_Count
INTO #exit
FROM exits detail
GROUP BY
	detail.DEPARTMENT_ID,
	detail.PAT_ENC_CSN_ID,
    detail.WQ_USER,
	detail.event_date,
    detail.RULE_ID,
    detail.RULE_NAME
	
CREATE CLUSTERED INDEX exit_index ON #exit (DEPARTMENT_ID, PAT_ENC_CSN_ID, WQ_USER, event_date, RULE_ID)
;
WITH summary_cte AS
(
SELECT
	COALESCE(entry.DEPARTMENT_ID,[exit].DEPARTMENT_ID)  AS DEPARTMENT_ID,
	COALESCE(entry.PAT_ENC_CSN_ID,[exit].PAT_ENC_CSN_ID)  AS PAT_ENC_CSN_ID,
	COALESCE(entry.WQ_USER,[exit].WQ_USER)  AS WQ_USER,
	COALESCE(entry.event_date,[exit].event_date)  AS event_date,
	COALESCE(entry.RULE_ID,[exit].RULE_ID)  AS RULE_ID,
	COALESCE(entry.RULE_NAME,[exit].RULE_NAME)  AS RULE_NAME,
    CASE WHEN entry.Entry_Count IS NULL THEN 0 ELSE entry.Entry_Count END AS Entry_Count,
    CASE WHEN [exit].Exit_Count IS NULL THEN 0 ELSE [exit].Exit_Count END AS Exit_Count
FROM #entry entry
FULL OUTER JOIN #exit [exit]
ON [exit].DEPARTMENT_ID = entry.DEPARTMENT_ID
AND [exit].PAT_ENC_CSN_ID = entry.PAT_ENC_CSN_ID
AND [exit].WQ_USER = entry.WQ_USER
AND [exit].event_date = entry.event_date
AND [exit].RULE_ID = entry.RULE_ID
)

SELECT summary.DEPARTMENT_ID,
	   summary.PAT_ENC_CSN_ID,
       summary.WQ_USER,
       summary.event_date,
       summary.RULE_ID,
       summary.RULE_NAME,
       summary.Entry_Count,
       summary.Exit_Count
INTO #summary
FROM summary_cte summary
ORDER BY WQ_USER, event_date, RULE_ID, DEPARTMENT_ID
CREATE CLUSTERED INDEX summary_index ON #summary (WQ_USER, DEPARTMENT_ID, event_date, RULE_ID)
;
WITH user_index_cte AS
(
SELECT DISTINCT
	WQ_USER
FROM #summary
)

SELECT WQ_USER, emp.NAME AS WQ_USER_NAME, emp.SYSTEM_LOGIN AS WQ_USER_UVAID INTO #user_index FROM user_index_cte wq LEFT JOIN	CLARITY..CLARITY_EMP emp ON emp.USER_ID = wq.WQ_USER
ORDER BY emp.SYSTEM_LOGIN
CREATE CLUSTERED INDEX user_index ON #user_index (WQ_USER_UVAID)
;
WITH user_cte AS
(
SELECT
	wq.WQ_USER,
	wq.WQ_USER_NAME,
	wq.WQ_USER_UVAID,
	WDemp.wd_Supervisory_Organization_ID,
	WDemp.wd_Supervisory_Organization_Description,
	at1.access_team_id,
	at1.access_team_name
FROM #user_index wq
		LEFT JOIN 
		(			 
				SELECT UVA_Computing_ID, wd_Employee_ID, wd_Supervisory_Organization_id, wd_Supervisory_Organization_Description, ROW_NUMBER() OVER (PARTITION BY wd_Employee_ID ORDER BY wd_sk_Effective_Date DESC) AS rownum
				FROM CLARITY_App.rptg.vwCrosswalk_All_ActiveWorkers 
				WHERE wd_Is_Active = 1 
				AND wd_IS_Position_Active = 1
				AND wd_Is_Primary_Job = 1		
		) WDemp ON wq.WQ_USER_UVAID = UPPER(WDemp.UVA_Computing_ID) AND WDemp.rownum = 1
		LEFT JOIN CLARITY_App.[Mapping].REF_Access_SupervisoryOrg_Map so1 ON so1.workday_supervisory_org_name = WDemp.wd_Supervisory_Organization_Description
		LEFT JOIN CLARITY_App.[Mapping].REF_Access_Team_Map at1         ON at1.sk_Ref_Access_Team_Map = so1.sk_Ref_Access_Team_Map
		)

SELECT user_cte.WQ_USER, user_cte.WQ_USER_NAME, user_cte.WQ_USER_UVAID, user_cte.wd_Supervisory_Organization_ID, user_cte.wd_Supervisory_Organization_Description, user_cte.access_team_id, user_cte.access_team_name INTO #user FROM user_cte
ORDER BY user_cte.WQ_USER
CREATE CLUSTERED INDEX [user] ON #user (WQ_USER)
-----------------------------------------------------------------
----BDD 4/8/2025 added insert to stage table. Assumes truncate handled in SSIS package
--INSERT INTO Stage.Dash_AccessManagement_MissingReqItem
--           (event_date
--           ,event_count
--           ,person_name
--           ,person_id
--           ,person_birth_date
--           ,person_gender
--           ,Fmonth_num
--           ,Fyear_num
--           ,Fyear_name
--           ,report_period
--           ,report_date
--           ,provider_id
--           ,provider_name
--           ,prov_service_line_id
--           ,prov_service_line
--           ,financial_division_id
--           ,financial_division_name
--           ,financial_sub_division_id
--           ,financial_sub_division_name
--           ,epic_department_id
--           ,epic_department_name
--           ,epic_department_name_external
--           ,rev_location_id
--           ,rev_location
--           ,pod_id
--           ,pod_name
--           ,hub_id
--           ,hub_name
--           ,hs_area_id
--           ,hs_area_name
--           ,practice_group_id
--           ,practice_group_name
--           ,upg_practice_region_id
--           ,upg_practice_region_name
--           ,upg_practice_id
--           ,upg_practice_name
--           ,upg_practice_flag
--           ,som_hs_area_id
--           ,som_hs_area_name
--           ,som_group_id
--           ,som_group_name
--           ,som_department_id
--           ,som_department_name
--           ,som_division_id
--           ,som_division_name
--           ,event_type
--           ,event_category
--           ,sk_Dim_Pt
--           ,peds
--           ,transplant
--           ,oncology
--           ,sk_Fact_Pt_Acct
--           ,sk_Fact_Pt_Enc_Clrt
--           ,sk_dim_physcn
--		   ,access_team_id
--		   ,access_team_name
--		   ,wd_Supervisory_Organization_ID
--		   ,wd_Supervisory_Organization_Description
--		   ,DEPARTMENT_ID
--		   ,PAT_ENC_CSN_ID
--		   ,WQ_USER
--		   ,WQ_USER_NAME
--		   ,RULE_ID
--		   ,RULE_NAME
--		   ,Entry_Count
--		   ,Exit_Count
--		   ,WQ_USER_UVAID -- VARCHAR(254)
		   --)
SELECT
	wq.event_date
,	1 AS event_count
/* Standard Fields */
	/* Patient info */
,	'person_name'					=	CAST(NULL AS VARCHAR(150))
,	'person_id'						=	CAST(NULL AS INT)
,	'person_birth_date'			=	CAST(NULL AS DATETIME)
,	'person_gender'				=	CAST(NULL AS VARCHAR(255))

	/* Date/times */
,	'Fmonth_num'					=	dd.Fmonth_num
,	'Fyear_num'						=	dd.Fyear_num
,	'Fyear_name'					=	dd.Fyear_name
,	'report_period'					=	CAST(LEFT(DATENAME(MONTH, dd.day_date), 3) + ' ' + CAST(DAY(dd.day_date) AS VARCHAR(2)) AS VARCHAR(10)) 
,	'report_date'						=	dd.day_date

/* Provider info */
,   'provider_id'					=	CAST(NULL AS INT)  -- cte.Prov_ID
,	'provider_name'					=	CAST(NULL AS VARCHAR(150)) --mdmprov.Prov_Nme
,	'prov_service_line_id'			=	CAST(NULL AS INT)	
,	'prov_service_line'				=	CAST(NULL AS VARCHAR(150)) --mdmprov.Service_Line		
,	'financial_division_id'				=	CAST(NULL AS INT)  -- CAST(REPLACE(mdmprov.Financial_Division,'na',NULL) AS INT) 
,	'financial_division_name'			=	CAST(NULL AS VARCHAR(150)) --CAST(mdmprov.Financial_Division_Name AS VARCHAR(150))
,	'financial_sub_division_id'			=	CAST(NULL AS INT)  --CAST(REPLACE(mdmprov.Financial_SubDivision,'na',NULL) AS INT) 
,	'financial_sub_division_name'			=	CAST(NULL AS VARCHAR(150)) --CAST(mdmprov.Financial_SubDivision_Name AS VARCHAR(150))

/* Fac/Org info */
,   'epic_department_id'			=	wq.DEPARTMENT_ID
,	'epic_department_name'			=	mdmdept.epic_department_name							
,	'epic_department_name_external'	=	mdmdept.epic_department_name_external							
,	'rev_location_id'				=	mdmdept.LOC_ID					
,	'rev_location'					=	mdmdept.REV_LOC_NAME				
,	'pod_id'						=	CAST(mdmdept.POD_ID	AS VARCHAR(66))	
,	'pod_name'						=	mdmdept.PFA_POD						
,	'hub_id'						=	CAST(mdmdept.HUB_ID AS VARCHAR(66))						
,	'hub_name'						=	mdmdept.HUB							

/* Service line info */
/*
,	'service_line_id'				=	mdmdept.service_line_id
,	'service_line'					=	mdmdept.service_line
,	'sub_service_line_id'			=	mdmdept.sub_service_line_id	
,	'sub_service_line'				=	mdmdept.sub_service_line 
,	'opnl_service_id'				=	mdmdept.opnl_service_id	
,	'opnl_service_name'				=	mdmdept.opnl_service_name 
,	'corp_service_line_id'			=	mdmdept.corp_service_line_id 
,	'corp_service_line_name'		=	mdmdept.corp_service_line
*/
,	'hs_area_id'					=	mdmdept.hs_area_id 
,	'hs_area_name'					=	mdmdept.hs_area_name 
,	'practice_group_id'				=	mdmdept.practice_group_id 
,	'practice_group_name'			=	mdmdept.practice_group_name	

/* UPG Practice Info */
,	'upg_practice_region_id'		=	CAST(vwdep.UPG_PRACTICE_REGION_ID	AS INT) 
,	'upg_practice_region_name'		=	CAST(vwdep.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) 
,	'upg_practice_id'				=	CAST(vwdep.UPG_PRACTICE_ID			AS INT) 
,	'upg_practice_name'				=	CAST(vwdep.UPG_PRACTICE_NAME		AS VARCHAR(150)) 
,	'upg_practice_flag'				=	CAST(vwdep.UPG_PRACTICE_FLAG		AS INT) 	

/* SOM info */
,	'som_hs_area_id'				=	CAST(NULL AS INT)  --orgmap.som_hs_area_id 
,	'som_hs_area_name'				=	CAST(NULL AS VARCHAR(150)) --CAST(orgmap.som_hs_area_name		AS VARCHAR(150))	
,	'som_group_id'					=	CAST(NULL AS INT)  --orgmap.som_group_id	
,	'som_group_name'				=	CAST(NULL AS VARCHAR(150))  --CAST(orgmap.som_group_name			AS VARCHAR(150)) 
,	'som_department_id'				=	CAST(NULL AS INT) --orgmap.department_id 
,	'som_department_name'			=	CAST(NULL AS VARCHAR(150))  --CAST(orgmap.department				AS VARCHAR(150)) 
,	'som_division_id'				=	CAST(NULL AS INT)  --orgmap.Org_Number 
,	'som_division_name'				=	CAST(NULL AS VARCHAR(150))  --CAST(orgmap.Organization			AS VARCHAR(150))	

/* Others */		
,	'event_type'					=	CAST('Missing Reg Items - Activity Freqency' AS VARCHAR(50)) 
,	'event_category'				=	CAST(NULL AS VARCHAR(150)) 
,	'sk_Dim_Pt'						=	CAST(NULL AS INT)
,	'peds' 								=	CAST(NULL AS SMALLINT)
,	'transplant'					=	CAST(NULL AS SMALLINT) 
,	'oncology'						=	CAST(NULL AS SMALLINT) 
,	'sk_Fact_Pt_Acct'				=	CAST(NULL AS BIGINT) 
,	'sk_Fact_Pt_Enc_Clrt'			=	CAST(NULL AS INT) 
,	'sk_dim_physcn'					=	CAST(NULL AS INT) 
,	[user].access_team_id
,	[user].access_team_name
,	[user].wd_Supervisory_Organization_ID
,	[user].wd_Supervisory_Organization_Description
,	wq.DEPARTMENT_ID
,   wq.PAT_ENC_CSN_ID
,	wq.WQ_USER
,	[user].WQ_USER_NAME
,	wq.RULE_ID
,	wq.RULE_NAME
,	wq.Entry_Count
,	wq.Exit_Count
,	[user].WQ_USER_UVAID -- VARCHAR(254)
FROM #summary wq
		LEFT JOIN #user [user]	ON [user].WQ_USER = wq.WQ_USER
		LEFT JOIN CLARITY..CLARITY_DEP	   DEP ON wq.DEPARTMENT_ID = DEP.DEPARTMENT_ID
		LEFT JOIN
					( --mdm history to replace vwRef_MDM_Location_Master_EpicSvc and vwRef_MDM_Location_Master
						SELECT DISTINCT
								hx.max_dt
								,rmlmh.EPIC_DEPARTMENT_ID
								,rmlmh.EPIC_DEPT_NAME AS epic_department_name
								,rmlmh.EPIC_EXT_NAME  AS epic_department_name_external
								,rmlmh.LOC_ID
								,rmlmh.REV_LOC_NAME
								,rmlmh.POD_ID
								,rmlmh.PFA_POD
								,rmlmh.HUB_ID
								,rmlmh.HUB
								,rmlmh.SERVICE_LINE_ID
								,rmlmh.SERVICE_LINE
								,rmlmh.SUB_SERVICE_LINE_ID
								,rmlmh.SUB_SERVICE_LINE
								,rmlmh.OPNL_SERVICE_ID
								,rmlmh.OPNL_SERVICE_NAME
								,rmlmh.CORP_SERVICE_LINE_ID
								,rmlmh.CORP_SERVICE_LINE
								,rmlmh.HS_AREA_ID
								,rmlmh.HS_AREA_NAME
								,rmlmh.PRACTICE_GROUP_ID
								,rmlmh.PRACTICE_GROUP_NAME
						FROM CLARITY_App.dbo.Ref_MDM_Location_Master_History AS rmlmh
							INNER JOIN
							( --hx--most recent batch date per dep id
								SELECT mdmhx.EPIC_DEPARTMENT_ID
										,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
								FROM CLARITY_App.dbo.Ref_MDM_Location_Master_History AS mdmhx
								GROUP BY mdmhx.EPIC_DEPARTMENT_ID
							)                                                 AS hx
								ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
									AND rmlmh.BATCH_RUN_DT = hx.max_dt
					)                                                        AS mdmdept
						ON mdmdept.EPIC_DEPARTMENT_ID = wq.DEPARTMENT_ID	
		LEFT JOIN	CLARITY_App.Rptg.vwCLARITY_DEP	vwdep		ON	wq.DEPARTMENT_ID = vwdep.DEPARTMENT_ID
		LEFT JOIN CLARITY_App.dbo.Dim_Date	dd	ON	wq.event_date = dd.day_date
--ORDER BY
--	access_team_id,
--	access_team_name,
--	wd_Supervisory_Organization_ID,
--	wd_Supervisory_Organization_Description,
--    DEPARTMENT_ID,
--	PAT_ENC_CSN_ID,
--    WQ_USER,
--	WQ_USER_NAME,
--    event_date,
--    RULE_ID,
--    RULE_NAME

GO


