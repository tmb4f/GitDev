USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-------------------------------------------------------------------------------
/**********************************************************************************************************************
WHAT: Data Portal Metric for Missing Registration Items Workqueue User Productivity
WHO : Tom Burgan
WHEN: 2025/02/21
WHY : Monitoring user activity in the Missing Reg Items workqueues
-----------------------------------------------------------------------------------------------------------------------
INFO: 
	Metric showing elapsed times for released workqueue items 
	Duration between the creation instant and the end/released time of a workqueue item.
	This is filtered specifically to user activity in the Missing Reg Items workqueues.
     
		INPUTS:	  


		OUTPUTS:
			Granularity at the workqueue item level, query pulls the items that have been released from the workqueue..  
			Includes items in the "Patient" workqueues that contain the phrase "MISSING REG ITEMS".
			Elapsed time is the number of minutes between the START_TIME and END_TIME for released workqueue items.
			The EXIT_USER-level count of released workqueue items measures workqueue activity productivity.
-----------------------------------------------------------------------------------------------------------------------
MODS: 	
	2025/03/09 - TMB-	Initital Creation
	2025/03/19 - TMB-	Edit logic to match existing report extract

**********************************************************************************************************************/

--ALTER PROCEDURE [ETL].[uspSrc_Missing_Reg_Item_WQ_Monitoring]
--	   (
--		@WORKQUEUE_ID VARCHAR(10) = NULL
--	   )
--AS 

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
--DECLARE  @WORKQUEUE_ID VARCHAR(10)

SET @startdate = NULL
SET @enddate = NULL
--SET @WORKQUEUE_ID = '2555'

/*----Get default Balanced Scorecard date range*/
IF			@startdate 	IS NULL
    	AND @enddate 	IS NULL
--EXEC	Clarity_App.ETL.usp_Get_Dash_Dates_BalancedScorecard 		@startdate 	OUTPUT
--																,	@enddate 	OUTPUT; 
SET @startdate = DATEADD(DAY,-62,CAST(GETDATE() AS DATE))
SET @enddate = DATEADD(DAY,-1,CAST(GETDATE() AS DATE))

--SELECT @startdate, @enddate

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#item_index ') IS NOT NULL
DROP TABLE #item_index

IF OBJECT_ID('tempdb..#user ') IS NOT NULL
DROP TABLE #user

IF OBJECT_ID('tempdb..#wqitem ') IS NOT NULL
DROP TABLE #wqitem
;

WITH item_index AS 
	(	SELECT	ITEM_ID
		FROM	CLARITY..PAT_WQ_ITEMS		pawi
		INNER JOIN @WQs ON [@WQs].WORKQUEUE_ID = pawi.WORKQUEUE_ID
		--WHERE	WORKQUEUE_ID = @WORKQUEUE_ID
		--	AND ENTRY_DATE		BETWEEN @startdate	AND @enddate
		WHERE	1 = 1
			AND ENTRY_DATE		BETWEEN @startdate	AND @enddate

			UNION

		SELECT	ITEM_ID
		FROM	CLARITY..PAT_WQ_ITEMS		pawi			
		INNER JOIN @WQs ON [@WQs].WORKQUEUE_ID = pawi.WORKQUEUE_ID		
		--WHERE	WORKQUEUE_ID = @WORKQUEUE_ID
		--	AND RELEASE_DATE	BETWEEN @startdate	AND @enddate	
		WHERE	1 = 1
			AND RELEASE_DATE	BETWEEN @startdate	AND @enddate
	)

SELECT * INTO #item_index FROM item_index
CREATE CLUSTERED INDEX  items ON #item_index (ITEM_ID)
;
/*
WITH wq_user AS 
	(	SELECT	DISTINCT pawi.CUR_ASGN_USER_ID AS [USER_ID]
		FROM	CLARITY..PAT_WQ_ITEMS		pawi
		INNER JOIN @WQs ON [@WQs].WORKQUEUE_ID = pawi.WORKQUEUE_ID
		--WHERE	WORKQUEUE_ID = @WORKQUEUE_ID
		--	AND ENTRY_DATE		BETWEEN @startdate	AND @enddate
		WHERE	1 = 1
			AND ENTRY_DATE		BETWEEN @startdate	AND @enddate

			UNION

		SELECT	DISTINCT pawi.CUR_ASGN_USER_ID AS [USER_ID]
		FROM	CLARITY..PAT_WQ_ITEMS		pawi			
		INNER JOIN @WQs ON [@WQs].WORKQUEUE_ID = pawi.WORKQUEUE_ID		
		--WHERE	WORKQUEUE_ID = @WORKQUEUE_ID
		--	AND RELEASE_DATE	BETWEEN @startdate	AND @enddate	
		WHERE	1 = 1
			AND RELEASE_DATE	BETWEEN @startdate	AND @enddate
	)

SELECT wq_user.USER_ID,
	   emp.NAME AS USER_NAME,
       emp.SYSTEM_LOGIN,
       WDemp.UVA_Computing_ID,
       WDemp.wd_Employee_ID,
       WDemp.wd_Supervisory_Organization_ID,
       WDemp.wd_Supervisory_Organization_Description,
       so1.workday_supervisory_org_name,
       so1.workday_supervisory_org_id,
       at1.access_team_id,
       at1.access_team_name
INTO #user
FROM wq_user
LEFT OUTER JOIN CLARITY..CLARITY_EMP emp ON emp.USER_ID = wq_user.USER_ID
LEFT JOIN 
(			 
		SELECT UVA_Computing_ID, wd_Employee_ID, wd_Supervisory_Organization_id, wd_Supervisory_Organization_Description, ROW_NUMBER() OVER (PARTITION BY wd_Employee_ID ORDER BY wd_sk_Effective_Date DESC) AS rownum
		FROM CLARITY_App.rptg.vwCrosswalk_All_ActiveWorkers 
		WHERE wd_Is_Active = 1 
		AND wd_IS_Position_Active = 1
		AND wd_Is_Primary_Job = 1			
) WDemp ON emp.SYSTEM_LOGIN = UPPER(WDemp.UVA_Computing_ID) AND WDemp.rownum = 1
LEFT JOIN CLARITY_App.[Mapping].REF_Access_SupervisoryOrg_Map so1 ON so1.workday_supervisory_org_name = WDemp.wd_Supervisory_Organization_Description
LEFT JOIN CLARITY_App.[Mapping].REF_Access_Team_Map at1         ON at1.sk_Ref_Access_Team_Map = so1.sk_Ref_Access_Team_Map
CREATE CLUSTERED INDEX  wq_user ON #user ([USER_ID])
*/
;

WITH detail AS (
SELECT	pawi.WORKQUEUE_ID
	,	wi.WORKQUEUE_NAME
	,	wi.DESCRIPTION
	,	pawi.ITEM_ID
	,	pawi.PAT_ENC_CSN_ID
	,	pawi.PAT_ID
	,   pe.EFFECTIVE_DEPT_ID AS DEPARTMENT_ID
	,	CASE WHEN pawi.RELEASE_DATE IS NULL THEN 'ACTIVE' WHEN pawi.RELEASE_DATE IS NOT NULL THEN 'RELEASED' ELSE 'OTHER' END 'ITEM_STATUS'
	,	wquh.START_TIME 'ENTRY_DATE'
	,	wqux.END_TIME 'EXIT_DATE'
	,	DATEDIFF(MINUTE,wquh.START_TIME,COALESCE(wqux.START_TIME,CAST(GETDATE() AS DATE)))	'Elapsed_Time'
	,	wquh.USER_ID 'ENTRY_USER'
	,	wqux.USER_ID 'EXIT_USER'
	,	CASE WHEN wquh.USER_ID = wqux.USER_ID THEN 'SELF' ELSE 'OTHER' END	'USER_MATCH_YN'
	,	pawr.RULE_ID
	,	ccer.RULE_NAME
	,	ROW_NUMBER() OVER (PARTITION BY pawi.ITEM_ID ORDER BY pawr.LINE) 'RN'

FROM CLARITY..PAT_WQ_ITEMS		pawi			
		INNER JOIN	#item_index			itin	ON pawi.ITEM_ID = itin.ITEM_ID
		INNER JOIN CLARITY..WORKQUEUE_INFO wi ON pawi.WORKQUEUE_ID = wi.WORKQUEUE_ID
        INNER JOIN CLARITY..PAT_ENC		   pe ON pawi.PAT_ENC_CSN_ID	   = pe.PAT_ENC_CSN_ID
		LEFT JOIN	CLARITY..DATE_DIMENSION		dadi		ON pawi.ENTRY_DATE = dadi.CALENDAR_DT
		LEFT JOIN	CLARITY..DATE_DIMENSION		dadx		ON pawi.RELEASE_DATE = dadx.CALENDAR_DT
		LEFT JOIN	CLARITY..WQ_USR_HISTORY		wquh	ON itin.ITEM_ID = wquh.WQ_ITM_ID		AND wquh.WQ_ACTIVITY_C = 1		--1=ENTRY
		LEFT JOIN	CLARITY..WQ_USR_HISTORY		wqux	ON itin.ITEM_ID = wqux.WQ_ITM_ID		AND wqux.WQ_ACTIVITY_C IN (3,9) --3=Release, 9=Manually removed		
		LEFT JOIN	CLARITY..WQ_USR_HISTORY		wqxr		ON itin.ITEM_ID = wqxr.WQ_ITM_ID		AND wqux.LINE - 1 = wqxr.LINE	--Get prev line for rule count
		LEFT JOIN	CLARITY..PAT_WQI_RULES		pawr			ON pawi.ITEM_ID = pawr.ITEM_ID			AND wquh.LINE = pawr.LINE
		LEFT JOIN	CLARITY..CL_CHRG_EDIT_RULE	ccer	ON pawr.RULE_ID = ccer.RULE_ID
)

SELECT
    wq.event_date,
    wq.event_count,
	wq.person_name,
	wq.person_id,
	wq.person_birth_date,
	wq.person_gender,
	wq.Fmonth_num,
	wq.Fyear_num,
	wq.Fyear_name,
	wq.report_date,
	wq.report_period,
    wq.provider_id,
    wq.provider_name,
    wq.prov_service_line_id,
    wq.prov_service_line,
    wq.financial_division_id,
    wq.financial_division_name,
    wq.financial_sub_division_id,
    wq.financial_sub_division_name,
	wq.epic_department_id,
	wq.epic_department_name,
	wq.epic_department_name_external,
	wq.rev_location_id,
	wq.rev_location,
    wq.pod_id,
	wq.pod_name,
	wq.hub_id,
	wq.hub_name,
	wq.service_line_id,
	wq.service_line,
	wq.sub_service_line_id,
	wq.sub_service_line,
	wq.corp_service_line_id,
	wq.corp_service_line_name,
	wq.opnl_service_id,
	wq.opnl_service_name,
	wq.hs_area_id,
	wq.hs_area_name,
	wq.practice_group_id,
	wq.practice_group_name,
	wq.upg_practice_region_id,
	wq.upg_practice_region_name,
	wq.upg_practice_id,
	wq.upg_practice_name,
	wq.upg_practice_flag,
    wq.som_hs_area_id,
    wq.som_hs_area_name,
    wq.som_group_id,
    wq.som_group_name,
    wq.som_department_id,
    wq.som_department_name,
    wq.som_division_id,
    wq.som_division_name,
    wq.event_type,
    wq.event_category,
	wq.sk_Dim_Pt,
    wq.peds,
    wq.transplant,
    wq.oncology,
    wq.sk_Fact_Pt_Acct,
    wq.sk_Fact_Pt_Enc_Clrt,
    wq.sk_dim_physcn,
	wq.WORKQUEUE_ID,
	wq.WORKQUEUE_NAME,
	wq.[DESCRIPTION],
    wq.ITEM_ID,
    wq.PAT_ENC_CSN_ID,
	wq.PAT_ID,
	wq.DEPARTMENT_ID,
    wq.ITEM_STATUS,
	wq.ENTRY_DATE,
	wq.EXIT_DATE,
    wq.Elapsed_Time,
    wq.ENTRY_USER,
	wq.ENTRY_NAME,
	wq.EXIT_USER,
	wq.EXIT_NAME,
    wq.USER_MATCH_YN,
    wq.RULE_ID,
    wq.RULE_NAME
	--wq.WORKQUEUE_ID, -- VARCHAR(18)
 --   wq.WORKQUEUE_NAME, -- VARCHAR(200)
 --   wq.DESCRIPTION, -- VARCHAR(1000)
	--wq.ITEM_ID, -- VARCHAR(18)
 --   wq.PAT_ID, -- VARCHAR(18)
 --   wq.ENTRY_DATE, -- DATETIME
 --   wq.ENTRY_USER, -- VARCHAR(60)
 --   wq.EXIT_DATE, -- DATETIME
 --   wq.EXIT_USER, -- VARCHAR(60)
 --   wq.RULE_NAMES, -- VARCHAR(1016)
	--wq.Elapsed_Time, -- INTEGER
	--wq.PAT_ENC_CSN_ID  -- NUMERICAL(18,0)

INTO #wqitem

FROM
(
SELECT
	mri.WORKQUEUE_ID
,  	mri.WORKQUEUE_NAME
,  	mri.[DESCRIPTION]
,   mri.ITEM_ID
,   mri.PAT_ENC_CSN_ID
,   mri.PAT_ID
,   mri.DEPARTMENT_ID
,   mri.ITEM_STATUS
,   mri.ENTRY_DATE
,   mri.EXIT_DATE
,   mri.Elapsed_Time
,   mri.ENTRY_USER
,   emp_entry.NAME AS ENTRY_NAME
--,   emp_entry.USER_NAME AS ENTRY_NAME
,   mri.EXIT_USER
,   emp_exit.NAME AS EXIT_NAME
--,   emp_exit.USER_NAME AS EXIT_NAME
,   mri.USER_MATCH_YN
,   mri.RULE_ID
,   mri.RULE_NAME
,   CONVERT(DATE, mri.EXIT_DATE) AS event_date--, -- Item Release Date
,   CASE WHEN mri.EXIT_DATE IS NOT NULL THEN 1 ELSE 0 END AS event_count
/* Standard Fields */
	/* Patient info */
,	'person_name'					=	pat.PAT_NAME
,	'person_id'						=	pat.PAT_MRN_ID
,	'person_birth_date'			=	pat.BIRTH_DATE
,	'person_gender'				=	zs.NAME

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
,   'epic_department_id'			=	mri.DEPARTMENT_ID
,	'epic_department_name'			=	mdmdept.epic_department_name							
,	'epic_department_name_external'	=	mdmdept.epic_department_name_external							
,	'rev_location_id'				=	mdmdept.LOC_ID					
,	'rev_location'					=	mdmdept.REV_LOC_NAME				
,	'pod_id'						=	CAST(mdmdept.POD_ID	AS VARCHAR(66))	
,	'pod_name'						=	mdmdept.PFA_POD						
,	'hub_id'						=	CAST(mdmdept.HUB_ID AS VARCHAR(66))						
,	'hub_name'						=	mdmdept.HUB							

/* Service line info */
,	'service_line_id'				=	mdmdept.service_line_id
,	'service_line'					=	mdmdept.service_line
,	'sub_service_line_id'			=	mdmdept.sub_service_line_id	
,	'sub_service_line'				=	mdmdept.sub_service_line 
,	'opnl_service_id'				=	mdmdept.opnl_service_id	
,	'opnl_service_name'				=	mdmdept.opnl_service_name 
,	'corp_service_line_id'			=	mdmdept.corp_service_line_id 
,	'corp_service_line_name'		=	mdmdept.corp_service_line 
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
,	'event_type'					=	CAST('Missing Reg Items - Elapsed Time' AS VARCHAR(50)) 
,	'event_category'				=	CAST(NULL AS VARCHAR(150)) 
,	'sk_Dim_Pt'						=	pt.sk_Dim_Pt
,	'peds' 								=	CAST(CASE
											WHEN FLOOR((CAST(dd.day_date AS INTEGER)
											-CAST(CAST(pat.BIRTH_DATE AS DATETIME) AS INTEGER)
											)/365.25
											)<18 THEN
											1
											ELSE 
											0
										END AS SMALLINT)
,	'transplant'					=	CAST(NULL AS SMALLINT) 
,	'oncology'						=	CAST(NULL AS SMALLINT) 
,	'sk_Fact_Pt_Acct'				=	CAST(NULL AS BIGINT) 
,	'sk_Fact_Pt_Enc_Clrt'			=	CAST(NULL AS INT) 
,	'sk_dim_physcn'					=	NULL --mdmprov.sk_Dim_Physcn

FROM
(
SELECT
	detail.WORKQUEUE_ID,
	detail.WORKQUEUE_NAME,
	detail.DESCRIPTION,
    detail.ITEM_ID,
    detail.PAT_ENC_CSN_ID,
	detail.PAT_ID,
	detail.DEPARTMENT_ID,
    detail.ITEM_STATUS,
	detail.ENTRY_DATE,
	detail.EXIT_DATE,
    detail.Elapsed_Time,
    detail.ENTRY_USER,
    detail.EXIT_USER,
    detail.USER_MATCH_YN,
    detail.RULE_ID,
    detail.RULE_NAME
FROM detail
WHERE detail.RN = 1
--AND detail.EXIT_DATE IS NOT NULL
) mri

		LEFT JOIN CLARITY..CLARITY_DEP	   DEP ON mri.DEPARTMENT_ID = DEP.DEPARTMENT_ID
		INNER JOIN CLARITY..PATIENT		   pat ON mri.PAT_ID			   = pat.PAT_ID
		LEFT JOIN CLARITY_App.dbo.Dim_Date	dd			ON	CONVERT(DATE, mri.EXIT_DATE) = dd.day_date
		LEFT JOIN CLARITY..ZC_SEX zs ON zs.RCPT_MEM_SEX_C = pat.SEX_C
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
						ON mdmdept.EPIC_DEPARTMENT_ID = mri.DEPARTMENT_ID	
		LEFT JOIN	CLARITY_App.Rptg.vwCLARITY_DEP	vwdep		ON	mri.DEPARTMENT_ID = vwdep.DEPARTMENT_ID			
		LEFT JOIN	CLARITY_App.Rptg.vwDim_Clrt_Pt pt ON mri.PAT_ID = pt.Clrt_PAT_ID
		LEFT JOIN	CLARITY..CLARITY_EMP emp_entry ON emp_entry.USER_ID = mri.ENTRY_USER
		LEFT JOIN	CLARITY..CLARITY_EMP emp_exit ON emp_exit.USER_ID = mri.EXIT_USER
		--LEFT JOIN	#user emp_entry ON emp_entry.USER_ID = mri.ENTRY_USER
		--LEFT JOIN	#user emp_exit ON emp_exit.USER_ID = mri.EXIT_USER
/*
		LEFT JOIN 
		(			 
				SELECT UVA_Computing_ID, wd_Employee_ID, wd_Supervisory_Organization_id, wd_Supervisory_Organization_Description, ROW_NUMBER() OVER (PARTITION BY wd_Employee_ID ORDER BY wd_sk_Effective_Date DESC) AS rownum
				FROM CLARITY_App.rptg.vwCrosswalk_All_ActiveWorkers 
				WHERE wd_Is_Active = 1 
				AND wd_IS_Position_Active = 1
				AND wd_Is_Primary_Job = 1			
		) WDemp_entry ON emp_entry.SYSTEM_LOGIN = UPPER(WDemp_entry.UVA_Computing_ID) AND WDemp_entry.rownum = 1
		LEFT JOIN CLARITY_App.[Mapping].REF_Access_SupervisoryOrg_Map so1 ON so1.workday_supervisory_org_name = WDemp_entry.wd_Supervisory_Organization_Description
		LEFT JOIN CLARITY_App.[Mapping].REF_Access_Team_Map at1         ON at1.sk_Ref_Access_Team_Map = so1.sk_Ref_Access_Team_Map
		LEFT JOIN 
		(			 
				SELECT UVA_Computing_ID, wd_Employee_ID, wd_Supervisory_Organization_id, wd_Supervisory_Organization_Description, ROW_NUMBER() OVER (PARTITION BY wd_Employee_ID ORDER BY wd_sk_Effective_Date DESC) AS rownum
				FROM CLARITY_App.rptg.vwCrosswalk_All_ActiveWorkers 
				WHERE wd_Is_Active = 1 
				AND wd_IS_Position_Active = 1
				AND wd_Is_Primary_Job = 1			
		) WDemp_exit ON emp_exit.SYSTEM_LOGIN = UPPER(WDemp_exit.UVA_Computing_ID) AND WDemp_exit.rownum = 1
		LEFT JOIN CLARITY_App.[Mapping].REF_Access_SupervisoryOrg_Map so2 ON so2.workday_supervisory_org_name = WDemp_exit.wd_Supervisory_Organization_Description
		LEFT JOIN CLARITY_App.[Mapping].REF_Access_Team_Map at2         ON at2.sk_Ref_Access_Team_Map = so2.sk_Ref_Access_Team_Map
*/
/*LEFT JOIN 

	(			 

							SELECT UVA_Computing_ID, wd_Employee_ID, wd_Supervisory_Organization_id, wd_Supervisory_Organization_Description, ROW_NUMBER() OVER (PARTITION BY wd_Employee_ID ORDER BY wd_sk_Effective_Date DESC) AS rownum

							FROM CLARITY_App.rptg.vwCrosswalk_All_ActiveWorkers 

							WHERE wd_Is_Active = 1 

							AND wd_IS_Position_Active = 1

							AND wd_Is_Primary_Job = 1			

	) WDemp ON emp.SYSTEM_LOGIN = UPPER(WDemp.UVA_Computing_ID) AND WDemp.rownum = 1

LEFT JOIN CLARITY_App.[Mapping].REF_Access_SupervisoryOrg_Map so1 ON so1.workday_supervisory_org_name = WDemp.wd_Supervisory_Organization_Description

LEFT JOIN CLARITY_App.[Mapping].REF_Access_Team_Map at1         ON at1.sk_Ref_Access_Team_Map = so1.sk_Ref_Access_Team_Map
 */

) wq
--ORDER BY
--	wq.WORKQUEUE_NAME,
--	wq.ITEM_ID

SELECT
    WORKQUEUE_NAME,
    ENTRY_USER,
    ENTRY_NAME,
	--CASE WHEN ENTRY_NAME = 'HAYS, ANDREA' THEN 1 ELSE 0 END AS SelfEntry,
    EXIT_USER,
    EXIT_NAME,
	--CASE WHEN EXIT_NAME = 'HAYS, ANDREA' THEN 1 ELSE 0 END AS SelfExit,
    ENTRY_DATE,
    EXIT_DATE,
    WORKQUEUE_ID,
    DESCRIPTION,
    ITEM_ID,
    PAT_ENC_CSN_ID,
    PAT_ID,
    DEPARTMENT_ID,
    epic_department_name,
    ITEM_STATUS,
    Elapsed_Time,
    USER_MATCH_YN,
    RULE_ID,
    RULE_NAME
	--event_date,
 --   event_count,
 --   person_name,
 --   person_id,
 --   person_birth_date,
 --   person_gender,
 --   Fmonth_num,
 --   Fyear_num,
 --   Fyear_name,
 --   report_date,
 --   report_period,
 --   provider_id,
 --   provider_name,
 --   prov_service_line_id,
 --   prov_service_line,
 --   financial_division_id,
 --   financial_division_name,
 --   financial_sub_division_id,
 --   financial_sub_division_name,
 --   epic_department_id,
 --   epic_department_name_external,
 --   rev_location_id,
 --   rev_location,
 --   pod_id,
 --   pod_name,
 --   hub_id,
 --   hub_name,
 --   service_line_id,
 --   service_line,
 --   sub_service_line_id,
 --   sub_service_line,
 --   corp_service_line_id,
 --   corp_service_line_name,
 --   opnl_service_id,
 --   opnl_service_name,
 --   hs_area_id,
 --   hs_area_name,
 --   practice_group_id,
 --   practice_group_name,
 --   upg_practice_region_id,
 --   upg_practice_region_name,
 --   upg_practice_id,
 --   upg_practice_name,
 --   upg_practice_flag,
 --   som_hs_area_id,
 --   som_hs_area_name,
 --   som_group_id,
 --   som_group_name,
 --   som_department_id,
 --   som_department_name,
 --   som_division_id,
 --   som_division_name,
 --   event_type,
 --   event_category,
 --   sk_Dim_Pt,
 --   peds,
 --   transplant,
 --   oncology,
 --   sk_Fact_Pt_Acct,
 --   sk_Fact_Pt_Enc_Clrt,
 --   sk_dim_physcn,
FROM #wqitem wq
--WHERE (wq.ENTRY_NAME = 'HAYS, ANDREA' OR wq.EXIT_NAME = 'HAYS, ANDREA') -- 89402
--WHERE (wq.ENTRY_USER = '89402' OR wq.EXIT_USER = '89402')
--AND wq.ENTRY_NAME NOT IN  (
-- 'JOB, BATCH'
--,'PRELUDE, RTE0005'
--,'RTE, USER'
--,'BATCH, RTEIN'
--,'HB, BACKGROUND'
--,'INTERFACE, RTE INCOMING'
--,'MYCHART, GENERIC'
--,'EOD, CADENCE'
--,'EPIC, USER'
--,'ORDERS, RADIOLOGY/CARDIOLOGY'
--)
--AND wq.EXIT_NAME NOT IN  (
-- 'JOB, BATCH'
--,'PRELUDE, RTE0005'
--,'RTE, USER'
--,'BATCH, RTEIN'
--,'HB, BACKGROUND'
--,'INTERFACE, RTE INCOMING'
--,'MYCHART, GENERIC'
--,'EOD, CADENCE'
--,'EPIC, USER'
--,'ORDERS, RADIOLOGY/CARDIOLOGY'
--)

ORDER BY
    wq.WORKQUEUE_NAME,
	wq.EXIT_NAME,
	wq.ENTRY_NAME,
	wq.EXIT_DATE

GO


