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
	2025/02/21 - TMB-	Initital Creation

**********************************************************************************************************************/

--ALTER PROCEDURE [ETL].[uspSrc_Missing_Reg_Item_WQ_Monitoring.sql]
--AS 

DECLARE @startdate		SMALLDATETIME 
DECLARE @enddate		SMALLDATETIME 

/*----Get default Balanced Scorecard date range*/
IF			@startdate 	IS NULL
    	AND @enddate 	IS NULL
EXEC	Clarity_App.ETL.usp_Get_Dash_Dates_BalancedScorecard 		@startdate 	OUTPUT
																,	@enddate 	OUTPUT;  
SET NOCOUNT ON;

SELECT
	wq.ITEM_ID,
    wq.CREATION_INSTANCE,
    wq.EXIT_DATE,
    wq.PAT_ID,
    --wq.PAT_NAME,
    --wq.PAT_MRN_ID,
    wq.ENTRY_USER,
    wq.EXIT_USER,
    wq.EXIT_ACTIVITY,
    wq.Elapsed_Time,
    wq.WORKQUEUE_NAME,
    wq.WORKQUEUE_ID,
    wq.EFFECTIVE_DEPT_ID,
    wq.DEPARTMENT_NAME,
    wq.PAT_ENC_CSN_ID,
	--wq.LINE,
	--wq.EXTRACT_DATE,
	wq.RULE_ID,
	wq.RULE_NAME,
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
    wq.sk_dim_physcn
FROM
(
SELECT
    p.ITEM_ID
  , p.CREATION_INSTANCE
  , hx_out.EXIT_DATE
  , pe.PAT_ID
  --, pat.PAT_NAME
  --, pat.PAT_MRN_ID
  , hx_in.ENTRY_USER
  , hx_out.EXIT_USER
  , hx_out.EXIT_ACTIVITY
  , DATEDIFF(MINUTE, hx_in.ENTRY_DATE, hx_out.EXIT_DATE) AS Elapsed_Time
  , WI.WORKQUEUE_NAME
  , p.WORKQUEUE_ID
  , pe.EFFECTIVE_DEPT_ID
  , DEP.DEPARTMENT_NAME
  , p.PAT_ENC_CSN_ID
  , CONVERT(DATE, hx_out.EXIT_DATE) AS event_date--, -- Item Release Date
  , CASE WHEN hx_out.EXIT_DATE IS NOT NULL THEN 1 ELSE 0 END AS event_count
  --, iid.IDENTITY_ID AS MRN
  --, [PATIENT_MRN]	= CONCAT(pat.PAT_NAME, ' [', iid.IDENTITY_ID, ']')
  --, [HAR_BALANCE]	= ISNULL(har.TOT_ACCT_BAL, 0)
  --, DEP.DEPT_ABBREVIATION
  --, pwr.LINE
  , pwr.RULE_ID
  --, pwr.EXTRACT_DATE
  , ccer.RULE_NAME
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
,   'epic_department_id'			=	pe.EFFECTIVE_DEPT_ID
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
--,	'peds' 							=	CAST(NULL AS SMALLINT) 
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

FROM	CLARITY..PAT_WQ_ITEMS			   p
		INNER JOIN CLARITY..WORKQUEUE_INFO WI ON p.WORKQUEUE_ID		   = WI.WORKQUEUE_ID AND wi.WORKQUEUE_TYPE_C = 3 -- PATIENT WQS ONLY
		INNER JOIN CLARITY..PAT_ENC		   pe ON p.PAT_ENC_CSN_ID	   = pe.PAT_ENC_CSN_ID
		LEFT JOIN CLARITY..CLARITY_DEP	   DEP ON pe.EFFECTIVE_DEPT_ID = DEP.DEPARTMENT_ID
		--LEFT JOIN CLARITY..HSP_ACCOUNT	   har ON pe.HSP_ACCOUNT_ID	   = har.HSP_ACCOUNT_ID
		INNER JOIN CLARITY..PATIENT		   pat ON pe.PAT_ID			   = pat.PAT_ID
		--INNER JOIN CLARITY..IDENTITY_ID	   iid ON pat.PAT_ID		   = iid.PAT_ID AND iid.IDENTITY_TYPE_ID = 14

		INNER JOIN (
			SELECT
			   hitem.WQ_ITM_ID
			  ,[ENTRY_DATE] = CASE WHEN hitem.WQ_ACTIVITY_C = 1 AND line = 1 THEN hitem.START_TIME END
			  ,[ENTRY_USER] = CASE WHEN hitem.WQ_ACTIVITY_C = 1 AND line = 1 THEN hitem.NAME    END
	
			FROM(
				SELECT hx.WQ_ITM_ID
					 , hx.LINE
					 , hx.START_TIME
					 , hx.END_TIME
					 , emp.NAME
					 , hx.WQ_ACTIVITY_C
					 ,ZC_WQ_ACTIVITY.NAME[ACTIVITY]
	
				FROM clarity..WQ_USR_HISTORY hx
					LEFT JOIN CLARITY..ZC_WQ_ACTIVITY ZC_WQ_ACTIVITY ON ZC_WQ_ACTIVITY.WQ_ACTIVITY_C = hx.WQ_ACTIVITY_C
					INNER JOIN clarity..clarity_emp emp ON hx.USER_ID = emp.USER_ID
				WHERE 1=1
					AND hx.WQ_ACTIVITY_C =1 -- Entry
				)hitem
			)hx_in ON p.ITEM_ID = hx_in.WQ_ITM_ID 
	INNER JOIN (
			SELECT
				hitem.WQ_ITM_ID
			  ,[EXIT_DATE] = CASE WHEN hitem.WQ_ACTIVITY_C = 3 AND hitem.END_TIME IS NOT NULL  THEN hitem.END_TIME END
			  ,[EXIT_USER] = CASE WHEN hitem.WQ_ACTIVITY_C = 3 AND hitem.END_TIME IS NOT NULL  THEN hitem.name END
			  ,[EXIT_ACTIVITY] = CASE WHEN hitem.WQ_ACTIVITY_C = 3 AND hitem.END_TIME IS NOT NULL  THEN hitem.ACTIVITY END
			FROM(
				SELECT hx.WQ_ITM_ID
					 , hx.LINE
					 , hx.START_TIME
					 , hx.END_TIME
					 , emp.name
					 , hx.WQ_ACTIVITY_C
					 ,ZC_WQ_ACTIVITY.NAME[ACTIVITY]
				FROM clarity..WQ_USR_HISTORY hx
					LEFT JOIN CLARITY..ZC_WQ_ACTIVITY ZC_WQ_ACTIVITY ON ZC_WQ_ACTIVITY.WQ_ACTIVITY_C = hx.WQ_ACTIVITY_C
					INNER JOIN clarity..clarity_emp emp ON hx.USER_ID = emp.USER_ID
				WHERE 1=1		
				AND (hx.WQ_ACTIVITY_C = 3 AND hx.HX_TAB_NUMBER_C IS NULL) -- Release
				)hitem
	)hx_out ON p.ITEM_ID = hx_out.WQ_ITM_ID 
		LEFT JOIN dbo.Dim_Date	dd			ON	CONVERT(DATE, hx_out.EXIT_DATE) = dd.day_date
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
						ON mdmdept.EPIC_DEPARTMENT_ID = pe.EFFECTIVE_DEPT_ID	
		LEFT JOIN	Rptg.vwCLARITY_DEP	vwdep		ON	pe.EFFECTIVE_DEPT_ID = vwdep.DEPARTMENT_ID			
		LEFT JOIN	Rptg.vwDim_Clrt_Pt pt ON pe.PAT_ID = pt.Clrt_PAT_ID
		LEFT JOIN
		(
			SELECT
				ITEM_ID,
                --LINE,
                RULE_ID,
                --EXTRACT_DATE,
				ROW_NUMBER() OVER(PARTITION BY ITEM_ID ORDER BY EXTRACT_DATE DESC) AS ltst_row
			FROM CLARITY..PAT_WQI_RULES
		) pwr		ON pwr.ITEM_ID = p.ITEM_ID AND pwr.ltst_row = 1
		LEFT OUTER JOIN CLARITY..CL_CHRG_EDIT_RULE ccer	ON ccer.RULE_ID = pwr.RULE_ID
WHERE 1 = 1
--WHERE p.WORKQUEUE_ID IN ( @Workqueue)
--WHERE WI.WORKQUEUE_NAME LIKE '%MISSING REG ITEMS%'
AND p.WORKQUEUE_ID = 2555 -- OP PRIMARY CARE POD MISSING REG ITEMS
--AND p.WORKQUEUE_ID = 2560 -- OP MUSCULOSKELETAL POD MISSING REG ITEMS
--AND p.ITEM_ID = '658937011'
--AND p.ITEM_ID BETWEEN '645763369' AND '648698901'
--AND  CAST(hx_out.EXIT_DATE AS DATE)  BETWEEN @startdate AND @enddate
AND  CAST(hx_out.EXIT_DATE AS DATE)  BETWEEN '1/1/2025' AND '1/9/2025'
--WHERE wq.ENTRY_USER = wq.EXIT_USER
) wq
ORDER BY
	wq.WORKQUEUE_NAME,
	wq.ITEM_ID--,
	--wq.LINE
-----------------------------------------------------------------------------------------------------------------------------------------------------
/*
SELECT cte.REQUEST_ID,
       cte.Status_Name,
	   cte.PAT_ID,
       cte.PAT_NAME,
       cte.PAT_MRN_ID,
       cte.PRIMARY_PRC,
       cte.APPT_SERIAL_NUM,
       cte.APPT_DTTM,
       cte.VisitStatus,
       cte.APPOINTMENT_DEPARTMENT,
	   cte.RESPONSIBLE_DEPARTMENT_ID,
       cte.RESPONSIBLE_DEPARTMENT,
       cte.Responsible_Department_OneTeam_Specialty,
       cte.Responsible_Department_Wave,
       cte.Responsible_Department_StartDate,
       cte.Responsible_Department_WaveStartDate,
       cte.REFERRED_TO_DEPARTMENT,
       cte.CREATED_DATE,
       cte.Canceled,
       cte.CanceledDTTM,
       cte.CanceledReason,
       cte.APPT_MADE_DTTM,
       cte.TIMEOUT_ID,
       cte.PAT_ENC_CSN_ID,
       cte.DeficiencyID,
       cte.TaskName,
       cte.AddedDateTime,
       cte.TaskResult,
       cte.CompletedNotNeededDateTime,
       cte.CompletionUser,
       cte.CompletedByDOS,
       cte.APPTScheduledWithin5BusinessDays,
       cte.APPTScheduledOver5BusinessDays,
       cte.Numerator,
       cte.Denominator,
	   CONVERT(DATE,cte.AddedDateTime) AS event_date, -- Task Added Date
	   cte.Numerator AS event_count
/* Standard Fields */
	/* Date/times */
,	'Fmonth_num'					=	dd.Fmonth_num
,	'Fyear_num'						=	dd.Fyear_num
,	'Fyear_name'					=	dd.Fyear_name


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
,   'epic_department_id'			=	cte.RESPONSIBLE_DEPARTMENT_ID
,	'epic_department_name'			=	mdmdept.epic_department_name							
,	'epic_department_name_external'	=	mdmdept.epic_department_name_external							
,	'rev_location_id'				=	mdmdept.LOC_ID					
,	'rev_location'					=	mdmdept.REV_LOC_NAME				
,	'pod_id'						=	CAST(mdmdept.POD_ID	AS VARCHAR(66))	
,	'pod_name'						=	mdmdept.PFA_POD								

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
,	'event_type'					=	CAST('Collect Records - Access Completion' AS VARCHAR(50)) 
,	'event_category'				=	CAST(NULL AS VARCHAR(150)) 
,	'sk_Dim_Pt'						=	p.sk_Dim_Pt
,	'peds' 							=	CAST(NULL AS SMALLINT) 
,	'transplant'					=	CAST(NULL AS SMALLINT) 
,	'oncology'						=	CAST(NULL AS SMALLINT) 
,	'sk_Fact_Pt_Acct'				=	CAST(NULL AS BIGINT) 
,	'sk_Fact_Pt_Enc_Clrt'			=	CAST(NULL AS INT) 
,	'sk_dim_physcn'					=	NULL --mdmprov.sk_Dim_Physcn
,cte.SpinalFlag AS Spinal_Flag  --New field 
,cte.Incomplete_Task_Flag
,cte.Incomplete_Task_Reason
,cte.Incomplete_Task_Add_Date
,cte.Incomplete_Task_Completed_User
FROM 
(

	SELECT r.REQUEST_ID,
			s.NAME AS Status_Name,
			p.PAT_ID,
			p.PAT_NAME,
			p.PAT_MRN_ID,
			prc.PRC_NAME AS PRIMARY_PRC,
			CONVERT( VARCHAR(15),appt.APPT_SERIAL_NUM) APPT_SERIAL_NUM,
		   appt.APPT_DTTM,
		   appt.VisitStatus,
		   apptd.DEPARTMENT_NAME AS APPOINTMENT_DEPARTMENT,
		   r.RESPONSIBLE_DEPARTMENT_ID,
		   rd.DEPARTMENT_NAME AS RESPONSIBLE_DEPARTMENT,
		   od.Specialty AS Responsible_Department_OneTeam_Specialty,
		   od.Wave AS Responsible_Department_Wave,
		   od.StartDate AS Responsible_Department_StartDate,
		   od.WaveStartDate  AS Responsible_Department_WaveStartDate,
		   refdep.DEPARTMENT_NAME AS REFERRED_TO_DEPARTMENT,
		   CASE WHEN r.RESPONSIBLE_DEPARTMENT_ID IN
			(10211021,10214001,10214003,10250001,10348010,10363002,10385001,10419013,10419016) 
			THEN 1 ELSE 0 END AS SpinalFlag,
		   r.CREATED_DATE,
		   CASE WHEN r.CANCEL_DTTM IS NOT NULL THEN 'Y'
		   ELSE 'N' END AS Canceled,
		   r.CANCEL_DTTM AS CanceledDTTM,
		   rr.NAME AS CanceledReason,
		   appt.APPT_MADE_DTTM,
		   t.TIMEOUT_ID,
		  CONVERT( VARCHAR(15),e.PAT_ENC_CSN_ID) AS PAT_ENC_CSN_ID,
		   dd.DFI_ID AS DeficiencyID,
		   --d.DEF_ID,
		   dd.DEF_NAME AS TaskName,
		   AuditAction.AUDIT_TRAIL_INST AS AddedDateTime,
			AuditStatus.Status AS TaskResult,
		   AuditStatus.AUDIT_TRAIL_INST AS CompletedNotNeededDateTime,
		   ae.NAME AS CompletionUser,
			CASE WHEN  AuditStatus.AUDIT_TRAIL_INST IS NOT NULL AND AuditStatus.AUDIT_TRAIL_INST <= appt.APPT_DTTM THEN 'Yes' 
				WHEN  AuditStatus.AUDIT_TRAIL_INST IS NOT NULL AND  AuditStatus.AUDIT_TRAIL_INST > appt.APPT_DTTM THEN 'No'
				ELSE NULL END AS CompletedByDOS,
			CASE WHEN appt.APPT_MADE_DTTM IS NOT NULL
							--Business time difference between task creation and appointment date
				AND CLARITY_App.rptg.BusinessTime(AuditAction.AUDIT_TRAIL_INST,appt.APPT_DTTM)/540.0 <= 5 
				--AND DATEDIFF(MINUTE,AuditAction.AUDIT_TRAIL_INST,appt.APPT_DTTM)/540.0 <= 5 
				THEN 1
				ELSE 0 END AS APPTScheduledWithin5BusinessDays,
			CASE WHEN appt.APPT_MADE_DTTM IS NOT NULL
				--Business time difference between task creation and appointment date6
				AND CLARITY_App.rptg.BusinessTime(AuditAction.AUDIT_TRAIL_INST,appt.APPT_DTTM)/540.0 > 5 
				--AND DATEDIFF(MINUTE,AuditAction.AUDIT_TRAIL_INST,appt.APPT_DTTM)/540.0 > 5 
				THEN 1
				ELSE 0 END AS APPTScheduledOver5BusinessDays,
		   CASE WHEN AuditStatus.AUDIT_TRAIL_INST IS NOT NULL THEN 1 
			ELSE 0 END AS Numerator,
		   CASE WHEN AuditAction.AUDIT_TRAIL_INST IS NOT NULL THEN 1
			ELSE 0 END AS Denominator,
			CASE WHEN AuditStatus.AUDIT_TRAIL_INST IS NOT NULL AND result.REQUEST_ID IS NOT NULL THEN 1 
			ELSE 0 END AS Incomplete_Task_Flag,
				CASE WHEN result.next_status IS NOT NULL THEN 'Multiple Reasons' ELSE result.DEF_NAME END AS Incomplete_Task_Reason,
				result.ADD_DATE AS Incomplete_Task_Add_Date,
				result.COMP_USER_NAME AS Incomplete_Task_Completed_User
	FROM CLARITY.dbo.APPT_REQUEST r
		INNER JOIN CLARITY.dbo.TIMEOUT t
			ON r.PXPASS_ID = t.TIMEOUT_ID
		INNER JOIN CLARITY.dbo.PAT_ENC e
			ON t.PAT_CSN = e.PAT_ENC_CSN_ID
		INNER JOIN  
		(SELECT dd.DFI_ID, dd.DEF_ID, dd.PAT_ENC_CSN_ID, d.DEF_NAME, ROW_NUMBER() OVER (PARTITION BY dd.PAT_ENC_CSN_ID ORDER BY dd.DFI_ID) rownum
			FROM CLARITY.dbo.DFI_DETAILS dd
			LEFT JOIN CLARITY.dbo.CLARITY_DEF d
			ON d.DEF_ID = dd.DEF_ID
			WHERE 
			d.DEF_ID = '117000005'
			) dd ON dd.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID AND dd.rownum = 1 
		LEFT JOIN CLARITY_App.Stage.AmbOpt_OneTeam_Departments od ON r.RESPONSIBLE_DEPARTMENT_ID = od.DEPARTMENT_ID
		LEFT JOIN CLARITY.dbo.ZC_APPT_REQ_STATUS s 
			ON s.STATUS_C = r.STATUS_C
		LEFT JOIN CLARITY.dbo.ZC_REMOVAL_REASON	rr 
			ON rr.REMOVAL_REASON_C = r.CANCEL_REASON_C
		INNER JOIN CLARITY.dbo.PATIENT p 
			ON p.PAT_ID = r.PAT_ID 
		INNER JOIN CLARITY.dbo.VALID_PATIENT vp 
			ON vp.PAT_ID = p.PAT_ID
		LEFT JOIN CLARITY.dbo.CLARITY_PRC prc 
			ON r.PRIMARY_PRC_ID = prc.PRC_ID
		LEFT JOIN CLARITY.dbo.CLARITY_DEP rd 
			ON rd.DEPARTMENT_ID = r.RESPONSIBLE_DEPARTMENT_ID
		LEFT JOIN CLARITY.dbo.REFERRAL ref 
			ON ref.REFERRAL_ID = r.REFERRAL_ID 
		LEFT JOIN CLARITY.dbo.CLARITY_DEP refdep 
			ON ref.REFD_TO_DEPT_ID = refdep.DEPARTMENT_ID 
		LEFT JOIN
			(
				SELECT l.REQUEST_ID,
					   l.LINE,
					   l.CM_PHY_OWNER_ID,
					   l.CM_LOG_OWNER_ID,
					   l.APPT_SERIAL_NUM,
					   l.PAT_ENC_CSN_ID,
					   a.APPT_DTTM,
					   a.APPT_MADE_DTTM,
					   a.APPT_MADE_UTC_DTTM,
					   l.LINK_STATUS_C,
					   l.ORDER_RETURNED_YN, 
					   s.NAME AS ScheduledStatus, 
					   aps.NAME AS VisitStatus,
					   e.DEPARTMENT_ID,
					   ROW_NUMBER() OVER (PARTITION BY l.REQUEST_ID ORDER BY l.PAT_ENC_CSN_ID) rownum
				FROM 
				CLARITY.dbo.APPT_REQ_APPT_LINKS l  LEFT JOIN
				CLARITY.dbo.ZC_APPT_REQ_LINK_STATUS s ON s.LINK_STATUS_C = l.LINK_STATUS_C LEFT JOIN
				CLARITY.dbo.PAT_ENC e ON l.APPT_SERIAL_NUM = e.PAT_ENC_CSN_ID LEFT JOIN
				CLARITY.dbo.ZC_APPT_STATUS aps ON aps.APPT_STATUS_C = e.APPT_STATUS_C LEFT JOIN
				CLARITY.dbo.F_SCHED_APPT a ON a.PAT_ENC_CSN_ID = e.PAT_ENC_CSN_ID
				WHERE s.NAME = 'Scheduled'
			) appt ON r.REQUEST_ID = appt.REQUEST_ID  AND appt.rownum = 1
		LEFT JOIN CLARITY.dbo.CLARITY_DEP apptd ON apptd.DEPARTMENT_ID = appt.DEPARTMENT_ID
		--Find the Deficiency Added
		LEFT JOIN 
			(
				SELECT da.DFI_ID, da.AUDIT_TRAIL_INST, ROW_NUMBER() OVER (PARTITION BY da.DFI_ID ORDER BY da.AUDIT_TRAIL_INST) AS rownum
				FROM
				(SELECT dd.DFI_ID, dd.DEF_ID, dd.PAT_ENC_CSN_ID, d.DEF_NAME, ROW_NUMBER() OVER (PARTITION BY dd.PAT_ENC_CSN_ID ORDER BY dd.DFI_ID) rownum
					FROM CLARITY.dbo.DFI_DETAILS dd
					LEFT JOIN CLARITY.dbo.CLARITY_DEF d
					ON d.DEF_ID = dd.DEF_ID
					WHERE 
					d.DEF_ID = '117000005'
				) dd INNER JOIN 
				CLARITY.dbo.DFI_AUDIT_TRAIL da 
					ON da.DFI_ID = dd.DFI_ID AND dd.rownum = 1 
				LEFT JOIN CLARITY.dbo.ZC_DEF_ACTION act
					ON da.AUDIT_ACTION_C = act.DEF_ACTION_C
				WHERE act.NAME = 'Added Deficiency' 
			) AuditAction ON AuditAction.DFI_ID = dd.DFI_ID AND AuditAction.rownum = 1
		--Find the Deficiency Completion
		LEFT JOIN 
			(
				SELECT da.DFI_ID, da.AUDIT_TRAIL_INST, da.AUDIT_USER_ID, ds.name AS defName, ps.name AS psName, CASE WHEN ps.name IS NOT NULL THEN ps.name ELSE ds.NAME END AS Status,  ROW_NUMBER() OVER (PARTITION BY da.DFI_ID ORDER BY da.AUDIT_TRAIL_INST) AS rownum
				FROM
				(SELECT dd.DFI_ID, dd.DEF_ID, dd.PAT_ENC_CSN_ID, d.DEF_NAME,  ROW_NUMBER() OVER (PARTITION BY dd.PAT_ENC_CSN_ID ORDER BY dd.DFI_ID) rownum
					FROM CLARITY.dbo.DFI_DETAILS dd
					LEFT JOIN CLARITY.dbo.CLARITY_DEF d
					ON d.DEF_ID = dd.DEF_ID
					WHERE 
					d.DEF_ID = '117000005'
				) dd INNER JOIN 
				CLARITY.dbo.DFI_AUDIT_TRAIL da 
					ON da.DFI_ID = dd.DFI_ID --AND dd.rownum = 1 removed to include all deficienies for an appointment request     
				LEFT JOIN CLARITY.dbo.ZC_DEF_STATUS ds 
					ON ds.DEF_STATUS_C = da.AUDIT_STATUS_C
				LEFT JOIN CLARITY.dbo.ZC_PXPASS_STATUS ps 
					ON ps.PXPASS_STATUS_C = da.PXPASS_STATUS_C
				WHERE ps.name  = 'Not Needed' OR ds.NAME = 'Complete'
			) AuditStatus ON AuditStatus.DFI_ID = dd.DFI_ID AND Auditstatus.rownum = 1
		LEFT JOIN CLARITY.dbo.CLARITY_EMP ae ON ae.USER_ID = AuditStatus.AUDIT_USER_ID
		LEFT JOIN 
			(

			SELECT dd.DFI_ID, 
				dd.DEF_ID, 
				dd.PAT_ENC_CSN_ID, 
				d.DEF_NAME, 
				dd.ADD_DATE,
				dd.COMPLETED_DATE,
				dd.COMP_USER_ID,
				emp.NAME AS COMP_USER_NAME,
				t.TIMEOUT_ID,
				r.REQUEST_ID,
				 ROW_NUMBER() OVER (PARTITION BY r.REQUEST_ID ORDER BY dd.DFI_ID DESC) rownum,
				LEAD(d.DEF_NAME, 1) OVER (PARTITION BY r.REQUEST_ID ORDER BY dd.DFI_ID DESC) next_status
				FROM CLARITY.dbo.DFI_DETAILS dd WITH(NOLOCK)
				LEFT JOIN CLARITY.dbo.CLARITY_DEF d WITH(NOLOCK) ON d.DEF_ID = dd.DEF_ID
				LEFT JOIN CLARITY.dbo.TIMEOUT t WITH(NOLOCK) ON  t.PAT_CSN = dd.PAT_ENC_CSN_ID 
				LEFT JOIN CLARITY.dbo.APPT_REQUEST r WITH(NOLOCK) ON r.PXPASS_ID = t.TIMEOUT_ID
				LEFT JOIN CLARITY.dbo.CLARITY_EMP emp WITH(NOLOCK) ON emp.USER_ID = dd.COMP_USER_ID

				WHERE 
				d.DEF_ID IN (
					'117000008',
					'117000009',
					'117000010'
					)

			) result ON result.REQUEST_ID = r.REQUEST_ID AND result.rownum = 1

	WHERE --CONVERT(DATE, r.CREATED_UTC_DTTM) >='2022-11-01'
	(vp.IS_VALID_PAT_YN = 'Y' OR vp.IS_VALID_PAT_YN IS NULL)
	AND CONVERT(DATE, r.CREATED_DATE) BETWEEN @StartDate AND @EndDate
	AND (r.RESPONSIBLE_DEPARTMENT_ID IN (SELECT DEPARTMENT_ID FROM CLARITY_App.Stage.AmbOpt_OneTeam_Departments) 
			OR ref.REFD_TO_DEPT_ID IN  (SELECT DEPARTMENT_ID FROM CLARITY_App.Stage.AmbOpt_OneTeam_Departments )
		)

)  cte
/*Joins for standard fields*/
	LEFT JOIN   Clarity_App..		Dim_Date								dd			ON	CONVERT(DATE, cte.AddedDateTime ) = dd.day_date
	LEFT JOIN	CLARITY_App.Rptg.	vwRef_MDM_Location_Master_EpicSvc		mdmdept		ON	cte.RESPONSIBLE_DEPARTMENT_ID		= mdmdept.epic_department_id		
	LEFT JOIN	CLARITY..			CLARITY_DEP								cdep		ON	cte.RESPONSIBLE_DEPARTMENT_ID= cdep.DEPARTMENT_ID 
	LEFT JOIN	[CLARITY_App].[Rptg].[vwDim_Clrt_Pt] p ON cte.PAT_ID = p.Clrt_PAT_ID
	--LEFT JOIN	CLARITY_App.Rptg.	vwDim_Clrt_SERsrc						mdmprov		ON	cte.Prov_ID				= mdmprov.PROV_ID				
	--LEFT JOIN	CLARITY_App.Rptg.	vwRef_OracleOrg_to_EpicFinancialSubdiv	orgmap		ON  mdmprov.Financial_SubDivision	= orgmap.Epic_Financial_Subdivision_Code
-----------------------------------------------------------------------------------------------------------------------------------------------------
/* Standard Fields */
	/* Patient info */
		,	'person_name'					=	dpat.PAT_NAME
		,	'person_id'						=	dpat.MRN_Clrt
		,	'person_birth_date'				=	dpat.BIRTH_DATE
		,	'person_gender'					=	dpat.Gender_Identity_Name

	/* Date/times */
		,	'Fmonth_num'					=	All_Dates.Fmonth_num
		,	'Fyear_num'						=	All_Dates.Fyear_num
		,	'Fyear_name'					=	All_Dates.Fyear_name
		,	'report_period'					=	CAST(LEFT(DATENAME(MONTH, All_Dates.day_date), 3) + ' ' + CAST(DAY(All_Dates.day_date) AS VARCHAR(2)) AS VARCHAR(10)) 
		,	'report_date'					=	All_Dates.day_date

	/* Provider info */
		,	'provider_name'					=	mdmprov.Prov_Nme
		,	'prov_service_line_id'			=	CAST(NULL AS INT)	
		,	'prov_service_line'				=	mdmprov.Service_Line		
		,	'financial_div_id'				=	CAST(REPLACE(mdmprov.Financial_Division,'na',NULL) AS INT) 
		,	'financial_div_name'			=	CAST(mdmprov.Financial_Division_Name AS VARCHAR(150))
		,	'financial_subdiv_id'			=	CAST(REPLACE(mdmprov.Financial_SubDivision,'na',NULL) AS INT) 
		,	'financial_subdiv_name'			=	CAST(mdmprov.Financial_SubDivision_Name AS VARCHAR(150))

	/* Fac/Org info */
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
		,	'som_hs_area_id'				=	orgmap.som_hs_area_id 
		,	'som_hs_area_name'				=	CAST(orgmap.som_hs_area_name		AS VARCHAR(150))	
		,	'som_group_id'					=	orgmap.som_group_id	
		,	'som_group_name'				=	CAST(orgmap.som_group_name			AS VARCHAR(150)) 
		,	'som_department_id'				=	orgmap.department_id 
		,	'som_department_name'			=	CAST(orgmap.department				AS VARCHAR(150)) 
		,	'som_division_id'				=	orgmap.Org_Number 
		,	'som_division_name'				=	CAST(orgmap.Organization			AS VARCHAR(150))	

	/* Others */		
		,	'event_type'					=	CAST(NULL AS VARCHAR(50)) 
		,	'event_category'				=	CAST(NULL AS VARCHAR(150)) 
		,	'sk_Dim_Pt'						=	CAST(NULL AS INT) 
		--,	'peds' 							=	CAST(NULL AS SMALLINT) 
		,'peds' 								=	CAST(CASE
													WHEN FLOOR((CAST(msgs.handled_date AS INTEGER)
													-CAST(CAST(dpat.BIRTH_DATE AS DATETIME) AS INTEGER)
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
		,	'sk_dim_physcn'					=	CAST(NULL AS INT)
		,	CASE WHEN msgs.provider_type_c IN ('6', '9', '2', '2721', '2527', '101')  THEN 1
			ELSE 0 END AS app_flag

FROM	(	SELECT	day_date, Fyear_num, Fyear_name, Fmonth_num			/* Get all dates in range even if no data */
			FROM	Clarity_App..Dim_Date
			WHERE	day_date >= @startdate 
			    AND day_date < @enddate
		)	All_Dates	
				LEFT JOIN 																msgs		ON	All_Dates.day_date				= msgs.handled_date
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
								ON mdmdept.EPIC_DEPARTMENT_ID = msgs.epic_department_id	
				--LEFT JOIN	CLARITY_App.Rptg.	vwRef_MDM_Location_Master_EpicSvc		mdmdept		ON	msgs.epic_department_id			= mdmdept.epic_department_id removed and replaced with the mdmdept above
				LEFT JOIN	CLARITY_App.Rptg.	vwCLARITY_DEP							vwdep		ON	msgs.epic_department_id			= vwdep.DEPARTMENT_ID			
				LEFT JOIN	CLARITY..			CLARITY_DEP								cdep		ON	msgs.epic_department_id			= cdep.DEPARTMENT_ID

				LEFT JOIN	CLARITY_App.Rptg.	vwDim_Clrt_SERsrc						mdmprov		ON	msgs.provider_id				= mdmprov.PROV_ID				
				LEFT JOIN	CLARITY_App.Rptg.	vwRef_OracleOrg_to_EpicFinancialSubdiv	orgmap		ON  mdmprov.Financial_SubDivision	= orgmap.Epic_Financial_Subdivision_Code
				LEFT JOIN	CLARITY_App.Rptg.	vwDim_Clrt_Pt							dpat		ON  msgs.pat_id						= dpat.Clrt_PAT_ID
*/
GO


