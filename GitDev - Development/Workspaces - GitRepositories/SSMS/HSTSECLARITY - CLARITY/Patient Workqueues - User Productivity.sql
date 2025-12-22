USE CLARITY_App

/**************************************************************************************
[NAME] PATIENT WORKQUEUE TYPE USER PRODUCTIVITY - 31624
[AUTHOR(s)]		: SUE GRONDIN SYG2D
[ORGANIZATION]	: UVA - OPERATIONAL BUSINESS INTELLIGENCE
[DESCRIPTION]	: FOR ANY DATE RANGE FOR WQ EXIT DATE, LIST ITEMS EXITED BY USER
					THIS REPORT ONLY SELECTS PATIENT WQ TYPES.  
WORKQUEUE_ID					WORKQUEUE_NAME								WORKQUEUE_TYPE_C	PARENT_WQ_ID	IS_PARENT_YN
18873	-- INCLUSION TEST		CHART PREP CARDIOLOGY						3-Patient 			18873			N 
19132	-- EXCLUSION TEST		CARDIOLOGY CHART PREP REFERRAL WORKQUEUE	51	-Referrals		19132			N


[REVISION HISTORY]:
Date        Author			Version		Comment
----------	--------------	--------	-------------------------------------
2023.01.10 	SYG2D			1			CREATED

************************************************************************************/
--/*
--DECLARE @Workqueue varchar(max) = '18873'
DECLARE @Workqueue VARCHAR(MAX) = '2555'
--DECLARE @StartDate DATETIME = '11/1/2022'
--DECLARE @EndDate DATETIME = '11/1/2022'
DECLARE @StartDate DATETIME = '7/1/2024'
DECLARE @EndDate DATETIME = '11/1/2024'
--*/

SELECT
	--  , t.MRN
	    t.WORKQUEUE_NAME
	  , t.WORKQUEUE_ID
	  , t.WQ_CONTACT_ID
	  , t.ENTRY_TIME
	  , t.EXIT_TIME
	  , [MINUTES_SPENT_IN_WQ] = t.tmin
	  , t.ENTRY_USER
	  , t.EXIT_USER
	  , t.EXIT_ACTIVITY
	  , t.PATIENT
	  , t.MRN
	  , t.PATIENT_MRN
	  , t.EFFECTIVE_DEPT_ID
	  --, t.DEPT_ABBREVIATION
	  , t.DEPARTMENT_NAME
	  , t.HAR_BALANCE
	  --,[ELAPSED_TIME_DD_HH_MM] = CASE 
			--when t.tmin < 60 then cast(t.tmin as varchar(10)) + ' Min'
			--when t.tmin < 1440 then cast(t.tmin/60 as varchar(10)) + ' Hr, ' + cast(t.tmin%60 as varchar(10)) + ' Min'
			--else cast(t.tmin/(1440 ) as varchar(10)) + ' Days, ' + cast((t.tmin%1440 )/60 as varchar(10)) + ' Hr, ' + cast(((t.tmin%1440 )%60) as varchar(10)) + ' Min'
			--end
FROM (
SELECT
	pat.PAT_NAME AS PATIENT
  , iid.IDENTITY_ID AS MRN
  , [PATIENT_MRN]	= CONCAT(pat.PAT_NAME, ' [', iid.IDENTITY_ID, ']')
  , [WQ_CONTACT_ID] = p.ITEM_ID
  , [ENTRY_TIME]	= p.CREATION_INSTANCE
  , [EXIT_TIME]		= hx_out.EXIT_DATE
  , hx_in.ENTRY_USER
  , [tmin]			= DATEDIFF(MINUTE, hx_in.ENTRY_DATE, hx_out.EXIT_DATE)
  , hx_out.EXIT_USER
  , hx_out.EXIT_ACTIVITY
  , [HAR_BALANCE]	= ISNULL(har.TOT_ACCT_BAL, 0)
  , WI.WORKQUEUE_NAME
  , p.WORKQUEUE_ID
  , pe.EFFECTIVE_DEPT_ID
  --, DEP.DEPT_ABBREVIATION
  , DEP.DEPARTMENT_NAME

FROM	CLARITY..PAT_WQ_ITEMS			   p
		INNER JOIN CLARITY..WORKQUEUE_INFO WI ON p.WORKQUEUE_ID		   = WI.WORKQUEUE_ID AND wi.WORKQUEUE_TYPE_C = 3 -- PATIENT WQS ONLY
		INNER JOIN CLARITY..PAT_ENC		   pe ON p.PAT_ENC_CSN_ID	   = pe.PAT_ENC_CSN_ID
		LEFT JOIN CLARITY..CLARITY_DEP	   DEP ON pe.EFFECTIVE_DEPT_ID = DEP.DEPARTMENT_ID
		LEFT JOIN CLARITY..HSP_ACCOUNT	   har ON pe.HSP_ACCOUNT_ID	   = har.HSP_ACCOUNT_ID
		INNER JOIN CLARITY..PATIENT		   pat ON pe.PAT_ID			   = pat.PAT_ID
		INNER JOIN CLARITY..IDENTITY_ID	   iid ON pat.PAT_ID		   = iid.PAT_ID AND iid.IDENTITY_TYPE_ID = 14

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
WHERE p.WORKQUEUE_ID IN ( @Workqueue)
AND  CAST(hx_out.EXIT_DATE AS DATE)  BETWEEN @StartDate AND @EndDate

)t

ORDER BY
	t.EXIT_USER,
	t.EXIT_TIME
