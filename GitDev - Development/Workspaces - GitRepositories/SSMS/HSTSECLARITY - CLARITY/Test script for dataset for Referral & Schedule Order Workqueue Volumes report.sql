USE CLARITY

DECLARE @startdate SMALLDATETIME = NULL, 
        @enddate SMALLDATETIME = NULL

SET @startdate = '4/16/2024 00:00 AM'
SET @enddate = '4/30/2024 11:59 PM'

DECLARE @locstartdate DATETIME,
        @locenddate DATETIME
			   
SET @locstartdate = CAST(CAST(@startdate AS DATE) AS DATETIME)
+ CAST(CAST('00:00' AS TIME) AS DATETIME)

SET @locenddate = CAST(CAST(@enddate AS DATE) AS DATETIME)
+ CAST(CAST('23:59' AS TIME) AS DATETIME)

/*
SCHED ORDER WQ
20227	Plastic Surgery ApRq Scheduler Review
20151	Plastic Surgery ApRq Ready to Schedule
*/

SELECT  ref_items.WORKQUEUE_ID ,
ref_items.WORKQUEUE_NAME,
ref_items.POD ,
ref_items.NAME,
prev_ct_items = SUM(prev_ct_items),
ct_items = SUM(ct_items),
net_change = SUM(ct_items) - SUM(prev_ct_items),
ct_active =SUM(active),
ct_def= SUM(deferred),
ct_added = SUM(added),
ct_removed = SUM(removed),
ct_age_active = COUNT(CASE WHEN active = 1 AND DATEDIFF(dd,ref_items.ENTRY_DATE,@locenddate) >= 15
						THEN 1 END), 
ct_age_def = COUNT(CASE WHEN deferred = 1 AND DATEDIFF(dd,ref_items.ENTRY_DATE,@locenddate) >= 15
						THEN 1 END), 
sum_age_active = SUM(CASE WHEN active = 1
						THEN age END),
sum_age_def = SUM(CASE WHEN deferred = 1
						THEN age END),
max_age_active = MAX(CASE WHEN active = 1
						THEN age END),
max_age_def = MAX(CASE WHEN deferred = 1
						THEN age END),
avg_age_active = AVG(CASE WHEN active = 1 
						THEN age END),
avg_age_def = AVG(CASE WHEN deferred = 1 
						THEN age END),

header = 'UNSCHED REF WQ'

FROM  (
			SELECT  ritems.WORKQUEUE_ID ,
					rwq.WORKQUEUE_NAME,
					ritems.ITEM_ID,
			        refoa.NAME AS POD ,
					clarity_emp.NAME,
					CASE 
						WHEN 1=1
						AND ((activity.last_action <> 3) AND (ritems.RELEASE_DATE IS NULL OR ritems.RELEASE_DATE > = @locstartdate)  AND (activity.ENTRY_DATE < = @locstartdate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locstartdate) AND (activity.ENTRY_DATE < = @locstartdate ))
							THEN  1
					END prev_ct_items,
					CASE 
						WHEN 1=1
						AND ((activity.last_action <> 3) AND (ritems.RELEASE_DATE IS NULL OR ritems.RELEASE_DATE > = @locenddate)  AND (activity.ENTRY_DATE < = @locenddate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locenddate) AND (activity.ENTRY_DATE < = @locenddate ))
							THEN 1
					END ct_items,
					CASE 
						WHEN (activity.last_tab = 0 OR activity.last_tab IS NULL)
						AND ((activity.last_action <> 3) AND (ritems.RELEASE_DATE IS NULL OR ritems.RELEASE_DATE > = @locenddate) AND (activity.ENTRY_DATE < = @locenddate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locenddate) AND (activity.ENTRY_DATE < = @locenddate ))
							THEN 1 
					END active,
					CASE 
						WHEN activity.last_tab = 1 
						AND ((activity.last_action <> 3) AND (ritems.RELEASE_DATE IS NULL OR ritems.RELEASE_DATE > = @locenddate) AND (activity.ENTRY_DATE < = @locenddate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locenddate) AND (activity.ENTRY_DATE < = @locenddate ))
							THEN 1 
					END deferred,
					CASE 
						WHEN activity.ENTRY_DATE >= @locstartdate AND activity.ENTRY_DATE <= @locenddate
					    THEN 1
						ELSE 0
					END AS added ,
					CASE 
						WHEN (activity.release_date IS NOT NULL) AND activity.release_date >= @locstartdate AND activity.release_date <= @locenddate
					    THEN 1
						ELSE 0
					END AS removed ,

					activity.ENTRY_DATE,   
					age= DATEDIFF(dd,activity.ENTRY_DATE,@locenddate)
				
		

			FROM    clarity..REFERRAL_WQ_ITEMS ritems
					--INNER JOIN clarity..REFERRAL_WQ_ITEMS ritems ON ritems.ITEM_ID = ractivity.ITEM_ID
			        INNER JOIN CLARITY..REFERRAL_WQ rwq ON rwq.WORKQUEUE_ID = ritems.WORKQUEUE_ID
			        --INNER JOIN clarity..ZC_OWNING_AREA_2 refoa ON rwq.OWNING_AREA_C = refoa.OWNING_AREA_2_C
			        LEFT OUTER JOIN clarity..ZC_OWNING_AREA_2 refoa ON rwq.OWNING_AREA_C = refoa.OWNING_AREA_2_C
			        --INNER JOIN clarity..V_ZC_SCHED_WQ_TAB ON V_ZC_SCHED_WQ_TAB.TAB_NUMBER_C = ritems.TAB_STATUS_C
			        LEFT OUTER JOIN clarity..V_ZC_SCHED_WQ_TAB ON V_ZC_SCHED_WQ_TAB.TAB_NUMBER_C = ritems.TAB_STATUS_C
					LEFT JOIN clarity_emp ON rwq.SUPERVISOR_ID = clarity_emp.USER_ID
					INNER JOIN (

								SELECT
									ractivity.LINE,
									ractivity.ITEM_ID,
									entry_date = MAX(CASE WHEN HISTORY_ACTIVITY_C IN (1,2) THEN START_INSTANT_DTTM END) OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID),
									release_date = MAX(CASE WHEN HISTORY_ACTIVITY_C IN (3) THEN START_INSTANT_DTTM END) OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID),
									last_action = LAST_VALUE(ractivity.HISTORY_ACTIVITY_C) OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID) ,
									last_tab= LAST_VALUE(ractivity.TAB_NUMBER_C)OVER (PARTITION BY ractivity.ITEM_ID ORDER BY ractivity.ITEM_ID)

								FROM clarity..REFERRAL_WQ_USR_HX ractivity
								WHERE 1=1
									AND START_INSTANT_DTTM < @locenddate
								

								) activity ON activity.item_id=ritems.ITEM_ID AND activity.LINE = 1
			WHERE   1=1
			        AND rwq.WORKQUEUE_NAME LIKE '%unsched%'
			        AND QUEUE_ACTIVE_YN = 'Y' -- active wq only
					

			) ref_items

GROUP BY ref_items.WORKQUEUE_ID ,
        ref_items.WORKQUEUE_NAME ,
        ref_items.POD,
		ref_items.name


UNION

SELECT  sord_items.WORKQUEUE_ID ,
sord_items.WORKQUEUE_NAME,
sord_items.POD ,
sord_items.NAME,
prev_ct_items = SUM(prev_ct_items),
ct_items = SUM(ct_items),
net_change = SUM(ct_items) - SUM(prev_ct_items),
ct_active =SUM(active),
ct_def= SUM(deferred),
ct_added = SUM(added),
ct_removed = SUM(removed),
ct_age_active = COUNT(CASE WHEN active = 1 AND DATEDIFF(dd,sord_items.ENTRY_DATE,@locenddate) >= 15
						THEN 1 END), 
ct_age_def = COUNT(CASE WHEN deferred = 1 AND DATEDIFF(dd,sord_items.ENTRY_DATE,@locenddate) >= 15
						THEN 1 END), 
sum_age_active = SUM(CASE WHEN active = 1
						THEN age END),
sum_age_def = SUM(CASE WHEN deferred = 1
						THEN age END),
max_age_active = MAX(CASE WHEN active = 1
						THEN age END),
max_age_def = MAX(CASE WHEN deferred = 1
						THEN age END),
avg_age_active = AVG(CASE WHEN active = 1 
						THEN age END),
avg_age_def = AVG(CASE WHEN deferred = 1 
						THEN age END),

header = 'SCHED ORDER WQ'

FROM  (
			SELECT  sitems.WORKQUEUE_ID ,
					swq.WORKQUEUE_NAME,
					sitems.ITEM_ID,
			        refoa.NAME AS POD ,
					clarity_emp.NAME,
						CASE 
						WHEN activity.last_tab <> 2 
						AND ((activity.last_action <> 3) and (sitems.RELEASE_DATE IS NULL OR sitems.RELEASE_DATE > = @locstartdate)  AND (activity.ENTRY_DATE < = @locstartdate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locstartdate) AND (activity.ENTRY_DATE < = @locstartdate ))
							THEN  1
					END prev_ct_items,
					CASE 
						WHEN activity.last_tab <> 2 
						AND ((activity.last_action <> 3) and (sitems.RELEASE_DATE IS NULL OR sitems.RELEASE_DATE > = @locenddate)  AND (activity.ENTRY_DATE < = @locenddate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locenddate) AND (activity.ENTRY_DATE < = @locenddate ))
							THEN 1
					END ct_items,
					CASE 
						WHEN (activity.last_tab = 0 OR activity.last_tab IS NULL)
						AND ((activity.last_action <> 3) and (sitems.RELEASE_DATE IS NULL OR sitems.RELEASE_DATE > = @locenddate) AND (activity.ENTRY_DATE < = @locenddate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locenddate) AND (activity.ENTRY_DATE < = @locenddate ))
							THEN 1 
					END active,
				
					CASE 
						WHEN activity.last_tab = 1 
						aND ((activity.last_action <> 3) and (sitems.RELEASE_DATE IS NULL OR sitems.RELEASE_DATE > = @locenddate) AND (activity.ENTRY_DATE < = @locenddate))
						OR ((activity.last_action = 3 AND activity.release_date >= @locenddate) AND (activity.ENTRY_DATE < = @locenddate ))
							THEN 1 
					END deferred,
					CASE 
						WHEN activity.ENTRY_DATE >= @locstartdate AND activity.ENTRY_DATE <= @locenddate
					    THEN 1
						ELSE 0
					END AS added ,
					CASE 
						WHEN (activity.release_date IS NOT NULL) AND activity.release_date >= @locstartdate AND activity.release_date <= @locenddate
					    THEN 1
						ELSE 0
					END AS removed ,
					activity.ENTRY_DATE,   
					age= DATEDIFF(dd,activity.ENTRY_DATE,@locenddate)
				

			FROM    clarity..SCHED_ORDERS_WQ_ITEMS sitems
					--INNER JOIN clarity..REFERRAL_WQ_ITEMS ritems ON ritems.ITEM_ID = ractivity.ITEM_ID
			        INNER JOIN CLARITY..SCHED_ORDERS_WQ swq ON swq.WORKQUEUE_ID = sitems.WORKQUEUE_ID
			        --INNER JOIN clarity..ZC_OWNING_AREA_2 refoa ON swq.OWNING_AREA_C = refoa.OWNING_AREA_2_C
			        LEFT OUTER JOIN clarity..ZC_OWNING_AREA_2 refoa ON swq.OWNING_AREA_C = refoa.OWNING_AREA_2_C
			        --INNER JOIN clarity..V_ZC_SCHED_WQ_TAB ON V_ZC_SCHED_WQ_TAB.TAB_NUMBER_C = sitems.TAB_STATUS_C
			        LEFT OUTER JOIN clarity..V_ZC_SCHED_WQ_TAB ON V_ZC_SCHED_WQ_TAB.TAB_NUMBER_C = sitems.TAB_STATUS_C
					LEFT JOIN clarity_emp ON swq.SUPERVISOR_ID = clarity_emp.USER_ID
					inner JOIN (

								SELECT
									sactivity.LINE,
									sactivity.ITEM_ID,
									entry_date = MAX(case when ACTIVITY_C IN (1,2) then START_DTTM end) OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID),
									release_date = MAX(case when ACTIVITY_C IN (3) then START_DTTM end) OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID),
									last_action = LAST_VALUE(sactivity.ACTIVITY_C) OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID) ,
									last_tab= LAST_VALUE(sactivity.TAB_STATUS_C)OVER (PARTITION BY sactivity.ITEM_ID ORDER BY sactivity.ITEM_ID)

								FROM clarity..SCHED_ORDERS_HX sactivity
								WHERE 1=1
									AND START_DTTM < @locenddate

								) activity ON activity.item_id=sitems.ITEM_ID AND activity.line = 1
								
			WHERE   1=1
			       -- AND TAB_NUMBER_C IN ( 0, 1 ) -- active and deferred tab only
			        AND QUEUE_ACTIVE_YN = 'Y' -- active wq only
					

			) sord_items

GROUP BY sord_items.WORKQUEUE_ID  ,
        sord_items.WORKQUEUE_NAME ,
        sord_items.POD,
		sord_items.name

ORDER BY
	header,
	WORKQUEUE_ID