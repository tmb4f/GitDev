USE CDW_App;

--Declare @startdate date = '06/01/2024',@enddate date = '04/16/2025';
DECLARE @startdate DATE = '05/01/2024',@enddate DATE = '05/12/2025';


/* Pajama Time

Average number of minutes a provider spent in charting activities on weekdays outside the hours of 7:00 AM - 5:30 PM or
outside scheduled hours on weekends or non-scheduled holidays. Weekend days are determined by locale. This metric does not
include time spent during scheduled hours on any day of the week, nor does it include time spent personalizing tools like
SmartPhrases and Preference Lists or using reporting tools such as SlicerDicer and Reporting Workbench. To be included, 
a provider needs at least 5 appointments scheduled per week within the reporting period.

This metric is only calculated if your organization has submitted data for at least 90% of the days within the reporting period

Numerator: Minutes spent in charting activities outside 7 AM to 5:30 PM on weekdays and outside scheduled hours on weekends.

Denominator: Scheduled days where time was spent in the system within the reporting period.

Final value: Numerator/Denominator

*/

/* get core providers; Pilot through Wave 5 */
DROP TABLE IF EXISTS #ProviderCore;
select distinct emp.DurableKey [EmployeeDurableKey]
,emp.EmployeeEpicId
,emp.name [EmployeeName] 
,dax.Provider_Email
,prov.DurableKey [ProviderDurableKey]
,prov.name [ProviderName]
,prov.ProviderEpicId
into #ProviderCore
from 
[CDW_App_Dev].Rptg.TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider dax 
inner join cdw.FullAccess.EmployeeDim emp on emp.email=dax.Provider_Email
inner join cdw.FullAccess.ProviderDim prov on prov.EmployeeDurableKey=emp.DurableKey
where 1=1
and emp.IsCurrent=1
and prov.IsCurrent=1
and prov.DurableKey>0

AND emp.DurableKey = 14222

--SELECT
--	*
--FROM #ProviderCore
--ORDER BY
--	EmployeeName
/*
SELECT pc.ProviderDurableKey,
         pc.ProviderName,
         CAST(paf.instant AS DATE) AS ActivityDate,
         paf.instant [ActivityDTTM],
		 paf.[Count],
		ad.ActivityEpicId,
        ad.name [ActivityName],
        paf.ChartReviewTabKey,
        chartrev.ChartReviewTabEpicId,
        paf.activeSeconds
    FROM cdw.fullaccess.UserActionLogActivityHourFact paf
    inner join cdw.fullaccess.EncounterFact enc on paf.encounterkey=enc.encounterkey
    inner join cdw.fullaccess.ChartReviewTabDim ChartRev on chartrev.chartreviewtabkey=paf.chartreviewtabkey
    inner join #ProviderCore pc on pc.ProviderDurableKey=enc.ProviderDurableKey
    inner join cdw.fullaccess.ActivityDim ad on paf.activitykey = ad.activitykey
    JOIN cdw.fullaccess.DateDim dd ON dd.DateKey = paf.DateKey
    where 1=1 
    and paf.count=1
    and dd.datevalue between @startdate and @enddate
    --and ChartRev.ChartReviewTabEpicId IS NOT NULL
    --and (ChartRev.ChartReviewTabEpicId IS NOT NULL AND chartrev.ChartReviewTabEpicId>0)
	--AND CAST(paf.Instant AS TIME) < '07:00:00' OR CAST(paf.Instant AS TIME) > '17:30:00'
	AND ad.ActivityEpicId IN (
-- 20133 -- UCW_CHART_REVIEW
--,94120 -- 	HKU_SUMMARY
--,94121 --	HKU_ENCOUNTERS
--,94139 --	HKU_MEDIA
 17001 -- MR_REVIEW
,17178 -- MR_CHART_REDIRECTOR_NOFILTER
,17379 -- UCW_CHART_SEARCH_SIDEBAR
,20133 -- UCW_CHART_REVIEW
,23071 -- MR_CHART_REVIEW_TAB_EDIT
,23072 -- MR_CHART_REVIEW_QUICKFILTER_EDIT
,23073 -- MR_CHART_REVIEW_FILTERTYPEDEFINITION_EDIT
,34203 -- IP_PAT_SUMMARY_M
,49028 -- ER_EDCOURSE_ADD
,56097 -- CV_CHART_REVIEW
,89316 -- AN_LS_WS_RELAUNCH_MODAL
,94120 -- HKU_SUMMARY
,94121 -- HKU_ENCOUNTERS
,94139 -- HKU_MEDIA
)
	ORDER BY
		pc.ProviderName,
		paf.Instant
*/

-- Step 1: Charting activity during off-hours (excluding personalization/reporting tools)
;WITH ChartingActivity AS (
     SELECT pc.ProviderDurableKey,
         pc.ProviderName,
         CAST(dd.DateValue AS DATE) AS ActivityDate,
         paf.instant [ActivityDTTM],
		 ad.ActivityEpicId,
        ad.name [ActivityName],
        paf.ChartReviewTabKey,
        chartrev.ChartReviewTabEpicId,
		SUM(paf.activeSeconds) ActiveSeconds,
        sum(paf.activeSeconds/60.0) ActiveMinutes
    FROM cdw.fullaccess.UserActionLogActivityHourFact paf
    inner join cdw.fullaccess.EncounterFact enc on paf.encounterkey=enc.encounterkey
    inner join cdw.fullaccess.ChartReviewTabDim ChartRev on chartrev.chartreviewtabkey=paf.chartreviewtabkey
    inner join #ProviderCore pc on pc.ProviderDurableKey=enc.ProviderDurableKey
    inner join cdw.fullaccess.ActivityDim ad on paf.activitykey = ad.activitykey
    JOIN cdw.fullaccess.DateDim dd ON dd.DateKey = paf.DateKey
    where 1=1 
    and paf.count=1
    and dd.datevalue between @startdate and @enddate
    --and chartrev.ChartReviewTabEpicId>0
	AND ad.ActivityEpicId IN (
-- 20133 -- UCW_CHART_REVIEW
--,94120 -- 	HKU_SUMMARY
--,94121 --	HKU_ENCOUNTERS
--,94139 --	HKU_MEDIA
 17001 -- MR_REVIEW
,17178 -- MR_CHART_REDIRECTOR_NOFILTER
,17379 -- UCW_CHART_SEARCH_SIDEBAR
,20133 -- UCW_CHART_REVIEW
,23071 -- MR_CHART_REVIEW_TAB_EDIT
,23072 -- MR_CHART_REVIEW_QUICKFILTER_EDIT
,23073 -- MR_CHART_REVIEW_FILTERTYPEDEFINITION_EDIT
,34203 -- IP_PAT_SUMMARY_M
,49028 -- ER_EDCOURSE_ADD
,56097 -- CV_CHART_REVIEW
,89316 -- AN_LS_WS_RELAUNCH_MODAL
,94120 -- HKU_SUMMARY
,94121 -- HKU_ENCOUNTERS
,94139 -- HKU_MEDIA
)
    group by pc.ProviderDurableKey,pc.ProviderName,CAST(dd.DateValue AS DATE),ad.ActivityEpicId,ad.name,paf.instant,paf.ChartReviewTabKey,chartrev.ChartReviewTabEpicId

)--,

SELECT
	*
FROM ChartingActivity
ORDER BY
	ChartingActivity.ActivityDate
/*
-- Step 2: Identify Pajama Time (before 7:00 AM or after 5:30 PM on weekdays, or unscheduled weekends)
PajamaTimeRaw AS (
    SELECT 
        ca.ProviderDurableKey,
        ca.ProviderName,
        ca.ActivityDate,
        ca.ActivityDTTM,
        ca.ActiveMinutes,
        CASE 
            WHEN DATENAME(WEEKDAY, ca.ActivityDate) IN ('Saturday', 'Sunday') THEN 1
            WHEN CAST(ca.ActivityDTTM AS TIME) < '07:00:00' OR CAST(ca.ActivityDTTM AS TIME) > '17:30:00' THEN 1
            ELSE 0
        END AS IsPajamaTime
    FROM ChartingActivity ca
),

EligibleProviders AS (
    SELECT distinct pc.ProviderDurableKey
    FROM cdw.fullaccess.VisitFact vf
    inner join #ProviderCore pc on pc.ProviderDurableKey=vf.primaryvisitproviderdurablekey
    inner join cdw.FullAccess.DateDim appt on appt.DateKey=vf.AppointmentDateKey
    WHERE 1=1 
        and vf.AppointmentStatus in ('Scheduled','Completed')
        AND vf.count = 1
        AND appt.DateValue BETWEEN @startdate AND @enddate
    GROUP BY pc.ProviderDurableKey,pc.ProviderName, DATEPART(WEEK, appt.datevalue)
    HAVING COUNT(DISTINCT vf.VisitKey) >= 5
), 

PajamaMinutesPerProviderDaily AS (
    SELECT 
        pt.ProviderDurableKey,
        pt.ProviderName,
        pt.ActivityDate,
        SUM(pt.ActiveMinutes) AS TotalPajamaMinutes
    FROM PajamaTimeRaw pt
    INNER JOIN EligibleProviders ep ON pt.ProviderDurableKey = ep.ProviderDurableKey
    WHERE pt.IsPajamaTime = 1
    GROUP BY pt.ProviderDurableKey,pt.ProviderName,pt.ActivityDate
), 

ChartingDaysDaily AS (
    SELECT 
        ProviderDurableKey,
        ActivityDate
    FROM ChartingActivity
    group by ProviderDurableKey,ActivityDate
)

-- Final select: 1 row per provider per day
SELECT 
    pc.ProviderDurableKey,
    pc.ProviderName,
    dd.DateValue AS ActivityDate,
    ISNULL(pmp.TotalPajamaMinutes, 0) AS Numerator,
    CASE 
        WHEN cdd.ProviderDurableKey IS NOT NULL THEN 1 
        ELSE 0 
    END AS Denominator,
    CASE 
        WHEN cdd.ProviderDurableKey IS NOT NULL THEN ROUND(ISNULL(pmp.TotalPajamaMinutes, 0), 2) 
        ELSE 0 
    END AS PajamaMinutesPerDay
FROM #ProviderCore pc
CROSS JOIN (
    SELECT DateValue 
    FROM cdw.FullAccess.DateDim 
    WHERE DateValue BETWEEN @startdate AND @enddate
) dd
LEFT JOIN PajamaMinutesPerProviderDaily pmp 
    ON pc.ProviderDurableKey = pmp.ProviderDurableKey 
    AND pmp.ActivityDate = dd.DateValue
LEFT JOIN ChartingDaysDaily cdd 
    ON pc.ProviderDurableKey = cdd.ProviderDurableKey 
    AND cdd.ActivityDate = dd.DateValue
INNER JOIN EligibleProviders ep 
    ON pc.ProviderDurableKey = ep.ProviderDurableKey
WHERE dd.DateValue BETWEEN @startdate AND @enddate
ORDER BY pc.ProviderName, dd.DateValue
*/
