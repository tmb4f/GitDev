use CDW_App;

Declare @startdate date = '06/01/2024',@enddate date = '04/16/2025';


/*
Definition: Time spent in notes per day
Time in Notes per Day

Time spent writing notes per provider per day.

Numerator: Minutes providers spent in a notes activity or navigator section within the reporting period.

Denominator: Sum of days providers logged in within the reporting period.

Final value: Numerator/Denominator

*/

/* get core providers; Pilot through Wave 5 */
drop table if exists #ProviderCore;
select distinct emp.DurableKey [EmployeeDurableKey]
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

/* Step 1: All Provider-Day combinations within range */
DROP TABLE IF EXISTS #ProviderDayCalendar;
SELECT 
    p.ProviderDurableKey,
    p.ProviderName,
    d.DateValue AS CalendarDay
INTO #ProviderDayCalendar
FROM #ProviderCore p
CROSS JOIN (
    SELECT DateValue 
    FROM cdw.FullAccess.DateDim 
    WHERE DateValue BETWEEN @startdate AND @enddate
) d;

/* Step 2: Numerator – Notes Time Per Day Per Provider */
DROP TABLE IF EXISTS #NoteTimeNumerator;
SELECT 
    cnf.AuthoringProviderDurableKey AS ProviderDurableKey,
    pc.ProviderName,
    dd.DateValue AS NoteDay,
    SUM(cnf.TotalEditTime / 60.0) AS TotalNoteMinutes  -- seconds to minutes
INTO #NoteTimeNumerator
FROM cdw.FullAccess.ClinicalNoteFact cnf
INNER JOIN #ProviderCore pc ON pc.ProviderDurableKey = cnf.AuthoringProviderDurableKey
JOIN cdw.FullAccess.DateDim dd ON dd.DateKey = cnf.ServiceDateKey
WHERE cnf.Status = 'Signed' 
    AND cnf.Count = 1
    AND dd.DateValue BETWEEN @startdate AND @enddate
GROUP BY cnf.AuthoringProviderDurableKey, pc.ProviderName, dd.DateValue;

/* Step 3: Denominator – Days with Signed Notes */
-- DROP TABLE IF EXISTS #LoginDaysDenominator;
-- SELECT 
--     cnf.AuthoringProviderDurableKey AS ProviderDurableKey,
--     pc.ProviderName,
--     COUNT(DISTINCT dd.DateValue) AS LoggedInDays
-- INTO #LoginDaysDenominator
-- FROM cdw.FullAccess.ClinicalNoteFact cnf
-- INNER JOIN #ProviderCore pc ON pc.ProviderDurableKey = cnf.AuthoringProviderDurableKey
-- JOIN cdw.FullAccess.DateDim dd ON dd.DateKey = cnf.ServiceDateKey
-- WHERE cnf.Status = 'Signed'
--     AND cnf.Count = 1
--     AND dd.DateValue BETWEEN @startdate AND @enddate
-- GROUP BY cnf.AuthoringProviderDurableKey, pc.ProviderName;
/* Step 3: Denominator – Logged in (1/0) per Provider per Day */
DROP TABLE IF EXISTS #LoginDaysDenominator;
SELECT 
    cal.ProviderDurableKey,
    cal.ProviderName,
    cal.CalendarDay,
    CASE WHEN cnf.AuthoringProviderDurableKey IS NOT NULL THEN 1 ELSE 0 END AS WasLoggedIn
INTO #LoginDaysDenominator
FROM #ProviderDayCalendar cal
LEFT JOIN (
    SELECT DISTINCT 
        cnf.AuthoringProviderDurableKey,
        dd.DateValue AS LoginDay
    FROM cdw.FullAccess.ClinicalNoteFact cnf
    JOIN cdw.FullAccess.DateDim dd ON dd.DateKey = cnf.ServiceDateKey
    WHERE cnf.Status = 'Signed'
        AND cnf.Count = 1
        AND dd.DateValue BETWEEN @startdate AND @enddate
) cnf 
    ON cnf.AuthoringProviderDurableKey = cal.ProviderDurableKey
    AND cnf.LoginDay = cal.CalendarDay;

/* Final Step: Merge with calendar and compute per-day note time */
-- SELECT 
--     cal.ProviderDurableKey,
--     cal.ProviderName,
--     cal.CalendarDay,
--     ISNULL(nt.TotalNoteMinutes, 0) AS TotalNoteMinutes,
--     ld.LoggedInDays,
--     CASE 
--         WHEN ISNULL(ld.LoggedInDays, 0) = 0 THEN 0
--         ELSE CAST(ISNULL(nt.TotalNoteMinutes, 0) AS FLOAT) / ld.LoggedInDays
--     END AS AvgNoteMinutesPerDay
-- FROM #ProviderDayCalendar cal
-- LEFT JOIN #NoteTimeNumerator nt 
--     ON cal.ProviderDurableKey = nt.ProviderDurableKey 
--     AND cal.CalendarDay = nt.NoteDay
-- LEFT JOIN #LoginDaysDenominator ld 
--     ON cal.ProviderDurableKey = ld.ProviderDurableKey
-- ORDER BY 
--     cal.ProviderName, cal.CalendarDay;
/* Final Step: Merge with calendar and compute per-day note time */
SELECT 
    cal.ProviderDurableKey,
    cal.ProviderName,
    cal.CalendarDay,
    ISNULL(nt.TotalNoteMinutes, 0) AS TotalNoteMinutes,
    ld.WasLoggedIn,
    CASE 
        WHEN ISNULL(ld.WasLoggedIn, 0) = 0 THEN 0
        ELSE ISNULL(nt.TotalNoteMinutes, 0)
    END AS AvgNoteMinutesPerDay
FROM #ProviderDayCalendar cal
LEFT JOIN #NoteTimeNumerator nt 
    ON cal.ProviderDurableKey = nt.ProviderDurableKey 
    AND cal.CalendarDay = nt.NoteDay
LEFT JOIN #LoginDaysDenominator ld 
    ON cal.ProviderDurableKey = ld.ProviderDurableKey
    AND cal.CalendarDay = ld.CalendarDay
ORDER BY 
    cal.ProviderName, cal.CalendarDay;