use CDW_App;

Declare @startdate date = '06/01/2024',@enddate date = '04/16/2025';


/*
o	Same-day appointments closed 
Appointments Closed [1031]
The percentage of appointments within the reporting period that were closed in time period (same day).
Numerator: Appointments closed within the time period.
Denominator: All appointments closed during the week or unclosed from the prior week.
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


-- All relevant appointments
;WITH Appointments AS (
    SELECT 
        vf.VisitKey, 
        vf.Closed,
        pc.ProviderDurableKey,
        pc.ProviderName,
        appt.DateValue AS ApptDate,
        closed.DateValue AS CloseDate
    FROM cdw.FullAccess.VisitFact vf 
    JOIN #ProviderCore pc ON pc.ProviderDurableKey = vf.PrimaryVisitProviderDurableKey 
    JOIN cdw.FullAccess.DateDim appt ON appt.DateKey = vf.AppointmentDateKey
    JOIN cdw.FullAccess.DateDim closed ON closed.DateKey = vf.ClosedDateKey
    WHERE vf.Count = 1 AND appt.DateValue BETWEEN @startdate AND @enddate
),

-- Denominator: all appointments closed (or still open) on the appointment date
Denominator AS (
    SELECT 
        ProviderDurableKey,
        ApptDate AS MetricDate,
        VisitKey
    FROM Appointments
),

-- Numerator: only visits closed on same day
Numerator AS (
    SELECT 
        ProviderDurableKey,
        ApptDate AS MetricDate,
        VisitKey
    FROM Appointments
    WHERE Closed = 1 AND ApptDate = CloseDate
)

-- Final daily metric output
SELECT 
    ddates.ProviderDurableKey,
    ddates.ProviderName,
    ddates.MetricDate [ActivityDate],
    COUNT(DISTINCT d.VisitKey) AS TotalAppointments,
    COUNT(DISTINCT n.VisitKey) AS SameDayClosed,
    ROUND(
        CAST(COUNT(DISTINCT n.VisitKey) AS FLOAT) / NULLIF(COUNT(DISTINCT d.VisitKey), 0) * 100, 
        2
    ) AS SameDayClosingPercentage
FROM (
    -- build calendar x provider matrix
    SELECT 
        p.ProviderDurableKey,
        p.ProviderName,
        d.DateValue AS MetricDate
    FROM #ProviderCore p
    CROSS JOIN (
        SELECT DateValue 
        FROM cdw.FullAccess.DateDim 
        WHERE DateValue BETWEEN @startdate AND @enddate
    ) d
) ddates
LEFT JOIN Denominator d 
    ON d.ProviderDurableKey = ddates.ProviderDurableKey 
    AND d.MetricDate = ddates.MetricDate
LEFT JOIN Numerator n 
    ON n.ProviderDurableKey = ddates.ProviderDurableKey 
    AND n.MetricDate = ddates.MetricDate 
    AND n.VisitKey = d.VisitKey
GROUP BY 
    ddates.ProviderDurableKey, ddates.ProviderName, ddates.MetricDate
ORDER BY 
    ddates.ProviderName, ddates.MetricDate;