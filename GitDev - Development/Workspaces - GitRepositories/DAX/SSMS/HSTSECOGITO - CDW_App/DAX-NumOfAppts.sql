use CDW_App;

Declare @startdate date = '06/01/2024',@enddate date = '04/16/2025';


/*
o	Number of appointments per day
Appointments per Day [43]
Average number of appointments per day within the reporting period.
Numerator: Appointments within the reporting period.
Denominator: Scheduled days within the reporting period.
Final Value: Numerator/ Denominator

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


-- Step 1: All scheduled appointments within period
DROP TABLE IF EXISTS #NumApptsDenom;
DROP TABLE IF EXISTS #NumApptsNumerator;

;WITH NumApptsDenom AS (
	SELECT DISTINCT
		vf.VisitKey, 
		vf.EncounterEpicCsn,
		vf.EncounterKey,
		vf.AppointmentStatus,
		pc.ProviderDurableKey,
		pc.ProviderName,
		pc.ProviderEpicID,
		vf.AppointmentDateKey,
		appt.DateValue AS AppointmentDate,
		appt.DayOfWeek
	FROM cdw.FullAccess.VisitFact vf 
	JOIN #ProviderCore pc ON pc.ProviderDurableKey = vf.PrimaryVisitProviderDurableKey 
	JOIN cdw.FullAccess.DateDim appt ON appt.DateKey = vf.AppointmentDateKey
	WHERE 
		vf.Count = 1
		AND appt.DateValue BETWEEN @startdate AND @enddate
)
SELECT * INTO #NumApptsDenom FROM NumApptsDenom;

/* Step 2: Completed appointments only (numerator) */
;WITH NumApptsNumerator AS (
    SELECT * 
    FROM #NumApptsDenom
    WHERE AppointmentStatus = 'Completed'
)
SELECT * INTO #NumApptsNumerator FROM NumApptsNumerator;

/* Step 3: Final metric with ScheduledDays column */
SELECT 
    calendar.ProviderDurableKey,
    calendar.ProviderName,
    calendar.AppointmentDay,
    ISNULL(appt.ScheduledDays, 0) AS ScheduledDays,
    ISNULL(appt.CompletedAppointments, 0) AS CompletedAppointments,
    CASE 
        WHEN ISNULL(appt.CompletedAppointments, 0) = 0 THEN 0
        ELSE CAST(NULLIF(ISNULL(appt.CompletedAppointments, 0), 0) AS FLOAT) / CAST(ISNULL(appt.ScheduledDays, 0) AS FLOAT) * 100
    END AS [AppointmentsPerDay%]
FROM (
    SELECT 
        p.ProviderDurableKey,
        p.ProviderName,
        d.DateValue AS AppointmentDay
    FROM #ProviderCore p
    CROSS JOIN (
        SELECT DateValue 
        FROM cdw.FullAccess.DateDim 
        WHERE DateValue BETWEEN @startdate AND @enddate
    ) d
) calendar
LEFT JOIN (
    SELECT 
        vf.PrimaryVisitProviderDurableKey AS ProviderDurableKey,
        pc.ProviderName,
        dd.DateValue AS AppointmentDay,
        COUNT(DISTINCT dd.DateValue) AS ScheduledDays,
        COUNT(DISTINCT vf.VisitKey) AS CompletedAppointments
    FROM cdw.FullAccess.VisitFact vf
    JOIN cdw.FullAccess.DateDim dd ON dd.DateKey = vf.AppointmentDateKey
    JOIN #ProviderCore pc ON pc.ProviderDurableKey = vf.PrimaryVisitProviderDurableKey
    WHERE 
        vf.Count = 1
        AND vf.AppointmentStatus = 'Completed'
        AND dd.DateValue BETWEEN @startdate AND @enddate
    GROUP BY 
        vf.PrimaryVisitProviderDurableKey, pc.ProviderName, dd.DateValue
) appt 
    ON calendar.ProviderDurableKey = appt.ProviderDurableKey 
    AND calendar.AppointmentDay = appt.AppointmentDay
ORDER BY 
    calendar.ProviderName, calendar.AppointmentDay;