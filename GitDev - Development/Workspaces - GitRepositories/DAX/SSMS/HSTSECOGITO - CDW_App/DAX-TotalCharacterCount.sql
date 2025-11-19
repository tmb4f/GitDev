use CDW_App;

Declare @startdate date = '06/01/2024',@enddate date = '04/16/2025';


/*
o	Length of documentation (character count) per appointment
Length of Documentation per Appointment
Average number of characters documented per appointment.
Numerator: Characters the provider documented during appointments.
Denominator: Appointments within the reporting period.
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

-- Step 1: Get raw appointment + note char counts per appointment per day
DROP TABLE IF EXISTS #CharsPerAppointment;
SELECT 
    vf.PrimaryVisitProviderDurableKey AS ProviderDurableKey,
    pc.ProviderName,
    dd.DateValue AS AppointmentDay,
    vf.VisitKey,
    SUM(cnf.TotalCharCount) AS TotalChars
INTO #CharsPerAppointment
FROM cdw.FullAccess.VisitFact vf
JOIN cdw.FullAccess.ClinicalNoteFact cnf ON vf.EncounterKey = cnf.EncounterKey
JOIN cdw.FullAccess.DateDim dd ON dd.DateKey = vf.AppointmentDateKey
JOIN #ProviderCore pc ON pc.ProviderDurableKey = vf.PrimaryVisitProviderDurableKey
WHERE 
    vf.Count = 1 
    AND cnf.Count = 1 
    AND cnf.Status = N'Signed'
    AND dd.DateValue BETWEEN @startdate AND @enddate
GROUP BY 
    vf.PrimaryVisitProviderDurableKey, pc.ProviderName, dd.DateValue, vf.VisitKey;

-- Step 2: Roll up to daily per-provider metrics
DROP TABLE IF EXISTS #CharsPerDay;
SELECT 
    ProviderDurableKey,
    ProviderName,
    AppointmentDay,
    COUNT(DISTINCT VisitKey) AS TotalAppointments,
    SUM(TotalChars) AS TotalChars,
    CAST(SUM(TotalChars) AS FLOAT) / NULLIF(COUNT(DISTINCT VisitKey), 0) AS AvgCharsPerAppointment
INTO #CharsPerDay
FROM #CharsPerAppointment
GROUP BY ProviderDurableKey, ProviderName, AppointmentDay;

-- Step 3: Fill in 0s for missing days
SELECT 
    calendar.ProviderDurableKey,
    calendar.ProviderName,
    calendar.AppointmentDay,
    ISNULL(cp.TotalAppointments, 0) AS TotalAppointments,
    ISNULL(cp.TotalChars, 0) AS TotalChars,
    CASE 
        WHEN ISNULL(cp.TotalAppointments, 0) = 0 THEN 0
        ELSE ISNULL(cp.AvgCharsPerAppointment, 0)
    END AS AvgCharsPerAppointment
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
LEFT JOIN #CharsPerDay cp 
    ON calendar.ProviderDurableKey = cp.ProviderDurableKey 
    AND calendar.AppointmentDay = cp.AppointmentDay
ORDER BY 
   calendar.ProviderName, calendar.AppointmentDay;









