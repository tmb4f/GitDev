USE CDW

IF OBJECT_ID('tempdb..#transport ') IS NOT NULL
DROP TABLE #transport

SELECT DISTINCT
             hlrf.LogisticsRequestKey,
			 hlrf.Region,
			 hlrf.RegionEpicId,
			 hlrf.CreationDepartmentKey,
			 dep.LocationEpicId,
			 dep.LocationName,
			 RootTable0.LogisticsRequestEpicId,
			 RootTable0.LogisticsRequestJobKey AS LogisticsRequestJobKey,
			 RootTable0.TaskSubtype,
			 hlraf.LogisticsRequestAssignmentKey,
			 hlraf._CreationInstant,
			 hlraf.ResolutionDateKey,
			 --hlraf. LogisticsRequestEpicCsn,
			 --ed.EmployeeKey,
			 --ed.Name AS EmployeeName,
			 --ed.EmployeeNumber,
			 --ed.EmployeeHkrEpicId,
			 RootTable0.ActivationDateKey,
			 RootTable0.Status AS LogisticsRequestJobName,
			 RootTable0.Delayed,
			 RootTable0.Postponed,
			 RootTable0.Completed,
			 RootTable0.Canceled,
			 RootTable0.CancelReason,
			 RootTable0.Held,
			 RootTable0._HasSourceClarity,
			 RootTable0.UnplannedInstant,
			 RootTable0.PlannedInstant,
			 RootTable0.AssignedInstant,
			 RootTable0.AcknowledgedInstant,
			 RootTable0.InProgressInstant,
			 RootTable0.CompletedInstant,
			 RootTable0.TotalDelayedTimeInMinutes,
			 RootTable0.PostponedAfterInProgress,
			 RootTable0.TotalPostponedTimeInMinutes,
			 RootTable0.CanceledInstant,
			 RootTable0.TotalHeldTimeInMinutes,
			 dd.DateValue,
			 dd.MonthName,
			 dd.MonthNumber
			INTO #transport
            FROM
             dbo.LogisticsRequestJobFact AS RootTable0
			 LEFT OUTER JOIN dbo.LogisticsRequestFact AS hlrf
			 ON hlrf.LogisticsRequestKey = RootTable0.LogisticsRequestKey
			 LEFT OUTER JOIN dbo.LogisticsRequestAssignmentFact hlraf
			 ON hlraf.LogisticsRequestJobKey = RootTable0.LogisticsRequestJobKey
			 --LEFT OUTER JOIN
			 --(
			 --SELECT
				--EmployeeKey,
    --            DurableKey,
    --            EmployeeEpicId,
    --            Name,
    --            EmployeeNumber,
    --            EmployeeHkrEpicId
			 --FROM dbo.EmployeeDim
			 --WHERE IsCurrent = 1
			 --) ed
			 --ON ed.DurableKey = hlraf.EmployeeDurableKey
			 LEFT OUTER JOIN dbo.DepartmentDim dep ON dep.DepartmentKey = hlrf.CreationDepartmentKey
			 LEFT OUTER JOIN dbo.DateDim dd ON dd.DateKey = hlraf.ResolutionDateKey
            WHERE (
              (  
				--(  (  ( RootTable0.ActivationDateKey < '20240501' ) 
    --                    AND NOT  ( RootTable0.ActivationDateKey < 0 )  ) 
    --                AND  (  ( RootTable0.ActivationDateKey >= '20240401' )  OR  ( RootTable0.ActivationDateKey < 0 )  )  ) )
				--(  ( CAST(hlraf._CreationInstant AS DATE) < '4/1/2024' ) 
    --                AND  (  CAST( hlraf._CreationInstant AS DATE) >= '3/1/2024' )  )  ) 
				--(  ( CAST(hlraf._CreationInstant AS DATE) < '5/1/2024' ) 
    --                AND  (  CAST( hlraf._CreationInstant AS DATE) >= '4/1/2024' )  )  )   
				--(  (  ( hlraf.ResolutionDateKey < '20240501' ) 
				(  (  ( hlraf.ResolutionDateKey < '20250110' ) 
                        AND NOT  ( hlraf.ResolutionDateKey < 0 )  ) 
                    --AND  (  ( hlraf.ResolutionDateKey >= '20240301' )  OR  ( hlraf.ResolutionDateKey < 0 )  )  ) )
                    AND  (  ( hlraf.ResolutionDateKey >= '20250101' )  OR  ( hlraf.ResolutionDateKey < 0 )  )  ) )
                AND  ( RootTable0.PatientDurableKey IN  ( 
                        SELECT
                         DurableKey
                        FROM
                         PatientDim
                        WHERE
                          ( IsValid = 1
                            AND IsCurrent = 1
                            AND IsHistoricalPatient = 0 )  OR DurableKey < 0  )  ) 
                --AND  (  RootTable0.TaskSubtype = 'Patient Transport'  ) 
                AND  (  RootTable0.TaskSubtype IN ('Patient Transport','Other')  ) 
                AND EXISTS  ( 
                    SELECT
                     1
                    FROM
                     dbo.DepartmentDim AS FilterTable1
                    --WHERE
                    --  (   (   (  FilterTable1.ServiceAreaEpicId = '10'  )  OR  (  FilterTable1.ServiceAreaEpicId = '30'  )   ) 
                    --    AND  (  RootTable0.ServiceAreaKey = FilterTable1.DepartmentKey  )  )  )   )
                    --WHERE
                    --  (  RootTable0.ServiceAreaKey = FilterTable1.DepartmentKey  )))
  --                  WHERE
  --                    (  RootTable0.ServiceAreaKey = FilterTable1.DepartmentKey  )
  --AND dep.LocationEpicId = '10243' -- UVA University Hospital East
  --AND hlrf.RegionEpicId IN
  --(3100000086, -- General - UVA GRAND CENTRAL UVA HOSPITAL TRANSPORT
  -- 3100000108, -- General - UVA GRAND CENTRAL CULPEPER HOSPITAL TRANSPORT
  -- 3100000113  -- General - UVA GRAND CENTRAL PRINCE WILLIAM MEDICAL CENTER TRANSPORT
  -- )))
  /*
                    WHERE
                      --dep.LocationEpicId = '10243' -- UVA University Hospital East
                      (dep.LocationEpicId = '10243' -- UVA University Hospital East
					   OR dep.LocationEpicId = '10354') -- UVA Children's Hospital Clinics Battle Building
  AND hlrf.RegionEpicId IN
  (3100000086, -- General - UVA GRAND CENTRAL UVA HOSPITAL TRANSPORT
   3100000108, -- General - UVA GRAND CENTRAL CULPEPER HOSPITAL TRANSPORT
   3100000113  -- General - UVA GRAND CENTRAL PRINCE WILLIAM MEDICAL CENTER TRANSPORT
   )))
   */
                    WHERE 1 =1
  --AND hlrf.RegionEpicId IN (3100000086 -- General - UVA GRAND CENTRAL UVA HOSPITAL TRANSPORT
  --                                          )
  AND RootTable0.Status <> 'Canceled'
  AND RootTable0.Delayed = 1
  ))
--ORDER BY
--	RootTable0.TaskSubtype,
--	hlrf.LogisticsRequestKey,
--	RootTable0.LogisticsRequestJobKey,
--	--hlraf.LogisticsRequestAssignmentKey,
--	RootTable0.ActivationDateKey
----ORDER BY
----	RootTable0.TaskSubtype,
----	RootTable0.ActivationDateKey,
----	hlrf.LogisticsRequestKey,
----	RootTable0.LogisticsRequestJobKey
----	--hlraf.LogisticsRequestAssignmentKey,

SELECT
	*
FROM #transport
ORDER BY ResolutionDateKey
/*
SELECT
    LogisticsRequestEpicId,
    TaskSubtype,
	ResolutionDateKey,
    ActivationDateKey,
	LogisticsRequestKey,
    Region,
    RegionEpicId,
    CreationDepartmentKey,
    LocationEpicId,
    LocationName,
    LogisticsRequestJobKey,
    LogisticsRequestAssignmentKey,
    _CreationInstant,
    LogisticsRequestJobName,
    Delayed,
    Postponed,
    Completed,
    Canceled,
    CancelReason,
    Held,
    _HasSourceClarity,
    UnplannedInstant,
    PlannedInstant,
    AssignedInstant,
    AcknowledgedInstant,
    InProgressInstant,
    CompletedInstant,
    TotalDelayedTimeInMinutes,
    PostponedAfterInProgress,
    TotalPostponedTimeInMinutes,
    CanceledInstant,
    TotalHeldTimeInMinutes
FROM #transport
--ORDER BY
--	TaskSubtype,
--	LogisticsRequestKey,
--	LogisticsRequestJobKey,
--	LogisticsRequestAssignmentKey,
--	ActivationDateKey
--ORDER BY
--	RootTable0.TaskSubtype,
--	RootTable0.ActivationDateKey,
--	hlrf.LogisticsRequestKey,
--	RootTable0.LogisticsRequestJobKey
--	--hlraf.LogisticsRequestAssignmentKey,
ORDER BY
	LogisticsRequestEpicId

--SELECT DISTINCT
--	LogisticsRequestKey
--FROM #transport
--ORDER BY
--	LogisticsRequestKey

SELECT
    MonthName,
	MonthNumber,
    Region,
    LocationName,
	TaskSubtype,
	COUNT(*) AS Total_Requests
FROM #transport
GROUP BY
	MonthName,
	MonthNumber,
	TaskSubtype,
    Region,
    LocationName
ORDER BY
	MonthNumber,
	MonthName,
	TaskSubtype,
    Region,
    LocationName
--ORDER BY
--	TaskSubtype,
--	LogisticsRequestKey,
--	LogisticsRequestJobKey,
--	LogisticsRequestAssignmentKey,
--	ActivationDateKey
--ORDER BY
--	RootTable0.TaskSubtype,
--	RootTable0.ActivationDateKey,
--	hlrf.LogisticsRequestKey,
--	RootTable0.LogisticsRequestJobKey
--	--hlraf.LogisticsRequestAssignmentKey,
--ORDER BY
--	LogisticsRequestEpicId

SELECT
    MonthName,
	MonthNumber,
	TaskSubtype AS Transport_Type,
	COUNT(*) AS Total_Requests
FROM #transport
GROUP BY
	MonthName,
	MonthNumber,
	TaskSubtype
ORDER BY
	MonthNumber,
	MonthName,
	TaskSubtype
*/