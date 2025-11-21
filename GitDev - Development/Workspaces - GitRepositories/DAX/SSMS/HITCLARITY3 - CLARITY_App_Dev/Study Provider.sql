USE [CLARITY]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET NOCOUNT ON;

DECLARE @StartDate	DATE 
DECLARE @EndDate		SMALLDATETIME

/*	Completed visit dates	*/
SET @StartDate = '5/1/2024'
SET @EndDate = CAST(GETDATE() AS DATE)

-- Convert dates to BIGINT to allow filtering on DateKeys.
DECLARE @locStartDateKey BIGINT = CAST(CONVERT(CHAR(8), @StartDate, 112) AS BIGINT);
DECLARE @locEndDateKey BIGINT = CAST(CONVERT(CHAR(8), DATEADD(DAY, 1, @EndDate), 112) AS BIGINT);

IF OBJECT_ID('tempdb..#prov_study ') IS NOT NULL
DROP TABLE #prov_study

IF OBJECT_ID('tempdb..#ee_index ') IS NOT NULL
DROP TABLE #ee_index

IF OBJECT_ID('tempdb..#prov_index ') IS NOT NULL
DROP TABLE #prov_index


SELECT
	*
FROM [CLARITY_App_Dev].Rptg.TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider_20251118 email
WHERE 1 = 1
  --      AND email.Provider_Email IS NOT NULL
		--AND email.Rollout_Wave_Assignment IN ('Wave 1','Wave 2','Wave 3','Wave 4','Wave 5')
ORDER BY
	email.Provider_Email
;
-- This CTE parses out the study provider computing id from the email address in the source data table
--WITH cte_study (Provider_Name, Provider_Email, first_name, last_name, RowId, ComputingId)
WITH cte_study (Provider_Name, Provider_Email, first_name, last_name, ComputingId)
AS (SELECT DISTINCT
           list.Provider_Name,
		   list.Provider_Email,
           --CASE
           --    WHEN CHARINDEX(' ', list.first_name) > 0 THEN
           --        SUBSTRING(list.first_name, 1, CHARINDEX(' ', list.first_name) - 1)
           --    ELSE
           --        list.first_name
           --END AS [first_name],
		   list.first_name,
		   --list.last_name,
           CASE
               WHEN CHARINDEX(' ', list.last_name) > 0 THEN
                   --SUBSTRING(list.last_name, 1, CHARINDEX(' ', list.last_name) - 1)
                   SUBSTRING(list.last_name, CHARINDEX(' ', list.last_name) + 1,150)
               ELSE
                   list.last_name
           END AS [last_name],
		   --list.RowId,
		   list.ComputingId
    FROM
    (
        SELECT email.Provider_Name,
			   email.Provider_Email,
               --SUBSTRING(email.Provider_Name, CHARINDEX(' ', email.Provider_Name) + 1, LEN(email.Provider_Name) - CHARINDEX(' ', email.Provider_Name)) AS [first_name],
               SUBSTRING(email.Provider_Name, CHARINDEX(' ', email.Provider_Name) + 1, LEN(email.Provider_Name) - CHARINDEX(' ', email.Provider_Name)) AS [last_name],
               --SUBSTRING(email.Provider_Name, 1, CHARINDEX(' ', email.Provider_Name) - 2) AS [last_name],
               --SUBSTRING(email.Provider_Name, 1, CHARINDEX(' ', email.Provider_Name) - 2) AS [first_name],
               SUBSTRING(email.Provider_Name, 1, CHARINDEX(' ', email.Provider_Name) - 1) AS [first_name],
			   --email.RowId,
		       CASE WHEN CHARINDEX('@',email.Provider_Email,1) > 0 THEN UPPER(LEFT(email.Provider_Email,CHARINDEX('@',email.Provider_Email,1) - 1)) ELSE NULL END AS ComputingId
        FROM [CLARITY_App_Dev].Rptg.TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider_20251118 email
        WHERE 1 = 1
     --         AND email.Provider_Email IS NOT NULL
			  --AND email.Rollout_Wave_Assignment IN ('Wave 1','Wave 2','Wave 3','Wave 4','Wave 5')
    ) list)

SELECT * INTO #prov_study FROM cte_study
CREATE CLUSTERED INDEX  study ON #prov_study (ComputingId)

SELECT
	*
FROM #prov_study
ORDER BY
	ComputingId

SELECT DISTINCT
	Provider_Email,
    ComputingId
FROM #prov_study
ORDER BY
	Provider_Email

/*
;
-- This CTE parses out the employee computing id from the email address in the EmployeeDim table
-- It also parses out the employee's First Name and Last Name from EmployeeDim.Name.
-- The outer SELECT is used to remove the Middle Initial / Name.
WITH cte_ee (EmployeeDurableKey, Name, Email, first_name, last_name, ComputingId)
--WITH cte_ee (EmployeeDurableKey, Name, Email, ComputingId)
AS (SELECT list.EmployeeDurableKey,
           list.Name,
		   list.Email,
           CASE
               WHEN list.NoComma = 0 AND CHARINDEX(' ', list.first_name) > 0 THEN
                   SUBSTRING(list.first_name, 1, CHARINDEX(' ', list.first_name) - 1)
               ELSE
                   list.first_name
           END AS [first_name],
		   list.last_name,
		   list.ComputingId
    FROM
    (
        SELECT ee.DurableKey AS [EmployeeDurableKey],
               ee.Name,
			   ee.Email,
               --SUBSTRING(ee.Name, CHARINDEX(' ', ee.Name) + 1, LEN(ee.Name) - CHARINDEX(' ', ee.Name)) AS [first_name],
               --SUBSTRING(ee.Name, 1, CHARINDEX(' ', ee.Name) - 2) AS [last_name],
               CASE WHEN CHARINDEX(',', TRIM(ee.Name)) = 0 THEN 1 ELSE 0 END AS NoComma,
               CASE WHEN CHARINDEX(',', TRIM(ee.Name)) = 0 THEN TRIM(ee.Name)
			              ELSE TRIM(SUBSTRING(TRIM(ee.Name), CHARINDEX(',', TRIM(ee.Name)) + 1, LEN(TRIM(ee.Name)) - CHARINDEX(',', TRIM(ee.Name)))) END AS [first_name],
               CASE WHEN CHARINDEX(',', TRIM(ee.Name)) = 0 THEN NULL
			              ELSE SUBSTRING(TRIM(ee.Name), 1, CHARINDEX(',', TRIM(ee.Name)) - 1) END AS [last_name],
		       CASE WHEN LEN(ee.Email) > 0 AND CHARINDEX('@',ee.[Email],1) > 0 THEN UPPER(LEFT(ee.[Email],CHARINDEX('@',ee.[Email],1) - 1)) ELSE NULL END AS ComputingId
        FROM [CDW].[FullAccess].[EmployeeDim] ee
        WHERE 1 = 1
              --AND prov.DurableKey = @ProviderDurableKey
              AND ee.IsCurrent = 1
			  --AND LEN(ee.Email) > 0
			  --AND CHARINDEX('@',ee.[Email],1) > 0
    ) list
		INNER JOIN
		(
		SELECT DISTINCT
			Provider_Email,
            ComputingId
		FROM #prov_study
		) prov_study
		ON list.ComputingId = prov_study.ComputingId
	)

SELECT * INTO #ee_index FROM cte_ee
CREATE CLUSTERED INDEX  ee ON #ee_index (EmployeeDurableKey)

SELECT
	*
FROM #ee_index
--ORDER BY
--	EmployeeDurableKey
ORDER BY
	ComputingId
*/
/*
;

-- This CTE parses out the provider's First Name and Last Name from ProviderDim.Name.
-- The outer SELECT is used to remove the Middle Initial / Name.
--WITH cte_prov (ProviderDurableKey, Name, Email, first_name, last_name, EmployeeDurableKey, ComputingId, RowId)
WITH cte_prov (ProviderDurableKey, Name, Email, first_name, last_name, EmployeeDurableKey, ComputingId, PrimarySpecialty, IndexSpecialty, OfficeAddress, IndexLocation, Type, PrimaryLocation, PrimaryDepartment)
AS (SELECT list.ProviderDurableKey,
           list.Name,
		   list.Email,
           CASE
               WHEN CHARINDEX(' ', list.first_name) > 0 THEN
                   SUBSTRING(list.first_name, 1, CHARINDEX(' ', list.first_name) - 1)
               ELSE
                   list.first_name
           END AS [first_name],
           list.last_name,
		   list.EmployeeDurableKey,
		   list.ComputingId,
		   --list.RowId
		   list.PrimarySpecialty,
		   COALESCE(list.PrimarySpecialty, list.Type) AS IndexSpecialty,
		   list.OfficeAddress,
		   list.OfficeAddress AS IndexLocation,
		   list.Type,
		   list.PrimaryLocation,
		   list.PrimaryDepartment
    FROM
    (
        SELECT prov.DurableKey AS [ProviderDurableKey],
               prov.Name,
			   ee.Email,
               SUBSTRING(prov.Name, CHARINDEX(' ', prov.Name) + 1, LEN(prov.Name) - CHARINDEX(' ', prov.Name)) AS [first_name],
               SUBSTRING(prov.Name, 1, CHARINDEX(' ', prov.Name) - 2) AS [last_name],
			   prov.EmployeeDurableKey,
			   ee.ComputingId,
			   --email.RowId
			   CASE WHEN LEN(prov.PrimarySpecialty) > 0 THEN prov.PrimarySpecialty ELSE NULL END AS PrimarySpecialty,
			   prov.OfficeAddress,
			   prov.Type,
			   prov.PrimaryLocation,
			   prov.PrimaryDepartment
        --FROM #ee_index ee
        FROM
		(
		SELECT
			ee.EmployeeDurableKey,
            --ee.Name,
            ee.Email,
            --ee.first_name,
            --ee.last_name,
            ee.ComputingId
		FROM #ee_index ee
		WHERE 1 = 1
		AND LEN(ee.Email) > 0
		AND CHARINDEX('@',ee.[Email],1) > 0
		) ee
		INNER JOIN #prov_study email
			ON ee.ComputingId = email.ComputingId
		LEFT JOIN [CDW].[FullAccess].[ProviderDim] prov
			ON prov.EmployeeDurableKey = ee.EmployeeDurableKey
        WHERE 1 = 1
              --AND prov.DurableKey = @ProviderDurableKey
              AND prov.IsCurrent = 1
    ) list )

SELECT * INTO #prov_index FROM cte_prov
CREATE CLUSTERED INDEX  prov ON #prov_index (ProviderDurableKey, EmployeeDurableKey)

SELECT --DISTINCT
	--IndexSpecialty,
 --   IndexLocation
	*
FROM #prov_index
--ORDER BY
--	--IndexLocation,
--	--IndexSpecialty
--	ProviderDurableKey
ORDER BY
	Email
*/
GO


