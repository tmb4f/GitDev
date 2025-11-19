USE [CDW_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [Rptg].[uspSrc_RAD_Imaging_Studies_Attributed_to_Provider]
--(
--    @StartDate AS DATE = NULL,
--    @EndDate AS DATE = NULL,
--	@ProviderDurableKey AS BIGINT = NULL
--)
--AS

/*************************************************************************************************************************
WHAT:	[CDW_App].[Rptg].[uspSrc_RAD_Imaging_Studies_Attributed_to_Provider]
WHO :	Monica Seale (MSF8H)
WHEN:	08/21/2023
WHY :	User Story #37902 - Rad All Studies by Reading Doctor by CPT Report.
--------------------------------------------------------------------------------------------------------------------------
INFO: 
	INPUTS:

		[CDW].[FullAccess].[ImagingFact]				-- Imaging Studies
		[CDW].[FullAccess].[PatientDim]					-- Patient
        [CDW].[FullAccess].[ProcedureDim]				-- First Orderable / Performable Procedure
        [CDW].[FullAccess].[ProviderDim]				-- Ordering Provider & Attending (Finalizing) Provider
        [CDW].[FullAccess].[ProviderBridge]				-- Reading Providers (providers in a combination)
        [CDW].[FullAccess].[EmployeeDim]				-- Technologist (employee)
        [CDW].[FullAccess].[DepartmentDim]				-- Ordering Contact Department & Performing Department
        [CDW].[FullAccess].[ImagingTextFact]			-- Used to perform full-text search on provider's first and last names in Result Text.
        [CDW_App].[Rptg].[vwRef_MDM_Location_Master]	-- Used to get Hospital Code for filtering between MC and CH locations.
			
	OUTPUTS:

		* The granularity is one row per imaging study (Accession Number).

    NOTES:

		* The Hospital Code is based on the Performing Department.

--------------------------------------------------------------------------------------------------------------------------
MODS:  08/24/2023 MSF8H - Modified to perform SQL Server full-text search instead of using SQL LIKE operator.
       09/21/2023 MSF8H - Added Reading Providers combined in one column along with other changes & fixes.
**************************************************************************************************************************/

SET NOCOUNT ON;

-- For testing.
/*
DECLARE @StartDate DATE = '2022-07-01',
        @EndDate DATE = '2023-06-30',
        @ProviderDurableKey BIGINT = 914608; -- KHOZOUZ, OMAR
*/

-- Set default dates.
/*
IF @StartDate IS NULL
   AND @EndDate IS NULL
BEGIN
    -- First day of prior month.
    SET @StartDate = DATEADD(DAY, 1, EOMONTH(GETDATE(), -2));
    -- Last day of prior month.
    SET @EndDate = EOMONTH(GETDATE(), -1);
END;
*/
DECLARE @StartDate	DATE 
DECLARE @EndDate		SMALLDATETIME

/*	Completed visit dates	*/
SET @StartDate = '5/1/2024'
SET @EndDate = CAST(GETDATE() AS DATE)

-- Convert dates to BIGINT to allow filtering on DateKeys.
DECLARE @locStartDateKey BIGINT = CAST(CONVERT(CHAR(8), @StartDate, 112) AS BIGINT);
DECLARE @locEndDateKey BIGINT = CAST(CONVERT(CHAR(8), DATEADD(DAY, 1, @EndDate), 112) AS BIGINT);

/*
-- These variables are used to perform a full-text search on the provider's first and last names in the Result Text.
-- Can't have a NULL or empty full-text predicate.
DECLARE @FirstName NVARCHAR(200) = 'TempFirstName';
DECLARE @LastName NVARCHAR(300) = 'TempLastName';
*/

IF OBJECT_ID('tempdb..#prov_study ') IS NOT NULL
DROP TABLE #prov_study

IF OBJECT_ID('tempdb..#ee_index ') IS NOT NULL
DROP TABLE #ee_index

IF OBJECT_ID('tempdb..#prov_index ') IS NOT NULL
DROP TABLE #prov_index

IF OBJECT_ID('tempdb..#control_prov_index ') IS NOT NULL
DROP TABLE #control_prov_index
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
        FROM [CDW_App_Dev].Rptg.TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider email
        WHERE 1 = 1
              AND email.Provider_Email IS NOT NULL
			  AND email.Rollout_Wave_Assignment IN ('Wave 1','Wave 2','Wave 3','Wave 4','Wave 5')
    ) list)

SELECT * INTO #prov_study FROM cte_study
CREATE CLUSTERED INDEX  study ON #prov_study (ComputingId)

--SELECT
--	*
--FROM #prov_study
--ORDER BY
--	ComputingId

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
    ) list )

SELECT * INTO #ee_index FROM cte_ee
CREATE CLUSTERED INDEX  ee ON #ee_index (EmployeeDurableKey)

--SELECT
--	*
--FROM #ee_index
--ORDER BY
--	EmployeeDurableKey
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
ORDER BY
	--IndexLocation,
	--IndexSpecialty
	ProviderDurableKey
/*
		SELECT
			   prov.DurableKey AS [ProviderDurableKey],
               prov.Name,
			   CHARINDEX(' ', prov.Name) + 1 AS FirstSubstrStart,
			   LEN(prov.Name) - CHARINDEX(' ', prov.Name) AS FirstSubstrLength,
               --SUBSTRING(prov.Name, CHARINDEX(' ', prov.Name) + 1, LEN(prov.Name) - CHARINDEX(' ', prov.Name)) AS [first_name],
               CASE WHEN CHARINDEX(' ', prov.Name) = 0 THEN prov.NAME
						WHEN LEN(TRIM(prov.name)) = CHARINDEX(',', prov.Name) THEN NULL
						ELSE SUBSTRING(prov.Name, CHARINDEX(' ', prov.Name) + 1, LEN(prov.Name) - CHARINDEX(' ', prov.Name)) END AS [first_name],
			   CHARINDEX(' ', prov.Name) - 2 AS LastSubstrLength,
               --SUBSTRING(prov.Name, 1, CHARINDEX(' ', prov.Name) - 2) AS [last_name],
               CASE WHEN LEN(TRIM(prov.name)) = CHARINDEX(',', prov.Name) THEN SUBSTRING(prov.Name, 1, CHARINDEX(',', prov.Name) - 1)
						WHEN CHARINDEX(' ', prov.Name) = 0 THEN NULL
						ELSE SUBSTRING(prov.Name, 1, CHARINDEX(' ', prov.Name) - 2) END AS [last_name],
			   prov.EmployeeDurableKey,
			   CASE WHEN LEN(prov.PrimarySpecialty) > 0 THEN prov.PrimarySpecialty ELSE NULL END AS PrimarySpecialty,
			   prov.OfficeAddress,
			   prov.Type,
			   prov.PrimaryLocation,
			   prov.PrimaryDepartment
		FROM [CDW].[FullAccess].[ProviderDim] prov
        WHERE 1 = 1
              AND prov.IsCurrent = 1
			  AND prov.Name NOT IN ('*Deleted','*Not Applicable','*Unknown','*Unspecified')
			  AND LEN(TRIM(prov.Name)) > 0
*/
/*
;

-- This CTE parses out the provider's First Name and Last Name from ProviderDim.Name.
-- The outer SELECT is used to remove the Middle Initial / Name.
--WITH cte_prov (ProviderDurableKey, Name, Email, first_name, last_name, EmployeeDurableKey, ComputingId, RowId)
WITH cte_control_prov (ProviderDurableKey, Name, Email, first_name, last_name, EmployeeDurableKey, ComputingId, PrimarySpecialty, IndexSpecialty, OfficeAddress, IndexLocation, Type, PrimaryLocation, PrimaryDepartment)
AS --(
/*
SELECT list.ProviderDurableKey,
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
*/
		(
		SELECT
			ee.EmployeeDurableKey,
            ee.Email,
            ee.ComputingId,
            prov.ProviderDurableKey,
            prov.Name,
            prov.first_name,
            prov.last_name,
            prov.PrimarySpecialty,
            prov.IndexSpecialty,
            prov.OfficeAddress,
            prov.IndexLocation,
            prov.Type,
            prov.PrimaryLocation,
            prov.PrimaryDepartment
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
		--WHERE 1 = 1
		--AND LEN(ee.Email) > 0
		--AND CHARINDEX('@',ee.[Email],1) > 0
		) ee
		INNER JOIN
		(
		SELECT
			list.ProviderDurableKey,
            list.Name,
            list.first_name,
            list.last_name,
            list.EmployeeDurableKey,
            list.PrimarySpecialty,
            list.IndexSpecialty,
            list.OfficeAddress,
            list.IndexLocation,
            list.Type,
            list.PrimaryLocation,
            list.PrimaryDepartment
		FROM
		(
		SELECT
		   prov.ProviderDurableKey,
           prov.Name,
		   --prov.Email,
           CASE
               WHEN CHARINDEX(' ', prov.first_name) > 0 THEN
                   SUBSTRING(prov.first_name, 1, CHARINDEX(' ', prov.first_name) - 1)
               ELSE
                   prov.first_name
           END AS [first_name],
           prov.last_name,
		   prov.EmployeeDurableKey,
		   --prov.ComputingId,
		   --prov.RowId
		   prov.PrimarySpecialty,
		   COALESCE(prov.PrimarySpecialty, prov.Type) AS IndexSpecialty,
		   prov.OfficeAddress,
		   prov.OfficeAddress AS IndexLocation,
		   prov.Type,
		   prov.PrimaryLocation,
		   prov.PrimaryDepartment
		FROM 
		(
		SELECT
			   prov.DurableKey AS [ProviderDurableKey],
               prov.Name,
               SUBSTRING(prov.Name, CHARINDEX(' ', prov.Name) + 1, LEN(prov.Name) - CHARINDEX(' ', prov.Name)) AS [first_name],
               SUBSTRING(prov.Name, 1, CHARINDEX(' ', prov.Name) - 2) AS [last_name],
			   prov.EmployeeDurableKey,
			   CASE WHEN LEN(prov.PrimarySpecialty) > 0 THEN prov.PrimarySpecialty ELSE NULL END AS PrimarySpecialty,
			   prov.OfficeAddress,
			   prov.Type,
			   prov.PrimaryLocation,
			   prov.PrimaryDepartment
		FROM [CDW].[FullAccess].[ProviderDim] prov
        WHERE 1 = 1
              AND prov.IsCurrent = 1
			  AND prov.Name NOT IN ('*Deleted','*Not Applicable','*Unknown','*Unspecified')
		) prov
        ) list
		INNER JOIN
		(
		SELECT DISTINCT
			prov.IndexSpecialty,
			prov.IndexLocation
		FROM #prov_index prov
		WHERE prov.IndexSpecialty <> '*Unspecified'
			AND LEN(prov.IndexLocation) > 0
		) prov_study
		ON list.IndexSpecialty = prov_study.IndexSpecialty
			AND list.IndexLocation = prov_study.IndexLocation
		) prov
		--#prov_study email
		ON ee.EmployeeDurableKey = prov.EmployeeDurableKey
		--LEFT JOIN [CDW].[FullAccess].[ProviderDim] prov
		--	ON prov.EmployeeDurableKey = ee.EmployeeDurableKey
  --      WHERE 1 = 1
  --            --AND prov.DurableKey = @ProviderDurableKey
  --            AND prov.IsCurrent = 1
  )

SELECT * INTO #control_prov_index FROM cte_control_prov
CREATE CLUSTERED INDEX  prov ON #control_prov_index (ProviderDurableKey, EmployeeDurableKey)

SELECT
	*
FROM #control_prov_index
ORDER BY
	--RowId
	ProviderDurableKey
*/
GO


