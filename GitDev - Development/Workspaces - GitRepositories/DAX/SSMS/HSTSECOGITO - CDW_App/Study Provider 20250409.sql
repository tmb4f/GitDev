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

--IF OBJECT_ID('tempdb..#user ') IS NOT NULL
--DROP TABLE #user

--IF OBJECT_ID('tempdb..#wqitem ') IS NOT NULL
--DROP TABLE #wqitem
;
-- This CTE parses out the study provider computing id from the email address in the source data table
WITH cte_study (Name, Email, first_name, last_name, RowId, ComputingId)
AS (SELECT list.Name,
		   list.Email,
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
		   list.RowId,
		   list.ComputingId
    FROM
    (
        SELECT email.Name,
			   email.Email,
               --SUBSTRING(email.Name, CHARINDEX(' ', email.Name) + 1, LEN(email.Name) - CHARINDEX(' ', email.Name)) AS [first_name],
               SUBSTRING(email.Name, CHARINDEX(' ', email.Name) + 1, LEN(email.Name) - CHARINDEX(' ', email.Name)) AS [last_name],
               --SUBSTRING(email.Name, 1, CHARINDEX(' ', email.Name) - 2) AS [last_name],
               --SUBSTRING(email.Name, 1, CHARINDEX(' ', email.Name) - 2) AS [first_name],
               SUBSTRING(email.Name, 1, CHARINDEX(' ', email.Name) - 1) AS [first_name],
			   email.RowId,
		       CASE WHEN CHARINDEX('@',email.[Email],1) > 0 THEN UPPER(LEFT(email.[Email],CHARINDEX('@',email.[Email],1) - 1)) ELSE NULL END AS ComputingId
        FROM [CDW_App_Dev].Rptg.TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider email
        WHERE 1 = 1
              AND email.Email IS NOT NULL
    ) list)

SELECT * INTO #prov_study FROM cte_study
CREATE CLUSTERED INDEX  study ON #prov_study (ComputingId)

--SELECT
--	*
--FROM #prov_study
--ORDER BY
--	RowId

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
               WHEN CHARINDEX(' ', list.first_name) > 0 THEN
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
               CASE WHEN CHARINDEX(' ', ee.Name) > 0 THEN SUBSTRING(ee.Name, CHARINDEX(' ', ee.Name) + 1, LEN(ee.Name) - CHARINDEX(' ', ee.Name)) ELSE ee.NAME END AS [first_name],
               CASE WHEN CHARINDEX(' ', ee.Name) > 0 THEN SUBSTRING(ee.Name, 1, CHARINDEX(' ', ee.Name) - 2) ELSE NULL END AS [last_name],
		       CASE WHEN CHARINDEX('@',ee.[Email],1) > 0 THEN UPPER(LEFT(ee.[Email],CHARINDEX('@',ee.[Email],1) - 1)) ELSE NULL END AS ComputingId
        FROM [CDW].[FullAccess].[EmployeeDim] ee
        WHERE 1 = 1
              --AND prov.DurableKey = @ProviderDurableKey
              AND ee.IsCurrent = 1
			  AND LEN(ee.Email) > 0
			  AND CHARINDEX('@',ee.[Email],1) > 0
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
        FROM #ee_index ee
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

SELECT
	*
FROM #prov_index
ORDER BY
	--RowId
	ProviderDurableKey
/*
-- This CTE parses out the employee computing id from the email address in the EmployeeDim table
WITH cte_ee (Name, Email, RowId, ComputingId, DurableKey)
AS (SELECT list.ProviderDurableKey,
           list.Name,
           CASE
               WHEN CHARINDEX(' ', list.first_name) > 0 THEN
                   SUBSTRING(list.first_name, 1, CHARINDEX(' ', list.first_name) - 1)
               ELSE
                   list.first_name
           END AS [first_name],
           list.last_name,
		   list.EmployeeDurableKey,
		   list.IsCurrent,
		   list.RowId
    FROM
    (
        SELECT prov.DurableKey AS [ProviderDurableKey],
               prov.Name,
               SUBSTRING(prov.Name, CHARINDEX(' ', prov.Name) + 1, LEN(prov.Name) - CHARINDEX(' ', prov.Name)) AS [first_name],
               SUBSTRING(prov.Name, 1, CHARINDEX(' ', prov.Name) - 2) AS [last_name],
			   prov.EmployeeDurableKey,
			   prov.IsCurrent,
			   email.RowId
        FROM [CDW].[FullAccess].[EmployeeDim] ee
		INNER JOIN [CDW_App_Dev].Rptg.TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider email
			ON email.Email = UPPER(prov.Email)
		LEFT JOIN [CDW].[FullAccess].[ProviderDim] prov
			ON prov.EmployeeDurableKey = ee.EmployeeKey
        WHERE 1 = 1
              --AND prov.DurableKey = @ProviderDurableKey
              --AND prov.IsCurrent = 1
    ) list )

SELECT * INTO #prov_index FROM cte_prov
CREATE CLUSTERED INDEX  prov ON #prov_index (ProviderDurableKey, EmployeeDurableKey)

SELECT
	*
FROM #prov_index
ORDER BY
	RowId
*/
/*
;
-- This CTE parses out the provider's First Name and Last Name from ProviderDim.Name.
-- The outer SELECT is used to remove the Middle Initial / Name.
WITH cte_prov (ProviderDurableKey, Name, first_name, last_name, EmployeeDurableKey, IsCurrent, RowId)
AS (SELECT list.ProviderDurableKey,
           list.Name,
           CASE
               WHEN CHARINDEX(' ', list.first_name) > 0 THEN
                   SUBSTRING(list.first_name, 1, CHARINDEX(' ', list.first_name) - 1)
               ELSE
                   list.first_name
           END AS [first_name],
           list.last_name,
		   list.EmployeeDurableKey,
		   list.IsCurrent,
		   list.RowId
    FROM
    (
        SELECT prov.DurableKey AS [ProviderDurableKey],
               prov.Name,
               SUBSTRING(prov.Name, CHARINDEX(' ', prov.Name) + 1, LEN(prov.Name) - CHARINDEX(' ', prov.Name)) AS [first_name],
               SUBSTRING(prov.Name, 1, CHARINDEX(' ', prov.Name) - 2) AS [last_name],
			   prov.EmployeeDurableKey,
			   prov.IsCurrent,
			   email.RowId
        FROM [CDW].[FullAccess].[EmployeeDim] ee
		INNER JOIN [CDW_App_Dev].Rptg.TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider email
			ON email.Email = UPPER(prov.Email)
		LEFT JOIN [CDW].[FullAccess].[ProviderDim] prov
			ON prov.EmployeeDurableKey = ee.EmployeeKey
        WHERE 1 = 1
              --AND prov.DurableKey = @ProviderDurableKey
              --AND prov.IsCurrent = 1
    ) list )

SELECT * INTO #prov_index FROM cte_prov
CREATE CLUSTERED INDEX  prov ON #prov_index (ProviderDurableKey, EmployeeDurableKey)

SELECT
	*
FROM #prov_index
ORDER BY
	RowId
*/
/*
-- This SELECT statement assigns the provider's First Name and Last Name to the variables.
SELECT @FirstName = CAST(cte_prov.first_name AS NVARCHAR(200)),
       @LastName = CAST(cte_prov.last_name AS NVARCHAR(300))
FROM cte_prov;

SELECT main.provider_first_name,
       main.provider_last_name,
       main.AccessionNumber,
       main.BasePatientClass,
       main.date_of_study,
       main.OrderPriority,
       main.ordering_department_name,
       main.ordering_provider_name,
       main.attending_provider_name,

	   -- Combine multiple Reading Providers in one column.
	   (SELECT STUFF(
        (
            SELECT ' | ' + pb.Name
            FROM [CDW].[FullAccess].[ProviderBridge] pb
            WHERE pb.ProviderComboKey = main.ReadingProviderComboKey
            FOR XML PATH('')
        ),
        1,
        3,
        ''
             )) AS [reading_provider_name],

       main.proc_code,
       main.proc_name,
       main.cptcode,
       main.technologist_name,
       main.ExamStartInstant,
       main.ExamEndInstant,
       main.performing_department_name,
       main.reading_priority,
       main.patient_age_at_exam,
       main.hospital_code,
       main.PrimaryMrn,
       main.patient_name,
       main.BirthDate,
       main.is_peds,
       main.is_peds_yesno,
       main.age_group,
       main.Narrative,
       main.Impression,
       main.Addenda	

FROM

-- Main SQL query.

(SELECT DISTINCT

       -- NULL columns are placeholders for columns to be added later.

	   @FirstName AS [provider_first_name],
       @LastName AS [provider_last_name],

       img.AccessionNumber,																							-- Accession Number
       img.BasePatientClass,																						-- Patient Class
       NULL														AS [date_of_study],									-- Date Of Study
       img.OrderPriority,																							-- Order Priority
       ord_dep.Name												AS [ordering_department_name],						-- Order Location
       ord_prov.Name											AS [ordering_provider_name],						-- Ordering Prov Name
       att_prov.Name											AS [attending_provider_name],						-- Attending Prov Name

	   -- Excluded to prevent addition of rows for other Reading Providers.
	   --pb.Name													AS [reading_provider_name],							-- Reading Prov Name
	   -- Will use ReadingProviderComboKey to get the Reading Providers latter.
       img.ReadingProviderComboKey,

	   ord_proc.Code											AS [proc_code],										-- Proc Code
       ord_proc.Name											AS [proc_name],										-- Proc Name
       NULL														AS [cptcode],										-- CPT Code
       emp.Name													AS [technologist_name],								-- Study Technologist
       img.ExamStartInstant,																						-- Begin Exam Date
       img.ExamEndInstant,																							-- End Exam Date
	   perf_dep.Name											AS [performing_department_name],					-- Department Name
       NULL														AS [reading_priority],								-- Reading Priority (End Exam Read Priority)
       FLOOR(DATEDIFF(DAY, pat.BirthDate, CAST(img.ExamStartInstant AS DATE)) / 365.25)	AS [patient_age_at_exam],	-- Patient Age At Study Time

	   -- Additional columns not included in Rad All Studies by Reading Doctor by CPT report.

	   COALESCE(mdm.HOSPITAL_CODE, 'UVA-MC')					AS [hospital_code],

       pat.PrimaryMrn,
       pat.Name													AS [patient_name],
       pat.BirthDate,

       CASE
           WHEN FLOOR(DATEDIFF(DAY, pat.BirthDate, CAST(img.ExamStartInstant AS DATE)) / 365.25) < 18 THEN
               1
           ELSE
               0
       END														AS [is_peds],

       CASE
           WHEN FLOOR(DATEDIFF(DAY, pat.BirthDate, CAST(img.ExamStartInstant AS DATE)) / 365.25) < 18 THEN
               'Yes'
           ELSE
               'No'
       END														AS [is_peds_yesno],

       CASE
           WHEN FLOOR(DATEDIFF(DAY, pat.BirthDate, CAST(img.ExamStartInstant AS DATE)) / 365.25) < 18 THEN
               'Peds'
           ELSE
               'Adults'
       END														AS [age_group],

       txt.Narrative,
       txt.Impression,
       txt.Addenda

FROM [CDW].[FullAccess].[ImagingFact] img

    INNER JOIN [CDW].[FullAccess].[PatientDim] pat
        ON img.PatientDurableKey = pat.DurableKey
           AND pat.IsCurrent = 1
		   AND pat.IsValid = 1 -- Exclude test patients.

    INNER JOIN [CDW].[FullAccess].[ProcedureDim] ord_proc
        ON img.FirstProcedureDurableKey = ord_proc.DurableKey
           AND ord_proc.IsCurrent = 1

    INNER JOIN [CDW].[FullAccess].[ProviderDim] ord_prov
        ON img.OrderingProviderDurableKey = ord_prov.DurableKey
           AND ord_prov.IsCurrent = 1

    INNER JOIN [CDW].[FullAccess].[ProviderDim] att_prov
        ON img.FinalizingProviderDurableKey = att_prov.DurableKey
           AND att_prov.IsCurrent = 1

    INNER JOIN [CDW].[FullAccess].[ProviderBridge] pb
        ON img.ReadingProviderComboKey = pb.ProviderComboKey

    INNER JOIN [CDW].[FullAccess].[EmployeeDim] emp
        ON img.TechnologistEmployeeDurableKey = emp.DurableKey
           AND emp.IsCurrent = 1

    INNER JOIN [CDW].[FullAccess].[DepartmentDim] ord_dep
        ON img.OrderingContactDepartmentKey = ord_dep.DepartmentKey

    INNER JOIN [CDW].[FullAccess].[DepartmentDim] perf_dep
        ON img.PerformingDepartmentKey = perf_dep.DepartmentKey

    -- Use the dbo instead of FullAccess schema.
    -- Cannot use a CONTAINS or FREETEXT predicate on table or indexed view 'CDW.FullAccess.ImagingTextFact' because it is not full-text indexed.
    -- Using LEFT OUTER JOIN in case there isn't any Result Text.
	LEFT OUTER JOIN [CDW].[dbo].[ImagingTextFact] txt
        ON img.ImagingKey = txt.ImagingKey
           AND txt.Count = 1

    LEFT OUTER JOIN
    (
        SELECT DISTINCT
               CONVERT(VARCHAR(18), EPIC_DEPARTMENT_ID) AS [epic_department_id],
               HOSPITAL_CODE
        FROM [CDW_App].[Rptg].[vwRef_MDM_Location_Master]
    ) mdm
        ON perf_dep.DepartmentEpicId = mdm.epic_department_id

WHERE 1 = 1

      AND img.ExamEndDateKey >= @locStartDateKey
      AND img.ExamEndDateKey <= @locEndDateKey

      AND img.StudyStatus = 'Final'

      AND img.OrderType = 'Imaging'

      --AND ord_proc.Code LIKE 'IMG%' -- Radiology orderable / performable imaging procedures.

      AND
      (
          pb.ProviderDurableKey = @ProviderDurableKey

          -- Perform SQL Server full-text search instead of using SQL LIKE operator.
		  -- Search all of the Result Text (Impression, Narrative, and Addendum).
          OR
          (
              (CONTAINS((txt.Impression, txt.Narrative, txt.Addenda), @FirstName)
              AND CONTAINS((txt.Impression, txt.Narrative, txt.Addenda), @LastName))
          )
*/
/*
		  OR
          (
              (txt.Impression LIKE '%' + @FirstName + '%'
              AND txt.Impression LIKE '%' + @LastName + '%')
          )

          OR
          (
              (txt.Narrative LIKE '%' + @FirstName + '%'
              AND txt.Narrative LIKE '%' + @LastName + '%')
          )

          OR
          (
              (txt.Addenda LIKE '%' + @FirstName + '%'
              AND txt.Addenda LIKE '%' + @LastName + '%')
          )
*/
/*
      )

      AND img.Canceled = 0 -- Not canceled.

      AND img.Count = 1 -- Not deleted or img.ImagingKey < 0 (one of the three special rows).

-- Sorting will be done in the SSRS report.	  
--ORDER BY

) main;
*/
GO


