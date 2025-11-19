USE [DS_HSDW_Prod]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET NOCOUNT ON;

DECLARE @StartDate	DATE 
DECLARE @EndDate		SMALLDATETIME

/*	Completed visit dates	*/
SET @StartDate = '6/1/2024'
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
;
-- This CTE parses out the study provider computing id from the email address in the source data table
WITH cte_study (Provider_Name, Provider_Email, first_name, last_name, ComputingId)
AS (SELECT DISTINCT
           list.Provider_Name,
		   list.Provider_Email,
		   list.first_name,
           CASE
               WHEN CHARINDEX(' ', list.last_name) > 0 THEN
                   SUBSTRING(list.last_name, CHARINDEX(' ', list.last_name) + 1,150)
               ELSE
                   list.last_name
           END AS [last_name],
		   list.ComputingId
    FROM
    (
        SELECT email.Provider_Name,
			   email.Provider_Email,
               SUBSTRING(email.Provider_Name, CHARINDEX(' ', email.Provider_Name) + 1, LEN(email.Provider_Name) - CHARINDEX(' ', email.Provider_Name)) AS [last_name],
               SUBSTRING(email.Provider_Name, 1, CHARINDEX(' ', email.Provider_Name) - 1) AS [first_name],
			   CASE WHEN CHARINDEX('@',email.Provider_Email,1) > 0 THEN UPPER(LEFT(email.Provider_Email,CHARINDEX('@',email.Provider_Email,1) - 1)) ELSE NULL END AS ComputingId
        FROM [DS_HSDW_Prod].Rptg.TEMP_Nuance_DAX_Study_Provider email
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
WITH cte_ee (EmployeeUserId, Name, first_name, last_name, ComputingId, ProviderId)
AS (SELECT list.EmployeeUserId,
           list.Name,
           CASE
               WHEN list.NoComma = 0 AND CHARINDEX(' ', list.first_name) > 0 THEN
                   SUBSTRING(list.first_name, 1, CHARINDEX(' ', list.first_name) - 1)
               ELSE
                   list.first_name
           END AS [first_name],
		   list.last_name,
		   list.ComputingId,
		   list.ProviderId
    FROM
    (
        SELECT ee.EMPlye_Usr_ID AS [EmployeeUserId],
               ee.EMPlye_Nme AS [Name],
			   CASE WHEN CHARINDEX(',', TRIM(ee.EMPlye_Nme)) = 0 THEN 1 ELSE 0 END AS NoComma,
               CASE WHEN CHARINDEX(',', TRIM(ee.EMPlye_Nme)) = 0 THEN TRIM(ee.EMPlye_Nme)
			              ELSE TRIM(SUBSTRING(TRIM(ee.EMPlye_Nme), CHARINDEX(',', TRIM(ee.EMPlye_Nme)) + 1, LEN(TRIM(ee.EMPlye_Nme)) - CHARINDEX(',', TRIM(ee.EMPlye_Nme)))) END AS [first_name],
               CASE WHEN CHARINDEX(',', TRIM(ee.EMPlye_Nme)) = 0 THEN NULL
			              ELSE SUBSTRING(TRIM(ee.EMPlye_Nme), 1, CHARINDEX(',', TRIM(ee.EMPlye_Nme)) - 1) END AS [last_name],
		       ee.EMPlye_Systm_Login AS ComputingId,
			   ee.EMPlye_PROV_ID AS ProviderId
        FROM [DS_HSDW_Prod].[dbo].[Dim_Clrt_EMPlye] ee
    ) list )

SELECT * INTO #ee_index FROM cte_ee
CREATE CLUSTERED INDEX  ee ON #ee_index (EmployeeUserId)

--SELECT
--	*
--FROM #ee_index
--ORDER BY
--	ComputingId
;

-- This CTE parses out the provider's First Name and Last Name from ProviderDim.Name.
-- The outer SELECT is used to remove the Middle Initial / Name.
WITH cte_prov (Name, Email, first_name, last_name, Usr_ID, ComputingId, [Type], ProviderEpicId)
AS (SELECT
           list.Prov_Nme AS [Name],
		   list.Email,
           CASE
               WHEN CHARINDEX(' ', list.first_name) > 0 THEN
                   SUBSTRING(list.first_name, 1, CHARINDEX(' ', list.first_name) - 1)
               ELSE
                   list.first_name
           END AS [first_name],
           list.last_name,
		   list.Usr_ID,
		   list.ComputingId,
		   list.Type,
		   list.ProviderEpicId
    FROM
    (
        SELECT
               prov.Prov_Nme,
			   email.Provider_Email AS Email,
               SUBSTRING(prov.Prov_Nme, CHARINDEX(' ', prov.Prov_Nme) + 1, LEN(prov.Prov_Nme) - CHARINDEX(' ', prov.Prov_Nme)) AS [first_name],
               SUBSTRING(prov.Prov_Nme, 1, CHARINDEX(' ', prov.Prov_Nme) - 2) AS [last_name],
			   prov.Usr_ID,
			   ee.ComputingId,
			   prov.Prov_Typ AS [Type],
			   prov.PROV_ID AS ProviderEpicId
        FROM
		(
		SELECT
			ee.EmployeeUserId,
            ee.ComputingId
		FROM #ee_index ee
		) ee
		INNER JOIN #prov_study email
			ON ee.ComputingId = email.ComputingId
		LEFT JOIN [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc] prov
			ON prov.Usr_ID = ee.EmployeeUserId
    ) list )

SELECT * INTO #prov_index FROM cte_prov
CREATE CLUSTERED INDEX  prov ON #prov_index (Usr_ID)

SELECT
	*
FROM #prov_index
ORDER BY
	Usr_ID

GO


