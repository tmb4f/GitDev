USE CLARITY

DECLARE @StartDate AS DATETIME
DECLARE @EndDate AS DATETIME

SET @StartDate = CAST(DATEADD(DAY,1,CAST(GETDATE() AS DATE)) AS DATETIME)
--SET @EndDate = DATEADD(DAY,13,@StartDate)
SET @EndDate = DATEADD(MONTH,6,@StartDate)

IF OBJECT_ID('tempdb..#avail_slot ') IS NOT NULL
DROP TABLE #avail_slot

IF OBJECT_ID('tempdb..#avail_slot_summary ') IS NOT NULL
DROP TABLE #avail_slot_summary

IF OBJECT_ID('tempdb..#booked_by_block ') IS NOT NULL
DROP TABLE #booked_by_block

IF OBJECT_ID('tempdb..#org_avail_blocks ') IS NOT NULL
DROP TABLE #org_avail_blocks

IF OBJECT_ID('tempdb..#org_avail_blocks_summary ') IS NOT NULL
DROP TABLE #org_avail_blocks_summary

IF OBJECT_ID('tempdb..#booked_by_block_plus ') IS NOT NULL
DROP TABLE #booked_by_block_plus

IF OBJECT_ID('tempdb..#avail_block ') IS NOT NULL
DROP TABLE #avail_block

IF OBJECT_ID('tempdb..#avail_by_block ') IS NOT NULL
DROP TABLE #avail_by_block

IF OBJECT_ID('tempdb..#unique_booked_available ') IS NOT NULL
DROP TABLE #unique_booked_available

IF OBJECT_ID('tempdb..#booked_available_by_block ') IS NOT NULL
DROP TABLE #booked_available_by_block

IF OBJECT_ID('tempdb..#booked_available_by_block_agg ') IS NOT NULL
DROP TABLE #booked_available_by_block_agg

IF OBJECT_ID('tempdb..#booked_available_summary ') IS NOT NULL
DROP TABLE #booked_available_summary

IF OBJECT_ID('tempdb..#booked_available_summary2 ') IS NOT NULL
DROP TABLE #booked_available_summary2

IF OBJECT_ID('tempdb..#availability ') IS NOT NULL
DROP TABLE #availability

IF OBJECT_ID('tempdb..#RptgTmp ') IS NOT NULL
DROP TABLE #RptgTmp

/*
Hierarchy											FINANCIAL DIVISION				8
Hierarchy Values								Family Medicine						48000
*/

--DECLARE @HierarchySelect TABLE (ID INTEGER, LABEL VARCHAR(20))
DECLARE @HierarchySelect TABLE (ID VARCHAR(50), LABEL VARCHAR(20))

INSERT INTO @HierarchySelect
(
    ID,
    LABEL
)
--DEPARTMENT BASED SELECTIONS
SELECT	'1' AS ID,			'ORGANIZATION NAME'	 AS LABEL	
	UNION ALL 
SELECT	'2',			'SERVICE AREA NAME'		
	UNION ALL 
SELECT	'3',			'CLINICAL AREA NAME'		
	UNION ALL 
SELECT	'4',			'POD'		
	UNION ALL 
SELECT	'5',			'SERVICE LINE'
    UNION ALL
SELECT  '6',          'LOCATION'
	UNION ALL
SELECT  '7',          'SPECIALTY'
	UNION ALL
SELECT '8',			'FINANCIAL DIVISION'

DECLARE @Hierarchy NVARCHAR(MAX)

SET @Hierarchy = 
(
SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), ISNULL(HierarchySelect.ID,'N/A')), ',') AS csv
FROM (SELECT TOP 1 ID FROM @HierarchySelect ORDER BY NEWID()) HierarchySelect
);

--SET @Hierarchy = '7' -- SPECIALTY  -- Test
--SET @Hierarchy = '5' -- SERVICE LINE  -- Test
--SET @Hierarchy = '2' -- SERVICE AREA NAME  -- Test
--SET @Hierarchy = '8' -- FINANCIAL DIVISION  -- Summary
--SET @Hierarchy = '4' -- POD  -- Test
--SET @Hierarchy = '3' -- CLINICAL AREA NAME -- Test
SET @Hierarchy = '1' -- ORGANIZATION NAME -- Test

--*--SELECT value AS ID, [@HierarchySelect].LABEL FROM STRING_SPLIT(@Hierarchy,',') Hierarchy LEFT OUTER JOIN @HierarchySelect ON [@HierarchySelect].ID = Hierarchy.value

DECLARE @HierarchyLookup VARCHAR(50)

SET @HierarchyLookup = @Hierarchy

--DECLARE @HierarchyValuesSelect TABLE (Sort INTEGER, ID VARCHAR(200), LABEL VARCHAR(200))
DECLARE @HierarchyValuesSelect TABLE (Sort INTEGER, ID VARCHAR(50), LABEL VARCHAR(200))

INSERT INTO @HierarchyValuesSelect
(
    Sort,
    ID,
    LABEL
)

SELECT HierarchyValues.Sort, HierarchyValues.ID, UPPER(HierarchyValues.LABEL) AS LABEL
FROM
(
	SELECT DISTINCT 1 AS Sort
		--,o.organization_id AS ID
		,CAST(o.organization_id AS VARCHAR(50)) AS ID
		,o.organization_name AS LABEL
	FROM CLARITY_App.[Mapping].Ref_Organization_Map o WITH (NOLOCK) INNER JOIN
	CLARITY_App.[Mapping].Ref_Service_Map s WITH (NOLOCK)  ON s.organization_id = o.organization_id INNER JOIN
	CLARITY_App.[Mapping].Ref_Clinical_Area_Map c WITH (NOLOCK)  ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map INNER JOIN
	CLARITY_App.[Mapping].Epic_Dept_Groupers g WITH (NOLOCK)   ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
	WHERE 1=1
	--AND 1 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup = 1
	AND @HierarchyLookup = 1
	AND g.ambulatory_flag = 1

UNION ALL

	SELECT DISTINCT 1
		,CAST(s.sk_Ref_Service_Map AS VARCHAR(50))
		,o.organization_name + ': ' + s.service_name
	FROM CLARITY_App.[Mapping].Ref_Service_Map s WITH (NOLOCK)  INNER JOIN
	CLARITY_APP.mapping.Ref_Organization_Map o WITH (NOLOCK)  ON o.organization_id = s.organization_id INNER JOIN
	CLARITY_App.[Mapping].Ref_Clinical_Area_Map c WITH (NOLOCK)  ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map INNER JOIN
	CLARITY_App.[Mapping].Epic_Dept_Groupers g WITH (NOLOCK)   ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
	WHERE 1=1
	--AND 2 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 2 
	AND @HierarchyLookup = 2
	AND  g.ambulatory_flag = 1

UNION ALL

	SELECT DISTINCT 1
	,CAST(c.clinical_area_id AS VARCHAR(50)) 
	,c.clinical_area_name
	FROM CLARITY_App.[Mapping].Ref_Clinical_Area_Map c WITH (NOLOCK)  INNER JOIN
	CLARITY_App.[Mapping].Epic_Dept_Groupers g WITH (NOLOCK)  ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
	--WHERE 3 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 3
	WHERE @HierarchyLookup = 3
	AND  g.ambulatory_flag = 1

UNION ALL

	SELECT DISTINCT		1               
        ,   CAST(six.RPT_GRP_SIX AS VARCHAR(50))     
        ,	six.NAME            
    FROM CLARITY.dbo.ZC_DEP_RPT_GRP_6 six WITH (NOLOCK)   INNER JOIN
	CLARITY.dbo.CLARITY_DEP dep WITH (NOLOCK)  ON dep.RPT_GRP_SIX = six.RPT_GRP_SIX INNER JOIN
	CLARITY_App.[Mapping].Epic_Dept_Groupers g WITH (NOLOCK)  ON g.epic_department_id = dep.DEPARTMENT_ID
    WHERE 1=1
	--AND 4 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 4
	AND @HierarchyLookup = 4
	AND g.ambulatory_flag = 1
    	UNION ALL 
    SELECT 2, '0', '(Undefined Pod)'
    --WHERE 4 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 4
	WHERE @HierarchyLookup = 4
    
UNION ALL

--List all Department based Service Lines
    SELECT DISTINCT 
    	        CAST(CASE WHEN RPT_GRP_THIRTY IS NOT NULL THEN 1 ELSE 2 END AS VARCHAR(50))
            , 	COALESCE(RPT_GRP_THIRTY, '(Undefined ServLine)')
            ,	COALESCE(RPT_GRP_THIRTY, '(Undefined ServLine)')  --, LEN(RPT_GRP_FIVE)
    FROM CLARITY.dbo.CLARITY_DEP dep WITH (NOLOCK)  INNER JOIN
	CLARITY_App.[Mapping].Epic_Dept_Groupers g WITH (NOLOCK)   ON g.epic_department_id = dep.DEPARTMENT_ID
    --WHERE 5 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 5
	WHERE @HierarchyLookup = 5
	AND g.ambulatory_flag = 1

UNION ALL

--List all Locations
    SELECT DISTINCT 1
        ,   CAST(l.LOC_ID AS VARCHAR(50))
        ,   l.LOC_NAME
    FROM CLARITY.dbo.CLARITY_LOC l WITH (NOLOCK) INNER JOIN
	CLARITY.dbo.CLARITY_DEP dep WITH (NOLOCK) ON dep.REV_LOC_ID = l.LOC_ID INNER JOIN
	CLARITY_App.[Mapping].Epic_Dept_Groupers g WITH (NOLOCK)  ON g.epic_department_id = dep.DEPARTMENT_ID
    WHERE 1=1
	--AND 6 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup = 6
	AND @HierarchyLookup = 6
	AND g.ambulatory_flag = 1
    	UNION ALL 
    SELECT 2, '0', '(Undefined Location)'
    --WHERE 6 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 6
	WHERE @HierarchyLookup = 6
    
UNION ALL

--List all Specialties
    SELECT DISTINCT  1
        ,   CAST(s.SPECIALTY_DEP_C AS VARCHAR(50))
        ,   s.NAME 
    FROM CLARITY.dbo.ZC_SPECIALTY_DEP s  WITH (NOLOCK) INNER JOIN
	CLARITY.dbo.CLARITY_DEP dep WITH (NOLOCK) ON dep.SPECIALTY_DEP_C = s.SPECIALTY_DEP_C INNER JOIN 
	CLARITY_App.[Mapping].Epic_Dept_Groupers g WITH (NOLOCK)  ON g.epic_department_id = dep.DEPARTMENT_ID
    WHERE 1=1
	--AND 7 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 7
	AND @HierarchyLookup = 7
	AND g.ambulatory_flag = 1
    	UNION ALL 
    SELECT 2, '0', '(Undefined Dept Specialty)'
    --WHERE 7 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup  = 7
	WHERE @HierarchyLookup = 7

UNION ALL

    SELECT  1 AS Sort
        ,   CAST(FIN_DIV_ID AS VARCHAR(50)) AS ID
        ,   FIN_DIV_NM AS Label
    FROM CLARITY.dbo. FIN_DIV
    --WHERE 8 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,','))--@HierarchyLookup = 8
	WHERE @HierarchyLookup = 8
) HierarchyValues
--ORDER BY 1, 3

--SELECT Sort, ID, LABEL FROM @HierarchyValuesSelect ORDER BY 1, 3
--*--SELECT Sort, ID, LABEL FROM @HierarchyValuesSelect ORDER BY 1, 2

DECLARE @HierarchyValues NVARCHAR(MAX)

SET @HierarchyValues = 
(
SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), ISNULL(HierarchyValuesSelect.ID,'N/A')), ',') AS csv
--FROM (SELECT TOP 1 ID FROM @HierarchyValuesSelect ORDER BY NEWID()) HierarchyValuesSelect
FROM (SELECT ID FROM @HierarchyValuesSelect) HierarchyValuesSelect
);

--SET @HierarchyValues = '27' -- ORTHOPEDIC SURGERY -- Test
--SET @HierarchyValues = '114' -- OTHER AMBULATORY SERVICES: IVY ROAD P&O -- Test
--SET @HierarchyValues = '3' -- PRIMARY CARE -- Test
--SET @HierarchyValues = '161' -- Continuum Home Health -- Test
--SET @HierarchyValues = '13' -- Community Health Medical Group -- Test
--SET @HierarchyValues = '48000' -- Community Health Medical Group -- Test
--SET @HierarchyValues = '7' -- UPG -- Test
SET @HierarchyValues = '4' -- University Medical Center Ambulatory -- Test

--SELECT @HierarchyValues AS HierarchyValuesSelect

--*--SELECT @HierarchyValues AS ID, HierarchyValuesSelect.LABEL FROM @HierarchyValuesSelect AS HierarchyValuesSelect WHERE HierarchyValuesSelect.ID = @HierarchyValues

DECLARE @DepartmentsSelect TABLE (ID VARCHAR(50), [Name] VARCHAR(200))

INSERT INTO @DepartmentsSelect
(
    ID,
    [Name]
)

SELECT DepartmentsValues.ID, DepartmentsValues.[Name]
FROM
(
--Get list of Departments
    SELECT  CAST(CLARITY_DEP.DEPARTMENT_ID AS VARCHAR(18))	AS ID
        ,   CLARITY_DEP.DEPARTMENT_NAME + ' [' + CAST(CLARITY_DEP.DEPARTMENT_ID AS VARCHAR(18)) + ']' AS Name

    FROM CLARITY.dbo.CLARITY_DEP WITH (NOLOCK) INNER JOIN
	CLARITY_App.Mapping.Epic_Dept_Groupers g WITH (NOLOCK)  ON g.epic_department_id = CLARITY_DEP.DEPARTMENT_ID
    LEFT JOIN CLARITY_App.Mapping.Ref_Clinical_Area_Map c WITH (NOLOCK) ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN CLARITY_App.Mapping.Ref_Service_Map s WITH (NOLOCK) ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN CLARITY_App.Mapping.Ref_Organization_Map o WITH (NOLOCK) ON s.organization_id = o.organization_id
    WHERE 1=1
		AND g.ambulatory_flag = 1
        --AND (   (   1 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 1
        AND (   (   @HierarchyLookup = 1
                AND COALESCE(o.[organization_id], '999')					IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))	)		--Organization Name
            --OR  (   2 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 2
            OR  (   @HierarchyLookup = 2
                AND COALESCE(s.[sk_Ref_Service_Map], '999')					IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))	)		--Service Area
            --OR  (   3 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 3
            OR  (   @HierarchyLookup = 3
                AND COALESCE(c.[clinical_area_id], '999')		IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))	)			--Clinical Area
            )

UNION ALL

--Get list of Departments
    SELECT  CAST(DEPARTMENT_ID AS VARCHAR(18))	
        ,   DEPARTMENT_NAME + ' [' + CAST(DEPARTMENT_ID AS VARCHAR(18)) + ']'

    FROM CLARITY.dbo.CLARITY_DEP WITH (NOLOCK) INNER JOIN
	CLARITY_App.Mapping.Epic_Dept_Groupers g WITH (NOLOCK)  ON g.epic_department_id = CLARITY_DEP.DEPARTMENT_ID
    WHERE 1=1
		AND g.ambulatory_flag = 1
        --AND (   (   4 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 4
        AND (   (   @HierarchyLookup = 4
                AND COALESCE(RPT_GRP_SIX,'0')						IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))	)		--Pod
            --OR  (   5 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 5
            OR  (   @HierarchyLookup = 5
                AND COALESCE(RPT_GRP_THIRTY,'(Undefined ServLine)')	IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))	)			--Dept Service Line
            --OR  (   6 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 6
            OR  (   @HierarchyLookup = 6
                AND COALESCE(REV_LOC_ID,'0')						IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))	)			--Location
            --OR  (   7 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 7
            OR  (   @HierarchyLookup = 7
                AND COALESCE(SPECIALTY_DEP_C,'0')					IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))	)			--Dept Specialty
            )

UNION ALL /*Finaincal SubDivision*/

    SELECT  CAST(FIN_SUBDIV_ID AS VARCHAR(18))
        ,   FIN_SUBDIV_NM
    FROM CLARITY.dbo.FIN_SUBDIV
    --WHERE 8 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 8
    WHERE @HierarchyLookup = 8
	 AND COALESCE(FIN_DIV_ID,'0') IN (SELECT value FROM STRING_SPLIT(@HierarchyValues,','))
) DepartmentsValues

--*--SELECT ID, [Name] FROM @DepartmentsSelect ORDER BY 2

DECLARE @Departments NVARCHAR(MAX)

SET @Departments =
(
SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), ISNULL(DepartmentsSelect.ID,'N/A')), ',') AS csv
--FROM (SELECT TOP 1 ID FROM @DepartmentsSelect ORDER BY NEWID()) DepartmentsSelect
FROM (SELECT ID FROM @DepartmentsSelect) DepartmentsSelect
);

--SET @Departments = '10293031' --  CPSN UVA ORTHO SPINE -- Test
--SET @Departments = '10419012' --  OCIR PROSTHETICS/ORTCS -- Test
--SET @Departments = '10211004' --  F415 UNIV PHYS -- Test
--SET @Departments = '10206001' --  CHHC CONTINUUM -- Test
--SET @Departments = '10244004' --  UVWC OPHTHALMOLOGY -- Test
SET @Departments = '10419014' -- OCIR SPORTS MED -- Test

--SELECT @Departments AS DepartmentsSelect

--*--SELECT @Departments, DepartmentsSelect.Name FROM @DepartmentsSelect AS DepartmentsSelect WHERE DepartmentsSelect.ID = @Departments

DECLARE @ProviderTypesSelect TABLE (PROV_TYPE_C VARCHAR(50), PROV_TYPE VARCHAR(200))

INSERT INTO @ProviderTypesSelect
(
    PROV_TYPE_C,
    PROV_TYPE
)

SELECT ProviderTypesValues.PROV_TYPE_C, ProviderTypesValues.PROV_TYPE
FROM
(
SELECT DISTINCT COALESCE(ptot.PROV_TYPE_OT_C, ser.PROVIDER_TYPE_C, NULL) AS PROV_TYPE_C,
COALESCE(ptot.PROV_TYPE_OT_NAME, ser.PROV_TYPE, NULL) AS PROV_TYPE 
FROM CLARITY.dbo.CLARITY_SER ser INNER JOIN
CLARITY.dbo.V_AVAILABILITY v ON v.PROV_ID = ser.PROV_ID LEFT OUTER JOIN
CLARITY_App.Rptg.vwCLARITY_SER_OT_PROV_TYPE ptot ON ser.PROV_ID = ptot.PROV_ID AND v.SLOT_DATE BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
WHERE 1=1
AND v.APPT_NUMBER = 0
AND v.ORG_REG_OPENINGS  > 0
AND v.DEPARTMENT_ID IN (SELECT value FROM STRING_SPLIT(@Departments,','))
AND CONVERT(DATE,v.SLOT_DATE) BETWEEN @startdate AND @enddate
--AND 8 NOT IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup <> 8
AND @HierarchyLookup <> 8
--AND ser.PROVIDER_TYPE_C IN ('1' , '4', '6', '9', '10','101','105','108','2','2506','2527','2721')
--1=Physician; 4=Anesthesiologist, 6=Physician Assistant, 9=Nurse Practitioner, 10 = Psychologist, 101=Audiologist, 105=Optometrist, 108=Dentist, 2=Nurse Anesthetist, 2506=Doctor of Philosophy, 2527=Genetic Counselor, 2721=Clinical Nurse Specialist  

UNION

SELECT DISTINCT COALESCE(ptot.PROV_TYPE_OT_C, ser.PROVIDER_TYPE_C, NULL) AS PROV_TYPE_C,
COALESCE(ptot.PROV_TYPE_OT_NAME, ser.PROV_TYPE, NULL) AS PROV_TYPE  
FROM CLARITY.dbo.CLARITY_SER ser INNER JOIN
CLARITY.dbo.V_AVAILABILITY v ON v.PROV_ID = ser.PROV_ID LEFT OUTER JOIN
CLARITY_App.Rptg.vwCLARITY_SER_OT_PROV_TYPE ptot ON ser.PROV_ID = ptot.PROV_ID AND v.SLOT_DATE BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
WHERE 1=1
AND v.APPT_NUMBER = 0
AND v.ORG_REG_OPENINGS  > 0
AND CONVERT(DATE,v.SLOT_DATE) BETWEEN @startdate AND @enddate
--AND 8 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 8
AND @HierarchyLookup = 8
--AND ser.PROVIDER_TYPE_C IN ('1' , '4', '6', '9', '10','101','105','108','2','2506','2527','2721')  
AND COALESCE(ser.RPT_GRP_EIGHT,'0')	IN (SELECT value FROM STRING_SPLIT(@Departments,',')) -- says department but the department parameter does department and financial subdivision
--ORDER BY ser.PROV_NAME
) ProviderTypesValues

--*--SELECT PROV_TYPE_C, PROV_TYPE FROM @ProviderTypesSelect ORDER BY 2

DECLARE @ProviderTypes NVARCHAR(MAX)

SET @ProviderTypes = 
(
SELECT STRING_AGG(CONVERT(NVARCHAR(max), ISNULL(ProviderTypesSelect.PROV_TYPE_C,'N/A')), ',') AS csv
--FROM (SELECT TOP 1 PROV_TYPE_C FROM @ProviderTypesSelect ORDER BY NEWID()) ProviderTypesSelect
FROM (SELECT PROV_TYPE_C FROM @ProviderTypesSelect) ProviderTypesSelect
);

--SET @ProviderTypes = '1' -- Physician		
--SET @ProviderTypes = '200,1' -- P&O Practitioner,Physician
--SET @ProviderTypes = '106' -- Physical Therapist
--SET @ProviderTypes = '113' -- Resident

--*--SELECT @ProviderTypes AS ProviderTypesSelect

DECLARE @ProvidersSelect TABLE (PROV_ID VARCHAR(50), PROV_NAME VARCHAR(200))

INSERT INTO @ProvidersSelect
(
    PROV_ID,
    PROV_NAME
)

SELECT ProvidersValues.PROV_ID, ProvidersValues.PROV_NAME
FROM
(
SELECT DISTINCT ser.PROV_ID, ser.PROV_NAME 
FROM CLARITY.dbo.CLARITY_SER ser INNER JOIN
CLARITY.dbo.V_AVAILABILITY v ON v.PROV_ID = ser.PROV_ID
WHERE 1=1
AND v.APPT_NUMBER = 0
AND v.ORG_REG_OPENINGS  > 0
AND v.DEPARTMENT_ID IN (SELECT value FROM STRING_SPLIT(@Departments,','))
AND ser.PROVIDER_TYPE_C IN (SELECT value FROM STRING_SPLIT(@ProviderTypes,','))
AND CONVERT(DATE,v.SLOT_DATE) BETWEEN @startdate AND @enddate
--AND 8 NOT IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup <> 8
AND @HierarchyLookup <> 8
--AND ser.PROVIDER_TYPE_C IN ('1' , '4', '6', '9', '10','101','105','108','2','2506','2527','2721')
--1=Physician; 4=Anesthesiologist, 6=Physician Assistant, 9=Nurse Practitioner, 10 = Psychologist, 101=Audiologist, 105=Optometrist, 108=Dentist, 2=Nurse Anesthetist, 2506=Doctor of Philosophy, 2527=Genetic Counselor, 2721=Clinical Nurse Specialist  

UNION

SELECT DISTINCT ser.PROV_ID, ser.PROV_NAME 
FROM CLARITY.dbo.CLARITY_SER ser INNER JOIN
CLARITY.dbo.V_AVAILABILITY v ON v.PROV_ID = ser.PROV_ID
WHERE 1=1
AND v.APPT_NUMBER = 0
AND v.ORG_REG_OPENINGS  > 0
AND ser.PROVIDER_TYPE_C IN (SELECT value FROM STRING_SPLIT(@ProviderTypes,','))
AND CONVERT(DATE,v.SLOT_DATE) BETWEEN @startdate AND @enddate
--AND 8 IN (SELECT value AS ID FROM STRING_SPLIT(@Hierarchy,',')) -- @HierarchyLookup = 8
AND @HierarchyLookup = 8
--AND ser.PROVIDER_TYPE_C IN ('1' , '4', '6', '9', '10','101','105','108','2','2506','2527','2721')  
AND COALESCE(ser.RPT_GRP_EIGHT,'0')	IN (SELECT value FROM STRING_SPLIT(@Departments,',')) -- says department but the department parameter does department and financial subdivision
--ORDER BY ser.PROV_NAME
) ProvidersValues

--*--SELECT PROV_ID, PROV_NAME FROM @ProvidersSelect ORDER BY 2

DECLARE @Providers NVARCHAR(MAX)

SET @Providers = 
(
SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), ISNULL(ProvidersSelect.PROV_ID,'N/A')), ',') AS csv
--FROM (SELECT TOP 2 PROV_ID FROM @ProvidersSelect ORDER BY NEWID()) ProvidersSelect
FROM (SELECT PROV_ID FROM @ProvidersSelect) ProvidersSelect
);

--SET @Providers = '62069' -- SINGLA, ANUJ
--SET @Providers = '109326,123607,125578,127338,146907,29094,36510,36534,36535,60271,77338,78768,94115'
/*"ASHOFF, ALEXANDER","BOECKMAN, CALIN","MALNOWSKI, GARY","ALLAIN, MATTHEW","RONKOS, CHARLES","GYPSON, WARD","MARTINEZ, NORMAN M.","STRONG, DWAYNE","SPROUSE, WESLEY","BRYANT, MICHELE","TILTON, JAMES N","SALEHIN, MEAGAN","ALUSCA, ACELINE",*/
--SET @Providers = '62707' -- DOWDELL, KIMBERLY J
--SET @Providers = '145026' -- MADSEN, RYAN
--SET @Providers = '115699' -- WRIGHT, JOSEPH
--SET @Providers = '133107' -- SU, CHARLES
--SET @Providers = '29073' -- WILDER, ROBERT
--SET @Providers = '57396' -- GWATHMEY JR, FRANK W
--SET @Providers = '29073' -- WILDER, ROBERT
--SET @Providers = '39919' -- BROCKMEIER, STEPHEN F
--SET @Providers = '133107,99437' -- SU, CHARLES;PUGH, GARY MICHAEL

--*--SELECT @Providers AS ProvidersSelect

   SELECT 
          avail.department_id, 
		  department_name = avail.DEPARTMENT_NAME,
		  department_service_line = dep.RPT_GRP_THIRTY ,
		  pod_name = zdrg6.name ,
          avail.prov_id, 
		  provider = PROV_NM_WID,
		  provider_type = zpt.NAME,
          person_or_resource = avail.PROV_SCHED_TYPE_NAME,
		  dd.day_of_week,
          slot_date,
		  slot_begin_time, 
		  slot_length,
		  booked_length=appt.APPT_LENGTH,
		  appt_slot_number = appt_number, 
          num_apts_scheduled, 
		  regular_openings = org_reg_openings,
		  overbook_openings = ORG_OVBK_OPENINGS, 
		  openings = COALESCE(org_reg_openings,0) + COALESCE(ORG_OVBK_OPENINGS,0),
		  template_block_name = COALESCE(avail.APPT_BLOCK_NAME, 'Unknown'),
		  unavailable_reason = rsn.NAME,
          overbook_yn = COALESCE(appt_overbook_yn, 'N'), 
          outside_template_yn = COALESCE(outside_template_yn, 'N'), 
		  held_yn = CASE WHEN COALESCE(avail.day_held_rsn_c, avail.time_held_rsn_c) IS NULL
							THEN 'N'
							ELSE 'Y'
					 END,
		  CASE WHEN avail.APPT_NUMBER > 0 AND COALESCE(appt_overbook_yn, 'N') = 'N' AND COALESCE(outside_template_yn, 'N') = 'N' THEN 'Y' ELSE 'N' END AS regular_opening_yn,
  
           MRN= IDENTITY_ID.IDENTITY_ID ,	
           visit_type = appt.PRC_NAME,
           appt_status = appt.APPT_STATUS_NAME,
		   --avail.UNAVAILABLE_RSN_C,							-- 12/17/2024
		   avail.UNAVAILABLE_RSN_NAME					-- 12/17/2024
			
   INTO #avail_slot

    FROM dbo.V_AVAILABILITY avail 

	INNER JOIN CLARITY_App.Rptg.vwDim_Date dd				ON avail.SLOT_DATE = dd.day_date
	LEFT OUTER JOIN dbo.CLARITY_SER ser			ON avail.PROV_ID = ser.PROV_ID
	LEFT OUTER JOIN dbo.CLARITY_DEP dep						ON dep.DEPARTMENT_ID = avail.DEPARTMENT_ID              
	LEFT OUTER JOIN dbo.ZC_DEP_RPT_GRP_6 zdrg6					ON dep.RPT_GRP_SIX=zdrg6.RPT_GRP_SIX
	LEFT OUTER JOIN dbo.ZC_DEP_RPT_GRP_7					ON dep.RPT_GRP_seven=ZC_DEP_RPT_GRP_7.RPT_GRP_seven

	LEFT OUTER JOIN dbo.V_SCHED_APPT	 appt					ON appt.PAT_ENC_CSN_ID = avail.PAT_ENC_CSN_ID
	LEFT OUTER JOIN dbo.PATIENT PATIENT					ON appt.PAT_ID = PATIENT.PAT_ID
            
	LEFT OUTER JOIN dbo.IDENTITY_ID						ON IDENTITY_ID.PAT_ID = appt.PAT_ID AND IDENTITY_ID.IDENTITY_TYPE_ID = 14
	LEFT OUTER JOIN dbo.ZC_UNAVAIL_REASON rsn	ON rsn.UNAVAILABLE_RSN_C = avail.UNAVAILABLE_RSN_C
	LEFT OUTER JOIN dbo.ZC_PROV_TYPE	 zpt				    ON zpt.PROV_TYPE_C = ser.PROVider_TYPE_C

   WHERE 1=1
	AND ( 
			(@HierarchyLookup <> 8 AND avail.DEPARTMENT_ID IN (SELECT value FROM STRING_SPLIT(@Departments,',')))
			OR
			(@HierarchyLookup = 8 AND COALESCE(ser.RPT_GRP_EIGHT,'0')	IN (SELECT value FROM STRING_SPLIT(@Departments,',')) ) -- says department but the department parameter does department and financial subdivision
			 )
	AND avail.PROV_ID IN (SELECT value FROM STRING_SPLIT(@Providers,','))
    AND dd.day_date >= @StartDate
	AND dd.day_date <= @EndDate

	--AND avail.SLOT_DATE = '1/2/2025'
	--AND avail.SLOT_BEGIN_TIME = '11/19/2024 10:00:00'
	--AND avail.SLOT_DATE BETWEEN '1/1/2025' AND '6/30/2025'

	AND avail.UNAVAILABLE_RSN_NAME IS NULL			-- 12/16/2024

	ORDER BY 
          avail.department_id, 
		  avail.prov_id, 
		  avail.slot_begin_time,
		  avail.appt_number

  -- Create index for temp table #avail_slot
  CREATE UNIQUE CLUSTERED INDEX IX_avail_slot ON #avail_slot (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, appt_slot_number)

	--SELECT
	--	*
	--FROM #avail_slot
	--ORDER BY
	--	DEPARTMENT_ID,
	--	PROV_ID,
	--	SLOT_BEGIN_TIME

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name,
	COUNT(*) AS booked--,
	--STUFF((
	----SELECT ',' + CAST(innerTable.visit_type AS varchar(30))
	--SELECT ',' + CAST(innerTable.template_block_name AS varchar(30)) + ' ' + CAST(COUNT(innerTable.template_block_name) AS VARCHAR(5))
	--FROM #avail_slot AS innerTable
	--WHERE innerTable.department_id = p.department_id
	--AND innerTable.prov_id = p.prov_id
	--AND innerTable.slot_begin_time = p.slot_begin_time
	--GROUP BY
	--	innerTable.department_id,
	--	innerTable.prov_id,
	--	innerTable.slot_begin_time,
	--	innerTable.template_block_name
	--FOR XML PATH('')
	--),1,1,'') AS blocks_booked
	--p.UNAVAILABLE_RSN_C,
	--p.UNAVAILABLE_RSN_NAME
INTO #booked_by_block
FROM #avail_slot p
WHERE appt_slot_number > 0 -- booked records
GROUP BY
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name
ORDER BY
	department_id,
	prov_id,
	slot_begin_time,
	template_block_name
--GROUP BY
--	department_id,
--	prov_id,
--	slot_begin_time,
--	template_block_name,
--	p.UNAVAILABLE_RSN_C,
--	p.UNAVAILABLE_RSN_NAME
--ORDER BY
--	department_id,
--	prov_id,
--	slot_begin_time,
--	template_block_name,
--	p.UNAVAILABLE_RSN_C,
--	p.UNAVAILABLE_RSN_NAME

  -- Create index for temp table #booked_by_block
  CREATE UNIQUE CLUSTERED INDEX IX_booked_by_block ON #booked_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, template_block_name)
  --CREATE UNIQUE CLUSTERED INDEX IX_booked_by_block ON #booked_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, template_block_name,UNAVAILABLE_RSN_C,UNAVAILABLE_RSN_NAME)

 -- SELECT
	--bbb.DEPARTMENT_ID,
	--dep.DEPARTMENT_NAME,
 --   bbb.PROV_ID,
	--ser.PROV_NAME,
 --   bbb.SLOT_BEGIN_TIME,
 --   bbb.template_block_name,
 --   bbb.booked,
 --   bbb.UNAVAILABLE_RSN_C,
 --   bbb.UNAVAILABLE_RSN_NAME
 -- FROM #booked_by_block bbb
 -- LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep ON dep.DEPARTMENT_ID = bbb.DEPARTMENT_ID
 -- LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser ON ser.PROV_ID = bbb.PROV_ID
 -- WHERE bbb.UNAVAILABLE_RSN_C IS NOT NULL
 ---- ORDER BY
	----SLOT_BEGIN_TIME
 -- ORDER BY
	--department_id,
	--prov_id,
	--slot_begin_time,
	--template_block_name

--SELECT
--	department_id,
--	prov_id,
--	slot_begin_time,
--	MAX(regular_openings) AS regular_openings,
--	MAX(overbook_openings) AS overbook_openings,
--	MAX(openings) AS openings,
--	SUM(CASE WHEN appt_slot_number > 0 AND outside_template_yn = 'Y' THEN 1 ELSE 0 END) AS outside_template_openings,
--	MAX(NUM_APTS_SCHEDULED) AS num_apts_scheduled,
--	SUM(CASE WHEN appt_slot_number > 0 AND regular_opening_yn = 'Y' THEN 1 ELSE 0 END) AS num_regular_opening_apts_scheduled,
--	SUM(CASE WHEN appt_slot_number > 0 AND overbook_yn = 'Y' THEN 1 ELSE 0 END) AS num_overbook_apts_scheduled,
--	SUM(CASE WHEN appt_slot_number > 0 AND outside_template_yn = 'Y' THEN 1 ELSE 0 END) AS num_outside_template_apts_scheduled
--INTO #avail_slot_summary
--FROM #avail_slot
--GROUP BY
--	department_id,
--	prov_id,
--	slot_begin_time

SELECT
	sum1.DEPARTMENT_ID,
    sum1.PROV_ID,
    sum1.SLOT_BEGIN_TIME,
    sum1.regular_openings,
    sum1.overbook_openings,
    sum1.openings,
    sum1.outside_template_openings,
    sum1.num_apts_scheduled,
    sum1.num_regular_opening_apts_scheduled,
    sum1.num_overbook_apts_scheduled,
    sum1.num_outside_template_apts_scheduled,
    sum1.provider_type,

	sum1.UNAVAILABLE_RSN_NAME	,		-- 12/17/2024

    sum2.visit_types
INTO #avail_slot_summary
FROM
(
SELECT
	department_id,
	prov_id,
	slot_begin_time,
	MAX(regular_openings) AS regular_openings,
	MAX(overbook_openings) AS overbook_openings,
	MAX(openings) AS openings,
	SUM(CASE WHEN appt_slot_number > 0 AND outside_template_yn = 'Y' THEN 1 ELSE 0 END) AS outside_template_openings,
	MAX(NUM_APTS_SCHEDULED) AS num_apts_scheduled,
	SUM(CASE WHEN appt_slot_number > 0 AND regular_opening_yn = 'Y' THEN 1 ELSE 0 END) AS num_regular_opening_apts_scheduled,
	SUM(CASE WHEN appt_slot_number > 0 AND overbook_yn = 'Y' THEN 1 ELSE 0 END) AS num_overbook_apts_scheduled,
	SUM(CASE WHEN appt_slot_number > 0 AND outside_template_yn = 'Y' THEN 1 ELSE 0 END) AS num_outside_template_apts_scheduled,
	MAX(provider_type) AS provider_type,

	MAX(UNAVAILABLE_RSN_NAME) AS UNAVAILABLE_RSN_NAME		-- 12/16/2024
FROM #avail_slot
GROUP BY
	department_id,
	prov_id,
	slot_begin_time
) sum1
LEFT OUTER JOIN
(
SELECT
	department_id,
	prov_id,
	slot_begin_time,
	--STUFF((
	----SELECT ',' + CAST(innerTable.visit_type AS varchar(30))
	--SELECT ',' + CAST(innerTable.appt_slot_number AS VARCHAR(5)) + ' ' + CAST(innerTable.visit_type AS varchar(30))
	--FROM #avail_slot AS innerTable
	--WHERE innerTable.department_id = p.department_id
	--AND innerTable.prov_id = p.prov_id
	--AND innerTable.slot_begin_time = p.slot_begin_time
	--FOR XML PATH('')
	--),1,1,'') AS visit_types
	STUFF((
	--SELECT ',' + CAST(innerTable.visit_type AS varchar(30))
	SELECT ',' + CAST(innerTable.visit_type AS VARCHAR(30)) + ' ' + CAST(COUNT(innerTable.visit_type) AS VARCHAR(5))
	FROM #avail_slot AS innerTable
	WHERE innerTable.department_id = p.department_id
	AND innerTable.prov_id = p.prov_id
	AND innerTable.slot_begin_time = p.slot_begin_time
	GROUP BY
		innerTable.department_id,
		innerTable.prov_id,
		innerTable.slot_begin_time,
		innerTable.visit_type
	FOR XML PATH('')
	),1,1,'') AS visit_types
FROM #avail_slot p
WHERE p.appt_slot_number > 0
GROUP BY
	department_id,
	prov_id,
	slot_begin_time
) sum2
ON sum2.department_id = sum1.department_id
AND sum2.prov_id = sum1.prov_id
AND sum2.slot_begin_time = sum1.slot_begin_time
ORDER BY
	sum1.DEPARTMENT_ID,
	sum1.PROV_ID,
	sum1.SLOT_BEGIN_TIME

  -- Create index for temp table #avail_slot_summary
  CREATE UNIQUE CLUSTERED INDEX IX_avail_slot_summary ON #avail_slot_summary (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)

	--SELECT
	--	*
	--FROM #avail_slot_summary
	--ORDER BY
	--	DEPARTMENT_ID,
	--	PROV_ID,
	--	SLOT_BEGIN_TIME

/*====================================================================================================================*/

SELECT blk.DEPARTMENT_ID
	 , blk.PROV_ID
	 , CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SLOT_DATE
	 , blk.SLOT_BEGIN_TIME
	 , blk.LINE
	 , COALESCE(zab.NAME, 'Unknown') AS BLOCK_NAME
	 , COALESCE(blk.ORG_AVAIL_BLOCKS, 999) AS ORG_AVAIL_BLOCKS
	 , COALESCE(blk.BLOCKS_USED,0) AS BLOCKS_USED
INTO #avail_block
FROM dbo.AVAIL_BLOCK blk
INNER JOIN CLARITY_App.Rptg.vwDim_Date dd				ON CAST(CAST(blk.SLOT_BEGIN_TIME AS DATE) AS SMALLDATETIME)  = dd.day_date
LEFT OUTER JOIN dbo.ZC_APPT_BLOCK zab
ON zab.APPT_BLOCK_C = blk.BLOCK_C
LEFT OUTER JOIN dbo.CLARITY_SER ser						ON blk.PROV_ID = ser.PROV_ID
   WHERE 1=1
	AND ( 
			(@HierarchyLookup <> 8 AND blk.DEPARTMENT_ID IN (SELECT value FROM STRING_SPLIT(@Departments,',')))
			OR
			(@HierarchyLookup = 8 AND COALESCE(ser.RPT_GRP_EIGHT,'0')	IN (SELECT value FROM STRING_SPLIT(@Departments,',')) ) -- says department but the department parameter does department and financial subdivision
			 )
	AND blk.PROV_ID IN (SELECT value FROM STRING_SPLIT(@Providers,','))
    AND dd.day_date >= @StartDate
	AND dd.day_date <= @EndDate

	AND CAST(blk.SLOT_BEGIN_TIME AS DATE) = '1/14/2025'
	--AND blk.SLOT_BEGIN_TIME = '11/19/2024 10:00:00'
ORDER BY blk.DEPARTMENT_ID
       , blk.PROV_ID
	   , CAST(blk.SLOT_BEGIN_TIME AS DATE)
	   , CAST(blk.SLOT_BEGIN_TIME AS TIME)

  -- Create index for temp table #avail_block
  CREATE UNIQUE CLUSTERED INDEX IX_avail_block ON #avail_block (DEPARTMENT_ID, PROV_ID, SLOT_DATE, SLOT_BEGIN_TIME, LINE, BLOCK_NAME)

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	block_name,
	SUM(ORG_AVAIL_BLOCKS) AS available
INTO #avail_by_block
FROM #avail_block
GROUP BY
	department_id,
	prov_id,
	slot_begin_time,
	block_name
ORDER BY
	department_id,
	prov_id,
	slot_begin_time,
	block_name

  -- Create index for temp table #avail_by_block
  CREATE UNIQUE CLUSTERED INDEX IX_avail_by_block ON #avail_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, BLOCK_NAME)

--SELECT DISTINCT
--	blk.DEPARTMENT_ID,
--	blk.PROV_ID,
--	blk.SLOT_BEGIN_TIME,
--   (SELECT blkt.BLOCK_NAME + ' (' + CONVERT(VARCHAR(3),blkt.ORG_AVAIL_BLOCKS) + '),' AS [text()]
--	FROM #avail_block blkt
--	WHERE blkt.DEPARTMENT_ID = blk.DEPARTMENT_ID
--	AND blkt.PROV_ID = blk.PROV_ID
--	AND blkt.SLOT_BEGIN_TIME = blk.SLOT_BEGIN_TIME
--	ORDER BY blkt.BLOCK_NAME DESC
--	FOR XML PATH ('')) AS org_avail_blocks_string
--INTO #org_avail_blocks
--FROM #avail_block blk

--/*
SELECT DISTINCT
	blk.DEPARTMENT_ID,
	blk.PROV_ID,
	blk.SLOT_BEGIN_TIME,
	COUNT(*) AS block_count,
	MAX(blk.BLOCK_NAME) AS block_name
INTO #org_avail_blocks_summary
FROM #avail_block blk
GROUP BY
	department_id,
	prov_id,
	slot_begin_time
ORDER BY
	department_id,
	prov_id,
	slot_begin_time

  -- Create index for temp table #org_avail_blocks_summary
  CREATE UNIQUE CLUSTERED INDEX IX_org_avail_blocks_summary ON #org_avail_blocks_summary (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)
--*/

SELECT
	bbb.DEPARTMENT_ID,
    bbb.PROV_ID,
    bbb.SLOT_BEGIN_TIME,
	--oab.org_avail_blocks_string,
    CASE WHEN oabs.block_name IS NOT NULL THEN oabs.block_name ELSE bbb.template_block_name END AS template_block_name,
	ass.openings,
    bbb.booked---,
	--bbb.blocks_booked
INTO #booked_by_block_plus
FROM #booked_by_block bbb
--LEFT OUTER JOIN
--(
--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    org_avail_blocks_string
--FROM #org_avail_blocks
--) oab
--ON oab.DEPARTMENT_ID = bbb.DEPARTMENT_ID
--AND oab.PROV_ID = bbb.PROV_ID
--AND  oab.SLOT_BEGIN_TIME = bbb.SLOT_BEGIN_TIME
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    block_name
FROM #org_avail_blocks_summary
WHERE block_count = 1
) oabs
ON oabs.DEPARTMENT_ID = bbb.DEPARTMENT_ID
AND oabs.PROV_ID = bbb.PROV_ID
AND  oabs.SLOT_BEGIN_TIME = bbb.SLOT_BEGIN_TIME
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    regular_openings,
    overbook_openings,
    --openings,
    openings + outside_template_openings AS openings,
    outside_template_openings,
    num_apts_scheduled,
	num_regular_opening_apts_scheduled,
    num_overbook_apts_scheduled,
    num_outside_template_apts_scheduled
FROM #avail_slot_summary
) ass
ON ass.DEPARTMENT_ID = bbb.DEPARTMENT_ID
AND ass.PROV_ID = bbb.PROV_ID
AND ass.SLOT_BEGIN_TIME = bbb.SLOT_BEGIN_TIME
ORDER BY
	bbb.department_id,
	bbb.prov_id,
	bbb.slot_begin_time,
	CASE WHEN oabs.block_name IS NOT NULL THEN oabs.block_name ELSE bbb.template_block_name END

  -- Create index for temp table #booked_by_block_plus
  --CREATE UNIQUE CLUSTERED INDEX IX_booked_by_block_plus ON #booked_by_block_plus (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, template_block_name)
  CREATE NONCLUSTERED INDEX IX_booked_by_block_plus ON #booked_by_block_plus (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, template_block_name)

 -- SELECT	
	--*
 -- FROM #booked_by_block_plus
 -- ORDER BY
	--SLOT_BEGIN_TIME
--/*
SELECT
	unique_booked_available.DEPARTMENT_ID,
    unique_booked_available.PROV_ID,
    unique_booked_available.SLOT_BEGIN_TIME,
    unique_booked_available.block_name
INTO #unique_booked_available
FROM
(
SELECT
	booked_available.DEPARTMENT_ID,
    booked_available.PROV_ID,
    booked_available.SLOT_BEGIN_TIME,
    booked_available.block_name,
	ROW_NUMBER() OVER(PARTITION BY booked_available.DEPARTMENT_ID, booked_available.PROV_ID, booked_available.SLOT_BEGIN_TIME, booked_available.block_name ORDER BY booked_available.block_name) AS seq
FROM  
(
SELECT
	booked.DEPARTMENT_ID,
    booked.PROV_ID,
    booked.SLOT_BEGIN_TIME,
    booked.template_block_name AS block_name
--FROM #booked_by_block booked
FROM #booked_by_block_plus booked
UNION ALL
SELECT
	available.DEPARTMENT_ID,
    available.PROV_ID,
    available.SLOT_BEGIN_TIME,
    available.BLOCK_NAME AS block_name
FROM #avail_by_block available
) booked_available
) unique_booked_available
WHERE unique_booked_available.seq = 1
ORDER BY
	unique_booked_available.DEPARTMENT_ID,
    unique_booked_available.PROV_ID,
    unique_booked_available.SLOT_BEGIN_TIME,
    unique_booked_available.block_name

  -- Create index for temp table #unique_booked_available
  CREATE UNIQUE CLUSTERED INDEX IX_unique_booked_available ON #unique_booked_available (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)
--*/

--SELECT
--	summary.DEPARTMENT_ID,
--    summary.PROV_ID,
--    summary.SLOT_BEGIN_TIME,
--    summary.regular_openings,
--    summary.overbook_openings,
--    summary.openings,
--    summary.outside_template_openings,
--    summary.num_apts_scheduled,
--	summary.num_regular_opening_apts_scheduled,
--    summary.num_overbook_apts_scheduled,
--    summary.num_outside_template_apts_scheduled,
--    --summary.block_name,
--    COALESCE(summary.block_name,'Unknown') AS block_name,
--    CASE WHEN summary.block_name IS NULL AND booked_by_block.booked IS NULL THEN 0
--			   WHEN summary.block_name IS NOT NULL AND booked_by_block.booked IS NULL THEN 0
--			   ELSE booked_by_block.booked
--	END AS booked,
--	--COALESCE(avail_by_block.available,0) AS org_available_block_openings
--	avail_by_block.available AS org_available_block_openings
--INTO #booked_available_by_block
--FROM
--(
--SELECT
--	avail_slot.DEPARTMENT_ID,
--    avail_slot.PROV_ID,
--    avail_slot.SLOT_BEGIN_TIME,
--    avail_slot.regular_openings,
--    avail_slot.overbook_openings,
--    --avail_slot.openings,
--    avail_slot.openings + avail_slot.outside_template_openings AS openings,
--    avail_slot.outside_template_openings,
--    avail_slot.num_apts_scheduled,
--	avail_slot.num_regular_opening_apts_scheduled,
--    avail_slot.num_overbook_apts_scheduled,
--    avail_slot.num_outside_template_apts_scheduled,
--    booked_available.block_name
--FROM #avail_slot_summary avail_slot
--LEFT OUTER JOIN #unique_booked_available booked_available
--ON booked_available.DEPARTMENT_ID = avail_slot.DEPARTMENT_ID
--AND booked_available.PROV_ID = avail_slot.PROV_ID
--AND booked_available.SLOT_BEGIN_TIME = avail_slot.SLOT_BEGIN_TIME
--) summary
--LEFT OUTER JOIN
--(
--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    template_block_name,
--    booked
----FROM #booked_by_block
--FROM #booked_by_block_plus
--) booked_by_block
--ON booked_by_block.DEPARTMENT_ID = summary.DEPARTMENT_ID
--AND booked_by_block.PROV_ID = summary.PROV_ID
--AND booked_by_block.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
--AND booked_by_block.template_block_name = summary.block_name
--LEFT OUTER JOIN
--(
--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    BLOCK_NAME,
--    available
--FROM #avail_by_block
--) avail_by_block
--ON avail_by_block.DEPARTMENT_ID = summary.DEPARTMENT_ID
--AND avail_by_block.PROV_ID = summary.PROV_ID
--AND avail_by_block.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
--AND avail_by_block.BLOCK_NAME = summary.block_name

SELECT
	summary.DEPARTMENT_ID,
    summary.PROV_ID,
    summary.SLOT_BEGIN_TIME,
    summary.regular_openings,
    summary.overbook_openings,
    summary.openings,
    summary.outside_template_openings,
    summary.num_apts_scheduled,
	summary.num_regular_opening_apts_scheduled,
    summary.num_overbook_apts_scheduled,
    summary.num_outside_template_apts_scheduled,
    --summary.block_name,
    COALESCE(summary.block_name,'Unknown') AS block_name,
    CASE WHEN summary.block_name IS NULL AND booked_by_block.booked IS NULL THEN 0
			   WHEN summary.block_name IS NOT NULL AND booked_by_block.booked IS NULL THEN 0
			   ELSE booked_by_block.booked
	END AS booked,
	--COALESCE(avail_by_block.available,0) AS org_available_block_openings
	avail_by_block.available AS org_available_block_openings,
	--booked_by_block.blocks_booked,

	summary.provider_type,
	summary.visit_types,

	summary.UNAVAILABLE_RSN_NAME		-- 12/16/2024
INTO #booked_available_by_block
FROM
(
SELECT
	avail_slot.DEPARTMENT_ID,
    avail_slot.PROV_ID,
    avail_slot.SLOT_BEGIN_TIME,
    avail_slot.regular_openings,
    avail_slot.overbook_openings,
    --avail_slot.openings,
    avail_slot.openings + avail_slot.outside_template_openings AS openings,
    avail_slot.outside_template_openings,
    avail_slot.num_apts_scheduled,
	avail_slot.num_regular_opening_apts_scheduled,
    avail_slot.num_overbook_apts_scheduled,
    avail_slot.num_outside_template_apts_scheduled,
    booked_available.block_name,

	avail_slot.provider_type,
	avail_slot.visit_types,

	avail_slot.UNAVAILABLE_RSN_NAME		-- 12/16/2024

FROM #avail_slot_summary avail_slot
LEFT OUTER JOIN #unique_booked_available booked_available
ON booked_available.DEPARTMENT_ID = avail_slot.DEPARTMENT_ID
AND booked_available.PROV_ID = avail_slot.PROV_ID
AND booked_available.SLOT_BEGIN_TIME = avail_slot.SLOT_BEGIN_TIME
) summary
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    template_block_name,
    booked--,
	--blocks_booked
--FROM #booked_by_block
FROM #booked_by_block_plus
) booked_by_block
ON booked_by_block.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND booked_by_block.PROV_ID = summary.PROV_ID
AND booked_by_block.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
AND booked_by_block.template_block_name = summary.block_name
LEFT OUTER JOIN
(
SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    BLOCK_NAME,
    available
FROM #avail_by_block
) avail_by_block
ON avail_by_block.DEPARTMENT_ID = summary.DEPARTMENT_ID
AND avail_by_block.PROV_ID = summary.PROV_ID
AND avail_by_block.SLOT_BEGIN_TIME = summary.SLOT_BEGIN_TIME
AND avail_by_block.BLOCK_NAME = summary.block_name
ORDER BY
	summary.DEPARTMENT_ID,
    summary.PROV_ID,
    summary.SLOT_BEGIN_TIME,
    COALESCE(summary.block_name,'Unknown')

  -- Create index for temp table #booked_available_by_block
  --CREATE UNIQUE CLUSTERED INDEX IX_booked_available_by_block ON #booked_available_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)
  CREATE NONCLUSTERED INDEX IX_booked_available_by_block ON #booked_available_by_block (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)

 -- SELECT
	--*
 -- FROM #booked_available_by_block
 -- ORDER BY SLOT_BEGIN_TIME

--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    MAX(regular_openings) AS regular_openings,
--    MAX(openings) AS openings,
--    SUM(booked) AS booked,
--	--COUNT(*) AS blocks
--	SUM(CASE WHEN block_name IS NOT NULL THEN 1 ELSE 0 END) AS blocks
--INTO #booked_available_by_block_agg
--FROM #booked_available_by_block
--GROUP BY
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME

--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    MAX(regular_openings) AS regular_openings,
--    MAX(openings) AS openings,
--    SUM(booked) AS booked,
--	--COUNT(*) AS blocks
--	SUM(CASE WHEN block_name IS NOT NULL THEN 1 ELSE 0 END) AS blocks,
--	MAX(provider_type) AS provider_type,
--	MAX(visit_types) AS visit_types--,
--	--MAX(blocks_booked) AS blocks_booked
--INTO #booked_available_by_block_agg
--FROM #booked_available_by_block
--GROUP BY
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME
--ORDER BY
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME

SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    MAX(regular_openings) AS regular_openings,
    MAX(openings) AS openings,
    SUM(booked) AS booked,
	--COUNT(*) AS blocks
	SUM(CASE WHEN block_name IS NOT NULL THEN 1 ELSE 0 END) AS blocks,
	MAX(provider_type) AS provider_type,
	MAX(visit_types) AS visit_types,

	MAX(UNAVAILABLE_RSN_NAME) AS UNAVAILABLE_RSN_NAME,		-- 12/16/2024
	--MAX(blocks_booked) AS blocks_booked
	STUFF((
	--SELECT ',' + CAST(innerTable.visit_type AS varchar(30))
	SELECT ',' + CAST(innerTable.block_name AS varchar(30)) + ' ' + CAST(booked AS VARCHAR(5))
	FROM #booked_available_by_block AS innerTable
	WHERE innerTable.department_id = p.department_id
	AND innerTable.prov_id = p.prov_id
	AND innerTable.slot_begin_time = p.slot_begin_time
	FOR XML PATH('')
	),1,1,'') AS blocks_booked
INTO #booked_available_by_block_agg
FROM #booked_available_by_block p
GROUP BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME
ORDER BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME

  -- Create index for temp table #booked_available_by_block_agg
  CREATE UNIQUE CLUSTERED INDEX IX_booked_available_by_block_agg ON #booked_available_by_block_agg (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)

 -- SELECT
	--*
 -- FROM #booked_available_by_block_agg
 -- ORDER BY SLOT_BEGIN_TIME

--SELECT
--	booked_available_by_block.DEPARTMENT_ID,
--    booked_available_by_block.PROV_ID,
--    booked_available_by_block.SLOT_BEGIN_TIME,
--    booked_available_by_block.regular_openings,
--    booked_available_by_block.openings,
--    overbook_openings,
--    outside_template_openings,
--    num_apts_scheduled,
--	num_regular_opening_apts_scheduled,
--    num_overbook_apts_scheduled,
--    num_outside_template_apts_scheduled,
--    block_name,
--    booked_available_by_block.booked,
--    org_available_block_openings,
--	booked_available_by_block_agg.booked AS booked_total,
--	booked_available_by_block_agg.blocks AS blocks_total
--INTO #booked_available_summary
--FROM #booked_available_by_block booked_available_by_block

--LEFT OUTER JOIN #booked_available_by_block_agg booked_available_by_block_agg
--ON booked_available_by_block_agg.DEPARTMENT_ID = booked_available_by_block.DEPARTMENT_ID
--AND booked_available_by_block_agg.PROV_ID = booked_available_by_block.PROV_ID
--AND booked_available_by_block_agg.SLOT_BEGIN_TIME = booked_available_by_block.SLOT_BEGIN_TIME

SELECT
	booked_available_by_block.DEPARTMENT_ID,
    booked_available_by_block.PROV_ID,
    booked_available_by_block.SLOT_BEGIN_TIME,
    booked_available_by_block.regular_openings,
    booked_available_by_block.openings,
    booked_available_by_block.overbook_openings,
    booked_available_by_block.outside_template_openings,
    booked_available_by_block.num_apts_scheduled,
	booked_available_by_block.num_regular_opening_apts_scheduled,
    booked_available_by_block.num_overbook_apts_scheduled,
    booked_available_by_block.num_outside_template_apts_scheduled,
    booked_available_by_block.block_name,
    booked_available_by_block.booked,
    booked_available_by_block.org_available_block_openings,
	booked_available_by_block_agg.booked AS booked_total,
	booked_available_by_block_agg.openings - booked_available_by_block_agg.booked AS openings_available_total,
	booked_available_by_block_agg.blocks AS blocks_total,
	booked_available_by_block_agg.provider_type,
	booked_available_by_block_agg.visit_types,

	booked_available_by_block_agg.UNAVAILABLE_RSN_NAME,			-- 12/16/2024

	--booked_available_by_block.blocks_booked
	booked_available_by_block_agg.blocks_booked
INTO #booked_available_summary 
FROM #booked_available_by_block booked_available_by_block
LEFT OUTER JOIN #booked_available_by_block_agg booked_available_by_block_agg
ON booked_available_by_block_agg.DEPARTMENT_ID = booked_available_by_block.DEPARTMENT_ID
AND booked_available_by_block_agg.PROV_ID = booked_available_by_block.PROV_ID
AND booked_available_by_block_agg.SLOT_BEGIN_TIME = booked_available_by_block.SLOT_BEGIN_TIME
ORDER BY
	booked_available_by_block.DEPARTMENT_ID,
    booked_available_by_block.PROV_ID,
    booked_available_by_block.SLOT_BEGIN_TIME,
    booked_available_by_block.block_name

  -- Create index for temp table #booked_available_summary
  --CREATE UNIQUE CLUSTERED INDEX IX_booked_available_summary ON #booked_available_summary (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)
  CREATE NONCLUSTERED INDEX IX_booked_available_summary ON #booked_available_summary (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME, block_name)

SELECT
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    openings,
    block_name,
    booked_total,
	openings - booked_total AS total_openings_available,
	openings_available_total
FROM #booked_available_summary
WHERE openings > booked_total
ORDER BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME	

SELECT
	department_id,
	prov_id,
	slot_begin_time,
	STUFF((
	SELECT ',' + CAST(innerTable.block_name AS varchar(30)) + ' ' + CAST((innerTable.openings - innerTable.booked_total) AS VARCHAR(10))
	FROM #booked_available_summary AS innerTable
	WHERE innerTable.department_id = p.department_id
	AND innerTable.prov_id = p.prov_id
	AND innerTable.slot_begin_time = p.slot_begin_time
	FOR XML PATH('')
	),1,1,'') AS total_openings_available
INTO #booked_available_summary2
FROM #booked_available_summary p
WHERE openings > booked_total
GROUP BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME	
ORDER BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME	

  -- Create index for temp table #booked_available_summary2
  CREATE UNIQUE CLUSTERED INDEX IX_booked_available_summary2 ON #booked_available_summary2 (DEPARTMENT_ID, PROV_ID, SLOT_BEGIN_TIME)

--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    regular_openings,
--    openings,
--    block_name,
--    booked,
--    org_available_block_openings,
--    booked_total,
--    blocks_total,
--	CASE
--		WHEN ((openings = booked) AND (booked_total = booked)) THEN 'Y'
--		WHEN ((openings > booked) AND (booked_total = booked)) THEN 'Y'
--		WHEN (blocks_total =1 AND booked = 0) THEN 'Y'
--		WHEN (blocks_total >1 AND (booked_total < openings)) THEN 'Y'
--		WHEN (openings > 0 AND booked_total = 0) THEN 'Y'
--		WHEN ((openings = booked) AND (booked_total > booked)) THEN 'Y'
--		WHEN ((booked > openings) AND (booked_total = booked)) THEN 'Y'
--		ELSE NULL
--	END AS 'Keep?',
--    overbook_openings,
--    outside_template_openings,
--    num_apts_scheduled,
--	num_regular_opening_apts_scheduled,
--    num_overbook_apts_scheduled,
--    num_outside_template_apts_scheduled
--INTO #availability
--FROM #booked_available_summary

SELECT
	s1.DEPARTMENT_ID,
	o.organization_name,
	s.service_name,
	c.clinical_area_name,
    s1.PROV_ID,
    s1.SLOT_BEGIN_TIME,
	CAST(s1.SLOT_BEGIN_TIME AS DATE) AS SLOT_BEGIN_DATE,
    s1.regular_openings,
    s1.openings,
    s1.block_name,
    s1.booked,
    s1.org_available_block_openings,
    s1.booked_total,
	s1.openings_available_total,
    s1.blocks_total,
	CASE
		WHEN ((s1.openings = s1.booked) AND (s1.booked_total = s1.booked)) THEN 'Y'
		WHEN ((s1.openings > s1.booked) AND (s1.booked_total = s1.booked)) THEN 'Y'
		WHEN (s1.blocks_total =1 AND s1.booked = 0) THEN 'Y'
		WHEN (s1.blocks_total >1 AND (s1.booked_total < s1.openings)) THEN 'Y'
		WHEN (s1.openings > 0 AND s1.booked_total = 0) THEN 'Y'
		WHEN ((s1.openings = s1.booked) AND (s1.booked_total > s1.booked)) THEN 'Y'
		WHEN ((s1.booked > s1.openings) AND (s1.booked_total = s1.booked)) THEN 'Y'
		ELSE NULL
	END AS 'Keep?',
    s1.overbook_openings,
    s1.outside_template_openings,
    s1.num_apts_scheduled,
	s1.num_regular_opening_apts_scheduled,
    s1.num_overbook_apts_scheduled,
    s1.num_outside_template_apts_scheduled,
	s1.provider_type,
	s1.visit_types,

	s1.UNAVAILABLE_RSN_NAME,				-- 12/16/2024

	s1.blocks_booked,
	s2.total_openings_available AS total_openings_available_string
INTO #availability
FROM #booked_available_summary s1
LEFT OUTER JOIN #booked_available_summary2 s2
ON s2.DEPARTMENT_ID = s1.DEPARTMENT_ID
AND s2.PROV_ID = s1.PROV_ID
AND s2.SLOT_BEGIN_TIME = s1.SLOT_BEGIN_TIME

LEFT JOIN [CLARITY_App].[Mapping].[Epic_Dept_Groupers] g ON s1.DEPARTMENT_ID = g.epic_department_id
LEFT JOIN [CLARITY_App].[Mapping].Ref_Clinical_Area_Map c on g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
LEFT JOIN [CLARITY_App].[Mapping].Ref_Service_Map s on c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
LEFT JOIN [CLARITY_App].[Mapping].Ref_Organization_Map o on s.organization_id = o.organization_id

	SELECT
		*
	FROM #availability
ORDER BY
	DEPARTMENT_ID,
    PROV_ID,
    SLOT_BEGIN_TIME,
    block_name

--SELECT
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    regular_openings,
--    openings,
--    block_name,
--    booked,
--    org_available_block_openings,
--    booked_total,
--    blocks_total,
--    --[Keep?],
--    overbook_openings,
--    outside_template_openings,
--    num_apts_scheduled,
--    num_regular_opening_apts_scheduled,
--    num_overbook_apts_scheduled,
--    num_outside_template_apts_scheduled
--FROM #availability
--WHERE [Keep?] = 'Y'
--ORDER BY
--	DEPARTMENT_ID,
--    PROV_ID,
--    SLOT_BEGIN_TIME,
--    block_name

-- Original
--SELECT
--	--avail.DEPARTMENT_ID,
--	--dep.DEPARTMENT_NAME,
-- --   avail.PROV_ID,
--	--ser.PROV_NAME,
-- --   SLOT_BEGIN_TIME,
--	avail.DEPARTMENT_ID AS Department_Id,
--	dep.DEPARTMENT_NAME AS Department_Name,
--	avail.organization_name AS Organization,
--	avail.service_name AS [Service],
--	avail.clinical_area_name AS Clinical_Area,
--    avail.PROV_ID AS Provider_Id,
--	ser.PROV_NAME AS Provider_Name,
--	avail.provider_type AS Provider_Type,
--    avail.SLOT_BEGIN_TIME AS Slot_Begin_Time,
--    avail.SLOT_BEGIN_DATE AS Slot_Begin_Date,
--    --regular_openings AS total_regular_openings,
--    --overbook_openings AS total_overbook_openings,
--    --outside_template_openings AS total_outside_template_openings,
--    --openings AS total_openings,
--    --booked_total AS total_booked,
--    --blocks_total AS blocks_available,
--    avail.regular_openings AS Total_Regular_Openings,
--    avail.overbook_openings AS Total_Overbook_Openings,
--    avail.outside_template_openings AS Total_Outside_Template_Openings,
--    avail.openings AS Total_Openings,
--    avail.booked_total AS Total_Booked,
--    avail.blocks_total AS Blocks_Available,
-- --   block_name,
--	--booked,
--    avail.block_name AS Block_Name,
--	avail.booked AS Booked,
--	--openings - booked_total AS total_openings_available--,
--	avail.openings - avail.booked_total AS Total_Openings_Available--,
--    --org_available_block_openings,
--    --booked_total,
--    --[Keep?],
--    --num_apts_scheduled,
--    --num_regular_opening_apts_scheduled,
--    --num_overbook_apts_scheduled,
--    --num_outside_template_apts_scheduled
--FROM #availability avail
--LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
--	ON dep.DEPARTMENT_ID = avail.DEPARTMENT_ID
--LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
--	ON ser.PROV_ID = avail.PROV_ID
--WHERE [Keep?] = 'Y'
--AND openings > booked
--ORDER BY
--	DEPARTMENT_ID,
--    avail.PROV_ID,
--    SLOT_BEGIN_TIME,
--    block_name

--====================================================================================================================
-- Detail
--SELECT --DISTINCT
--	avail.DEPARTMENT_ID AS Department_Id,
--	dep.DEPARTMENT_NAME AS Department_Name,
--    avail.PROV_ID AS Provider_Id,
--	ser.PROV_NAME AS Provider_Name,
--	avail.provider_type AS Provider_Type,
--    avail.SLOT_BEGIN_TIME AS Slot_Begin_Time,
--    avail.regular_openings AS Total_Regular_Openings,
--    avail.overbook_openings AS Total_Overbook_Openings,
--    avail.outside_template_openings AS Total_Outside_Template_Openings,
--    avail.openings AS Total_Openings,
--    avail.booked_total AS Total_Booked,
--    avail.blocks_total AS Blocks_Available,
--    avail.block_name AS Block_Name,
--	avail.booked AS Booked,
--	avail.blocks_booked AS Blocks_Booked,
--	avail.visit_types AS Booked_Visit_Types,
--	avail.openings - avail.booked_total AS Total_Openings_Available,
--	avail.total_openings_available_string AS Total_Openings_Available_String
--FROM #availability avail
--LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
--	ON dep.DEPARTMENT_ID = avail.DEPARTMENT_ID
--LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
--	ON ser.PROV_ID = avail.PROV_ID
--WHERE [Keep?] = 'Y'
--AND avail.openings > avail.booked
----AND avail.booked_total > 1
--ORDER BY
--	avail.DEPARTMENT_ID,
--    avail.PROV_ID,
--    avail.SLOT_BEGIN_TIME,
--    avail.block_name

-- Aggregate
SELECT DISTINCT
	avail.DEPARTMENT_ID AS Department_Id,
	dep.DEPARTMENT_NAME AS Department_Name,
	avail.organization_name AS Organization,
	avail.service_name AS [Service],
	avail.clinical_area_name AS Clinical_Area,
    avail.PROV_ID AS Provider_Id,
	ser.PROV_NAME AS Provider_Name,
	avail.provider_type AS Provider_Type,

	avail.UNAVAILABLE_RSN_NAME AS Unavailable_Reason_Name,


    avail.SLOT_BEGIN_TIME AS Slot_Begin_Time,
    avail.SLOT_BEGIN_DATE AS Slot_Begin_Date,
    avail.regular_openings AS Total_Regular_Openings,
    avail.overbook_openings AS Total_Overbook_Openings,
    avail.outside_template_openings AS Total_Outside_Template_Openings,
    avail.openings AS Total_Openings,
    avail.booked_total AS Total_Booked,
	avail.openings_available_total AS Total_Openings_Available,
    avail.blocks_total AS Blocks_Available,
    --avail.block_name AS Block_Name,
	--avail.booked AS Booked,
	avail.blocks_booked AS Booked_Blocks,
	avail.visit_types AS Booked_Visit_Types,
	--avail.openings - avail.booked_total AS Total_Openings_Available,
	--avail.total_openings_available_string AS Total_Openings_Available_String
	avail.total_openings_available_string AS Openings_Available
INTO #RptgTmp
FROM #availability avail
LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	ON dep.DEPARTMENT_ID = avail.DEPARTMENT_ID
LEFT OUTER JOIN CLARITY.dbo.CLARITY_SER ser
	ON ser.PROV_ID = avail.PROV_ID
WHERE [Keep?] = 'Y'
AND avail.openings > avail.booked

SELECT
	avail.Department_Id,
    avail.Department_Name,
    avail.Organization,
    avail.Service,
    avail.Clinical_Area,
    avail.Provider_Id,
    avail.Provider_Name,
    avail.Provider_Type,

	avail.Unavailable_Reason_Name,				-- 12/16/2024

    avail.Slot_Begin_Time,
    avail.Total_Regular_Openings,
    avail.Total_Overbook_Openings,
    avail.Total_Outside_Template_Openings,
    avail.Total_Openings,
    avail.Total_Booked,
    avail.Blocks_Available,
    avail.Booked_Blocks,
    avail.Booked_Visit_Types,
    avail.Openings_Available
FROM #RptgTmp avail
ORDER BY
	avail.DEPARTMENT_ID,
    avail.Provider_Id,
    avail.SLOT_BEGIN_TIME--,
    --avail.block_name

SELECT
	avail.Organization,
	avail.Service,
	avail.Clinical_Area,
	avail.Department_Name,
	avail.Provider_Name,
	avail.Slot_Begin_Date,
	avail.Slot_Begin_Time,
	avail.Openings_Available,
	--SUM(avail.Total_Openings) AS Total_Openings
	SUM(avail.Total_Openings_Available) AS Total_Openings
FROM #RptgTmp avail
GROUP BY
	avail.Organization,
	avail.Service,
	avail.Clinical_Area,
	avail.Department_Name,
	avail.Provider_Name,
	avail.Slot_Begin_Date,
	avail.Slot_Begin_Time,
	avail.Openings_Available
ORDER BY
	avail.Organization,
	avail.Service,
	avail.Clinical_Area,
	avail.Department_Name,
	avail.Provider_Name,
	avail.Slot_Begin_Date,
	avail.Slot_Begin_Time,
	avail.Openings_Available

GO