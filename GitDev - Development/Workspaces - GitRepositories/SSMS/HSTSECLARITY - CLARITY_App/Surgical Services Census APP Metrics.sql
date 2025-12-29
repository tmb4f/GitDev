USE CLARITY
GO

/*-----------------------------------------------------------------------------------------------------------------
	WHAT: Midnight patient census data, for past 2-years (11/1/2020 - 10/31/2022),
	for EPIC provider team "Trauma Surgery PIC#1450" (Acute Care Trauma).	Length of stay for same period and group.
	TFS User Story 31999
	WHO: Chris Mitchell
	WHEN: 1/25/23
	WHY: Staffing needs for Trauma Surgery team
	-----------------------------------------------------------------------------------------------------------------
	MODS: 
	2/1/23 cjm2vk: changed last_value on the action to pull the action id, not the name of the action
	2/2/23 cjm2vk: add location
		1.	If Trauma ICU team listed and patient located in STICU / 5-North / Other ICU or IMU, then do not include 
		in Trauma Surgery MN census numbers
		2.	If Trauma ICU team listed and patient located in acute ward bed (5-West, 5-Central, or 6-West), then include
		in Trauma Surgery MN census

		STICU / 5 NORTH (DON'T INCLUDE IN TS MN CENSUS)
		-- STICU 10243046
		-- 5 north 10243090
	
		ACUTE WARD (INCLUDE IN TS MN CENSUS)
		-- 5 west 10243060
		-- 5 central 10243058
		-- 6 west 10243063

	2/6/23 cjm2vk: possible there's not an audit on every day there's an ADT census - fill out audit vs adt dates
	2/6/23 cjm2vk: add patients w/ 101 service too, will count if on right unit
-----------------------------------------------------------------------------------------------------------------------
*/
--DECLARE @start DATETIME = '2021-01-01 00:00:00.000';
--DECLARE @END DATETIME = '2022-12-31 23:59:59';
--DECLARE @END DATETIME = '2022-05-31 23:59:59';
DECLARE @start DATETIME = '2023-06-01 00:00:00.000';
DECLARE @end DATETIME = '2024-05-31 23:59:59';

DROP TABLE IF EXISTS #pats;
DROP TABLE IF EXISTS #eta;
DROP TABLE IF EXISTS #adt;
--DROP TABLE IF EXISTS #audit_day;
--DROP TABLE IF EXISTS #audit_day2;
--DROP TABLE IF EXISTS #audit_day3;
--DROP TABLE IF EXISTS #adt2;
DROP TABLE IF EXISTS #summary;
DROP TABLE IF EXISTS #summary2;
DROP TABLE IF EXISTS #census;
--DROP TABLE IF EXISTS #los;

IF OBJECT_ID('tempdb..#AD_Events') IS NOT NULL
	DROP TABLE #AD_Events

CREATE TABLE #AD_Events (PAT_ID VARCHAR(18), PAT_ENC_CSN_ID  NUMERIC, Date_Time  DATETIME, SCHED_START_TIME DATETIME, bill_num  VARCHAR(50)
	, Event VARCHAR(254), Pt_Class VARCHAR(254), Unit VARCHAR(254), Room VARCHAR(254), id VARCHAR(254), Current_Service VARCHAR(254), Service VARCHAR(254)
    , revlocid VARCHAR(10), SERVICE_C VARCHAR(66), OR_SERVICE_NAME VARCHAR(66), HSP_ACCOUNT_ID NUMERIC)

/*"Green Surgery Team" ID 69) (Attendings: Allan Tsung 126209, Todd Bauer 30332, Victor Zaydfudim 57158)
b. Orange / Bariatric (EPIC listed as "Orange Surgery" ID 72) (Attendings: Peter Hallowell 30272, Bruce Schirmer 30317, Nick Levinsky 130914, Zequan Yang 46748 [After 11/2022, he switched from EGS to Orange])
c. EGS / Emergency General Surgery (EPIC listed as "EGS Team" ID 73) (Attendings: Zequan Yang [Until 11/2022], Carlos Tache Leon 34027, Molly Flannagan 105151, John Davis 45837, Jeff Young 30284, Michael D. Williams 51087, James Forrest Calland 30344)
d. Red / Endocrine (EPIC listed as "Red Surgery Team" ID 190) (Attendings: Phillip Smith 46621, Anna Fashandi 56357)
*/

/*
CARE_TEAMS_ID	RECORD_NAME

162			SICU
186			SICU NON TRAUMA
101			TRAUMA ICU
138			TRAUMA ICU
55			TRAUMA SURGERY
72			ORANGE SURGERY
170			GREEN INTERN LIST
171			GREEN INTERN LIST
69			GREEN SURGERY TEAM
202			NEUROSURGERY GREEN
99			PSYCH GREEN
1			BLUE TEAM
201			NEUROSURGERY BLUE
173			NICU BLUE
98			PSYCH BLUE
73			EGS TEAM

•	Surgical ICU (SICU, PIC 1820)
•	Trauma ICU (TICU, PIC 1294)
•	Bariatric / Minimally Invasive Surgery (Orange service, PIC 1247)
•	Hepatopancreatobiliary surgery (Green service, PIC 1504)
•	Colorectal surgery (Blue service, 1449)
•	Emergency General Surgery (EGS service, PIC 1445)

WHERE PAT_ID = 'Z1043999' AND PAT_ENC_CSN_ID = 200093554544

TEAM_AUDIT_ID	TEAM_ACTION_C	PRIMARYTEAM_AUDI_YN		CONTACT_AUDIT_ID	TEAMAUDIT_USER_ID	TEAM_AUDIT_INSTANT
73	1	Y	NULL	73154	2024-04-17 22:26:00.000
1	1	Y	NULL	72456	2024-04-18 02:24:00.000

PAT_ID	PAT_ENC_CSN_ID	TEAM_AUDIT_ID
Z1043999	200093554544	1
Z1043999	200093554544	73
*/

--DECLARE @PROVTEAM TABLE
--(
--PROV_ID VARCHAR(18),
--RECORD_NAME VARCHAR(200)
--)

--INSERT INTO @PROVTEAM
--(
--    PROV_ID,
--    RECORD_NAME
--)
--VALUES
-- ('105151', -- FLANNAGAN, MOLLY A
--  'EGS Team') -- "EGS Team" ID 73
--,('126209', -- TSUNG, ALLAN
--  'Green Surgery Team') -- "Green Surgery Team" ID 69
--,('130914', -- LEVINSKY JR, NICK C
--  'Orange Surgery') -- "Orange Surgery" ID 72
--,('30272', -- HALLOWELL, PETER
--  'Orange Surgery') -- "Orange Surgery" ID 72
--,('30284', -- YOUNG, JEFFREY SETH
--  'EGS Team') -- "EGS Team" ID 73
--,('30317', -- SCHIRMER, BRUCE
--  'Orange Surgery') -- "Orange Surgery" ID 72
--,('30332', -- BAUER, TODD W
--  'Green Surgery Team') -- "Green Surgery Team" ID 69
--,('30344', -- CALLAND, JAMES F
--  'EGS Team') -- "EGS Team" ID 73
--,('34027', -- TACHE-LEON, CARLOS
--  'EGS Team') -- "EGS Team" ID 73
--,('45837', -- DAVIS, JOHN P
--  'EGS Team') -- "EGS Team" ID 73
--,('46621', -- SMITH, PHILIP W
--  'Red Surgery Team') -- "Red Surgery Team" ID 190
--,('46748', -- YANG, ZEQUAN
--  'Orange Surgery') -- "Orange Surgery" ID 72
--,('51087', -- WILLIAMS, MICHAEL D
--  'EGS Team') -- "EGS Team" ID 73
--,('56357', -- FASHANDI, ANNA Z
--  'Red Surgery Team') -- "Red Surgery Team" ID 190
--,('57158', -- ZAYDFUDIM, VICTOR M
--  'Green Surgery Team') -- "Green Surgery Team" ID 69
--;

	SELECT DISTINCT 
		 eta.PAT_ID
		,eta.PAT_ENC_CSN_ID
	INTO #pats
	FROM CLARITY..EPT_TEAM_AUDIT eta
	WHERE 1 = 1
	    AND eta.PRIMARYTEAM_AUDI_YN = 'Y' -- Indicates whether this line of the team audit shows that the team was the primary team.
        AND eta.TEAM_AUDIT_ID IN (
			162,	-- SICU
			186, -- SICU NON TRAUMA
			101, -- TRAUMA ICU
			138, -- TRAUMA ICU
			55, --   TRAUMA SURGERY
			72,	-- ORANGE SURGERY
			69,	-- GREEN SURGERY TEAM
			1,		-- BLUE TEAM
			73	-- EGS TEAM
			)
		AND eta.TEAM_AUDIT_INSTANT >=  @start
		AND eta.TEAM_AUDIT_INSTANT <= @end
		--AND eta.PAT_ID = 'Z996364' AND eta.PAT_ENC_CSN_ID = 200075719543
		--AND eta.PAT_ID = 'Z3116517' AND eta.PAT_ENC_CSN_ID = 200082144584
		--AND eta.PAT_ID = 'Z1020125' AND eta.PAT_ENC_CSN_ID = 200090148311

--SELECT pats.*,
--pat.PAT_NAME
--FROM #pats pats
--LEFT OUTER JOIN CLARITY.dbo.PATIENT pat ON pat.PAT_ID = pats.PAT_ID
--ORDER BY PAT_ID, PAT_ENC_CSN_ID

INSERT INTO #AD_Events
SELECT
	orlog.PAT_ID,
    orlog.PAT_ENC_CSN_ID,
    orlog.Date_Time,
    orlog.SCHED_START_TIME,
    orlog.BILL_NUM,
    orlog.Event,
    orlog.Pt_Class,
    orlog.Unit,
    orlog.Room,
    orlog.id,
    orlog.Current_Service,
    orlog.Service,
    orlog.REV_LOC_ID,
    orlog.SERVICE_C,
    orlog.OR_SERVICE_NAME,
    orlog.HSP_ACCOUNT_ID
FROM
(
SELECT
adm.PAT_ID
--adm.PAT_ENC_CSN_ID
,adm.OR_LINK_CSN AS PAT_ENC_CSN_ID
			  ,vs.SURGERY_DATE AS Date_Time
			  ,vs.SCHED_START_TIME
			  --,hspa.BILL_NUM
			  ,BILL_NUM = CASE
				WHEN hspa.HSP_ACCOUNT_ID IS NOT NULL 
					THEN hspa.HSP_ACCOUNT_ID
				WHEN ISNUMERIC(hspa.BILL_NUM) = 1 
					THEN CAST(hspa.BILL_NUM AS BIGINT) 
				ELSE 0 END  --06/30/17
			  ,'Surgery' AS Event
			  ,zpc.NAME AS Pt_Class
			  ,loc.LOC_NAME AS Unit
			  ,cs.PROV_NAME AS Room
			  ,vs.CASE_ID	AS id
			  ,'Surgery'			"Current_Service"
			  ,COALESCE(zcp_h.name, '')		"Service"
			  ,dept.REV_LOC_ID
			  ,vs.SERVICE_C
			  ,zcs.NAME AS OR_SERVICE_NAME
			  ,hspa.HSP_ACCOUNT_ID
			  ,ROW_NUMBER() OVER(PARTITION BY adm.PAT_ID, adm.OR_LINK_CSN ORDER BY vs.SCHED_START_TIME DESC) AS seq
		FROM clarity.dbo.OR_LOG AS vs
		LEFT JOIN clarity.dbo.ZC_PAT_CLASS AS zpc
				ON vs.PAT_TYPE_C =zpc.ADT_PAT_CLASS_C
		INNER JOIN clarity.dbo.CLARITY_SER AS cs
				ON vs.ROOM_ID=cs.PROV_ID
		LEFT JOIN clarity.dbo.PAT_OR_ADM_LINK adm
				ON adm.OR_CASELOG_ID = vs.LOG_ID
		LEFT JOIN clarity.dbo.clarity_loc AS loc
				ON loc.LOC_ID = vs.LOC_ID
		INNER JOIN clarity.dbo.pat_enc_hsp hspa
				--ON hspa.pat_enc_csn_id = adm.PAT_ENC_CSN_ID
				ON hspa.pat_enc_csn_id = adm.OR_LINK_CSN
		LEFT JOIN clarity.dbo.zc_pat_service zcp_h
				ON zcp_h.hosp_serv_c = hspa.hosp_serv_c
		LEFT JOIN clarity.dbo.zc_disch_disp zcd
				ON zcd.disch_disp_c = hspa.disch_disp_c
        INNER JOIN CLARITY.dbo.CLARITY_DEP dept				ON hspa.DEPARTMENT_ID = dept.DEPARTMENT_ID
		INNER JOIN #pats pats
				--ON adm.PAT_ENC_CSN_ID = pats.PAT_ENC_CSN_ID
				ON adm.OR_LINK_CSN = pats.PAT_ENC_CSN_ID
		--INNER JOIN #eta eta
		--		ON hspa.HSP_ACCOUNT_ID = eta.HSP_ACCOUNT_ID
		INNER JOIN CLARITY.dbo.ZC_OR_SERVICE  AS zcs
			ON zcs.SERVICE_C = vs.SERVICE_C
--WHERE vs.PAT_ID = 'Z1002663'
) orlog
WHERE orlog.seq =  1

	--SELECT
	--	*
	--FROM #AD_Events
	--ORDER BY PAT_ID, pat_enc_csn_id

	SELECT DISTINCT 
		 pats.PAT_ID
		,pats.PAT_ENC_CSN_ID
		--,hsp.HOSP_DISCH_TIME
		,hsp.INPATIENT_DATA_ID
		--,flshtpvt.[7247]
		,hsp.HOSP_SERV_C
		,zps.NAME AS HOSP_SERV_NAME
		,hsp.HSP_ACCOUNT_ID
		,eta.TEAM_AUDIT_ID
		,ptri.RECORD_NAME
		,eta.TEAM_AUDIT_INSTANT
		,orlog.Pt_Class
		,orlog.OR_SERVICE_NAME
	INTO #eta
	FROM CLARITY..EPT_TEAM_AUDIT eta
	--INNER JOIN
	LEFT OUTER JOIN
	(
	SELECT DISTINCT
		PAT_ENC_CSN_ID
	   --,HOSP_DISCH_TIME
	   ,INPATIENT_DATA_ID
	   ,HOSP_SERV_C
	   ,HSP_ACCOUNT_ID
	FROM CLARITY..PAT_ENC_HSP
	) hsp
	ON eta.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
--	LEFT OUTER JOIN
--	(
--	SELECT * FROM 
--							(SELECT 
--							COALESCE(flwshtm.MEAS_VALUE, flwshtm.MEAS_COMMENT) 'MEAS_VALUE'
--							,flwshtm.FLO_MEAS_ID
--							,flwshtrw.INPATIENT_DATA_ID
--							, ROW_NUMBER() OVER (PARTITION BY flwshtrw.INPATIENT_DATA_ID, flwshtm.FLO_MEAS_ID ORDER BY flwshtm.RECORDED_TIME DESC) 'RNo'
--							 FROM CLARITY.dbo.IP_FLWSHT_MEAS flwshtm
--							 INNER JOIN CLARITY.dbo.IP_FLWSHT_REC flwshtrw		ON flwshtrw.FSD_ID = flwshtm.FSD_ID
--							 WHERE flwshtm.FLO_MEAS_ID IN ('7247')
--							) flsht
--					 PIVOT 
--					 (
--					 MAX(flsht.MEAS_VALUE) 
--					 FOR FLO_MEAS_ID IN ([7247])
--					) AS pvt
--) flshtpvt
--ON flshtpvt.INPATIENT_DATA_ID = hsp.INPATIENT_DATA_ID
    LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_SERVICE zps
		ON zps.HOSP_SERV_C = hsp.HOSP_SERV_C
	INNER JOIN #pats pats
		ON eta.PAT_ID =  pats.PAT_ID
		AND eta.PAT_ENC_CSN_ID = pats.PAT_ENC_CSN_ID
	LEFT OUTER JOIN #AD_Events orlog
		ON orlog.PAT_ID = eta.PAT_ID
		AND orlog.PAT_ENC_CSN_ID = eta.PAT_ENC_CSN_ID
	LEFT OUTER JOIN CLARITY.dbo.PROVTEAM_REC_INFO ptri
	  ON ptri.ID = eta.TEAM_AUDIT_ID
	WHERE 1 = 1
	    AND eta.PRIMARYTEAM_AUDI_YN = 'Y' -- Indicates whether this line of the team audit shows that the team was the primary team.
		----AND eta.TEAM_AUDIT_ID IN (55,101)  -- TRAUMA SURGERY, TRAUMA ICU
  --      AND eta.TEAM_AUDIT_ID IN (69,72,73,190) -- Green Surgery Team, Orange Surgery, EGS Team, Red Surgery Team
		----AND eta.PAT_ENC_CSN_ID = 200035563006
/*
        AND eta.TEAM_AUDIT_ID IN (
			162,	-- SICU
			101, -- TRAUMA ICU
			138, -- TRAUMA ICU
			72,	-- ORANGE SURGERY
			69,	-- GREEN SURGERY TEAM
			1,		-- BLUE TEAM
			73	-- EGS TEAM
			)
*/
		AND eta.TEAM_AUDIT_INSTANT >=  @start
		AND eta.TEAM_AUDIT_INSTANT <= @end
		AND eta.TEAM_AUDIT_ID IS NOT NULL
		--AND eta.PAT_ID = 'Z1043999' AND eta.PAT_ENC_CSN_ID = 200093554544
		--AND eta.PAT_ID = 'Z100026' AND eta.PAT_ENC_CSN_ID = 200085679451
		--AND eta.PAT_ID = 'Z101066' AND eta.PAT_ENC_CSN_ID = 200086061823
		--AND eta.PAT_ID = 'Z996364' AND eta.PAT_ENC_CSN_ID = 200075719543
		--AND eta.PAT_ID = 'Z3116517' AND eta.PAT_ENC_CSN_ID = 200082144584
    ORDER BY pats.PAT_ID, pats.PAT_ENC_CSN_ID, eta.TEAM_AUDIT_ID, eta.TEAM_AUDIT_INSTANT

CREATE UNIQUE CLUSTERED INDEX IX_eta
ON #eta (
            [PAT_ID]
		   ,[PAT_ENC_CSN_ID]
		   ,TEAM_AUDIT_ID
		   ,TEAM_AUDIT_INSTANT
);

--SELECT *
--FROM #eta
----ORDER BY PAT_ID, PAT_ENC_CSN_ID
----ORDER BY PAT_ID, PAT_ENC_CSN_ID, TEAM_AUDIT_ID
--ORDER BY PAT_ID, PAT_ENC_CSN_ID, TEAM_AUDIT_INSTANT, TEAM_AUDIT_ID
----ORDER BY PAT_ID, TEAM_AUDIT_ID, PAT_ENC_CSN_ID

	SELECT
		 adt.DEPARTMENT_ID
		,dep.DEPARTMENT_NAME
		,adt.ROOM_ID
		,rm.ROOM_NAME
		,adt.BED_ID
		,bed.BED_LABEL
		,adt.EFFECTIVE_TIME
		,CONVERT(DATE, adt.EFFECTIVE_TIME) AS EFFECTIVE_DT
		,adt.PAT_ID
		,adt.PAT_ENC_CSN_ID
		,adt.EVENT_ID
		,adt.EVENT_TYPE_C
		,adt.EVENT_SUBTYPE_C
		,MIN(CONVERT(DATE,adt.EFFECTIVE_TIME)) OVER (PARTITION BY adt.PAT_ENC_CSN_ID) AS MIN_EFFECTIVE_DT
		,MAX(CONVERT(DATE,adt.EFFECTIVE_TIME)) OVER (PARTITION BY adt.PAT_ENC_CSN_ID) AS MAX_EFFECTIVE_DT
		,zserv.NAME AS PAT_SERVICE_NAME
		----,CASE WHEN adt.DEPARTMENT_ID IN 
		----	(
		----		-- ACUTE WARDS (INCLUDE IN TRAUMA SURG MIDNIGHT CENSUS)
		----		'10243060', -- 5 WEST
		----		'10243058',-- 5 CENTRAL
		----		'10243063' -- 6 WEST
		----	)
		---- THEN 1 ELSE 0 END AS ACUTE_FLAG
		--,CASE WHEN adt.DEPARTMENT_ID IN 
		--	(
		--		-- excluding patients on each service who are in the STICU [5184 - 5198] or SIMU [5125 - 5136])
		--		'10243046', -- UVHE SURG TRAM ICU
		--		'10243090'  -- UVHE 5 NORTH
		--	)
		-- THEN 0 ELSE 1 END AS ACUTE_FLAG
	INTO #adt
	FROM dbo.CLARITY_ADT adt
	--INNER JOIN #pats pats
	--	ON adt.PAT_ID = pats.PAT_ID AND adt.PAT_ENC_CSN_ID = pats.PAT_ENC_CSN_ID
	INNER JOIN
	(
	SELECT DISTINCT
		PAT_ID,
        PAT_ENC_CSN_ID
	FROM #pats
	) pats
		ON adt.PAT_ID = pats.PAT_ID AND adt.PAT_ENC_CSN_ID = pats.PAT_ENC_CSN_ID
	LEFT OUTER JOIN CLARITY..CLARITY_DEP dep
		ON adt.DEPARTMENT_ID = dep.DEPARTMENT_ID
	LEFT OUTER JOIN
	(
	SELECT DISTINCT
		BED_ID
	   ,BED_LABEL
	FROM CLARITY..CLARITY_BED
	) bed
	ON adt.BED_ID = bed.BED_ID
	LEFT OUTER JOIN
	(
	SELECT DISTINCT
		ROOM_ID
	   ,ROOM_NAME
	FROM CLARITY..CLARITY_ROM
	) rm
	ON adt.ROOM_ID = rm.ROOM_ID
	LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_SERVICE AS zserv
		ON adt.PAT_SERVICE_C = zserv.HOSP_SERV_C
	WHERE adt.EFFECTIVE_TIME >= @start AND adt.EFFECTIVE_TIME <= @end
	AND adt.EVENT_TYPE_C = 6 -- census
	AND adt.EVENT_SUBTYPE_C IN (1,3) -- original, update
	AND adt.PAT_ENC_CSN_ID IS NOT NULL
	AND adt.DEPARTMENT_ID <> 10243026 -- UVHE EMERGENCY DEPT
    ORDER BY adt.PAT_ID, adt.PAT_ENC_CSN_ID, adt.EVENT_ID, adt.EFFECTIVE_TIME

CREATE UNIQUE CLUSTERED INDEX IX_adt
ON #adt (
            [PAT_ID]
		   ,[PAT_ENC_CSN_ID]
		   ,[EVENT_ID]
		   ,[EFFECTIVE_TIME]
);

--SELECT
--    PAT_ID,
--    PAT_ENC_CSN_ID,
--    EVENT_ID,
--    EFFECTIVE_TIME,
--	DEPARTMENT_ID,
--    DEPARTMENT_NAME,
--    ROOM_ID,
--    ROOM_NAME,
--    BED_ID,
--    BED_LABEL,
--    EFFECTIVE_DT,
--    EVENT_TYPE_C,
--    EVENT_SUBTYPE_C,
--    MIN_EFFECTIVE_DT,
--    MAX_EFFECTIVE_DT
--FROM #adt
--ORDER BY
--	PAT_ID,
--	PAT_ENC_CSN_ID,
--	EVENT_ID

	--SELECT 
 --          PAT_ENC_CSN_ID,
	--	   DEPARTMENT_ID,
 --          DEPARTMENT_NAME,
 --          ROOM_ID,
	--	   ROOM_NAME,
 --          BED_ID,
	--	   BED_LABEL,
	--	   EFFECTIVE_TIME,
 --          EFFECTIVE_DT,
 --          PAT_ID,
 --          EVENT_ID,
 --          EVENT_TYPE_C,
	--	   EVENT_SUBTYPE_C,
 --          MIN_EFFECTIVE_DT,
 --          MAX_EFFECTIVE_DT
	--FROM #adt
	----ORDER BY PAT_ENC_CSN_ID
	--ORDER BY PAT_ENC_CSN_ID, EVENT_ID

	--SELECT DISTINCT
	--	   DEPARTMENT_ID,
 --          DEPARTMENT_NAME,
	--	   ROOM_NAME
	--FROM #adt
	--ORDER BY DEPARTMENT_NAME, ROOM_NAME

	--SELECT
	--	adt.PAT_ID,
	--	adt.PAT_ENC_CSN_ID,
	--	adt.EFFECTIVE_TIME,
	--	adt.PAT_SERVICE_NAME,
	--	eta.TEAM_AUDIT_ID,
	--	eta.RECORD_NAME,
	--	eta.TEAM_AUDIT_INSTANT,
	--	eta.Pt_Class,
	--	eta.OR_SERVICE_NAME,
	--	ROW_NUMBER() OVER(PARTITION BY adt.PAT_ID, adt.PAT_ENC_CSN_ID, adt.EFFECTIVE_TIME ORDER BY eta.TEAM_AUDIT_INSTANT DESC) AS seq
	--FROM #adt adt
	--LEFT JOIN #eta eta
	--ON eta.PAT_ID = adt.PAT_ID
	--AND eta.PAT_ENC_CSN_ID = adt.PAT_ENC_CSN_ID
	--AND eta.TEAM_AUDIT_INSTANT <= adt.EFFECTIVE_TIME
	--ORDER BY
	--    adt.PAT_ID,
	--	adt.PAT_ENC_CSN_ID,
	--	adt.EFFECTIVE_TIME,
	--	seq

	SELECT
		adt.PAT_ID,
        adt.PAT_ENC_CSN_ID,
        adt.EFFECTIVE_TIME,
		adt.DEPARTMENT_ID,
		adt.DEPARTMENT_NAME,
		adt.PAT_SERVICE_NAME AS CENSUS_HOSPITAL_SERVICE,
        adt.TEAM_AUDIT_ID,
        adt.RECORD_NAME,
        adt.TEAM_AUDIT_INSTANT,
		adt.Pt_Class,
		adt.OR_SERVICE_NAME,
		ddte.day_date,
		ddte.day_of_week_num,
		ddte.day_of_week
	INTO #census
	FROM
	(
		SELECT
		adt.PAT_ID,
		adt.PAT_ENC_CSN_ID,
		adt.EFFECTIVE_TIME,
		adt.DEPARTMENT_ID,
		adt.DEPARTMENT_NAME,
		adt.PAT_SERVICE_NAME,
		eta.TEAM_AUDIT_ID,
		eta.RECORD_NAME,
		eta.TEAM_AUDIT_INSTANT,
		eta.Pt_Class,
		eta.OR_SERVICE_NAME,
		ROW_NUMBER() OVER(PARTITION BY adt.PAT_ID, adt.PAT_ENC_CSN_ID, adt.EFFECTIVE_TIME ORDER BY eta.TEAM_AUDIT_INSTANT DESC) AS seq
	FROM #adt adt
	LEFT JOIN #eta eta
	ON eta.PAT_ID = adt.PAT_ID
	AND eta.PAT_ENC_CSN_ID = adt.PAT_ENC_CSN_ID
	AND eta.TEAM_AUDIT_INSTANT <= adt.EFFECTIVE_TIME
	) adt
    LEFT OUTER JOIN CLARITY_App.Rptg.vwDim_Date ddte
    ON ddte.day_date = CAST(CAST(adt.EFFECTIVE_TIME AS DATE) AS SMALLDATETIME)
	WHERE adt.seq = 1
--/*
	SELECT
        RECORD_NAME AS PROVIDER_CARE_TEAM,
        --EFFECTIVE_TIME,
		CAST(EFFECTIVE_TIME AS DATE) AS CENSUS_DATE,
		PAT_ID,
        PAT_ENC_CSN_ID,
        --DEPARTMENT_ID,
        DEPARTMENT_NAME,
        CENSUS_HOSPITAL_SERVICE,
        --TEAM_AUDIT_ID,
        --TEAM_AUDIT_INSTANT,
        Pt_Class AS OR_Pt_Class,
        OR_SERVICE_NAME--,
        --day_date,
        --day_of_week_num,
        --day_of_week
	FROM #census
	WHERE RECORD_NAME IS NOT NULL
        AND TEAM_AUDIT_ID IN (
			162,	-- SICU
			186, -- SICU NON TRAUMA
			101, -- TRAUMA ICU
			138, -- TRAUMA ICU
			55, --   TRAUMA SURGERY
			72,	-- ORANGE SURGERY
			69,	-- GREEN SURGERY TEAM
			1,		-- BLUE TEAM
			73	-- EGS TEAM
			)
	--ORDER BY
	--    PAT_ID,
	--	PAT_ENC_CSN_ID,
	--	EFFECTIVE_TIME
	ORDER BY
	    RECORD_NAME,
		EFFECTIVE_TIME,
		PAT_ID,
		PAT_ENC_CSN_ID
--*/
	SELECT
		RECORD_NAME AS PROVIDER_CARE_TEAM,
		--CENSUS_HOSPITAL_SERVICE,
		--DEPARTMENT_NAME,
		CAST(EFFECTIVE_TIME AS DATE) AS CENSUS_DATE,
		COUNT(*) AS CENSUS
	INTO #summary
	FROM #census
	WHERE RECORD_NAME IS NOT NULL
        AND TEAM_AUDIT_ID IN (
			162,	-- SICU
			186, -- SICU NON TRAUMA
			101, -- TRAUMA ICU
			138, -- TRAUMA ICU
			55, --   TRAUMA SURGERY
			72,	-- ORANGE SURGERY
			69,	-- GREEN SURGERY TEAM
			1,		-- BLUE TEAM
			73	-- EGS TEAM
			)
	GROUP BY
		RECORD_NAME,
		--CENSUS_HOSPITAL_SERVICE,
		--DEPARTMENT_NAME,
		CAST(EFFECTIVE_TIME AS DATE)

	SELECT
		*
	FROM #summary
	ORDER  BY
		PROVIDER_CARE_TEAM,
		--CENSUS_HOSPITAL_SERVICE,
		--DEPARTMENT_NAME,
		CENSUS_DATE 
/*
	SELECT
		PROVIDER_CARE_TEAM,
		ddte.CALENDAR_DATE AS CENSUS_DATE,
		CASE WHEN CENSUS IS NULL THEN 0 ELSE CENSUS END AS CENSUS
	INTO #summary2
	FROM
	(
	SELECT
		CAST(day_date AS DATE) AS CALENDAR_DATE
	FROM CLARITY_App.Rptg.vwDim_Date
	WHERE day_date BETWEEN @start AND @end
	) ddte
	LEFT OUTER JOIN #summary summary
		ON summary.CENSUS_DATE = ddte.CALENDAR_DATE

	SELECT
		*
	FROM #summary2
	ORDER  BY
		PROVIDER_CARE_TEAM,
		CENSUS_DATE 
*/

  SELECT
		PROVIDER_CARE_TEAM,
		--CENSUS_HOSPITAL_SERVICE,
		--DEPARTMENT_NAME,
		--day_date AS CENSUS_DATE,
		--day_of_week AS DOW,
		--day_of_week_num AS DOW_NUM,
		MIN(CENSUS) AS MINIMUM_CENSUS,
		CAST(AVG(CAST(CENSUS AS NUMERIC(7,3))) AS NUMERIC(6,2)) AS AVERAGE_CENSUS,
		MAX(CENSUS) AS MAXIMUM_CENSUS,
		SUM(CENSUS) AS CENSUS_TOTAL,
		COUNT(*) AS CENSUS_MEASURES
  FROM #summary
  GROUP BY
		PROVIDER_CARE_TEAM--,
		--CENSUS_HOSPITAL_SERVICE,
		--DEPARTMENT_NAME
  ORDER BY
		PROVIDER_CARE_TEAM--,
		--CENSUS_HOSPITAL_SERVICE,
		--DEPARTMENT_NAME

DROP TABLE IF EXISTS #pats;
DROP TABLE IF EXISTS #AD_Events;
DROP TABLE IF EXISTS #eta;
DROP TABLE IF EXISTS #adt;
DROP TABLE IF EXISTS #census;
DROP TABLE IF EXISTS #summary;
--DROP TABLE IF EXISTS #summary2;

GO

	
