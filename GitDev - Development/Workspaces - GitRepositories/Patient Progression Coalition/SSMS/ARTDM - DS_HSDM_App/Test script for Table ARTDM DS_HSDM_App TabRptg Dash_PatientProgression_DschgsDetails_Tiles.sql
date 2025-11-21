USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL, 
        @enddate SMALLDATETIME = NULL

SET @startdate = '7/1/2022 00:00 AM'
SET @enddate = '10/31/2023 11:59 PM'

--/**********************************************************************************************************************
--WHAT: Check script for TabRptg table Dash_PatientProgression_DschgsDetails_Tiles
--WHO : Tom Burgan
--WHEN: 11/10/23
--WHY : Patient Progression Coalition Dashboard metrics
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:
--				DS_HSDM_App.TabRptg.Dash_PatientProgression_DschgsDetails_Tiles
--                
--      OUTPUTS:
-- 
--------------------------------------------------------------------------------------------------------------------------
--MODS:
--	11/10/2023--          --Tom Burgan - Create script
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
DECLARE @locstartdate DATETIME,
        @locenddate DATETIME
SET @locstartdate = CAST(@startdate AS DATETIME)
SET @locenddate   = CAST(@enddate AS DATETIME)
-------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#dschg ') IS NOT NULL
DROP TABLE #dschg

--Discharges-------------------------------------------------
SELECT
	tabrptg.sk_Dash_PatientProgression_DschgsDetails_Tiles,
    tabrptg.event_type,
    tabrptg.event_count,
    tabrptg.event_date,
    tabrptg.event_id,
    tabrptg.event_category,
    tabrptg.epic_department_id,
    tabrptg.epic_department_name,
    tabrptg.epic_department_name_external,
    tabrptg.fmonth_num,
    tabrptg.fyear_num,
    tabrptg.fyear_name,
    tabrptg.peds,
    tabrptg.transplant,
    tabrptg.oncology,
    tabrptg.App_Flag,
    tabrptg.sk_Dim_Pt,
    tabrptg.sk_Fact_Pt_Acct,
    tabrptg.sk_Fact_Pt_Enc_Clrt,
    tabrptg.sk_dim_physcn,
    tabrptg.person_birth_date,
    tabrptg.person_gender,
    tabrptg.person_id,
    tabrptg.person_name,
    tabrptg.provider_id,
    tabrptg.provider_name,
    tabrptg.prov_typ,
    tabrptg.hs_area_id,
    tabrptg.hs_area_name,
    tabrptg.pod_id,
    tabrptg.pod_name,
    tabrptg.rev_location_id,
    tabrptg.rev_location,
    tabrptg.som_group_id,
    tabrptg.som_group_name,
    tabrptg.som_department_id,
    tabrptg.som_department_name,
    tabrptg.som_division_id,
    tabrptg.som_division_name,
    tabrptg.financial_division_id,
    tabrptg.financial_division_name,
    tabrptg.financial_sub_division_id,
    tabrptg.financial_sub_division_name,
    tabrptg.w_hs_area_id,
    tabrptg.w_hs_area_name,
    tabrptg.w_pod_id,
    tabrptg.w_pod_name,
    tabrptg.w_rev_location_id,
    tabrptg.w_rev_location,
    tabrptg.w_som_group_id,
    tabrptg.w_som_group_name,
    tabrptg.w_som_department_id,
    tabrptg.w_som_department_name,
    tabrptg.w_som_division_id,
    tabrptg.w_som_division_name,
    tabrptg.w_financial_division_id,
    tabrptg.w_financial_division_name,
    tabrptg.w_financial_sub_division_id,
    tabrptg.w_financial_sub_division_name,
    tabrptg.DISCH_DISP_NAME,
    tabrptg.ED_DISPOSITION_NAME,
    tabrptg.hub_id,
    tabrptg.hub_name,
    tabrptg.practice_group_id,
    tabrptg.practice_group_name,
    tabrptg.prov_service_line_id,
    tabrptg.prov_service_line,
    tabrptg.prov_hs_area_id,
    tabrptg.prov_hs_area_name,
    tabrptg.Hospital_Code,
    tabrptg.PROVIDER_TYPE_C,
    tabrptg.PROV_TYPE,
    tabrptg.som_hs_area_id,
    tabrptg.som_hs_area_name,
    tabrptg.upg_practice_flag,
    tabrptg.upg_practice_region_id,
    tabrptg.upg_practice_region_name,
    tabrptg.upg_practice_id,
    tabrptg.upg_practice_name,
    tabrptg.AcctNbr_int,
    tabrptg.ADT_PAT_CLASS_C,
    tabrptg.ADT_PAT_CLASS_NAME,
    tabrptg.ED_Visit,
    tabrptg.IP_ADMIT_DATE_TIME,
    tabrptg.ADM_DATE_TIME,
    tabrptg.ADM_DTTM,
    tabrptg.DISCH_DATE_TIME,
    tabrptg.Acct_Base_Class,
    tabrptg.Length_of_Stay_Days,
    tabrptg.Length_of_Stay_Minutes,
    tabrptg.Length_of_Stay_Hours,
    tabrptg.Age_at_Service,
    tabrptg.pat_id,
    tabrptg.NormalNewborn,
    tabrptg.Hospice,
    tabrptg.PAT_ENC_CSN_ID,
    tabrptg.Load_Dtm
INTO #dschg
FROM TabRptg.Dash_PatientProgression_DschgsDetails_Tiles tabrptg

SELECT
	*
FROM #dschg dschg
	
--ORDER BY UOS_DTTM	
--ORDER BY dschg.AcctNbr_int, dschg.PRIM_ENC_CSN_ID, dschg.PROC_CODE, dschg.PROC_NAME
--ORDER BY dschg.AcctNbr_int, dschg.PRIM_ENC_CSN_ID, encseq
ORDER BY dschg.event_date, dschg.event_id

SELECT
	SUM(CASE WHEN dschg.ADT_PAT_CLASS_C = 104 THEN dschg.event_count ELSE 0 END) AS Observation_Count, -- Numerator
	SUM(dschg.event_count) AS Inpatient_Observation_Count -- Denominator
--FROM #dschg dschg
FROM TabRptg.Dash_PatientProgression_DschgsDetails_Tiles dschg
WHERE dschg.ADT_PAT_CLASS_C IN ('101','104') -- Inpatient, Observation
AND dschg.NormalNewborn = 0
AND dschg.Hospice = 0

SELECT
	SUM(CASE WHEN dschg.[LENGTH_OF_STAY_HOURS] > 48.0 THEN dschg.event_count ELSE 0 END) AS [Observation_LOS_>48_Count], -- Numerator
	SUM(dschg.event_count) AS Observation_Count -- Denominator
--FROM #dschg dschg
FROM TabRptg.Dash_PatientProgression_DschgsDetails_Tiles dschg
WHERE dschg.ADT_PAT_CLASS_C = '104' --  Observation
AND dschg.NormalNewborn = 0
AND dschg.Hospice = 0

SELECT
	SUM(dschg.[LENGTH_OF_STAY_HOURS]) AS [Observation_LOS_Hrs], -- Total LOS hours
	SUM(dschg.event_count) AS Observation_Count,
	CAST(AVG(CASE WHEN dschg.event_count =1 THEN CAST(dschg.[LENGTH_OF_STAY_HOURS] AS NUMERIC(10,2)) ELSE 0.00 END) AS NUMERIC(10,2)) AS Avg_LOS_Hrs_Aggr,
	CAST(
		CAST(SUM(dschg.[LENGTH_OF_STAY_HOURS]) AS NUMERIC(10,2)) /
		CAST(SUM(dschg.event_count) AS NUMERIC(10,2))
	AS NUMERIC(10,2)) AS Avg_LOS_Hrs_Calc
--FROM #dschg dschg
FROM TabRptg.Dash_PatientProgression_DschgsDetails_Tiles dschg
WHERE dschg.ADT_PAT_CLASS_C = '104' --  Observation
AND dschg.NormalNewborn = 0
AND dschg.Hospice = 0

GO
