USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
       ,@enddate SMALLDATETIME

--SET @startdate = NULL
--SET @enddate = NULL
--SET @startdate = '7/1/2019 00:00 AM'
--SET @startdate = '1/1/2020 00:00 AM'
--SET @startdate = '7/1/2020 00:00 AM'
--SET @startdate = '1/1/2021 00:00 AM'
--SET @startdate = '7/1/2021 00:00 AM'
SET @startdate = '1/1/2022 00:00 AM'
--SET @enddate = '5/31/2022 11:59 PM'
--SET @enddate = '12/31/2019 11:59 PM'
--SET @enddate = '6/30/2020 11:59 PM'
--SET @enddate = '12/31/2020 11:59 PM'
--SET @enddate = '6/30/2021 11:59 PM'
--SET @enddate = '12/31/2021 11:59 PM'
SET @enddate = '6/30/2022 11:59 PM'

--ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Scheduled_Appointment_Metric]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Scheduled_Appointment_Metric
--WHO : Tom Burgan
--WHEN: 5/7/18
--WHY : Report scheduled appointment metrics from Cadence.
-- 
--	Metric Calculations
--
--		Note: "SUM" can be interpreted as "SUM(event_count) WHERE ...."
--
-- No Show Rate ("True")
--
--				SUM(appt_event_No_Show = 1)
--              /
--              (SUM(appt_event_Canceled = 0) + SUM(appt_event_Canceled_Late = 1) + SUM(appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45))
--
-- Bump Rate
--				SUM((Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)
--              /
--              SUM((Prov_Typ = 'Fellow' OR Prov_Typ = 'Nurse Practitioner' OR Prov_Typ = 'Physician' OR Prov_Typ = 'Physician Assistant') AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
--
-- Percentage of New Patient Visits
--				SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1) / SUM(appt_event_Completed = 1)
--
-- Average Lag Time to Appointment for New Patients in days
--				SUM(CASE WHEN (appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Days >= 0) THEN Appointment_Lag_Days ELSE 0 END)
--              /
--              SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Days >= 0)
--
-- Total Visits
--				SUM(appt_event_Completed = 1)
--
-- Average Visit Time
--				SUM(CASE WHEN (appt_event_Completed = 1 OR appt_event_Arrived = 1) THEN CYCLE_TIME_MINUTES_Adjusted ELSE 0 END)
--              /
--              SUM((appt_event_Completed = 1 OR appt_event_Arrived = 1) AND CYCLE_TIME_MINUTES_Adjusted >= 0)
--
-- Percentage of New Patients Seen Within 7 Business Days
--				SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1 AND Appointment_Lag_Business_Days <= 6)
--              /
--              SUM(appt_event_Completed = 1 AND appt_event_New_to_Specialty = 1)
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDW_Prod.Rptg.vwDim_Date
--              DS_HSDM_App.Stage.Scheduled_Appointment
--              DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
--              DS_HSDW_Prod.Rptg.vwDim_Patient
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
--              DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
--              DS_HSDW_Prod.Rptg.vwDim_Physcn
--              DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
--              DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart
--              DS_HSDM_App.Rptg.vwRef_Crosswalk_HSEntity_Prov
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Scheduled_Appointment_Metric]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         05/07/2018 - TMB - create stored procedure
--         06/12/2018 - TMB - include columns added to the Stage.Scheduled_Appointment table
--         06/25/2018 - TMB - include columns added to the Stage.Scheduled_Appointment table; use CANCEL_INITIATOR
--                            to identify provider-initiated cancellations
--         06/27/2018 - TMB - include columns added to the Stage.Scheduled_Appointment table
--         07/06/2018 - TMB - use sk_Dim_Pt, sk_Fact_Pt_Acct, sk_Fact_Pt_Enc_Clrt, IDENTITY_ID (MRN) values populated
--                            in the loading of the Stage table.
--         07/12/2018 - TMB - add TIME_TO_ROOM_MINUTES, TIME_IN_ROOM_MINUTES, BEGIN_CHECKIN_DTTM, PAGED_DTTM, and FIRST_ROOM_ASSIGN_DTTM
--                            to TabRptg table; add logic to exclude departments; change Appointment_Lag_Days calculation;
--                            update Metric Calculations documentation
--         07/18/2018 - TMB - add CANCEL_LEAD_HOURS to TabRptg table
--         08/09/2018 - TMB - use IDENTITY_ID from staging table to set person_id value
--         08/17/2018 - TMB - add APPT_CANC_DTTM, Entry_UVaID, Canc_UVaID to TabRptg table
--         09/20/2018 - TMB - add PHONE_REM_STAT_NAME to TabRptg table
--         11/08/2018 - TMB - add CHANGE_DATE, Cancel_Lead_Days calculation to TabRptg table; update logic for calculating Appointment_Lag_Days;
--                            update Metric Calculations documentation
--         03/28/2019 - TMB - add APPT_MADE_DTTM, BUSINESS_UNIT, Prov_Typ, Staff_Resource, and the new standard portal columns
--
--         03/28/2019 - BDD     ---cast various columns as proper data type for portal tables removed w_ from new column names to match other portal processes.
--         04/05/2019 - TMB - correct statement setting value of Clrt_Financial_Division_Name
--         05/07/2019 - TMB - add logic for updated/new views Rptg.vwRef_Crosswalk_HSEntity_Prov and Rptg.vwRef_SOM_Hierarchy
--         05/10/2019 - TMB - edit logic to resolve issue resulting from multiple primary, active wd jobs for a provider;
--                            add place-holder columns for w_som_hs_area_id (smallint) and w_som_hs_area_name (VARCHAR(150))
--         07/09/2019 - TMB - change logic for setting SOM hierarchy values; add APPT_SERIAL_NUM, RESCHED_APPT_CSN_ID
--         07/26/2019 - TMB - add columns Appointment_Request_Date and Appointment_Lag_Business_Days
--         07/29/2019 - TMB - add column BILL_PROV_YN
--         08/07/2019 - TMB - edit Appointment_Lag_Business_Days calculation: exclude holidays from business days classification;
--                            change documentation defining Bump Rate calculation
--         02/12/2020 - TMB - add logic to set value of Prov_Typ
--         02/18/2020 - TMB - add UPG_PRACTICE_... columns
--         03/26/2020 - TMB - add join to SOM Div/Subdiv view
--         05/13/2020 - TMB - remove test/invalid patients
--         07/20/2020 - TMB - add Lip_Flag, FINANCE_COST_CODE, and Prov_Based_Clinic to extract
--         07/21/2020 - TMB - remove erroneous encounter types; add Map_Type to extract
--         07/26/2020 - TMB - edit logic used to assign pod and hub to scheduled appointments
--         09/22/2021 - TMB - edit No Show Rate definition
--         09/29/2021 - TMB - add join to Supp Department Subloc view; add SUBLOC_... columns
--         11/15/2021 - TMB - add MYCHART_STATUS_C, MYCHART_STATUS_NAME,
--                                          PAT_SCHED_MYC_STAT_C, PAT_SCHED_MYC_STAT_NAME to extract
--		   03/04/2021 - ARD - add flag that can be used to determine whether an encounter should be included in the denominator of the Telehealth Encounters metric.
--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL BEGIN 
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;

   ---BDD 3/13/2020 start date pushed back 6 months per request from Brian C.
   ---BDD 4/08/2022 remove this push back per Sue C. 
 ---  SET @startdate = DATEADD(mm,-6,@startdate)

END
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#denominator ') IS NOT NULL
DROP TABLE #denominator

IF OBJECT_ID('tempdb..#x_denominator ') IS NOT NULL
DROP TABLE #x_denominator

IF OBJECT_ID('tempdb..#numerator ') IS NOT NULL
DROP TABLE #numerator

IF OBJECT_ID('tempdb..#x_numerator ') IS NOT NULL
DROP TABLE #x_numerator

SELECT CAST('Appointment' AS VARCHAR(50)) AS event_type,
       CASE
           WHEN evnts.APPT_STATUS_FLAG IS NOT NULL THEN
               1
           ELSE
               0
       END AS event_count,
       date_dim.day_date AS event_date,
       date_dim.fmonth_num,
       date_dim.Fyear_num,
       date_dim.FYear_name,
	   date_dim.month_num,
	   date_dim.year_num,
	   date_dim.Year_name,
       CAST(LEFT(DATENAME(MM, date_dim.day_date), 3) + ' ' + CAST(DAY(date_dim.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period,
       CAST(CAST(date_dim.day_date AS DATE) AS SMALLDATETIME) AS report_date,
       evnts.event_category,
       evnts.pod_id,
       evnts.pod_name,
       evnts.hub_id,
       evnts.hub_name,
       evnts.epic_department_id,
       evnts.epic_department_name,
       evnts.epic_department_name_external,
       evnts.peds,
       evnts.transplant,
       evnts.sk_Dim_Pt,
       evnts.sk_Fact_Pt_Acct,
       evnts.sk_Fact_Pt_Enc_Clrt,
       evnts.person_birth_date,
       evnts.person_gender,
       evnts.person_id,
       evnts.person_name,
       evnts.practice_group_id,
       evnts.practice_group_name,
       evnts.provider_id,
       evnts.provider_name,
       evnts.service_line_id,
       evnts.service_line,
       evnts.prov_service_line_id,
       evnts.prov_service_line,
       evnts.sub_service_line_id,
       evnts.sub_service_line,
       evnts.opnl_service_id,
       evnts.opnl_service_name,
       evnts.corp_service_line_id,
       evnts.corp_service_line,
       evnts.hs_area_id,
       evnts.hs_area_name,
       evnts.prov_hs_area_id,
       evnts.prov_hs_area_name,
       evnts.APPT_STATUS_FLAG,
       evnts.APPT_STATUS_C,
       evnts.CANCEL_REASON_C,
       evnts.MRN_int,
       evnts.CONTACT_DATE,
       evnts.APPT_DT,
       evnts.PAT_ENC_CSN_ID,
       evnts.PRC_ID,
       evnts.PRC_NAME,
       evnts.sk_Dim_Physcn,
       evnts.UVaID,

---BDD 5/9/2018 per Sue, change these from Y/N varchar(1) to 1/0 ints. Null = 0 per Tom and Sue
       CASE WHEN evnts.VIS_NEW_TO_SYS_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SYS_YN,
       CASE WHEN evnts.VIS_NEW_TO_DEP_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_DEP_YN,
       CASE WHEN evnts.VIS_NEW_TO_PROV_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_PROV_YN,
       CASE WHEN evnts.VIS_NEW_TO_SPEC_YN      = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SPEC_YN,
       CASE WHEN evnts.VIS_NEW_TO_SERV_AREA_YN = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_SERV_AREA_YN,
       CASE WHEN evnts.VIS_NEW_TO_LOC_YN       = 'Y' THEN CAST(1 AS INT) ELSE CAST(0 AS INT)  END AS VIS_NEW_TO_LOC_YN,

       evnts.APPT_MADE_DATE,
       evnts.ENTRY_DATE,
       evnts.CHECKIN_DTTM,
       evnts.CHECKOUT_DTTM,
       evnts.VISIT_END_DTTM,
       evnts.CYCLE_TIME_MINUTES,

--For tableau calc purposes null = 0 per Sue.
       ISNULL(evnts.appt_event_No_Show,CAST(0 AS INT)) AS appt_event_No_Show,
       ISNULL(evnts.appt_event_Canceled_Late,CAST(0 AS INT)) AS appt_event_Canceled_Late,
       ISNULL(evnts.appt_event_Canceled,CAST(0 AS INT)) AS appt_event_Canceled,
       ISNULL(evnts.appt_event_Scheduled,CAST(0 AS INT)) AS appt_event_Scheduled,
       ISNULL(evnts.appt_event_Provider_Canceled,CAST(0 AS INT)) AS appt_event_Provider_Canceled,
       ISNULL(evnts.appt_event_Completed,CAST(0 AS INT)) AS appt_event_Completed,
       ISNULL(evnts.appt_event_Arrived,CAST(0 AS INT)) AS appt_event_Arrived,
       ISNULL(evnts.appt_event_New_to_Specialty,CAST(0 AS INT)) AS appt_event_New_to_Specialty,
	   
       CASE
           WHEN (evnts.APPT_STATUS_FLAG IS NOT NULL) THEN DATEDIFF(dd, evnts.Appointment_Request_Date, evnts.APPT_DT)
           ELSE CAST(NULL AS INT)
       END AS Appointment_Lag_Days,
       evnts.CYCLE_TIME_MINUTES_Adjusted,

	   evnts.DEPT_SPECIALTY_NAME,
	   evnts.PROV_SPECIALTY_NAME,
	   evnts.APPT_DTTM,
	   evnts.ENC_TYPE_C,
	   evnts.ENC_TYPE_TITLE,
	   evnts.APPT_CONF_STAT_NAME,
	   evnts.ZIP,
	   evnts.APPT_CONF_DTTM,
	   evnts.SIGNIN_DTTM,
	   evnts.ARVL_LIST_REMOVE_DTTM,
	   evnts.ROOMED_DTTM,
	   evnts.NURSE_LEAVE_DTTM,
	   evnts.PHYS_ENTER_DTTM,
	   evnts.CANCEL_REASON_NAME,
	   evnts.SER_RPT_GRP_SIX AS financial_division,
	   evnts.SER_RPT_GRP_EIGHT AS financial_subdivision,
	   evnts.CANCEL_INITIATOR,
	   evnts.F2F_Flag,
	   evnts.TIME_TO_ROOM_MINUTES,
	   evnts.TIME_IN_ROOM_MINUTES,
	   evnts.BEGIN_CHECKIN_DTTM,
	   evnts.PAGED_DTTM,
	   evnts.FIRST_ROOM_ASSIGN_DTTM,
	   evnts.CANCEL_LEAD_HOURS,
	   evnts.APPT_CANC_DTTM,
	   evnts.Entry_UVaID,
	   evnts.Canc_UVaID,
	   evnts.PHONE_REM_STAT_NAME,
	   evnts.CHANGE_DATE,
	   evnts.Cancel_Lead_Days,

	   evnts.APPT_MADE_DTTM,
	   evnts.BUSINESS_UNIT,
	   CAST(evnts.Prov_Typ AS VARCHAR(66)) AS Prov_Typ,
	   evnts.Staff_Resource,

	   evnts.som_group_id,
	   evnts.som_group_name,
	   evnts.rev_location_id,
	   evnts.rev_location,
	   evnts.financial_division_id,
	   evnts.financial_division_name,
	   evnts.financial_sub_division_id,
	   evnts.financial_sub_division_name,
	   evnts.som_department_id,
	   evnts.som_department_name,
	   evnts.som_division_id,
	   evnts.som_division_name,
	   evnts.som_hs_area_id,
	   evnts.som_hs_area_name,
	   evnts.APPT_SERIAL_NUM,
	   evnts.RESCHED_APPT_CSN_ID,
	   evnts.Appointment_Request_Date,
       (SELECT COUNT(*) FROM DS_HSDW_Prod.Rptg.vwDim_Date ddte LEFT OUTER JOIN DS_HSDM_App.Rptg.Holiday_Dates hdte ON hdte.Holiday_Date = ddte.day_date WHERE weekday_ind = 1 AND hdte.Holiday_Date IS NULL AND day_date >= evnts.Appointment_Request_Date AND day_date < evnts.APPT_DT) Appointment_Lag_Business_Days,
	   evnts.BILL_PROV_YN,
	   evnts.upg_practice_flag,
	   evnts.upg_practice_region_id,
	   evnts.upg_practice_region_name,
	   evnts.upg_practice_id,
	   evnts.upg_practice_name,
	   evnts.Lip_Flag,
	   evnts.FINANCE_COST_CODE,
	   evnts.Prov_Based_Clinic,
	   evnts.Map_Type,
	   evnts.SUBLOC_ID,
	   evnts.SUBLOC_NAME,
	   evnts.MYCHART_STATUS_C,
	   evnts.MYCHART_STATUS_NAME,
	   evnts.PAT_SCHED_MYC_STAT_C,
	   evnts.PAT_SCHED_MYC_STAT_NAME,

---ARD 03/03/22, adding flag that can be used to determine whether an encounter should be included in the denominator of the Telehealth Encounters metric.
	   CASE WHEN evnts.PRC_ID LIKE '19___' 
				 OR evnts.ALLOWED_TELEHEALTH_MODES LIKE ' 1'
				 OR evnts.ALLOWED_TELEHEALTH_MODES IS NULL
			THEN 0 ELSE 1 END								AS 'Telehealth_Flag' -- INTEGER
-- 05/03/2022 -- ARD --Added Self Scheduling flag
		, CASE WHEN evnts.AUDIT_TASK IS NULL THEN 0 ELSE 1 END  AS 'Self_Sched' --INTEGER
		, evnts.AUDIT_TYPE
		
INTO #denominator

FROM
(
    SELECT day_date,
           fmonth_num,
           Fyear_num,
           FYear_name,
		   month_num,
		   year_num,
		   Year_name
    FROM DS_HSDW_Prod.Rptg.vwDim_Date

) date_dim
    LEFT OUTER JOIN
    (
        SELECT DISTINCT
            CAST(NULL AS VARCHAR(150)) AS event_category,
            CAST(main.pod_id AS VARCHAR(66)) AS pod_id,
            main.pod_name,
            CAST(main.hub_id AS VARCHAR(66)) AS hub_id,
            main.hub_name,
            main.epic_department_id,
            main.epic_department_name,
            main.epic_department_name_external,
            main.peds,
            main.transplant,
            main.sk_Dim_Pt,
            main.sk_Fact_Pt_Acct,
            main.sk_Fact_Pt_Enc_Clrt,
            main.person_birth_date,
            main.person_gender,
            main.person_id,
            main.person_name,
            main.practice_group_id,
            main.practice_group_name,
            main.provider_id,
            main.provider_name,
            main.service_line_id,
            main.service_line,
            main.prov_service_line_id,
            main.prov_service_line,
            main.sub_service_line_id,
            main.sub_service_line,
            main.opnl_service_id,
            main.opnl_service_name,
            main.corp_service_line_id,
            main.corp_service_line,
            main.hs_area_id,
            main.hs_area_name,
            main.prov_hs_area_id,
            main.prov_hs_area_name,
            main.APPT_STATUS_FLAG,
            main.APPT_STATUS_C,
			main.CANCEL_INITIATOR,
            main.CANCEL_REASON_C,
            main.MRN_int,
            main.CONTACT_DATE,
            main.APPT_DT,
            main.PAT_ENC_CSN_ID,
            main.PRC_ID,
            main.PRC_NAME,
            main.sk_Dim_Physcn,
            main.UVaID,
            main.VIS_NEW_TO_SYS_YN,
            main.VIS_NEW_TO_DEP_YN,
            main.VIS_NEW_TO_PROV_YN,
            main.VIS_NEW_TO_SPEC_YN,
            main.VIS_NEW_TO_SERV_AREA_YN,
            main.VIS_NEW_TO_LOC_YN,
            main.APPT_MADE_DATE,
            main.ENTRY_DATE,
            main.CHECKIN_DTTM,
            main.CHECKOUT_DTTM,
            main.VISIT_END_DTTM,
            main.CYCLE_TIME_MINUTES,
                                                 -- Appt Status Flags
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'No Show' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_No_Show,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled Late' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Canceled_Late,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Scheduled' ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Scheduled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C = 3)
                    AND (main.CANCEL_INITIATOR = 'PROVIDER')
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Provider_Canceled,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 2 ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Completed,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_C IN ( 6 ))
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_Arrived,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.VIS_NEW_TO_SPEC_YN = 'Y')
                ) THEN
                    1
                ELSE
                    CAST(NULL AS SMALLINT)
            END AS appt_event_New_to_Specialty,
                                                 -- Calculated columns
-- Assumes that there is always a referral creation date (CHANGE_DATE) documented when a referral entry date (ENTRY_DATE) is documented
			CASE
				WHEN main.ENTRY_DATE IS NULL THEN
					main.APPT_MADE_DATE
				WHEN main.ENTRY_DATE >= main.APPT_MADE_DATE AND main.CHANGE_DATE >= main.APPT_MADE_DATE THEN
					main.APPT_MADE_DATE
				WHEN main.ENTRY_DATE < main.CHANGE_DATE THEN
					main.ENTRY_DATE
				ELSE
					main.CHANGE_DATE
			END AS Appointment_Request_Date,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.CYCLE_TIME_MINUTES >= 960)
                ) THEN
                    960 -- Operations has defined 960 minutes (16 hours) as the ceiling for the calculation to use for any times longer than 16 hours
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.CYCLE_TIME_MINUTES < 960)
                ) THEN
                    main.CYCLE_TIME_MINUTES
                ELSE
                    CAST(NULL AS INT)
            END AS CYCLE_TIME_MINUTES_Adjusted,

			main.DEPT_SPECIALTY_NAME,
			main.PROV_SPECIALTY_NAME,
			main.APPT_DTTM,
		    main.ENC_TYPE_C,
			main.ENC_TYPE_TITLE,
			main.APPT_CONF_STAT_NAME,
			main.ZIP,
			main.APPT_CONF_DTTM,
			main.SIGNIN_DTTM,
			main.ARVL_LIST_REMOVE_DTTM,
			main.ROOMED_DTTM,
			main.NURSE_LEAVE_DTTM,
			main.PHYS_ENTER_DTTM,
			main.CANCEL_REASON_NAME,
			main.SER_RPT_GRP_SIX,
			main.SER_RPT_GRP_EIGHT,
			main.F2F_Flag,
		    main.TIME_TO_ROOM_MINUTES,
			main.TIME_IN_ROOM_MINUTES,
			main.BEGIN_CHECKIN_DTTM,
			main.PAGED_DTTM,
			main.FIRST_ROOM_ASSIGN_DTTM,
			main.CANCEL_LEAD_HOURS,
			main.APPT_CANC_DTTM,
			main.Entry_UVaID,
			main.Canc_UVaID,
			main.PHONE_REM_STAT_NAME,
			main.CHANGE_DATE,
            CASE
                WHEN
                (
                    (main.APPT_STATUS_FLAG IS NOT NULL)
                    AND (main.APPT_STATUS_FLAG IN ( 'Canceled','Canceled Late' ))
                ) THEN
                    DATEDIFF(DAY, CAST(APPT_CANC_DTTM AS DATE), APPT_DT)
                ELSE
                    CAST(NULL AS INT)
            END AS Cancel_Lead_Days,
			main.APPT_MADE_DTTM,
			main.BUSINESS_UNIT,
		    main.Prov_Typ,
			main.Staff_Resource,
		    main.som_group_id,
			main.som_group_name,
			main.rev_location_id,
			main.rev_location,
			main.financial_division_id,
			main.financial_division_name,
			main.financial_sub_division_id,
			main.financial_sub_division_name,
			main.som_department_id,
			main.som_department_name,
			main.som_division_id,
			main.som_division_name,
			main.som_hs_area_id,
			main.som_hs_area_name,
			main.APPT_SERIAL_NUM,
			main.RESCHED_APPT_CSN_ID,
			main.BILL_PROV_YN,
			main.upg_practice_flag,
			main.upg_practice_region_id,
			main.upg_practice_region_name,
			main.upg_practice_id,
			main.upg_practice_name,
			main.Lip_Flag,
			main.FINANCE_COST_CODE,
			main.Prov_Based_Clinic,
			main.Map_Type,
	        main.SUBLOC_ID,
	        main.SUBLOC_NAME,
			main.MYCHART_STATUS_C,
			main.MYCHART_STATUS_NAME,
			main.PAT_SCHED_MYC_STAT_C,
			main.PAT_SCHED_MYC_STAT_NAME,
--03/04/2021 -- ARD -- Added Allowable Telehealth Modes given the visit type.
			main.ALLOWED_TELEHEALTH_MODES,
-- 05/03/2022 -- ARD --Added Self Scheduling flag
			main.AUDIT_TASK,
			main.AUDIT_TYPE
        FROM
        ( --main
            SELECT appts.RPT_GRP_THIRTY AS epic_service_line,
                   mdmloc.SERVICE_LINE AS mdmloc_service_line,
				   mdmloc.pod_id,
				   mdmloc.pod_name,
				   mdmloc.hub_id,
				   mdmloc.hub_name,
                   appts.DEPARTMENT_ID AS epic_department_id,
                   mdm.epic_department_name AS epic_department_name,
                   mdm.epic_department_name_external AS epic_department_name_external,
                   CAST(CASE
                            WHEN FLOOR((CAST(appts.APPT_DT AS INTEGER)
                                        - CAST(CAST(pat.BirthDate AS DATETIME) AS INTEGER)
                                       ) / 365.25
                                      ) < 18 THEN
                                1
                            ELSE
                                0
                        END AS SMALLINT) AS peds,
                   CAST(CASE
                            WHEN tx.pat_enc_csn_id IS NOT NULL THEN
                                1
                            ELSE
                                0
                        END AS SMALLINT) AS transplant,
                   appts.sk_Dim_Pt,
                   appts.sk_Fact_Pt_Acct,
                   appts.sk_Fact_Pt_Enc_Clrt,
                   pat.BirthDate AS person_birth_date,
                   pat.Sex AS person_gender,
                   CAST(appts.IDENTITY_ID AS INT) AS person_id,
                   pat.Name AS person_name,
                   CAST(NULL AS INT) AS practice_group_id,
                   CAST(NULL AS VARCHAR(150)) AS practice_group_name,
                   appts.PROV_ID AS provider_id,
                   appts.PROV_NAME AS provider_name,
                   -- MDM
                   mdm.service_line_id,
                   mdm.service_line,
                   physsvc.Service_Line_ID AS prov_service_line_id,
                   physsvc.Service_Line AS prov_service_line,
                   mdm.sub_service_line_id,
                   mdm.sub_service_line,
                   mdm.opnl_service_id,
                   mdm.opnl_service_name,
                   mdm.corp_service_line_id,
                   mdm.corp_service_line,
                   mdm.hs_area_id,
                   mdm.hs_area_name,
                   physsvc.hs_area_id AS prov_hs_area_id,
                   physsvc.hs_area_name AS prov_hs_area_name,
                   --Select
                   appts.APPT_STATUS_FLAG,
                   appts.APPT_STATUS_C,
				   appts.CANCEL_INITIATOR,
                   appts.CANCEL_REASON_C,
				   CAST(appts.IDENTITY_ID AS INTEGER) AS MRN_int,
                   appts.CONTACT_DATE,
                   appts.APPT_DT,
                   appts.PAT_ENC_CSN_ID,
                   appts.PRC_ID,
                   appts.PRC_NAME,
                   ser.sk_Dim_Physcn,
                   doc.UVaID,
                   appts.VIS_NEW_TO_SYS_YN,
                   appts.VIS_NEW_TO_DEP_YN,
                   appts.VIS_NEW_TO_PROV_YN,
                   appts.VIS_NEW_TO_SPEC_YN,
                   appts.VIS_NEW_TO_SERV_AREA_YN,
                   appts.VIS_NEW_TO_LOC_YN,
                   appts.APPT_MADE_DATE,
                   appts.ENTRY_DATE,
                   appts.CHECKIN_DTTM,
                   appts.CHECKOUT_DTTM,
                   appts.VISIT_END_DTTM,
                   appts.CYCLE_TIME_MINUTES,
				   appts.DEPT_SPECIALTY_NAME,
				   appts.PROV_SPECIALTY_NAME,
				   appts.APPT_DTTM,
				   appts.ENC_TYPE_C,
				   appts.ENC_TYPE_TITLE,
				   appts.APPT_CONF_STAT_NAME,
				   appts.ZIP,
				   appts.APPT_CONF_DTTM,
				   appts.SIGNIN_DTTM,
				   appts.ARVL_LIST_REMOVE_DTTM,
				   appts.ROOMED_DTTM,
				   appts.NURSE_LEAVE_DTTM,
				   appts.PHYS_ENTER_DTTM,
				   appts.CANCEL_REASON_NAME,
				   appts.SER_RPT_GRP_SIX,
				   appts.SER_RPT_GRP_EIGHT,
				   appts.F2F_Flag,
				   appts.TIME_TO_ROOM_MINUTES,
				   appts.TIME_IN_ROOM_MINUTES,
				   appts.BEGIN_CHECKIN_DTTM,
				   appts.PAGED_DTTM,
				   appts.FIRST_ROOM_ASSIGN_DTTM,
				   appts.CANCEL_LEAD_HOURS,
				   appts.APPT_CANC_DTTM,
				   entryemp.EMPlye_Systm_Login AS Entry_UVaID,
				   cancemp.EMPlye_Systm_Login AS Canc_UVaID,
				   appts.PHONE_REM_STAT_NAME,
				   appts.CHANGE_DATE,
				   appts.APPT_MADE_DTTM,
				   mdmloc.BUSINESS_UNIT,
				   COALESCE(appts.PROV_TYPE_OT_NAME, ser.Prov_Typ, NULL) AS Prov_Typ,
				   ser.Staff_Resource,
				   mdmloc.LOC_ID AS rev_location_id,
				   mdmloc.REV_LOC_NAME AS rev_location,				   
                   -- SOM
				   TRY_CAST(ser.Financial_Division AS INT) AS financial_division_id,
				   CASE WHEN ser.Financial_Division_Name <> 'na' THEN CAST(ser.Financial_Division_Name AS VARCHAR(150)) ELSE NULL END AS financial_division_name,
				   TRY_CAST(ser.Financial_SubDivision AS INT) AS financial_sub_division_id,
				   CASE WHEN ser.Financial_SubDivision_Name <> 'na' THEN CAST(ser.Financial_SubDivision_Name AS VARCHAR(150)) ELSE NULL END AS financial_sub_division_name,
				   dvsn.som_group_id,
				   dvsn.som_group_name,
				   dvsn.Department_ID AS som_department_id,
				   CAST(dvsn.Department AS VARCHAR(150)) AS som_department_name,
				   CAST(dvsn.Org_Number AS INT) AS som_division_id,
				   CAST(dvsn.Organization AS VARCHAR(150)) AS som_division_name,
				   dvsn.som_hs_area_id,
				   dvsn.som_hs_area_name,
				   appts.APPT_SERIAL_NUM,
				   appts.RESCHED_APPT_CSN_ID,
				   appts.BILL_PROV_YN,
				   mdmloc.UPG_PRACTICE_FLAG AS upg_practice_flag,
				   CAST(mdmloc.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id,
				   CAST(mdmloc.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name,
				   CAST(mdmloc.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id,
				   CAST(mdmloc.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name,
				   CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag,
				   mdmloc.FINANCE_COST_CODE,
				   dep.Prov_Based_Clinic,
				   map.Map_Type,
				   supp.SUBLOC_ID,
				   supp.SUBLOC_NAME,
				   appts.MYCHART_STATUS_C,
				   appts.MYCHART_STATUS_NAME,
				   appts.PAT_SCHED_MYC_STAT_C,
				   appts.PAT_SCHED_MYC_STAT_NAME,
--03/04/2021 -- ARD -- Added Allowable Telehealth Modes given the visit type.
				   appts.ALLOWED_TELEHEALTH_MODES,
-- 05/03/2022 -- ARD --Added Self Scheduling flag
				   appts.AUDIT_TASK,
				   appts.AUDIT_TYPE
            FROM Stage.Scheduled_Appointment AS appts
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser
                    ON ser.PROV_ID = appts.PROV_ID
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
                    ON pat.sk_Dim_Pt = appts.sk_Dim_Pt
			    LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
				    ON dep.DEPARTMENT_ID = appts.DEPARTMENT_ID
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                    ON appts.DEPARTMENT_ID = mdm.epic_department_id
                LEFT OUTER JOIN
                (
                    SELECT DISTINCT
                        EPIC_DEPARTMENT_ID,
                        SERVICE_LINE,
						POD_ID AS pod_id,
                        PFA_POD AS pod_name,
						HUB_ID AS hub_id,
                        HUB AS hub_name,
						BUSINESS_UNIT,
						LOC_ID,
						REV_LOC_NAME,
						UPG_PRACTICE_FLAG,
						UPG_PRACTICE_REGION_ID,
						UPG_PRACTICE_REGION_NAME,
						UPG_PRACTICE_ID,
						UPG_PRACTICE_NAME,
						FINANCE_COST_CODE	
                    FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                ) AS mdmloc
                    ON appts.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID
                LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                    ON map.Deptid = CAST(mdmloc.FINANCE_COST_CODE AS INTEGER)
                LEFT JOIN
                (
                    SELECT sk_Dim_Physcn,
                           UVaID,
                           Service_Line,
						   ProviderGroup
                    FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
                    WHERE current_flag = 1
                ) AS doc
                    ON ser.sk_Dim_Physcn = doc.sk_Dim_Physcn
                LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Service_Line physsvc
                    ON physsvc.Physician_Roster_Name = CASE
                                                           WHEN (ser.sk_Dim_Physcn > 0) THEN
                                                               doc.Service_Line
                                                           ELSE
                                                               'No Value Specified'
                                                       END
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye entryemp
				    ON entryemp.EMPlye_Usr_ID = appts.APPT_ENTRY_USER_ID
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye cancemp
				    ON cancemp.EMPlye_Usr_ID = appts.APPT_CANC_USER_ID

                -- -------------------------------------
                -- Identify transplant encounter--
                -- -------------------------------------
                LEFT OUTER JOIN
                (
                    SELECT DISTINCT
                        btd.pat_enc_csn_id,
                        btd.Event_Transplanted AS transplant_surgery_dt,
                        btd.hosp_admsn_time AS Adm_Dtm
                    FROM DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart AS btd
                    WHERE (
                              btd.TX_Episode_Phase = 'transplanted'
                              AND btd.TX_Stat_Dt >= @locstartdate 
							  AND btd.TX_Stat_Dt <  @locenddate
                          )
                          AND btd.TX_GroupedPhaseStatus = 'TX-ADMIT'
                ) AS tx
                    ON appts.PAT_ENC_CSN_ID = tx.pat_enc_csn_id

                -- -------------------------------------
                -- Excluded departments--
                -- -------------------------------------
                LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
				    ON excl.DEPARTMENT_ID = appts.DEPARTMENT_ID

                -- -------------------------------------
                -- SOM Financial Division Subdivision--
                -- -------------------------------------
				LEFT OUTER JOIN
				(
				    SELECT
					    Epic_Financial_Division_Code,
                        Epic_Financial_Subdivision_Code,
                        Department,
                        Department_ID,
                        Organization,
                        Org_Number,
                        som_group_id,
                        som_group_name,
						som_hs_area_id,
						som_hs_area_name
					FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
				    ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(ser.Financial_Division AS INT)
					    AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(ser.Financial_SubDivision AS INT))

                -- --------------------------------------
                -- Supplemental Dept Subloc --
                -- --------------------------------------
				LEFT OUTER JOIN
				(
				    SELECT
					    Department_Name,
                        Department_ID,
                        SUBLOC_ID,
						SUBLOC_NAME
					FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Supplemental_Dept_Subloc) supp
                    ON (supp.Department_ID = appts.DEPARTMENT_ID)

            WHERE (appts.APPT_DT >= @locstartdate
              AND appts.APPT_DT < @locenddate)
			--AND excl.DEPARTMENT_ID IS NULL
		AND ((excl.DEPARTMENT_ID IS NULL) OR (appts.DEPARTMENT_ID IN (10242001,10243126))) -- 10242001	UVPC TELEMEDICINE, 10243126	UVHE URGENT VIDEO CL
			AND pat.IS_VALID_PAT_YN = 'Y'
			AND appts.ENC_TYPE_C NOT IN ('2505','2506')

        ) AS main
    ) evnts
        ON (date_dim.day_date = CAST(evnts.APPT_DT AS SMALLDATETIME))

WHERE date_dim.day_date >= @locstartdate
      AND date_dim.day_date < @locenddate

--ORDER BY date_dim.day_date;

  -- Create index for temp table #denominator

  CREATE UNIQUE CLUSTERED INDEX IX_denominator ON #denominator (month_num, year_num, Year_name, event_date, PAT_ENC_CSN_ID)

SELECT denom.month_num,
	denom.year_num,
	denom.Year_name,
	SUM(denom.x_denominator) AS x_denominator

INTO #x_denominator

FROM
(
SELECT t.*, CASE WHEN Telehealth_Flag = 1 THEN event_count ELSE 0 END AS x_denominator
      ,o.[organization_id] AS w_organization_id
      ,o.[organization_name] AS w_organization_name 
      ,s.[service_id] AS w_service_id
      ,s.[service_name] AS w_service_name
      ,c.[clinical_area_id] AS w_clinical_area_id
      ,c.[clinical_area_name] AS w_clinical_area_name
      ,g.[ambulatory_flag] AS w_ambulatory_flag
      ,g.[upg_practice_flag] AS w_upg_practice_flag
      ,g.[childrens_flag] AS w_childrens_flag
      ,g.[serviceline_division_flag] AS w_serviceline_division_flag
      ,g.[mc_operation_flag] AS w_mc_operation_flag
      ,g.[inpatient_adult_flag] AS w_inpatient_adult_flag
      ,g.[childrens_ambulatory_id] AS w_childrens_ambulatory_id
      ,g.[childrens_ambulatory_name] AS w_childrens_ambulatory_name
      ,g.[mc_ambulatory_id] AS w_mc_ambulatory_id
      ,g.[mc_ambulatory_name] AS w_mc_ambulatory_name
      ,g.[ambulatory_operation_id] AS w_ambulatory_operation_id
      ,g.[ambulatory_operation_name] AS w_ambulatory_operation_name
      ,g.[childrens_id] AS w_childrens_id
      ,g.[childrens_name] AS w_childrens_name
      ,g.[serviceline_division_id] AS w_serviceline_division_id
      ,g.[serviceline_division_name] AS w_serviceline_division_name
      ,g.[mc_operation_id] AS w_mc_operation_id
      ,g.[mc_operation_name] AS w_mc_operation_name
      ,g.[inpatient_adult_id] AS w_inpatient_adult_id
      ,g.[inpatient_adult_name] AS w_inpatient_adult_name
      ,g.[upg_practice_region_id] AS w_upg_practice_region_id
      ,g.[upg_practice_region_name] AS w_upg_practice_region_name
      ,g.[upg_practice_id] AS w_upg_practice_id
      ,g.[upg_practice_name] AS w_upg_practice_name
FROM #denominator t
    LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON t.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id
WHERE 
    (appt_event_Completed = 1 OR appt_event_Arrived = 1)
	--AND g.ambulatory_flag = 1
	AND ((g.ambulatory_flag = 1) OR (t.epic_department_id IN (10242001,10243126))) -- 10242001	UVPC TELEMEDICINE, 10243126	UVHE URGENT VIDEO CL
) denom

GROUP BY denom.month_num,
	denom.year_num,
	denom.Year_name

SELECT *
FROM #x_denominator denom

--ORDER BY denom.month_num,
--	denom.year_num,
--	denom.Year_name

ORDER BY denom.year_num,
	denom.month_num

SELECT evnts.*, date_dim.fmonth_num, date_dim.Fyear_num, date_dim.FYear_name, date_dim.month_num, date_dim.year_num, date_dim.Year_name,
	1 AS x_is_telemed
   --,evnts.[event_count] AS x_numerator
		
INTO #numerator

FROM
(
    SELECT day_date,
           fmonth_num,
           Fyear_num,
           FYear_name,
		   month_num,
		   year_num,
		   Year_name
    FROM DS_HSDW_Prod.Rptg.vwDim_Date

) date_dim
    LEFT OUTER JOIN
    (

SELECT
      [event_category]
      ,[event_type]
      ,t.[epic_department_id]
      ,t.[epic_department_name]
      ,t.[Load_Dtm]
      ,[event_count]
      ,[event_date]
      --,[fmonth_num]
      --,[fyear_num]
      --,[fyear_name]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[provider_id]
      ,[provider_name]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,[w_pod_id]
      ,[w_pod_name]
      ,[w_hub_id]
      ,[w_hub_name]
      ,[w_som_group_id]
      ,[w_som_group_name]
      ,[w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
      ,[w_rev_location_id]
      ,[w_rev_location]
      ,[Encounter_CSN] AS PAT_ENC_CSN_ID
      ,[Visit_Type] AS PRC_NAME
      ,[Encounter_Type] AS ENC_TYPE_TITLE
      ,[Encounter_Status] AS APPT_STATUS_FLAG
      , Communication_Type
      ,[Smartphrase_Name]
      ,[Smartdata_Element]
      ,[Telehealth_Mode_Name]
      ,1 AS [Telehealth_Flag]
      ,o.[organization_id] AS w_organization_id
      ,o.[organization_name] AS w_organization_name 
      ,s.[service_id] AS w_service_id
      ,s.[service_name] AS w_service_name
      ,c.[clinical_area_id] AS w_clinical_area_id
      ,c.[clinical_area_name] AS w_clinical_area_name
      ,g.[ambulatory_flag] AS w_ambulatory_flag
      ,g.[upg_practice_flag] AS w_upg_practice_flag
      ,g.[childrens_flag] AS w_childrens_flag
      ,g.[serviceline_division_flag] AS w_serviceline_division_flag
      ,g.[mc_operation_flag] AS w_mc_operation_flag
      ,g.[inpatient_adult_flag] AS w_inpatient_adult_flag
      ,g.[childrens_ambulatory_id] AS w_childrens_ambulatory_id
      ,g.[childrens_ambulatory_name] AS w_childrens_ambulatory_name
      ,g.[mc_ambulatory_id] AS w_mc_ambulatory_id
      ,g.[mc_ambulatory_name] AS w_mc_ambulatory_name
      ,g.[ambulatory_operation_id] AS w_ambulatory_operation_id
      ,g.[ambulatory_operation_name] AS w_ambulatory_operation_name
      ,g.[childrens_id] AS w_childrens_id
      ,g.[childrens_name] AS w_childrens_name
      ,g.[serviceline_division_id] AS w_serviceline_division_id
      ,g.[serviceline_division_name] AS w_serviceline_division_name
      ,g.[mc_operation_id] AS w_mc_operation_id
      ,g.[mc_operation_name] AS w_mc_operation_name
      ,g.[inpatient_adult_id] AS w_inpatient_adult_id
      ,g.[inpatient_adult_name] AS w_inpatient_adult_name
      ,g.[upg_practice_region_id] AS w_upg_practice_region_id
      ,g.[upg_practice_region_name] AS w_upg_practice_region_name
      ,g.[upg_practice_id] AS w_upg_practice_id
      ,g.[upg_practice_name] AS w_upg_practice_name
FROM [DS_HSDM_App].[TabRptg].[Dash_Telemedicine_Encounters_Tiles] t
    LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON t.epic_department_id = g.epic_department_id
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
    LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id
WHERE Encounter_Status = 'Complete'
AND g.ambulatory_flag = 1
    ) evnts
        ON (date_dim.day_date = CAST(evnts.event_date AS SMALLDATETIME))
WHERE event_date >= @locstartdate AND event_date < @locenddate

  -- Create index for temp table #numerator

  CREATE UNIQUE CLUSTERED INDEX IX_numerator ON #numerator (month_num, year_num, Year_name, event_date, PAT_ENC_CSN_ID)

SELECT num.month_num,
	num.year_num,
	num.Year_name,
	SUM(num.x_numerator) AS x_numerator

INTO #x_numerator

FROM
(
SELECT t.*, event_count AS x_numerator
FROM #numerator t
) num

GROUP BY num.month_num,
	num.year_num,
	num.Year_name

SELECT *
FROM #x_numerator num

--ORDER BY denom.month_num,
--	denom.year_num,
--	denom.Year_name

ORDER BY num.year_num,
	num.month_num

GO


