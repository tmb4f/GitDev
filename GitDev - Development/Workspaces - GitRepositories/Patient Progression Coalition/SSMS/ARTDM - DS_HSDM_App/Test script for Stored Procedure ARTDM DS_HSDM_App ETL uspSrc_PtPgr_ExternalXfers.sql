USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--EXEC [ETL].[uspSrc_PtPgr_ExternalXfers]

--CREATE PROCEDURE [ETL].[uspSrc_PtPgr_ExternalXfers]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
	
--    )
--AS 

DECLARE @startdate DATETIME, @enddate DATETIME

SET @startdate = '2/1/2024'
SET @enddate = '4/30/2024'

--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_PtPgr_ExternalXfers
--WHO : Tom Burgan
--WHEN: 01/09/2024
--WHY : for Patient Progression Coalition metric
--			External Transfer Requests Acceptance Rate  
--			External Transfer Requests received by UVa Health - University Medical Center
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	
--              
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         01/09/2024 -TMB - create stored procedure

--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
		
SET @locstartdate = @startdate
SET @locenddate   = @enddate

-------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#trnsf ') IS NOT NULL
DROP TABLE #trnsf

IF OBJECT_ID('tempdb..#tabrptg ') IS NOT NULL
DROP TABLE #tabrptg

SELECT 
	  CAST('Incoming Transfer Request' AS VARCHAR(50))	AS event_type
	, CAST('Intake Request Completed' AS VARCHAR(150))	AS event_category
	, CASE WHEN xt.Disposition = 'Completed' THEN 1 ELSE 0 END AS event_count
	, dd.day_date AS event_date
	, dd.fmonth_num
	, dd.FYear_num
	, dd.FYear_name
	, CAST(LEFT(DATENAME(MM, dd.day_date), 3) + ' ' + CAST(DAY(dd.day_date) AS VARCHAR(2)) AS VARCHAR(10))			AS report_period
	, CAST(CAST(dd.day_date AS DATE) AS SMALLDATETIME)																AS report_date
	, [TransferID] AS event_id
	, xt.DestinationUnitID AS epic_department_id
	, mdm.epic_department_name AS epic_department_name
	, mdm.epic_department_name_external AS epic_department_external
	, CAST(CASE WHEN FLOOR((CAST(dd.day_date AS INTEGER) 
							- CAST(xt.PatientDOB AS INTEGER)
							) / 365.25
							) < 18 THEN
					1
				ELSE
					0
				END AS SMALLINT)																					AS peds
	, CAST(xt.PatientDOB AS DATE) AS person_birth_date
	, CAST(xt.PatientMR AS INT) AS person_id
    --, pat.MRN_int AS person_id_2
	--, UPPER(TRIM(pat.LastName) + ',' + TRIM(pat.FirstName) + ' ' + TRIM(CASE WHEN pat.MiddleName = 'Unknown' THEN '' ELSE pat.MiddleName END))	AS person_name
	, pat.Name AS person_name
	, xt.AcceptingMD_ID AS provider_id
	, xt.Accepting_MD AS provider_name
      ,[AdmissionCSN] AS PAT_ENC_CSN_ID
      ,[EntryTime]
      ,[AcctNbrint]
      ,[TierLevel]
      ,[Isolation]
      ,[referringProviderName]
      ,[Referring_Facility]
      ,[TransferReason]
      ,[TransferMode]
      ,[Diagnosis]
      ,[ServiceNme]
      ,[LevelOfCare]
	  ,xt.TransferTypeHx
      ,[PlacementStatusName]
      ,[XTPlacementStatusName]
      ,[XTPlacementStatusDateTime]
      ,[ETA]
      ,[PatientReferredTo]
      ,[AdtPatientFacilityID]
      ,[AdtPatientFacility]
      ,[BedAssigned]
      ,[BedType]
      ,[DispositionReason]
      ,[Disposition] AS Transfer_Center_Request_Status
      ,[Accepting_Timestamp]
      ,[Accepting_MD]
      ,[AcceptingMD_ServiceLine]
      ,[CloseTime]
      ,[PatientType]
      ,[ProtocolNme]
      ,xt.[Load_Dtm]
	  ,o.organization_name
	  ,s.service_name
	  ,c.clinical_area_name
	  ,pat.sk_Dim_Pt
	  ,pat.Sex AS person_gender
	  ,mdm.hs_area_id
	  ,mdm.hs_area_name
	  ,mdm.LOC_ID AS rev_location_id
	  ,mdm.REV_LOC_NAME AS rev_location

  INTO #trnsf

  FROM [DS_HSDM_Prod].[Rptg].[ADT_TransferCenter_ExternalTransfers] xt
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date dd ON dd.day_date = CAST(xt.EntryTime AS DATE)
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient pat ON pat.MRN_display = xt.PatientMR
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc mdm ON	xt.DestinationUnitID = mdm.epic_department_id
  LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON xt.DestinationUnitID = g.epic_department_id
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id

  WHERE 1=1
    AND TransferTypeID = 1 -- REQUEST_TYPE_MAPPING_C = 1,	REQUEST_TYPE_MAPPING_NAME = Transfer, 	REQUEST_TYPE_C = 2026,	REQUEST_TYPE_NAME = Incoming Transfer
	AND xt.TransferTypeHx = 'Incoming Transfer'

	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate

/*
SELECT DISTINCT
 --flags	
		
		CAST('Reverse Xfers' AS VARCHAR(50)) AS event_type
		,CASE WHEN Disposition LIKE 'Reverse%'
			THEN '1'
			ELSE '0'
			END											'Txf_Rqst'
		,CASE WHEN Disposition ='Reverse Xfer- Accepted'
			THEN '1'
			ELSE '0'
			END											'Txf_Cmplted'

		



--patient info
                    ,'NULL'													'PAT_ENC_CSN_ID' 
                    ,'NULL'													'PAT_id'
                    ,CAST(CONCAT(xfr.PatientLastName,',',xfr.PatientFirstName)AS VARCHAR(200))	'person_name'
                    ,CAST(xfr.PatientMR	AS INT)								'person_id'			--MRN
                    ,CAST(xfr.PatientDOB AS DATETIME)						'person_birth_date'
					,CAST(NULL AS VARCHAR(254))								'person_gender' 
					,CAST(NULL AS INT)										'sk_Dim_Pt'
					,CAST(NULL AS SMALLINT)									'peds' 
					,CAST(NULL AS SMALLINT)									'transplant'           


--dates/times
					,CAST(LEFT(DATENAME(MM, dmdt.day_date), 3) + ' ' + CAST(DAY(dmdt.day_date) AS VARCHAR(2)) AS VARCHAR(10)) 'report_period'
					,CAST(CAST(dmdt.day_date AS DATE) AS SMALLDATETIME)		'report_date'
					,CAST(xfr.EntryTime AS DATETIME)						'event_date'
					,dmdt.fmonth_num
					,dmdt.fmonth_name
					,dmdt.Fyear_num
					,dmdt.FYear_name
					,CAST(NULL AS VARCHAR(150))								'event_category'

--Provider/scheduler info
                
                    ,CAST(NULL AS INT)										'provider_id'
                    ,CAST(NULL AS VARCHAR(150))								'provider_Name'
                    ,CAST(NULL AS VARCHAR(150))								'prov_serviceline'  --service line
					,CAST(NULL AS INT)										'practice_group_id'
					,CAST(NULL AS VARCHAR(150))								'practice_group_name'

 --fac-org info
					,CAST(NULL AS NUMERIC(18,0))							'epic_department_id' 
                    ,CAST(NULL AS VARCHAR(254))								'epic_department_name' 
                    ,CAST(NULL AS VARCHAR(254))								'epic_department_name_external' 
                    ,CAST(NULL AS INT)										'pod_id'
					,CAST(NULL AS VARCHAR(100))								'pod_name'                -- pod
                    ,CAST(NULL AS INT)										'hub_id' -- hub
					,CAST(NULL AS VARCHAR(100))								'hub_name'
                   	,CAST(NULL AS INT)										'service_line_id'
					,CAST(NULL AS VARCHAR(100))								'service_line'
					,CAST(NULL AS INT)										'sub_service_line_id'
					,CAST(NULL AS VARCHAR(100))								'sub_service_line'
					,CAST(NULL AS INT)										'opnl_service_id'
					,CAST(NULL AS VARCHAR(100))								'opnl_service_name'
					,CAST(NULL AS INT)										'corp_service_line_id'
					,CAST(NULL AS VARCHAR(100))								'corp_service_line_name'
					,CAST(NULL AS INT)										'hs_area_id'
					,CAST(NULL AS VARCHAR(100))								'hs_area_name'

					

FROM DS_HSDW_Prod.dbo.Dim_Date dmdt
		LEFT OUTER JOIN  (SELECT   TransferID
									,EntryTime
									,Disposition
									,Status
									,PatientMR
									,PatientFirstName
									,PatientLastName
									,PatientDOB
							FROM [DS_HSDM_Prod].[Rptg].[VwTeletrack_External_Transfers]
							WHERE 1=1
							AND EntryTime>=@locstartdate
							AND EntryTime<=@locenddate
							AND Disposition LIKE 'reverse%'
							AND Status='Active'
						 ) xfr														ON CAST(dmdt.day_date AS DATE)=CAST(xfr.EntryTime AS DATE)
	
	


WHERE 1=1
		
		AND dmdt.day_date>=@locstartdate
		AND dmdt.day_date<=@locenddate
*/

SELECT
    event_count,
    event_date,
    Transfer_Center_Request_Status,
	event_type,
    event_category,
    fmonth_num,
    Fyear_num,
    FYear_name,
    report_period,
    report_date,
    event_id,
    epic_department_id,
    epic_department_name,
    epic_department_external,
    peds,
    person_birth_date,
    person_id,
    person_name,
    provider_id,
    provider_name,
    PAT_ENC_CSN_ID,
    EntryTime,
    AcctNbrint,
    TierLevel,
    Isolation,
    referringProviderName,
    Referring_Facility,
    TransferReason,
    TransferMode,
    Diagnosis,
    ServiceNme,
    LevelOfCare,
    TransferTypeHx,
    PlacementStatusName,
    XTPlacementStatusName,
    XTPlacementStatusDateTime,
    ETA,
    PatientReferredTo,
    AdtPatientFacilityID,
    AdtPatientFacility,
    BedAssigned,
    BedType,
    DispositionReason,
    Accepting_Timestamp,
    Accepting_MD,
    AcceptingMD_ServiceLine,
    CloseTime,
    PatientType,
    ProtocolNme,
    Load_Dtm,
    organization_name,
    service_name,
    clinical_area_name,
    sk_Dim_Pt,
    person_gender,
    hs_area_id,
    hs_area_name,
    rev_location_id,
    rev_location
FROM #trnsf
--WHERE hs_area_id = 1
ORDER BY
	event_count DESC,
	event_date

--SELECT
--	SUM(event_count) AS Numerator,
--	COUNT(*) AS Denominator
--FROM #trnsf

SELECT
    Fyear_num,
	fmonth_num,
	SUM(event_count) AS Numerator,
	COUNT(*) AS Denominator
FROM #trnsf
--WHERE hs_area_id = 1
GROUP BY
	Fyear_num,
	fmonth_num
ORDER BY
	Fyear_num,
	fmonth_num
/*
SELECT
	   SUM(event_count) AS Numerator,
	   COUNT(*) AS Denominator
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransfers_Tiles]
  WHERE event_date BETWEEN '2/1/2024' AND '2/29/2024'
*/	

SELECT [sk_Dash_PatientProgression_ExternalTransfers_Tiles]
      ,[event_type]
      ,[event_count]
      ,[event_date]
      ,[event_id]
      ,[event_category]
      ,[epic_department_id]
      ,[epic_department_name]
      ,[epic_department_name_external]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[peds]
      ,[transplant]
      ,[oncology]
      ,[App_Flag]
      ,[sk_Dim_Pt]
      ,[sk_Fact_Pt_Acct]
      ,[sk_Fact_Pt_Enc_Clrt]
      ,[sk_dim_physcn]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[provider_id]
      ,[provider_name]
      ,[prov_typ]
      ,[hs_area_id]
      ,[hs_area_name]
      ,[pod_id]
      ,[pod_name]
      ,[rev_location_id]
      ,[rev_location]
      ,[som_group_id]
      ,[som_group_name]
      ,[som_department_id]
      ,[som_department_name]
      ,[som_division_id]
      ,[som_division_name]
      ,[financial_division_id]
      ,[financial_division_name]
      ,[financial_sub_division_id]
      ,[financial_sub_division_name]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,[w_pod_id]
      ,[w_pod_name]
      ,[w_rev_location_id]
      ,[w_rev_location]
      ,[w_som_group_id]
      ,[w_som_group_name]
      ,[w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
      ,[w_financial_division_id]
      ,[w_financial_division_name]
      ,[w_financial_sub_division_id]
      ,[w_financial_sub_division_name]
      ,[epic_department_external]
      ,[PAT_ENC_CSN_ID]
      ,[EntryTime]
      ,[AcctNbrint]
      ,[TierLevel]
      ,[Isolation]
      ,[referringProviderName]
      ,[Referring_Facility]
      ,[TransferReason]
      ,[TransferMode]
      ,[Diagnosis]
      ,[ServiceNme]
      ,[LevelOfCare]
      ,[TransferTypeHx]
      ,[PlacementStatusName]
      ,[XTPlacementStatusName]
      ,[XTPlacementStatusDateTime]
      ,[ETA]
      ,[PatientReferredTo]
      ,[AdtPatientFacilityID]
      ,[AdtPatientFacility]
      ,[BedAssigned]
      ,[BedType]
      ,[DispositionReason]
      ,[Transfer_Center_Request_Status]
      ,[Accepting_Timestamp]
      ,[Accepting_MD]
      ,[AcceptingMD_ServiceLine]
      ,[CloseTime]
      ,[PatientType]
      ,[ProtocolNme]
      ,[organization_name]
      ,[service_name]
      ,[clinical_area_name]
      ,[Load_Dtm]
  INTO #tabrptg
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransfers_Tiles]

  WHERE 1=1
    AND event_date >= @locstartdate
	AND event_date <=  @locenddate
	--AND hs_area_id = 1

SELECT
    Fyear_num,
	fmonth_num,
	SUM(event_count) AS Numerator,
	COUNT(*) AS Denominator
FROM #tabrptg
GROUP BY
	Fyear_num,
	fmonth_num
ORDER BY
	Fyear_num,
	fmonth_num
GO


