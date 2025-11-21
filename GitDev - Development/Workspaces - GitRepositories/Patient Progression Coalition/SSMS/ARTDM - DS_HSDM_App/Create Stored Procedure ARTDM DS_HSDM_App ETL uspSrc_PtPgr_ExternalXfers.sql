USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--EXEC [ETL].[uspSrc_PtPgr_ExternalXfers]

CREATE PROCEDURE [ETL].[uspSrc_PtPgr_ExternalXfers]
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
	
    )
AS 

--DECLARE @startdate DATETIME, @enddate DATETIME

--SET @startdate = '12/1/2023'
--SET @enddate = '12/31/2023'

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

  FROM [DS_HSDM_Prod].[Rptg].[ADT_TransferCenter_ExternalTransfers] xt
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date dd
  ON dd.day_date = CAST(xt.EntryTime AS DATE)
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient pat							ON	pat.MRN_display = xt.PatientMR
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc mdm		ON	xt.DestinationUnitID = mdm.epic_department_id
  LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON xt.DestinationUnitID = g.epic_department_id
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id

  WHERE 1=1
    AND TransferTypeID = 1 -- REQUEST_TYPE_MAPPING_C = 1,	REQUEST_TYPE_MAPPING_NAME = Transfer, 	REQUEST_TYPE_C = 2026,	REQUEST_TYPE_NAME = Incoming Transfer
	AND xt.TransferTypeHx = 'Incoming Transfer'

	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate

	ORDER BY event_count, xt.EntryTime
	
GO


