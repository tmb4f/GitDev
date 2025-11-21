USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--EXEC [ETL].[uspSrc_PtPgr_ExternalXferStatus]

CREATE PROCEDURE [ETL].[uspSrc_PtPgr_ExternalXferStatus]
    (
     @startdate SMALLDATETIME = NULL
    ,@enddate SMALLDATETIME = NULL
	
    )
AS 

--DECLARE @startdate DATETIME, @enddate DATETIME

--SET @startdate = '7/1/2023'
--SET @enddate = '3/19/2024'
----6453, all TransferTypeHx values

--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_PtPgr_ExternalXferStatus
--WHO : Tom Burgan
--WHEN: 02/21/2024
--WHY : for Patient Progression Coalition metric
--			External Transfer Requests Status Frequencies (accepted, declined, consults, cancels)  
--			External Transfer Requests received by UVa Health - University Medical Center
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	
--              
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         03/19/2024 -TMB - create stored procedure

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
	  CAST('External Transfer Request' AS VARCHAR(50))	AS event_type
	, CAST('Intake Request Status' AS VARCHAR(150))	AS event_category
	, CASE WHEN xt.TransferTypeHx IN ('Incoming Transfer','Consult') THEN 1 ELSE 0 END AS event_count
	, CASE WHEN xt.TransferTypeHx = 'Incoming Transfer' AND [xt].[Physician Acceptance] = 'Completed' THEN 1 ELSE 0 END AS accepted
	, CASE WHEN xt.TransferTypeHx = 'Incoming Transfer' AND [xt].[Physician Acceptance] IN ('Skipped','Canceled')
		AND [DispositionReason] IN ('Administrative Review','Bed Availability/Capacity','Not Medically Necessary') THEN 1 ELSE 0 END AS declined
	, CASE WHEN xt.TransferTypeHx = 'Consult' THEN 1 ELSE 0 END AS consult
	, CASE WHEN xt.TransferTypeHx = 'Incoming Transfer' AND [xt].[Physician Acceptance]  IN ('Skipped','Canceled')
		AND [DispositionReason] IN ('Elected to go to Another Facility','Not Stable Enough to Transfer','Patient Expired','Patient condition not suitable for transfer','Patient declined transfer','Pt Left AMA','Pt to Remain at Sending Facility','Pt Treated and Released','Pt/Family Did Not Wish to Transfer','Referring location pulled request','Service Not Available','Other') THEN 1 ELSE 0 END AS canceled
	, dd.day_date AS event_date
	, dd.fmonth_num
	, dd.FYear_num
	, dd.FYear_name
	, CAST(LEFT(DATENAME(MM, dd.day_date), 3) + ' ' + CAST(DAY(dd.day_date) AS VARCHAR(2)) AS VARCHAR(10))			AS report_period
	, CAST(CAST(dd.day_date AS DATE) AS SMALLDATETIME)																AS report_date
	, [TransferID] AS event_id
	, xt.DestinationUnitID AS epic_department_id
	, mdmhst.epic_department_name AS epic_department_name
	, mdmhst.epic_department_name_external AS epic_department_external
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
	, pat.Sex AS person_gender
	, pat.Ethnicity
	, pat.FirstRace
	, pat.SecondRace
	, CASE
          WHEN DATEADD (YEAR, DATEDIFF (YEAR, xt.PatientDOB, xt.EntryTime), xt.PatientDOB) > xt.EntryTime
          THEN DATEDIFF (YEAR, xt.PatientDOB, xt.EntryTime) - 1
          ELSE DATEDIFF (YEAR, xt.PatientDOB, xt.EntryTime)
      END AS AgeAtRequest                                                                                                                  
	, xt.AcceptingMD_ID AS provider_id
	, xt.Accepting_MD AS provider_name
    , [AdmissionCSN] AS PAT_ENC_CSN_ID
    , [EntryTime]
    , [AcctNbrint]
    , [TierLevel]
    , [Isolation]
    , [referringProviderName]
    , [Referring_Facility]
    , [TransferReason]
    , [TransferMode]
    , [Diagnosis]
    , [ServiceNme]
    , [LevelOfCare]
	, xt.TransferTypeHx
    , [PlacementStatusName]
    , [XTPlacementStatusName]
    , [XTPlacementStatusDateTime]
    , [ETA]
    , [PatientReferredTo]
    , [AdtPatientFacilityID]
    , [AdtPatientFacility]
    , [BedAssigned]
    , [BedType]
    , [DispositionReason]
    , [Disposition] AS Transfer_Center_Request_Status
    , [Accepting_Timestamp]
    , [Accepting_MD]
    , [AcceptingMD_ServiceLine]
    , [CloseTime]
    , [PatientType]
    , [ProtocolNme]
	, xt.ProviderApproved
	, xt.PatientService
	, xt.[Financial Approval]
	, xt.[Capacity Approval]
	, xt.[Physician Acceptance]
    , xt.[Canceled_By]
    , xt.[Referred_From_UVA_HEALTH]
    , xt.[Destination]
    , xt.[Destination_UVA_HEALTH]
    , xt.[FINANCIAL_CLASS]
    , xt.[EntryTimehhmmss]
    , xt.[Primary_Dx_on_Account]
    , xt.[Prim_Dx]
    , xt.[DRG]
    , xt.[DRG_NAME]
    , xt.[UVAMC_Admission_Instant]
    , xt.[UVAMC_Discharge_Instant]
    , xt.[Primary_DX_Block]
    , xt.[Load_Dtm]
	, o.organization_name
	, s.service_name
	, c.clinical_area_name
	, pat.sk_Dim_Pt
	, mdmhst.hs_area_id
	, mdmhst.hs_area_name
	, mdmhst.LOC_ID AS rev_location_id
	, mdmhst.REV_LOC_NAME AS rev_location

  FROM [DS_HSDM_Prod].[Rptg].[ADT_TransferCenter_ExternalTransfers] xt
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date dd
  ON dd.day_date = CAST(xt.EntryTime AS DATE)
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient pat							ON	pat.MRN_display = xt.PatientMR
  LEFT OUTER JOIN
	  (
		  SELECT
			  history.MDM_BATCH_ID,
			  history.EPIC_DEPARTMENT_ID,
			  history.EPIC_DEPT_NAME AS epic_department_name,
			  history.EPIC_EXT_NAME AS epic_department_name_external,
			  history.LOC_ID,
			  history.REV_LOC_NAME,
			  history.HS_AREA_ID,
			  history.HS_AREA_NAME
		  FROM
		  (
			  SELECT
				  MDM_BATCH_ID,
				  EPIC_DEPARTMENT_ID,
				  EPIC_DEPT_NAME,
				  EPIC_EXT_NAME,
				  LOC_ID,
				  REV_LOC_NAME,
				  HS_AREA_ID,
				  HS_AREA_NAME,
				  ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
			  FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
		  ) history
		  WHERE history.seq = 1
	  ) mdmhst
	  ON mdmhst.EPIC_DEPARTMENT_ID = xt.DestinationUnitID
  LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON xt.DestinationUnitID = g.epic_department_id
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
  LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id

  WHERE 1=1
 --   AND TransferTypeID = 1 -- REQUEST_TYPE_MAPPING_C = 1,	REQUEST_TYPE_MAPPING_NAME = Transfer, 	REQUEST_TYPE_C = 2026,	REQUEST_TYPE_NAME = Incoming Transfer
	--AND xt.TransferTypeHx IN ('Incoming Transfer','Consult')

	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate

	ORDER BY xt.EntryTime
	
GO


