USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--EXEC [ETL].[uspSrc_PtPgr_ExternalXferStatus]

--ALTER PROCEDURE [ETL].[uspSrc_PtPgr_ExternalXferStatus]
--AS 

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
--         04/16/2024 -TMB - add request timestamps, turn-around time lengths
--         05/20/2024 -TMB - add discharge disposition for requests with an admission
--         09/04/2025 -TMB - edit logic used to place requests in the various status buckets;
--				add TransferType column

--************************************************************************************************************************

    SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#xtr ') IS NOT NULL
DROP TABLE #xtr

IF OBJECT_ID('tempdb..#xtrit ') IS NOT NULL
DROP TABLE #xtrit

DECLARE @startdate SMALLDATETIME,
        @enddate SMALLDATETIME

--SET @startdate = '7/1/2023 00:00:00'
SET @startdate = '7/1/2025 00:00:00'
--SET @enddate = '6/19/2025 23:59:59'
SET @enddate = '9/29/2025 23:59:59'
--SET @enddate = '7/31/2025 23:59:59'

 
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
	xt.event_type,
    xt.event_category,
    xt.event_count,
    xt.accepted,
    xt.declined,
    xt.consult,
    xt.canceled,
    xt.event_date,
    xt.fmonth_num,
    xt.Fyear_num,
    xt.FYear_name,
    xt.report_period,
    xt.report_date,
    xt.event_id,
    xt.epic_department_id,
    xt.epic_department_name,
    xt.epic_department_external,
    xt.peds,
    xt.person_birth_date,
    xt.person_id,
    xt.person_name,
    xt.person_gender,
    xt.Ethnicity,
    xt.FirstRace,
    xt.SecondRace,
    xt.AgeAtRequest,
    xt.provider_id,
    xt.provider_name,
    xt.PAT_ENC_CSN_ID,
    xt.EntryTime,
    xt.AcctNbrint,
    xt.TierLevel,
    xt.Isolation,
    xt.referringProviderName,
    xt.Referring_Facility,
    xt.TransferReason,
    xt.TransferMode,
    xt.Diagnosis,
    xt.ServiceNme,
    xt.LevelOfCare,
    xt.TransferTypeHx,
    xt.PlacementStatusName,
    xt.XTPlacementStatusName,
    xt.XTPlacementStatusDateTime,
    xt.ETA,
    xt.PatientReferredTo,
    xt.AdtPatientFacilityID,
    xt.AdtPatientFacility,
    xt.BedAssigned,
    xt.BedType,
    xt.DispositionReason,
    xt.Transfer_Center_Request_Status,
    xt.Accepting_Timestamp,
    xt.Accepting_MD,
    xt.AcceptingMD_ServiceLine,
    xt.PatientType,
    xt.ProtocolNme,
    xt.ProviderApproved,
    xt.PatientService,
    xt.[Financial Approval],
    xt.[Capacity Approval],
    xt.[Physician Acceptance],
    xt.Canceled_By,
    xt.Referred_From_UVA_HEALTH,
    xt.Destination,
    xt.Destination_UVA_HEALTH,
    xt.FINANCIAL_CLASS,
    xt.EntryTimehhmmss,
    xt.Primary_Dx_on_Account,
    xt.Prim_Dx,
    xt.DRG,
    xt.DRG_NAME,
    xt.UVAMC_Admission_Instant,
    xt.UVAMC_Discharge_Instant,
    xt.Primary_DX_Block,
    xt.Load_Dtm,
    xt.organization_name,
    xt.service_name,
    xt.clinical_area_name,
    xt.sk_Dim_Pt,
	xt.hs_area_id,
	xt.hs_area_name,
    xt.rev_location_id,
    xt.rev_location,
    xt.First_Incoming_DTTM,
    xt.Last_Incoming_DTTM,
    xt.First_Outgoing_DTTM,
    xt.Last_Outgoing_DTTM,
    xt.OpenTime,
    xt.CloseTime AS ResolutionTime,
	DATEDIFF(MINUTE,xt.OpenTime,xt.CloseTime) AS XTRequestOpenToResolution,
	CASE WHEN xt.Accepting_Timestamp IS NOT NULL THEN DATEDIFF(MINUTE,xt.OpenTime,xt.Accepting_Timestamp) ELSE NULL END AS XTRequestOpenToAcceptance,
	xt.Disch_Disp_Name,
	xt.TransferType -- VARCHAR(254)

INTO #xtr

FROM
(
SELECT 
	  CAST('External Transfer Request' AS VARCHAR(50))	AS event_type
	, CAST('Intake Request Status' AS VARCHAR(150))	AS event_category
	, CASE WHEN xt.TransferTypeHx IN ('Incoming Transfer','Consult','Medical Intrafacility Transfer') THEN 1 ELSE 0 END AS event_count
	, CASE WHEN xt.TransferTypeHx IN ('Incoming Transfer','Medical Intrafacility Transfer') AND [xt].[Physician Acceptance] = 'Completed' THEN 1 ELSE 0 END AS accepted
	, CASE WHEN xt.TransferTypeHx IN ('Incoming Transfer','Consult','Medical Intrafacility Transfer')
		AND ([xt].[Physician Acceptance] IS NULL OR [xt].[Physician Acceptance] IN ('Completed','Skipped','Canceled'))
		AND [DispositionReason] IN ('Administrative Review','Bed Availability/Capacity','Not Medically Necessary','Took too long to respond','Diversion/Disaster','Service Not Available','Other') THEN 1 ELSE 0 END AS declined
	, CASE WHEN xt.TransferTypeHx = 'Consult' THEN 1 ELSE 0 END AS consult
	, CASE WHEN xt.TransferTypeHx IN ('Incoming Transfer','Consult','Medical Intrafacility Transfer')
		AND ([xt].[Physician Acceptance] IS NULL OR [xt].[Physician Acceptance] IN ('Completed','Skipped','Canceled'))
		AND [DispositionReason] IN ('Elected to go to Another Facility','Not Stable Enough to Transfer','Patient Expired','Patient condition not suitable for transfer','Patient declined transfer',
		                                               'Pt Left AMA','Pt to Remain at Sending Facility','Pt Treated and Released','Pt/Family Did Not Wish to Transfer','Referring location pulled request','Consult Only',
													   'Transfer request order canceled') THEN 1 ELSE 0 END AS canceled
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
	, xt.First_Incoming_DTTM
	, xt.Last_Incoming_DTTM
	, xt.First_Outgoing_DTTM
	, xt.Last_Outgoing_DTTM
	, (SELECT MIN(First_Request_DTTM)
			FROM (VALUES (xt.First_Incoming_DTTM), (xt.First_Outgoing_DTTM), (xt.EntryTimehhmmss)) AS OpenTime(First_Request_DTTM))
				AS OpenTime
	, xt.Disch_Disp_Name
	, xt.TransferType

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

  --AND CAST(xt.PatientMR AS INT)  IN (
  --'1500309',
  --'4424981'
  --)
) xt

	--ORDER BY xt.EntryTime

	--ORDER BY event_count, xt.EntryTime
	--ORDER BY xt.TransferTypeHx, xt.EntryTime
	ORDER BY xt.event_id, EntryTime

	SELECT
	    xt.EntryTime,
	    xt.event_count,
		xt.accepted,
		xt.declined,
		xt.consult,
		xt.canceled,
		xt.TransferType,
		xt.TransferTypeHx,
		xt.PlacementStatusName,
		xt.XTPlacementStatusName,
		xt.DispositionReason,
		xt.Transfer_Center_Request_Status,
		hsp.ADMIT_CONF_STAT,
		xt.Accepting_Timestamp,
		xt.[Physician Acceptance],
		xt.Canceled_By,
		xt.person_id,
		xt.UVAMC_Admission_Instant,
		CASE WHEN xt.UVAMC_Admission_Instant IS NOT NULL THEN 1 ELSE 0 END AS UVAMC_Admission,
		xt.UVAMC_Discharge_Instant,
		xt.PAT_ENC_CSN_ID,
		adt.sk_Adm_Dte,
		CASE WHEN adt.sk_Adm_Dte IS NOT NULL THEN 1 ELSE 0 END AS ADT_Admission,
		adt.sk_Dsch_Dte,
		hsp.INPATIENT_DATA_ID,
		hsp.IP_EPISODE_ID,
		hsp.ED_EPISODE_ID,
		hsp. INSTANT_OF_ENTRY_TM
	INTO #xtrit
	FROM #xtr xt
	LEFT OUTER JOIN DS_HSDW_App.Rptg.PatientDischargeByService_v2 adt
		ON adt.PAT_ENC_CSN_ID = xt.PAT_ENC_CSN_ID
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt hsp
		ON hsp.PAT_ENC_CSN_ID = xt.PAT_ENC_CSN_ID
	--WHERE xt.TransferTypeHx <> 'Consult'
	WHERE xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer')

	SELECT
		*
	FROM #xtrit xt
	ORDER BY xt.EntryTime

	SELECT
        xt.Transfer_Center_Request_Status,
		xt.UVAMC_Admission,
        xt.UVAMC_Admission_Instant,
		xt.ADT_Admission,
        xt.sk_Adm_Dte,
		xt.EntryTime,
        xt.event_count,
        xt.accepted,
        xt.declined,
        xt.consult,
        xt.canceled,
        xt.TransferType,
        xt.TransferTypeHx,
        xt.PlacementStatusName,
        xt.XTPlacementStatusName,
        xt.DispositionReason,
        xt.ADMIT_CONF_STAT,
        xt.Accepting_Timestamp,
        xt.[Physician Acceptance],
        xt.Canceled_By,
        xt.person_id,
        xt.UVAMC_Discharge_Instant,
        xt.PAT_ENC_CSN_ID,
        xt.sk_Dsch_Dte,
        xt.INPATIENT_DATA_ID,
        xt.IP_EPISODE_ID,
        xt.ED_EPISODE_ID,
        xt.INSTANT_OF_ENTRY_TM
	FROM #xtrit xt
	WHERE xt.PAT_ENC_CSN_ID IS NOT NULL
	--ORDER BY xt.EntryTime
	ORDER BY xt.Transfer_Center_Request_Status, xt.UVAMC_Admission, xt.ADT_Admission, xt.EntryTime
/*
	SELECT
		*
	FROM #xtrit xt
	WHERE xt.PAT_ENC_CSN_ID IS NOT NULL
	ORDER BY xt.EntryTime

	SELECT
	    xt.EntryTime,
	    xt.event_count,
		xt.accepted,
		xt.declined,
		xt.consult,
		xt.canceled,
		xt.TransferType,
		xt.TransferTypeHx,
		xt.PlacementStatusName,
		xt.XTPlacementStatusName,
		xt.DispositionReason,
		xt.Transfer_Center_Request_Status,
		xt.ADMIT_CONF_STAT,
		xt.Accepting_Timestamp,
		xt.[Physician Acceptance],
		xt.Canceled_By,
		xt.person_id,
		xt.UVAMC_Admission_Instant,
		xt.UVAMC_Discharge_Instant,
		xt.PAT_ENC_CSN_ID,
		xt.sk_Adm_Dte,
		xt.sk_Dsch_Dte,
		xt.INPATIENT_DATA_ID,
		xt.IP_EPISODE_ID,
		xt.ED_EPISODE_ID,
		xt. INSTANT_OF_ENTRY_TM
	FROM #xtrit xt
	WHERE 1 = 1
	AND xt.PAT_ENC_CSN_ID IS NOT NULL
	AND (xt.sk_Adm_Dte IS NULL AND xt.UVAMC_Admission_Instant IS NULL)
	ORDER BY xt.EntryTime

	SELECT
	    xt.EntryTime,
	    xt.event_count,
		xt.accepted,
		xt.declined,
		xt.consult,
		xt.canceled,
		xt.TransferType,
		xt.TransferTypeHx,
		xt.PlacementStatusName,
		xt.XTPlacementStatusName,
		xt.DispositionReason,
		xt.Transfer_Center_Request_Status,
		xt.ADMIT_CONF_STAT,
		xt.Accepting_Timestamp,
		xt.[Physician Acceptance],
		xt.Canceled_By,
		xt.person_id,
		xt.UVAMC_Admission_Instant,
		xt.UVAMC_Discharge_Instant,
		xt.PAT_ENC_CSN_ID,
		xt.sk_Adm_Dte,
		xt.sk_Dsch_Dte,
		xt.INPATIENT_DATA_ID,
		xt.IP_EPISODE_ID,
		xt.ED_EPISODE_ID,
		xt. INSTANT_OF_ENTRY_TM
	FROM #xtrit xt
	WHERE 1 = 1
	AND xt.PAT_ENC_CSN_ID IS NOT NULL
	AND (xt.sk_Adm_Dte IS NULL AND xt.UVAMC_Admission_Instant IS NULL)
	AND LEN(xt.Canceled_By) = 0
	ORDER BY xt.EntryTime
*/
GO


