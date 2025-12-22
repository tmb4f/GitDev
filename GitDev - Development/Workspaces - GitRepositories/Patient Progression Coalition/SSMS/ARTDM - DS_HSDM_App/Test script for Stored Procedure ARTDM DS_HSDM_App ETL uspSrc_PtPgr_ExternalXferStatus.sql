USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--EXEC [ETL].[uspSrc_PtPgr_ExternalXferStatus]

--ALTER PROCEDURE [ETL].[uspSrc_PtPgr_ExternalXferStatus]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
	
--    )
--AS 

DECLARE @startdate DATETIME = NULL, @enddate DATETIME = NULL

--SET @startdate = '7/1/2023'
--SET @enddate = '3/19/2024'

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
--		   10/02/2025 -TMB - edit logic identifying accepted requests; add flags to designate accepted requests with no confirmed admission and
--				incoming transfers
--		   11/10/2025 -TMB - edit logic setting request status flags; add flags to designate accepted and completed requests with no confirmed admission,
--				and canceled requests with created admissions
--		   11/21/2025 -TMB - edit logic for 'accepted' status; add flag for requests having a request status value 'Pending'
--        12/03/2025 - TMB - add column sk_Adm_Dte; set default reporting period

--************************************************************************************************************************

    SET NOCOUNT ON;

	----get default date range	
    IF @startdate IS NULL
        AND @enddate IS NULL
		BEGIN
			DECLARE @FYStartMonth INT = 7; -- July 1st fiscal year start
			DECLARE @CurrentDate DATE = GETDATE();
			DECLARE @CurrentFYStartDate DATE;

			SET @enddate = @CurrentDate
			SET @CurrentFYStartDate = 
				CASE 
					WHEN MONTH(@CurrentDate) >= @FYStartMonth THEN 
						DATEFROMPARTS(YEAR(@CurrentDate), @FYStartMonth, 1)
					ELSE 
						DATEFROMPARTS(YEAR(@CurrentDate) - 1, @FYStartMonth, 1)
				END;
			SET @startdate = CAST(DATEADD(YEAR, -4, @CurrentFYStartDate) AS DATE);
		END
 
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
		
SET @locstartdate = @startdate
SET @locenddate   = @enddate
 
-------------------------------------------------------------------------------

--IF OBJECT_ID('tempdb..#xtr ') IS NOT NULL
--DROP TABLE #xtr

--SELECT @locstartdate, @locenddate

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
	xt.TransferType,
	xt.incoming_transfer,
	CASE WHEN xt.Transfer_Center_Request_Status = 'Accepted' AND xt.incoming_transfer = 1 AND (xt.sk_Adm_Dte IS NULL AND xt.UVAMC_Admission_Instant IS NULL) THEN 1 ELSE 0 END AS request_accepted_no_admission,
	CASE WHEN xt.Transfer_Center_Request_Status = 'Completed' AND xt.incoming_transfer = 1 AND (xt.sk_Adm_Dte IS NULL AND xt.UVAMC_Admission_Instant IS NULL) THEN 1 ELSE 0 END AS request_completed_no_admission,
	CASE WHEN xt.accepted = 0 AND xt.consult = 0 AND xt.PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS request_canceled_admission_created,
	xt.pending,
	xt.sk_Adm_Dte -- INTEGER
--INTO #xtr
FROM
(
SELECT 
	  CAST('External Transfer Request' AS VARCHAR(50))	AS event_type
	, CAST('Intake Request Status' AS VARCHAR(150))	AS event_category
	, CASE WHEN xt.TransferTypeHx IN ('Incoming Transfer','Consult','Medical Intrafacility Transfer') THEN 1 ELSE 0 END AS event_count
	, CASE WHEN xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer') AND xt.Disposition IN ('Accepted','Completed') AND (adt.sk_Adm_Dte IS NOT NULL OR xt.UVAMC_Admission_Instant IS NOT NULL) THEN 1 ELSE 0 END AS accepted
	, CASE WHEN xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer')
		AND xt.Disposition NOT IN ('Accepted','Completed')
		AND xt.DispositionReason <> 'Consult Only'
		AND xt.DispositionReason IN ('Administrative Review','Bed Availability/Capacity','Service Not Available') THEN 1 ELSE 0 END AS declined
	, CASE WHEN xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer') AND (xt.DispositionReason IS NULL OR xt.DispositionReason <> 'Consult Only') THEN 1 ELSE 0 END AS incoming_transfer
	, CASE WHEN xt.TransferTypeHx = 'Consult'
		OR (xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer') AND xt.DispositionReason = 'Consult Only') THEN 1 ELSE 0 END AS consult
	, CASE WHEN xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer')
		AND xt.Disposition NOT IN ('Accepted','Completed')
		AND xt.DispositionReason <> 'Consult Only'
		AND xt.DispositionReason IN ('Not Medically Necessary','Elected to go to Another Facility',
			'Pt Left AMA','Patient Expired','Diversion/Disaster','Not Stable Enough to Transfer',
			'Other','Patient condition not suitable for transfer','Patient declined transfer',
			'Pt to Remain at Sending Facility','Pt Treated and Released','Pt/Family Did Not Wish to Transfer',
			'Referring location pulled request','Took too long to respond','Transfer request order canceled') THEN 1 ELSE 0 END AS canceled
	, CASE WHEN xt.TransferTypeHx NOT IN ('Consult','Outgoing Transfer')
		AND xt.Disposition = 'Pending' THEN 1 ELSE 0 END AS pending
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
	, adt.sk_Adm_Dte
	, adt.sk_Dsch_Dte

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
  LEFT OUTER JOIN DS_HSDW_App.Rptg.PatientDischargeByService_v2 adt ON adt.PAT_ENC_CSN_ID = xt.AdmissionCSN

  WHERE 1=1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
) xt

	ORDER BY xt.EntryTime
/*
	SELECT
		event_type,
        event_category,
        event_count,
        accepted,
        declined,
        consult,
        canceled,
		pending,
        event_date,
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
        person_gender,
        Ethnicity,
        FirstRace,
        SecondRace,
        AgeAtRequest,
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
        Transfer_Center_Request_Status,
        Accepting_Timestamp,
        Accepting_MD,
        CASE WHEN [Accepting_MD] IS NOT NULL THEN 'Y' ELSE 'N' END AS Approving_MD_Documented,
        AcceptingMD_ServiceLine,
        PatientType,
        ProtocolNme,
        ProviderApproved,
        PatientService,
        [Financial Approval],
        [Capacity Approval],
        [Physician Acceptance],
        Canceled_By,
        Referred_From_UVA_HEALTH,
        Destination,
        Destination_UVA_HEALTH,
        FINANCIAL_CLASS,
        EntryTimehhmmss,
        Primary_Dx_on_Account,
        Prim_Dx,
        DRG,
        DRG_NAME,
		sk_Adm_Dte,
        UVAMC_Admission_Instant,
        UVAMC_Discharge_Instant,
        Primary_DX_Block,
        Load_Dtm,
        organization_name,
        service_name,
        clinical_area_name,
        sk_Dim_Pt,
        HS_AREA_ID,
        HS_AREA_NAME,
        rev_location_id,
        rev_location,
        First_Incoming_DTTM,
        Last_Incoming_DTTM,
        First_Outgoing_DTTM,
        Last_Outgoing_DTTM,
        OpenTime,
        ResolutionTime,
        XTRequestOpenToResolution,
        XTRequestOpenToAcceptance,
        Disch_Disp_Name,
        TransferType,
        incoming_transfer,
        request_accepted_no_admission,
        request_completed_no_admission,
        request_canceled_admission_created
	INTO #inctr
	FROM #xtr
  WHERE 1 = 1
  AND incoming_transfer = 1

 -- SELECT
	--*
 -- FROM #inctr
 -- WHERE pending = 1

  SELECT	
    ProviderApproved,
	Approving_MD_Documented,
	COUNT(*) AS Incoming_Transfer_Requests,
	SUM(accepted) AS Admitted,
	SUM(declined) AS Request_Status_Canceled_Flagged_As_Declined,
	SUM(canceled) AS Request_Status_Canceled_Flagged_As_Canceled,
	SUM(request_accepted_no_admission) AS Request_Status_Accepted_Not_Admitted,
	SUM(request_completed_no_admission) AS Request_Status_Completed_Not_Admitted,
	SUM(pending) AS Request_Status_Pending
  FROM #inctr
  WHERE 1 = 1
  GROUP BY
	ProviderApproved,
	Approving_MD_Documented
  ORDER BY
	ProviderApproved DESC,
	Approving_MD_Documented DESC

 -- SELECT	
	--*
 -- FROM #inctr
 -- WHERE 1 = 1
 -- AND ProviderApproved IS NULL
 -- AND Approving_MD_Documented = 'N'
 -- AND accepted = 0
 -- AND declined = 0
 -- AND canceled = 0
 -- AND request_accepted_no_admission = 0
 -- AND request_completed_no_admission = 0
 -- ORDER BY
	--ProviderApproved DESC,
	--Approving_MD_Documented DESC

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
        xt.HS_AREA_ID,
        xt.HS_AREA_NAME,
        xt.rev_location_id,
        xt.rev_location,
        xt.First_Incoming_DTTM,
        xt.Last_Incoming_DTTM,
        xt.First_Outgoing_DTTM,
        xt.Last_Outgoing_DTTM,
        xt.OpenTime,
        xt.ResolutionTime,
        xt.XTRequestOpenToResolution,
        xt.XTRequestOpenToAcceptance,
        xt.Disch_Disp_Name,
        xt.TransferType,
        xt.incoming_transfer,
        xt.request_accepted_no_admission,
        xt.request_completed_no_admission,
        xt.request_canceled_admission_created,
        xt.pending,
        xt.sk_Adm_Dte
	FROM #xtr xt
	--ORDER BY xt.EntryTime
	--ORDER BY xt.TransferTypeHx, xt.Transfer_Center_Request_Status, xt.PAT_ENC_CSN_ID DESC, accepted DESC, xt.EntryTime
	ORDER BY xt.event_count DESC, xt.EntryTime, xt.accepted DESC, xt.TransferTypeHx, xt.Transfer_Center_Request_Status, xt.PAT_ENC_CSN_ID DESC
	
	SELECT
		SUM(xt.event_count) AS event_count,
        SUM(xt.accepted) AS accepted,
        SUM(xt.declined) AS declined,
        SUM(xt.consult) AS consult,
        SUM(xt.canceled) AS canceled,
		SUM(xt.pending) AS pending,
        SUM(xt.incoming_transfer) AS incoming_transfer,
		SUM(xt.request_accepted_no_admission) AS request_accepted_no_admission,
		SUM(xt.request_completed_no_admission) AS request_completed_no_admission,
        SUM(xt.request_canceled_admission_created) AS request_canceled_admission_created
	FROM #xtr xt
*/
/*
SELECT DISTINCT 
	ProtocolNme--, -- R ADT TC PROTOCOLS
	--ServiceNme, -- Hospital Service for admission
	--PatientService--, -- Service based on documented tc provider communication
	--organization_name, -- admitting department grouper
	--service_name, -- admitting department grouper
	--clinical_area_name -- admitting department grouper

FROM #xtr
WHERE 1 = 1
AND incoming_transfer = 1
AND accepted = 1
AND accepted_no_admission = 0
AND PatientService IS NOT NULL

ORDER BY
	--ServiceNme
	ProtocolNme
*/	
/*
SELECT
	Fyear_num,
	fmonth_num,
	SUM(CASE WHEN accepted = 0 THEN 1 ELSE 0 END) AS Numerator,
	SUM(incoming_transfer) AS Denominator
FROM #xtr
WHERE 1 = 1
AND incoming_transfer = 1
GROUP BY
	Fyear_num,
	fmonth_num
ORDER BY
	Fyear_num,
	fmonth_num
*/
	
GO


