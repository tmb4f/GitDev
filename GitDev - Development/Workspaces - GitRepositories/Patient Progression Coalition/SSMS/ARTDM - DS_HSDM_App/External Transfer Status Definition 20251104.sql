USE DS_HSDM_APP

DECLARE @startdate DATETIME, @enddate DATETIME

--SET @startdate = '7/1/2025 00:00:00'
--SET @enddate = '10/12/2025 23:59:59'
--SET @startdate = '7/1/2024 00:00:00'
--SET @enddate = '11/3/2025 23:59:59' 
SET @startdate = '10/1/2025 00:00:00'
SET @enddate = '10/31/2025 23:59:59' 

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
		
SET @locstartdate = @startdate
SET @locenddate   = @enddate

SELECT
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN,
	COUNT(*) AS XTRs -- 1062
FROM
(
SELECT
       *,
	   CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND TransferTypeHx <> 'Outgoing Transfer'
) xtr
GROUP BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
  ORDER BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN

SELECT
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN,
	COUNT(*) AS XTRs -- 250
FROM
(
SELECT
       *,
	   CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND (TransferTypeHx = 'Consult' OR
			(TransferTypeHx NOT IN ('Consult','Outgoing Transfer') AND DispositionReason = 'Consult Only'))
) xtr
GROUP BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
  ORDER BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN

SELECT
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN,
	COUNT(*) AS XTRs -- 619
FROM
(
SELECT
       *,
	   CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND (TransferTypeHx NOT IN ('Consult','Outgoing Transfer'))
) xtr
WHERE 1 = 1
	AND xtr.Transfer_Center_Request_Status IN ('Accepted','Completed')
	AND xtr.AdmissionCSN = 1
GROUP BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
  ORDER BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
/*
Declination
	Administrative Review
	Bed Availability/Capacity
	Service Not Available
Cancellation
	Not Medically Necessary
	Elected to go to Another Facility
	Pt Left AMA
	Patient Expired
	Other
		Diversion/Disaster
		Not Stable Enough to Transfer
		Other
		Patient condition not suitable for transfer
		Patient declined transfer
		Pt to Remain at Sending Facility
		Pt Treated and Released
		Pt/Family Did Not Wish to Transfer
		Referring location pulled request		
		Took too long to respond
		Transfer request order canceled
*/
SELECT
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN,
	COUNT(*) AS XTRs -- 193
FROM
(
SELECT
       *,
	   CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND (TransferTypeHx NOT IN ('Consult','Outgoing Transfer'))
) xtr
WHERE 1 = 1
	AND xtr.Transfer_Center_Request_Status NOT IN ('Accepted','Completed')
	AND DispositionReason <> 'Consult Only'
GROUP BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
  ORDER BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN

SELECT
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN,
	COUNT(*) AS XTRs -- 27 Declinations
FROM
(
SELECT
       *,
	   CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND (TransferTypeHx NOT IN ('Consult','Outgoing Transfer'))
) xtr
WHERE 1 = 1
	AND xtr.Transfer_Center_Request_Status NOT IN ('Accepted','Completed')
	AND DispositionReason <> 'Consult Only'
	AND xtr.DispositionReason IN ('Administrative Review','Bed Availability/Capacity','Service Not Available')
GROUP BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
  ORDER BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN

SELECT
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN,
	COUNT(*) AS XTRs -- 166 Cancellations
FROM
(
SELECT
       *,
	   CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND (TransferTypeHx NOT IN ('Consult','Outgoing Transfer'))
) xtr
WHERE 1 = 1
	AND xtr.Transfer_Center_Request_Status NOT IN ('Accepted','Completed')
	AND DispositionReason <> 'Consult Only'
	AND xtr.DispositionReason IN ('Not Medically Necessary','Elected to go to Another Facility',
	'Pt Left AMA','Patient Expired','Diversion/Disaster','Not Stable Enough to Transfer',
	'Other','Patient condition not suitable for transfer','Patient declined transfer',
	'Pt to Remain at Sending Facility','Pt Treated and Released','Pt/Family Did Not Wish to Transfer',
	'Referring location pulled request','Took too long to respond','Transfer request order canceled')
GROUP BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
  ORDER BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
/*
SELECT
	xtr.sk_Dash_PatientProgression_ExternalTransferStatus_Tiles,
    xtr.event_type,
    xtr.event_count,
    xtr.event_date,
    xtr.event_id,
    xtr.event_category,
    xtr.epic_department_id,
    xtr.epic_department_name,
    xtr.epic_department_name_external,
    xtr.fmonth_num,
    xtr.fyear_num,
    xtr.fyear_name,
    xtr.peds,
    xtr.transplant,
    xtr.oncology,
    xtr.App_Flag,
    xtr.sk_Dim_Pt,
    xtr.sk_Fact_Pt_Acct,
    xtr.sk_Fact_Pt_Enc_Clrt,
    xtr.sk_dim_physcn,
    xtr.person_birth_date,
    xtr.person_gender,
    xtr.person_id,
    xtr.person_name,
    xtr.provider_id,
    xtr.provider_name,
    xtr.prov_typ,
    xtr.hs_area_id,
    xtr.hs_area_name,
    xtr.pod_id,
    xtr.pod_name,
    xtr.rev_location_id,
    xtr.rev_location,
    xtr.som_group_id,
    xtr.som_group_name,
    xtr.som_department_id,
    xtr.som_department_name,
    xtr.som_division_id,
    xtr.som_division_name,
    xtr.financial_division_id,
    xtr.financial_division_name,
    xtr.financial_sub_division_id,
    xtr.financial_sub_division_name,
    xtr.w_hs_area_id,
    xtr.w_hs_area_name,
    xtr.w_pod_id,
    xtr.w_pod_name,
    xtr.w_rev_location_id,
    xtr.w_rev_location,
    xtr.w_som_group_id,
    xtr.w_som_group_name,
    xtr.w_som_department_id,
    xtr.w_som_department_name,
    xtr.w_som_division_id,
    xtr.w_som_division_name,
    xtr.w_financial_division_id,
    xtr.w_financial_division_name,
    xtr.w_financial_sub_division_id,
    xtr.w_financial_sub_division_name,
    xtr.accepted,
    xtr.declined,
    xtr.consult,
    xtr.canceled,
    xtr.epic_department_external,
    xtr.Ethnicity,
    xtr.FirstRace,
    xtr.SecondRace,
    xtr.AgeAtRequest,
    xtr.PAT_ENC_CSN_ID,
    xtr.EntryTime,
    xtr.AcctNbrint,
    xtr.TierLevel,
    xtr.Isolation,
    xtr.referringProviderName,
    xtr.Referring_Facility,
    xtr.TransferReason,
    xtr.TransferMode,
    xtr.Diagnosis,
    xtr.ServiceNme,
    xtr.LevelOfCare,
    xtr.TransferTypeHx,
    xtr.PlacementStatusName,
    xtr.XTPlacementStatusName,
    xtr.XTPlacementStatusDateTime,
    xtr.ETA,
    xtr.PatientReferredTo,
    xtr.AdtPatientFacilityID,
    xtr.AdtPatientFacility,
    xtr.BedAssigned,
    xtr.BedType,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
    xtr.Accepting_Timestamp,
    xtr.Accepting_MD,
    xtr.AcceptingMD_ServiceLine,
    xtr.CloseTime,
    xtr.PatientType,
    xtr.ProtocolNme,
    xtr.ProviderApproved,
    xtr.PatientService,
    xtr.[Financial Approval],
    xtr.[Capacity Approval],
    xtr.[Physician Acceptance],
    xtr.Canceled_By,
    xtr.Referred_From_UVA_HEALTH,
    xtr.Destination,
    xtr.Destination_UVA_HEALTH,
    xtr.FINANCIAL_CLASS,
    xtr.EntryTimehhmmss,
    xtr.Primary_Dx_on_Account,
    xtr.Prim_Dx,
    xtr.DRG,
    xtr.DRG_NAME,
    xtr.UVAMC_Admission_Instant,
    xtr.UVAMC_Discharge_Instant,
    xtr.Primary_DX_Block,
    xtr.organization_name,
    xtr.service_name,
    xtr.clinical_area_name,
    xtr.Load_Dtm,
    xtr.First_Incoming_DTTM,
    xtr.Last_Incoming_DTTM,
    xtr.First_Outgoing_DTTM,
    xtr.Last_Outgoing_DTTM,
    xtr.OpenTime,
    xtr.ResolutionTime,
    xtr.XTRequestOpenToResolution,
    xtr.XTRequestOpenToAcceptance,
    xtr.Disch_Disp_Name,
    xtr.TransferType,
    xtr.incoming_transfer,
    xtr.accepted_no_admission,
    xtr.AdmissionCSN
FROM
(
SELECT
       *,
	   CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND (TransferTypeHx NOT IN ('Consult','Outgoing Transfer'))
) xtr
WHERE 1 = 1
	AND xtr.Transfer_Center_Request_Status NOT IN ('Accepted','Completed')
	AND DispositionReason = 'Other'
  ORDER BY
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
*/
/*
SELECT
	xtr.TransferTypeHx,
    xtr.DispositionReason,
    xtr.Transfer_Center_Request_Status,
	xtr.Canceled_By,
    xtr.AdmissionCSN
FROM
(
SELECT DISTINCT
       [TransferTypeHx]
      ,[DispositionReason]
      ,[Transfer_Center_Request_Status]
	  ,Canceled_By
	  ,CASE WHEN PAT_ENC_CSN_ID IS NOT NULL THEN 1 ELSE 0 END AS AdmissionCSN
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
	AND TransferTypeHx NOT IN ('Consult','Outgoing Transfer')
) xtr
WHERE xtr.AdmissionCSN = 0
  ORDER BY
	TransferTypeHx,
	DispositionReason
*/
GO