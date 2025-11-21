USE DS_HSDM_APP

IF OBJECT_ID('tempdb..#xtr') IS NOT NULL
DROP TABLE #xtr

SELECT [sk_Dash_PatientProgression_ExternalTransferStatus_Tiles]
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
      ,[accepted]
      ,[declined]
      ,[consult]
      ,[canceled]
      ,[epic_department_external]
      ,[Ethnicity]
      ,[FirstRace]
      ,[SecondRace]
      ,[AgeAtRequest]
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
      ,CASE WHEN [Accepting_MD] IS NOT NULL THEN 'Y' ELSE 'N' END AS Approving_MD_Documented
      ,[AcceptingMD_ServiceLine]
      ,[CloseTime]
      ,[PatientType]
      ,[ProtocolNme]
      ,[ProviderApproved]
      ,[PatientService]
      ,[Financial Approval]
      ,[Capacity Approval]
      ,[Physician Acceptance]
      ,[Canceled_By]
      ,[Referred_From_UVA_HEALTH]
      ,[Destination]
      ,[Destination_UVA_HEALTH]
      ,[FINANCIAL_CLASS]
      ,[EntryTimehhmmss]
      ,[Primary_Dx_on_Account]
      ,[Prim_Dx]
      ,[DRG]
      ,[DRG_NAME]
      ,[UVAMC_Admission_Instant]
      ,[UVAMC_Discharge_Instant]
      ,[Primary_DX_Block]
      ,[organization_name]
      ,[service_name]
      ,[clinical_area_name]
      ,[Load_Dtm]
      ,[First_Incoming_DTTM]
      ,[Last_Incoming_DTTM]
      ,[First_Outgoing_DTTM]
      ,[Last_Outgoing_DTTM]
      ,[OpenTime]
      ,[ResolutionTime]
      ,[XTRequestOpenToResolution]
      ,[XTRequestOpenToAcceptance]
      ,[Disch_Disp_Name]
      ,[TransferType]
      ,[incoming_transfer]
      ,[request_accepted_no_admission]
      ,[request_completed_no_admission]
      ,[request_canceled_admission_created]
  INTO #xtr
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]
  WHERE 1 = 1
  AND incoming_transfer = 1

  SELECT	
    event_count,
    provider_id,
    provider_name,
    accepted,
    declined,
    consult,
    canceled,
    DispositionReason,
    Transfer_Center_Request_Status,
    ProviderApproved,
    Accepting_Timestamp,
    Accepting_MD,
	Approving_MD_Documented,
    [Physician Acceptance],
    TransferType,
    incoming_transfer,
    request_accepted_no_admission,
    request_completed_no_admission,
    request_canceled_admission_created,
    Canceled_By
	--sk_Dash_PatientProgression_ExternalTransferStatus_Tiles,
 --   event_type,
 --   event_date,
 --   event_id,
 --   event_category,
 --   epic_department_id,
 --   epic_department_name,
 --   epic_department_name_external,
 --   fmonth_num,
 --   fyear_num,
 --   fyear_name,
 --   peds,
 --   transplant,
 --   oncology,
 --   App_Flag,
 --   sk_Dim_Pt,
 --   sk_Fact_Pt_Acct,
 --   sk_Fact_Pt_Enc_Clrt,
 --   sk_dim_physcn,
 --   person_birth_date,
 --   person_gender,
 --   person_id,
 --   person_name,
 --   prov_typ,
 --   hs_area_id,
 --   hs_area_name,
 --   pod_id,
 --   pod_name,
 --   rev_location_id,
 --   rev_location,
 --   som_group_id,
 --   som_group_name,
 --   som_department_id,
 --   som_department_name,
 --   som_division_id,
 --   som_division_name,
 --   financial_division_id,
 --   financial_division_name,
 --   financial_sub_division_id,
 --   financial_sub_division_name,
 --   w_hs_area_id,
 --   w_hs_area_name,
 --   w_pod_id,
 --   w_pod_name,
 --   w_rev_location_id,
 --   w_rev_location,
 --   w_som_group_id,
 --   w_som_group_name,
 --   w_som_department_id,
 --   w_som_department_name,
 --   w_som_division_id,
 --   w_som_division_name,
 --   w_financial_division_id,
 --   w_financial_division_name,
 --   w_financial_sub_division_id,
 --   w_financial_sub_division_name,
 --   epic_department_external,
 --   Ethnicity,
 --   FirstRace,
 --   SecondRace,
 --   AgeAtRequest,
 --   PAT_ENC_CSN_ID,
 --   EntryTime,
 --   AcctNbrint,
 --   TierLevel,
 --   Isolation,
 --   referringProviderName,
 --   Referring_Facility,
 --   TransferReason,
 --   TransferMode,
 --   Diagnosis,
 --   ServiceNme,
 --   LevelOfCare,
 --   TransferTypeHx,
 --   PlacementStatusName,
 --   XTPlacementStatusName,
 --   XTPlacementStatusDateTime,
 --   ETA,
 --   PatientReferredTo,
 --   AdtPatientFacilityID,
 --   AdtPatientFacility,
 --   BedAssigned,
 --   BedType,
 --   AcceptingMD_ServiceLine,
 --   CloseTime,
 --   PatientType,
 --   ProtocolNme,
 --   PatientService,
 --   [Financial Approval],
 --   [Capacity Approval],
 --   Referred_From_UVA_HEALTH,
 --   Destination,
 --   Destination_UVA_HEALTH,
 --   FINANCIAL_CLASS,
 --   EntryTimehhmmss,
 --   Primary_Dx_on_Account,
 --   Prim_Dx,
 --   DRG,
 --   DRG_NAME,
 --   UVAMC_Admission_Instant,
 --   UVAMC_Discharge_Instant,
 --   Primary_DX_Block,
 --   organization_name,
 --   service_name,
 --   clinical_area_name,
 --   Load_Dtm,
 --   First_Incoming_DTTM,
 --   Last_Incoming_DTTM,
 --   First_Outgoing_DTTM,
 --   Last_Outgoing_DTTM,
 --   OpenTime,
 --   ResolutionTime,
 --   XTRequestOpenToResolution,
 --   XTRequestOpenToAcceptance,
 --   Disch_Disp_Name,
  FROM #xtr
  WHERE 1 = 1
  AND (ProviderApproved IS NULL OR ProviderApproved = 'N')
  ORDER BY
	accepted DESC,
	canceled DESC,
	declined DESC

  SELECT	
    ProviderApproved,
	Approving_MD_Documented,
	COUNT(*) AS External_Transfer_Requests,
	SUM(accepted) AS Admitted,
	SUM(declined) AS Request_Status_Canceled_Flagged_As_Declined,
	SUM(canceled) AS Request_Status_Canceled_Flagged_As_Canceled,
	SUM(request_accepted_no_admission) AS Request_Status_Accepted_Not_Admitted,
	SUM(request_completed_no_admission) AS Request_Status_Completed_Not_Admitted
  FROM #xtr
  WHERE 1 = 1
  GROUP BY
	ProviderApproved,
	Approving_MD_Documented
  ORDER BY
	ProviderApproved DESC,
	Approving_MD_Documented DESC

  SELECT	
	*
  FROM #xtr
  WHERE 1 = 1
  AND ProviderApproved = 'Y'
  AND Approving_MD_Documented = 'Y'
  AND accepted = 0
  AND declined = 0
  AND canceled = 0
  AND request_accepted_no_admission = 0
  AND request_completed_no_admission = 0
  ORDER BY
	ProviderApproved DESC,
	Approving_MD_Documented DESC

