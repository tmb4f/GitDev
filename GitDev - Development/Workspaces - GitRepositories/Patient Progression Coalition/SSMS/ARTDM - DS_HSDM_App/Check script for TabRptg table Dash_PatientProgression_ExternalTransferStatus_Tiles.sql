USE DS_HSDM_App

IF OBJECT_ID('tempdb..#xtr ') IS NOT NULL
DROP TABLE #xtr

IF OBJECT_ID('tempdb..#itr ') IS NOT NULL
DROP TABLE #itr

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
  INTO #xtr
  FROM [DS_HSDM_APP].[TabRptg].[Dash_PatientProgression_ExternalTransferStatus_Tiles]

  SELECT
	--COUNT(*) AS 'Incoming_Transfer_Requests'
	*
  FROM #xtr xtr
  --WHERE TransferTypeHx = 'Incoming Transfer'
  WHERE person_id = 2258551
  ORDER BY
	xtr.person_id,
	xtr.event_date

  SELECT
	*
  INTO #itr
  FROM #xtr
  --WHERE TransferTypeHx = 'Incoming Transfer'
  --AND Transfer_Center_Request_Status = 'Canceled'

 -- SELECT
	--COUNT(*) AS Incoming_Transfer_Requests_Canceled
 -- FROM #itr
 -- --ORDER BY event_date

  SELECT
    itr.person_id,
	itr.event_date,
	itr.[PAT_ENC_CSN_ID],
	itr.[TransferTypeHx],
	itr. Referring_Facility,
	itr.Transfer_Center_Request_Status,
	itr.[accepted],
	itr.[declined],
	itr.[consult],
	itr.[canceled],
	enc.adm_DEPARTMENT_ID,
	enc.adm_mdm_epic_department,
	mdm.HOSPITAL_CODE,
	enc.adm_date_time,
	enc.dsch_date_time
  FROM #itr itr
  INNER JOIN DS_HSDW_App.Rptg.PatientDischargeByService_v2 enc
  ON itr.person_id = enc.MRN_int
  --LEFT OUTER JOIN DS_HSDW_App.Rptg.PatientDischargeByService_v2 enc
  --ON itr.person_id = enc.MRN_int
  LEFT OUTER JOIN
  (
  SELECT
	hst.EPIC_DEPARTMENT_ID,
    hst.HOSPITAL_CODE,
    hst.seq
  FROM
  (
  SELECT
	mdm.EPIC_DEPARTMENT_ID,
	mdm.HOSPITAL_CODE,
	mdm.Update_Dtm,
	ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY Update_Dtm DESC) AS seq
  FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL_History mdm
  ) hst
  WHERE hst.seq = 1
  ) mdm
  ON mdm.EPIC_DEPARTMENT_ID = enc.adm_DEPARTMENT_ID
  --WHERE DATEDIFF(DAY,itr.event_date,CAST(enc.adm_date_time AS DATE)) BETWEEN 0 AND 7
  WHERE itr.person_id = 2258551
  ORDER BY itr.person_id, itr.event_date, enc.adm_date_time