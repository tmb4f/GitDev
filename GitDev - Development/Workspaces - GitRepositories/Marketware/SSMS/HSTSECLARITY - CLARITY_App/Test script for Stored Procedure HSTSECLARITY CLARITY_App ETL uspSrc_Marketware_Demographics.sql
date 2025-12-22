USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

----exec [ETL].[uspSrc_Marketware_Demographics]
--CREATE PROCEDURE [ETL].[uspSrc_Marketware_Demographics]

--AS

SET NOCOUNT ON

/*******************************************************************************************
WHAT:	Physician demographic detail for Physician Relationship Management (PRM) application implementation project.
WHO :	Marketware
WHY :	Mercury Patient Engagement ingests a wide range of data and applies healthcare-specific, data science-based modeling and insights to build consumer/patient 360°
profiles that activate and inform consumer-driven, healthcare-oriented engagement strategies.
OWNER:	Tom Burgan (TMB4F)
SPEC:	[O:\Computing Services\INFSUP_S\Documentation\Projects\Mercury Healthcare]
--------------------------------------------------------------------------------------------
INPUTS FOR PROD:
OUTPUTS: 
   	1)	SEQUENCE:				Table 2 in File Layout Requirements
   	2)	FILE NAMING CONVENTION:	{Source}_{File Type}_{YYYYMMDD-YYYYMMDD} Ex. EPIC_VISITS_20140801-20150731 (append _TEST for files from TST)
   	3)	OUTPUT TYPE:			Pipe-delim ASCII text file
   	4)	TRANSFER METHOD:		sFTP
   	5)	OUTPUT LOCATION:
   	6)	FREQUENCY:				Monthly, 3rd of each month
   	7)	QUERY LOOKBACK PERIOD:
   	8)	FILE SPECIFIC NOTES:	
--------------------------------------------------------------------------------------------
MODS: 
	04/21/23 - tmb4f -	Initial creation per Marketware per PRM & Analytics Data Requirement
--------------------------------------------------------------------------------------------
INPUTS:
	CLARITY.dbo.CLARITY_DEP
	CLARITY_App.Rptg.vwRef_MDM_Location_Master
	CLARITY.dbo.CLARITY_DEP_2
	CLARITY.dbo.CLARITY_DEP_3
	CLARITY.dbo.CLARITY_DEP_ADDR
	CLARITY.dbo.ZC_STATE
	CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group
	CLARITY_App.Mapping.Epic_Dept_Groupers
	CLARITY_App.Mapping.Ref_Clinical_Area_Map
--------------------------------------------------------------------------------------------
OUTPUTS:
	[ETL].[uspSrc_Mercury_V8_Facility]
--------------------------------------------------------------------------------------------
RUNS:
TO DO:	

******************************************************************************************/
/*
Network Status
Taking New Patients
Website
Salutation
Ethnicity
*/

IF OBJECT_ID('tempdb..#facility ') IS NOT NULL
DROP TABLE #facility

DECLARE @FacilityType TABLE
(
mdm_DEPT_TYPE VARCHAR(100),
[Value] VARCHAR(10),
[Description] VARCHAR(100)
)

INSERT INTO @FacilityType
(
    mdm_DEPT_TYPE,
    Value,
    Description
)
VALUES
 ('EMERGENCY DEPARTMENT','1','Hospital')
,('HOSPITAL OUT-PATIENT DEPARTMENT','1','Hospital')
,('IN PATIENT','1','Hospital')
,('OUT PATIENT','6','Clinic')
,('UNKNOWN',NULL,'Unknown')
/*
  SELECT
	ser2.NPI																							AS			NPI,
	ser.PROV_ID																					AS			UniqueID,
	wd.wd_First_Name																			AS			FirstName,
	wd.wd_Middle_Name																		AS			MiddleName,
	wd.wd_Last_Name																			AS			LastName,
	CAST(NULL AS VARCHAR(50))														AS			Degree,
	CAST(NULL AS VARCHAR(20))														AS			Taxonomy,
	CASE WHEN zsp.NAME IS NULL THEN phys.Primary_Specialty2 ELSE zsp.NAME END 	AS			Specialty,
	CAST(NULL AS VARCHAR(200))													AS			SubSpecialty
*/
	SELECT DISTINCT
        ser2.NPI,
		ser.PROV_ID,
        ser.PROV_NAME,
        ser.PROV_TYPE,
		ser.PROVIDER_TYPE_C,
		zpt.NAME																						AS			PROV_TYPE_NAME,
        ser.PROV_ABBR,
        ser.GL_PREFIX,
        ser.RPT_GRP_ONE,
        ser.RPT_GRP_TWO,
        ser.RPT_GRP_THREE,
        ser.RPT_GRP_FOUR,
        ser.RPT_GRP_FIVE,
        ser.RPT_GRP_SIX,
        ser.RPT_GRP_SEVEN,
        ser.RPT_GRP_EIGHT,
        ser.RPT_GRP_NINE,
        ser.RPT_GRP_TEN,
        ser.IS_RESIDENT,
        ser.USER_ID,
        ser.EPIC_PROV_ID,
        ser.REFERRAL_SRCE_TYPE,
        ser.IS_VERIFIED_YN,
        ser.SER_REF_SRCE_ID,
        ser.UPIN,
        ser.SSN,
        ser.EMP_STATUS,
        ser.STAFF_RESOURCE,
        ser.CLINICIAN_TITLE,
        ser.EXTERNAL_NAME,
        ser.ACTIVE_STATUS,
        ser.REFERRAL_SOURCE_TYPE,
        ser.RECORD_TYPE,
        ser.BILL_PROV_YN,
        ser.BILL_UNDER_PROV_ID,
        ser.SUP_PROV_ID,
        ser.COUNTY_C,
        ser.COUNTRY_C,
        ser.OFFICE_PHONE_NUM,
        ser.OFFICE_FAX_NUM,
        ser.EMAIL AS ser_EMAIL,
        ser.DEA_NUMBER,
        ser.SEX,
        ser.BIRTH_DATE,
        ser.MEDICARE_PROV_ID,
        ser.MEDICAID_PROV_ID,
        ser.IS_PRIV_REVOKED,
        ser.NURSE_EMP_ID,
        ser.EPICCARE_PROV_YN,
        ser.MEDS_AUTH_PROV_YN,
        ser.ORDS_AUTH_PROV_YN,
        ser.TRANS_INTF_USER_YN,
        ser.PEER_REV_LAST_DATE,
        ser.TAKING_NEW_PAT_YN,
        ser.TAKING_WALKINS_YN,
        ser.LAST_RECOMMENDED_DATE,
        ser.BASE_COST,
        ser.SURG_REC_POOL_YN,
        ser.INSTRUMENT_TYPE_C,
        ser.EQUIP_SERVICE_DATE,
        ser.EQUIP_LASTSVC_DATE,
        ser.CLM_POS_REQD_YN,
        ser.DEFAULT_POS_CLM_YN,
        ser.MODALITY_TYPE_C,
        ser.MODALITY_YN,
        ser.SUPERV_POOL_ID,
        ser.SUPERV_POOL_NAME,
        ser.FLASH_CARD_PRT_ROU,
        ser.CTRL_SHEET_PRT_ROU,
        ser.PIN_ID,
        ser.PROV_ATTR_ID,
        ser.ATTND_PRIM_PAGER,
        ser.OO_OFFICE_FROM_DTE,
        ser.OO_OFFICE_TO_DTE,
        ser.DEF_DEPARTMENT_ID,
        ser.CM_PHY_OWNER_ID,
        ser.CM_LOG_OWNER_ID,
        ser.RPT_GRP_ELEVEN_C,
        ser.RPT_GRP_TWELVE_C,
        ser.RPT_GRP_THIRTEEN_C,
        ser.RPT_GRP_FOURTEEN_C,
        ser.RPT_GRP_FIFTEEN_C,
        ser.RPT_GRP_SIXTEEN_C,
        ser.RPT_GRP_SEVNTEEN_C,
        ser.RPT_GRP_EIGHTEEN_C,
        ser.RPT_GRP_NINETEEN_C,
        ser.RPT_GRP_TWENTY_C,
        ser.HOSPITALIST_YN,
        ser.DEF_DIVISION_C,
        ser.DEF_PROVIDER_YN,
        ser.PROV_REC_STATE_C,
        ser.PROV_START_DATE,
        ser.PRACTICE_NAME_C,
        ser.SURG_SCHED_OUT_YN,
        ser.SURG_EQP_SVCDAT_YN,
        ser.SURG_COST_TBL_ID,
        ser.TEAM_LEADER_ID,
        ser.TEAM_C,
        ser.SUP_PROV_YN,
        ser.EMPLOYED_CRNA_YN,
        ser.IS_INTERPRETER_YN,
        ser.DOCTORS_DEGREE,
        ser.REVENUE_DEPT_ID,
        ser.ENC_PROV_YN,
        ser.PHARMACIST_YN,
        ser.LAB_FAX_NUMBER,
        ser.PROV_PHOTO,
        ser.USE_DEPT_VT_LIM_YN,
        ser.VERIFYING_PERSON_ID,
        ser.OR_VLD_DT_OFST,
        ser.OR_CHARGE_CODE_ID,
        ser.DIRECTORY_INFO,
        ser.DBC_EXT_POS_ID,
        ser.RES_POOL_TYPE_C,
        ser.EDI_CLM_ACTIVE_YN,
        ser.PROV_CLM_PROC_STA_C,
        ser.PAYEE_NUM_DEFAULT,
        ser.SER_CLM_ID,
        ser.MCD_PROF_CD_C,
        ser.OP_ORD_PROV_YN,
        ser.IS_SUP_PROV_REQ_C,
        ser.EPRESCRIBING_YN,
        ser.EP_FLAG_YN,
        ser.SEX_C,
        ser.ACTIVE_STATUS_C,
        ser.REFERRAL_SOURCE_TYPE_C,
        ser.STAFF_RESOURCE_C,
        ser.REFERRAL_SRCE_TYPE_C,
        --ser2.PROV_ID,
        --ser2.CM_PHY_OWNER_ID,
        --ser2.CM_LOG_OWNER_ID,
        ser2.IP_ORD_PROV_YN,
        ser2.DEF_LETTER_PREF_C,
        ser2.DEF_CHART_STATN_ID,
        ser2.HOME_CITY,
        ser2.HOME_STATE_C,
        ser2.HOME_ZIP,
        ser2.PREVENT_REASGN_YN,
        ser2.LAB_PRINTER_ID,
        ser2.POS_DEV_TYP_C,
        ser2.CREATING_PATIENT_ID,
        ser2.REL_DT_OFST,
        ser2.REL_DT_OFST_TF_C,
        ser2.NON_PERSON_YN,
        ser2.SURG_AUTH_UPD_DTTM,
        ser2.PANEL_FACTOR,
        ser2.PANEL_WEIGHT,
        ser2.PANEL_STATUS_C,
        ser2.RESOURCE_TYPE_C,
        ser2.EPRESC_CNTRLD_YN,
        ser2.RES_SUP_PROV_ID,
        ser2.DICOM_AET_DEF_ID,
        ser2.GRP_OR_SITE_C,
        ser2.TPL_PROV_YN,
        ser2.AUTO_GEN_OR_TEMP_YN,
        ser2.OVRIDE_SYS_MEAS_YN,
        ser2.CREATED_ON_FLY_YN,
        ser2.OOO_POOL_HIP_ID,
        ser2.PREFRD_COMM_MTHD_C,
        ser2.RECV_ENCREP_POOL_ID,
        ser2.ENCREP_PROV_YN,
        ser2.MOD_CRT_FLMS_C,
        ser2.CREATING_USER_ID,
        ser2.RESIDENT_FOR_TRA_YN,
        ser2.IP_DEFAULT_TT_REL_C,
        ser2.INP_DISCIPLINE_ID,
        ser2.IGNORE_DEPT_ROUT_YN,
        ser2.EREFIL_MSG_POOL_ID,
        ser2.PLACE_OF_BIRTH,
        ser2.PAT_AGE_FROM,
        ser2.PAT_AGE_TO,
        ser2.AUTO_INT_RFL_APR_YN,
        ser2.AUTO_INT_RFL_AMT,
        ser2.AUTO_EXT_RFL_APR_YN,
        ser2.AUTO_EXT_RFL_AMT,
        ser2.RECRUITMENT_SRC_C,
        ser2.UTILIZTN_METRIC_C,
        ser2.UTILIZTN_COMMENT,
        ser2.DC_SENT_DATETIME,
        ser2.SURG_PRIMARY_SVC_C,
        ser2.COLL_RES_EXPR_YN,
        ser2.PAT_REVIEW_METRIC_C,
        ser2.PROV_GROUP_C,
        ser2.RECORD_CREATION_DT,
        ser2.REPLACEMNT_PROV_ID,
        ser2.ADMIN_ROLE_C,
        ser2.RSLT_ROUT_TYPE_C,
        ser2.TAP_CLMS_RESRC_YN,
        ser2.EPCS_ALLOW_SSN_YN,
        ser2.BRANCH_OF_SERVICE_C,
        ser2.ASGN_MIL_UNIT_ID,
        ser2.MILITARY_RANK_C,
        ser2.CUR_CRED_C,
        ser2.IS_RESIDENT_C,
        ser2.INP_LICENSURE_C,
        ser2.ALLOW_REFER_TO_YN,
        ser2.SERVICE_DEFAULT_C,
        ser2.PECOS_STATUS_YN,
        ser2.DBC_DFLT_RFL_SA_ID,
        ser2.UNVERIFIED_REASON_C,
        ser2.ALT_ID,
        ser2.REL_DT_PAST_TMPL_YN,
        ser2.DC_CAN_RESEND_YN,
        ser2.PRIMARY_DEPT_ID,
        ser2.NOTE_SERVICE_DEFAULT_C,
        ser2.A_PLACE_YN,
        ser2.GENERIC_YN,
        ser2.INSTANT_OF_UPDATE_DTTM,
        ser2.AUTH_ALL_LOCS_YN,
        ser2.TR_SKIP_SAT_YN,
        ser2.TR_SKIP_SUN_YN,
        ser2.TR_SKIP_HOL_YN,
        ser2.RELEASE_TIME,
        ser2.PALL_CARE_PROV_YN,
        ser2.PROFESSIONAL_GRP_C,
        ser2.DUTCH_AGB_CODE,
        ser2.UDS_PROV_TYPE_C,
        ser2.OFFICE_1_RAR_FAX,
        ser2.MIPS_EC_YN,
        ser2.MIPS_IMG_ENC_C,
        ser2.MIPS_QM_METHOD_C,
        ser2.APPT_TIME_TBD_YN,
        ser2.APPT_TBD_RECALC_YN,
        ser2.ANES_SVC_PROV_GRP_C,
        ser2.ADT_ADMT_PROVIDER_YN,
        ser2.ADT_ATTN_PROVIDER_YN,
        ser2.PROCEDURAL_ROOM_YN,
        phys.IDNumber,
        phys.ProviderGroup,
        phys.DisplayName,
        phys.LastName,
        phys.Division,
        phys.FirstName,
        phys.MI,
        phys.ProviderType,
        phys.UVaID,
        phys.NPINumber,
        phys.Status,
        phys.ActivePriv,
        phys.EmployedBy,
        phys.Primary_Specialty,
        phys.Secondary_Specialty,
        phys.Third_Specialty,
        phys.CSO_Dept_Designated,
        phys.CSO_Category,
        phys.CSO_Gender,
        phys.Service_Line,
        phys.DOB,
        phys.Email AS phys_Email,
        phys.Field_of_Licensure,
        phys.Primary_Specialty2,
        phys.ServiceLine_Division,
        --sersp.PROV_ID,
        sersp.SPECIALTY_C,
        zsp.SPECIALTY_DEP_C,
        zsp.NAME AS SPECIALTY_DEP_NAME,
        --zsp.TITLE,
        --zsp.ABBR,
        --zsp.INTERNAL_ID,
        zpt.PROV_TYPE_C--,
        --zpt.TITLE,
        --zpt.ABBR,
        --zpt.INTERNAL_ID
/*
        prov.dim_physcn_NPINumber								AS NPI,
		--prov.sk_Ref_Crosswalk_HSEntity_Prov,
        prov.sk_Dim_Physcn,
        prov.sk_Dim_UPG_Prov,
        prov.dim_Physcn_PROV_ID,
        prov.PROV_ID,
        prov.Prov_UHC_Spec_Cde,
        prov.Prov_UHC_Spec_Nme,
        prov.Clrt_Financial_Division,
        prov.Clrt_Financial_Division_Name,
        prov.Clrt_Financial_SubDivision,
        prov.Clrt_Financial_SubDivision_Name,
        prov.dim_physcn_Source_descr,
        prov.dim_physcn_ProviderGroup,
        prov.dim_physcn_DisplayName,
        prov.wd_Employee_Name,
        prov.dim_physcn_DEPT,
        prov.dim_physcn_Division,
        prov.dim_physcn_ProviderType,
        prov.dim_physcn_UVaID,
        prov.wd_Computing_ID,
        prov.dim_physcn_Status,
        prov.dim_physcn_Primary_Specialty,
        prov.dim_physcn_Secondary_Specialty,
        prov.dim_physcn_Third_Specialty,
        prov.dim_physcn_Service_Line,
        prov.cw_Legacy_src_system,
        prov.cw_Legacy_Src_Employee_ID,
        prov.wd_Employee_ID,
        prov.Is_Worker_Active,
        prov.wd_sk_Hire_Date,
        prov.wd_sk_Termination_Date,
        prov.cw_Legacy_src_Position_ID,
        prov.wd_Position_ID,
        prov.wd_Is_Position_Closed,
        prov.wd_Is_Primary_Job,
        prov.wd_sk_Earliest_Hire_Date,
        prov.wd_Is_Position_Active,
        prov.cw_Legacy_SRC_JOB_ID,
        prov.wd_Job_ID,
        prov.wd_Job_Title,
        prov.wd_Job_Posting_Title,
        prov.wd_Department_ID,
        prov.wd_Department_Code,
        prov.wd_Dept_Code,
        prov.wd_Department_Name,
        prov.wd_Supervisory_Organization_ID,
        prov.wd_Supervisory_Organization_Code,
        prov.wd_Supervisory_Organization_Name,
        prov.wd_company_Organization_ID,
        prov.wd_company_Organization_Code,
        prov.wd_company_Organization_Name,
        prov.Load_Dtm,
        prov.Som_DEPT_ID,
        prov.PS_service_line_id,
        prov.PS_corp_service_line_id,
        prov.PS_opnl_service_line_id,
        prov.PS_service_line_name,
        prov.PS_corp_service_line_name,
        prov.PS_opnl_service_line_name
*/
/*
  SELECT DISTINCT
	--mdmhg.HOSPITAL_CODE,
	ser2.NPI																							AS			NPI,
	wd.dim_physcn_NPINumber,
	--serdep.LINE,
	ser.PROV_ID																					AS			UniqueID,
	wd.PROV_ID AS wd_PROV_ID,
	ser.PROV_NAME,
	ser.EXTERNAL_NAME,
	--wd.wd_First_Name																			AS			FirstName,
	--wd.wd_Middle_Name																		AS			MiddleName,
	--wd.wd_Last_Name																			AS			LastName,
	wd.wd_Employee_Name,
    phys.DisplayName,
    phys.LastName AS phys_LastName,
    phys.FirstName AS phys_FirstName,
    phys.MI,
	wd.dim_physcn_DisplayName,
	--CAST(NULL AS VARCHAR(50))														AS			Degree,
	--CAST(NULL AS VARCHAR(20))														AS			Taxonomy,
	zsp.NAME																						AS			Specialty,
	--CAST(NULL AS VARCHAR(200))													AS			SubSpecialty,
    phys.Primary_Specialty,
    phys.Secondary_Specialty,
    phys.Third_Specialty,
    phys.Primary_Specialty2,
	wd.dim_physcn_Primary_Specialty,
	wd.dim_physcn_Secondary_Specialty,
	wd.dim_physcn_Third_Specialty,
	wd.Prov_UHC_Spec_Cde,
	wd.Prov_UHC_Spec_Nme,
	ser.PROVIDER_TYPE_C,
	zpt.NAME																						AS			PROV_TYPE_NAME,
    phys.ProviderGroup,
	wd.dim_physcn_ProviderGroup,
    phys.ProviderType,
	wd.dim_physcn_ProviderType,
    phys.Division,
	wd.dim_physcn_Division,
	wd.wd_Department_ID,
	wd.wd_Department_Code,
	wd.wd_Dept_Code,
/*
    CONVERT(VARCHAR(10),dep.DEPARTMENT_ID)						AS			facility_code,
	dep.DEPARTMENT_NAME,
	----CAST(NULL AS VARCHAR(10))														AS			ccn,
	----CAST(NULL AS VARCHAR(10))														AS			npi,
	CASE WHEN dep.DEPARTMENT_NAME LIKE '%HOME%' THEN 'Home Health'
	           WHEN  mdm.EPIC_DEPT_TYPE <> 'UNKNOWN' THEN ftype.[Description]
			   WHEN dep.DEPARTMENT_NAME LIKE '%UVHE%' THEN 'Hospital'
			   WHEN dep.DEPARTMENT_NAME LIKE '% INP %' THEN 'Hospital'
			   ELSE 'Clinic'
	 END																								AS			facility_type,
	mdm.EPIC_DEPT_TYPE,
	mdm.EPIC_SPCLTY,
	mdm.SERVICE_LINE AS mdm_SERVICE_LINE,
    dep.EXTERNAL_NAME																	AS			facility_name,
	serdep.TAKE_NEW_PAT_DEPT_YN,
*/
	--ROW_NUMBER() OVER(PARTITION BY NPI ORDER BY LINE) AS           LINE_seq,
    --addr.ADDRESS																				AS			address_line_1,
    --addr2.ADDRESS																				AS			address_line_2,
    --dep2.ADDRESS_CITY																	AS			city,
	--zs.ABBR																							AS			[state],
    --dep2.ADDRESS_ZIP_CODE															AS			postal_code,
	--pos.ADDRESS_LINE_1 AS pos_ADDRESS_LINE_1,
	--pos.ADDRESS_LINE_2 AS pos_ADDRESS_LINE_2,
	--pos.CITY AS pos_CITY,
	--pos.STATE_C AS pos_STATE_C,
	--wd.Work_Address_Line_1,
	--wd.Work_Address_Line_2,
	--wd.Work_City,
	--wd.Work_State,
	--wd.Work_Postal_Code,
	--dep2.ADDRESS_CITY																	AS			facility_group_level_1,
 --   dep.PHONE_NUMBER																	AS			phone_1,
	--CAST(NULL AS VARCHAR(10))														AS			phone_2,
	--CAST(NULL AS VARCHAR(10))														AS			phone_3,
	--dep3.FAX_NUM																				AS			fax,
	--pos.PHONE AS pos_PHONE,
	--pos.FAX_NUM AS pos_FAX_NUM,
	--CAST(NULL AS VARCHAR(100))													AS			[url],
	--pos.POS_NAME_ABBR,
	--pos.POS_NAME,
	--pos.POS_TYPE,
	--pos.RPT_GRP_ONE,
	--pos.POS_ID,
	--pos.PO_MEDICARE_NUM AS pos_PO_MEDICARE_NUM,
	--wd.wd_Supervisory_Organization_Description,
	wd.wd_Supervisory_Organization_Name,
	wd.wd_Department_Name,
	--wd.wd_Department_Description,
	wd.wd_company_Organization_Code,
	wd.wd_company_Organization_Name,
	wd.PS_service_line_name,
	wd.PS_opnl_service_line_name,
	wd.PS_corp_service_line_name,
 --   mdmhg.LOC_RPT_GRP_NINE_NAME											AS			facility_group_level_2,
	--COALESCE(c.clinical_area_name,'No Clinical Area Assigned')		AS			facility_group_level_3,
	--s.service_name,
	--o.organization_name,
	--mdmhg.REV_LOC_NAME,
	phys.IDNumber,
    phys.UVaID,
    phys.Status,
    phys.ActivePriv,
	phys.EmployedBy,
    phys.CSO_Dept_Designated,
    phys.CSO_Category,
    phys.CSO_Gender,
    phys.Service_Line,
    phys.DOB,
    phys.Email,
    phys.Field_of_Licensure,
    phys.ServiceLine_Division--,
    --phys.Culpeper_DEPT,
    --phys.Culpeper_Division,
    --phys.Culpeper_Employed_By,
    --phys.Culpeper_DEPTDESIG,
    --phys.Culpeper_CATEGORY,
    --phys.Culpeper_STATUS,
    --phys.Culpeper_STATUSASOF,
    --phys.Culpeper_FROM,
    --phys.Culpeper_TO,
    --phys.Culpeper_Entity_Title,
    --phys.Haymarket_DEPT,
    --phys.Haymarket_Division,
    --phys.Haymarket_Employed_By,
    --phys.Haymarket_DEPTDESIG,
    --phys.Haymarket_CATEGORY,
    --phys.Haymarket_STATUS,
    --phys.Haymarket_STATUSASOF,
    --phys.Haymarket_FROM,
    --phys.Haymarket_TO,
    --phys.Haymarket_Entity_Title,
    --phys.Prince_William_DEPT,
    --phys.Prince_William_Division,
    --phys.Prince_William_Employed_By,
    --phys.Prince_William_DEPTDESIG,
    --phys.Prince_William_CATEGORY,
    --phys.Prince_William_STATUS,
    --phys.Prince_William_STATUSASOF,
    --phys.Prince_William_FROM,
    --phys.Prince_William_TO,
    --phys.Prince_William_Entity_Title
*/
/*
  SELECT DISTINCT
	--ser.PROVIDER_TYPE_C,
	--mdmhg.REV_LOC_NAME,
	mdmhg.HOSPITAL_CODE,
	zpt.NAME																						AS			PROV_TYPE_NAME,
    phys.ProviderGroup--,
    --phys.ProviderType
*/
  INTO #facility

  FROM [CLARITY].[dbo].CLARITY_SER ser
  INNER JOIN [CLARITY].[dbo].CLARITY_SER_2 ser2
  --LEFT OUTER JOIN [CLARITY].[dbo].CLARITY_SER_2 ser2
  ON ser.PROV_ID = ser2.PROV_ID
/*
  INNER JOIN
  --LEFT OUTER JOIN
  (
  SELECT DISTINCT
	PROV_ID,
	DEPARTMENT_ID,
	LINE,
	TAKE_NEW_PAT_DEPT_YN
  FROM [CLARITY].[dbo].[CLARITY_SER_DEPT]
  WHERE INACT_CAD_DEPT_YN = 'N'
  ) serdep
  ON ser.PROV_ID = serdep.PROV_ID
*/
  LEFT OUTER JOIN
  (
  SELECT
	--sk_Dim_Physcn,
    IDNumber,
    --current_flag,
    --Staff_checksum,
    --insertdate,
    --lastupdate,
    --beg_date,
    --end_date,
    --source_descr,
    --Posted,
    --PostYrMo,
    ProviderGroup,
    DisplayName,
    LastName,
    --DEPT,
    --full_DEPT,
    Division,
    FirstName,
    MI,
    ProviderType,
    UVaID,
    NPINumber,
    --HCFA_UPIN_#,
    Status,
    --ResignationDate,
    --AppointmentDate,
    --ReappointmentDate,
    --Date_Reappointment,
    --OldIDNumber,
    --VASC_privelage,
    --DivisionGroup,
    --DivisionGroup_Cd,
    --DWProd_Physcn_sk,
    --ETL_guid,
    --load_dte,
    --Physcn_Src,
    --UVaID_9Digit,
    ActivePriv,
    --ResignStatus,
    --NotUVaQualDataRpts,
    EmployedBy,
    --Type_of_HSF_Contract,
    Primary_Specialty,
    Secondary_Specialty,
    Third_Specialty,
    --InitialPrivDate,
    --PQD_access,
    --UpDte_Dtm_CSO,
    --UpDte_Dtm_CSO_Pstd,
    CSO_Dept_Designated,
    CSO_Category,
    --CSO_Sts_Dte,
    --CSO_Appt_Beg,
    --CSO_Appt_End,
    CSO_Gender,
    Service_Line,
    --Primary_Supervisor_Id,
    --Primary_Supervisor_Name,
    --Alternate_Supervisor_Id,
    --Alternate_Supervisor_Name,
    DOB,
    Email,
    Field_of_Licensure,
    Primary_Specialty2,
    ServiceLine_Division--,
    --Culpeper_DEPT,
    --Culpeper_Division,
    --Culpeper_Employed_By,
    --Culpeper_DEPTDESIG,
    --Culpeper_CATEGORY,
    --Culpeper_STATUS,
    --Culpeper_STATUSASOF,
    --Culpeper_FROM,
    --Culpeper_TO,
    --Culpeper_Entity_Title,
    --Haymarket_DEPT,
    --Haymarket_Division,
    --Haymarket_Employed_By,
    --Haymarket_DEPTDESIG,
    --Haymarket_CATEGORY,
    --Haymarket_STATUS,
    --Haymarket_STATUSASOF,
    --Haymarket_FROM,
    --Haymarket_TO,
    --Haymarket_Entity_Title,
    --Prince_William_DEPT,
    --Prince_William_Division,
    --Prince_William_Employed_By,
    --Prince_William_DEPTDESIG,
    --Prince_William_CATEGORY,
    --Prince_William_STATUS,
    --Prince_William_STATUSASOF,
    --Prince_William_FROM,
    --Prince_William_TO,
    --Prince_William_Entity_Title
  FROM [CLARITY_App].dbo.Dim_Physcn
  WHERE current_flag =  1
  AND NPINumber IS NOT NULL
  AND NPINumber NOT IN ('-1','-2','-3','-4','-5','-6','0','Unknown')
  AND Status = 'Active'
  ) phys
  ON phys.NPINumber = ser2.NPI
/*
  --INNER JOIN [CLARITY].[dbo].CLARITY_EMP emp
 LEFT OUTER JOIN [CLARITY].[dbo].CLARITY_EMP emp
  ON ser.USER_ID = emp.USER_ID
  LEFT JOIN [CLARITY_App].[dbo].[Ref_Crosswalk_All_Workers] wd
  ON wd.UVA_Computing_ID = emp.SYSTEM_LOGIN
	AND wd.wd_Is_Primary_Job = 1
	AND wd.wd_Is_Position_Active = 1
*/
/*
  LEFT JOIN [CLARITY_App].[dbo].[Ref_Crosswalk_HSEntity_Prov] wd
  ON wd.PROV_ID = ser.PROV_ID
	AND wd.wd_Is_Primary_Job = 1
	AND wd.wd_Is_Position_Active = 1
*/
  LEFT JOIN
  (
  SELECT DISTINCT
	PROV_ID,
	SPECIALTY_C
  FROM [CLARITY].[dbo].[CLARITY_SER_SPEC]
  WHERE LINE = 1
  ) sersp
  ON sersp.PROV_ID = ser.PROV_ID
  --LEFT OUTER JOIN [CLARITY].[dbo].[CLARITY_DEP] dep
  --ON dep.DEPARTMENT_ID = serdep.DEPARTMENT_ID
  --LEFT OUTER JOIN [CLARITY].[dbo].CLARITY_POS pos
  --ON pos.POS_ID = dep.REV_LOC_ID
  ----INNER JOIN [CLARITY_App].Rptg.vwRef_MDM_Location_Master mdm
  --LEFT OUTER JOIN [CLARITY_App].Rptg.vwRef_MDM_Location_Master mdm
  --ON mdm.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
  ----INNER JOIN [CLARITY].[dbo].[CLARITY_DEP_2] dep2
  --LEFT OUTER JOIN [CLARITY].[dbo].[CLARITY_DEP_2] dep2
  --ON dep2.DEPARTMENT_ID = dep.DEPARTMENT_ID
  ----INNER JOIN [CLARITY].[dbo].[CLARITY_DEP_3] dep3
  --LEFT OUTER JOIN [CLARITY].[dbo].[CLARITY_DEP_3] dep3
  --ON dep3.DEPARTMENT_ID = dep.DEPARTMENT_ID
  --LEFT OUTER JOIN @FacilityType ftype
  --ON ftype.mdm_DEPT_TYPE = mdm.EPIC_DEPT_TYPE
  --LEFT OUTER JOIN
  --(
  --SELECT
  --       DEPARTMENT_ID,
  --       ADDRESS
  --FROM [CLARITY].[dbo].CLARITY_DEP_ADDR
  --WHERE LINE = 1
  --) addr
  --ON addr.DEPARTMENT_ID = dep.DEPARTMENT_ID
  --LEFT OUTER JOIN
  --(
  --SELECT
  --       DEPARTMENT_ID,
  --       ADDRESS
  --FROM [CLARITY].[dbo].CLARITY_DEP_ADDR
  --WHERE LINE = 2
  --) addr2
  --ON addr2.DEPARTMENT_ID = dep.DEPARTMENT_ID

  LEFT OUTER JOIN [CLARITY].[dbo].[ZC_SPECIALTY_DEP] zsp
  ON zsp.SPECIALTY_DEP_C = sersp.SPECIALTY_C
  LEFT OUTER JOIN [CLARITY].[dbo].[ZC_PROV_TYPE] zpt
  ON zpt.PROV_TYPE_C = ser.PROVIDER_TYPE_C
  --LEFT OUTER JOIN CLARITY.dbo.ZC_STATE zs
  --ON zs.STATE_C = dep2.ADDRESS_STATE_C
  --LEFT OUTER JOIN [CLARITY_App].Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhg
  --ON mdmhg.EPIC_DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID

  --LEFT OUTER JOIN CLARITY_App.Mapping.Epic_Dept_Groupers g
  --ON g.epic_department_id = dep.DEPARTMENT_ID
  --LEFT OUTER JOIN CLARITY_App.Mapping.Ref_Clinical_Area_Map c
  --ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
  
  --LEFT OUTER JOIN CLARITY_App.Mapping.Epic_Dept_Groupers g
  --ON g.epic_department_id = dep.DEPARTMENT_ID
  --LEFT OUTER JOIN CLARITY_App.Mapping.Ref_Clinical_Area_Map c
  --ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
  --LEFT OUTER JOIN CLARITY_App.Mapping.Ref_Service_Map s
  --ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
  --LEFT OUTER JOIN CLARITY_App.Mapping.Ref_Organization_Map o
  --ON o.organization_id = s.organization_id

/*
	FROM CLARITY_App.dbo.Ref_Crosswalk_HSEntity_Prov prov
	WHERE prov.dim_physcn_Status = 'Active'
	AND prov.dim_physcn_NPINumber IS NOT NULL
	AND prov.wd_Is_Primary_Job = 1
	AND prov.wd_Is_Position_Active = 1
*/

  WHERE 1 = 1
  AND ser.ACTIVE_STATUS = 'Active'
  AND ser.STAFF_RESOURCE = 'Person'
  AND ser2.NPI IS NOT NULL

  SELECT
	*
/*
		--PROVIDER_TYPE_C,
		--REV_LOC_NAME,
		HOSPITAL_CODE,
        PROV_TYPE_NAME,
        ProviderGroup--,
        --ProviderType
*/

  FROM #facility

  --GROUP BY Specialty,
  --       SubSpecialty,
  --       Primary_Specialty,
  --       Secondary_Specialty,
  --       Third_Specialty
	
  --ORDER BY HOSPITAL_CODE, NPI, LINE	
  --ORDER BY NPI, HOSPITAL_CODE, LINE
  --ORDER BY NPI
  ORDER BY PROV_NAME
  --ORDER BY PROV_TYPE_NAME
  --ORDER BY HOSPITAL_CODE, ProviderGroup, PROV_TYPE_NAME
  --ORDER BY HOSPITAL_CODE, PROV_TYPE_NAME, ProviderGroup

 -- SELECT-- DISTINCT
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[facility_code]),'') AS facility_code,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[ccn]),'') AS ccn,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[npi]),'') AS npi,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[facility_type]),'') AS facility_type,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[facility_name]),'') AS facility_name,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[address_line_1]),'') AS address_line_1,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[address_line_2]),'') AS address_line_2,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[city]),'') AS city,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[state]),'') AS [state],
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[postal_code]),'') AS postal_code,
	--ISNULL(CONVERT(VARCHAR(100),REPLACE([fclty].[phone_1],'-','')),'') AS phone_1,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[phone_2]),'') AS phone_2,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[phone_3]),'') AS phone_3,
	--ISNULL(CONVERT(VARCHAR(100),REPLACE([fclty].[fax],'-','')),'') AS fax,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[url]),'') AS url,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[facility_group_level_1]),'') AS facility_group_level_1,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[facility_group_level_2]),'') AS facility_group_level_2,
	--ISNULL(CONVERT(VARCHAR(100),[fclty].[facility_group_level_3]),'') AS facility_group_level_3
  
 -- FROM #facility fclty

 -- ORDER BY facility_group_level_1, facility_group_level_2, facility_group_level_3, facility_name

GO

