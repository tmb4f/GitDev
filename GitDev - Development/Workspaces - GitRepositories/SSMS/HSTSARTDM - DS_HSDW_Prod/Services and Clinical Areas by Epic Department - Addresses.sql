USE DS_HSDW_Prod

DECLARE @Organization TABLE (
	[organization_id] [INT] NOT NULL
   ,[organization_name] VARCHAR(150) NOT NULL)

DECLARE @Service TABLE (
	[sk_Ref_Service_Map] [INT] NOT NULL
   ,[service_name] VARCHAR(150) NOT NULL)

DECLARE @ClinicalArea TABLE (
	[sk_Ref_Clinical_Area_Map] [INT] NOT NULL
   ,[clinical_area_name] VARCHAR(150) NOT NULL)

INSERT INTO @Organization
(
    organization_id,
	organization_name
)
--SELECT Param AS organization_id FROM ETL.fn_ParmParse(@OrganizationId, ',')

--VALUES
--(1)
--,(2)
--,(3)
--,(4)
--,(5)
--,(6)
--,(7)
--,(8)
--,(9)
--,(10)
--,(11)
--,(12)
--,(13)
--,(14)
--,(15)
--,(16)
--,(17)
--,(18)
--,(19)
--,(20)
--,(21)
--,(22)
--,(23)
--,(999) -- No Organization Assigned
--;

--VALUES
--(999
--,'No Organization Assigned'
--)
--;

VALUES
 (1,'University Inpatient Adult')
,(2,'University Emergency Services')
,(3,'Children''s Hospital')
,(4,'University Medical Center Ambulatory')
,(5,'University Medical Center Operations')
,(6,'Service Lines')
,(7,'UPG')
,(8,'Other Ambulatory Services')
,(9,'Access Management')
,(10,'Haymarket Inpatient')
,(11,'Haymarket Emergency Services')
,(12,'Haymarket Medical Center Operations')
,(13,'Community Health Medical Group')
,(15,'Prince William Inpatient')
,(16,'Prince William Emergency Services')
,(17,'Prince William Medical Center Operations')
,(18,'Culpeper Inpatient')
,(19,'Culpeper Emergency Services')
,(20,'Culpeper Medical Center Operations')
,(21,'CHMG-Primary Care')
,(22,'CHMG-Specialty')
,(23,'CHMG-Surgical Services')
,(999,'No Organization Assigned')
;

--SELECT * FROM @Organization ORDER BY organization_id

INSERT INTO @Service
(
    sk_Ref_Service_Map,
   [service_name]
)
SELECT DISTINCT ids.sk_Ref_Service_Map, [service].[service_name]
FROM
(
	SELECT DISTINCT [organization].organization_id, COALESCE([service].sk_Ref_Service_Map, 43) AS sk_Ref_Service_Map
	FROM DS_HSDM_App.Mapping.Ref_Service_Map [service]
	INNER JOIN @Organization organization
	ON [service].organization_id = organization.organization_id
) ids
LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map [service]
ON [service].sk_Ref_Service_Map = ids.sk_Ref_Service_Map
ORDER BY 1,2;

--SELECT * FROM @Service ORDER BY sk_Ref_Service_Map

	INSERT INTO @ClinicalArea
	(
	    sk_Ref_Clinical_Area_Map,
	    clinical_area_name
	)
SELECT DISTINCT ids.sk_Ref_Clinical_Area_Map, clinical_area.clinical_area_name
FROM
(
	SELECT DISTINCT [service].sk_Ref_Service_Map, COALESCE(clinical_area.sk_Ref_Clinical_Area_Map,165) AS sk_Ref_Clinical_Area_Map
	FROM DS_HSDM_App.Mapping.Ref_Clinical_Area_Map clinical_area
	INNER JOIN @Service [service]
	ON clinical_area.sk_Ref_Service_Map = [service].sk_Ref_Service_Map
) ids

INNER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map clinical_area
ON ids.sk_Ref_Clinical_Area_Map = clinical_area.sk_Ref_Clinical_Area_Map
ORDER BY 1;

--SELECT * FROM @ClinicalArea ORDER BY sk_Ref_Clinical_Area_Map

--DECLARE @ClinicalArea TABLE (
--	[sk_Ref_Clinical_Area_Map] [int])

--INSERT INTO @ClinicalArea
--(
--    sk_Ref_Clinical_Area_Map
--)
--SELECT Param AS sk_Ref_Clinical_Area_Map FROM ETL.fn_ParmParse(@in_skrefclinicalareamap, ',')

SELECT DISTINCT
 org.organization_id
,org.organization_name
,org.service_id
,org.[service_name]
,org.clinical_area_id
,org.clinical_area_name
,mdm.EPIC_DEPARTMENT_ID
,mdm.EPIC_DEPT_NAME
,mdm.EPIC_EXT_NAME
,[add].DEPt_Addr_Street_corr AS Department_Address_Street
--,[add].DEPt_Addr_Street_corr_line2
--,[add].DEPt_Addr_Cty_corr
--,dep.Clrt_DEPt_Addr_Cty
,COALESCE([add].DEPt_Addr_Cty_corr,dep.Clrt_DEPt_Addr_Cty) AS Department_Address_City
,dep.Clrt_DEPt_Addr_Zip AS Department_Address_Zipcode
,dep.Clrt_DEPt_Addr_St AS Department_Address_State
,dep.Clrt_DEPt_Loctn_Nme AS Department_Location
,grouper.ambulatory_flag
,grouper.childrens_flag
,grouper.childrens_ambulatory_name
,grouper.mc_ambulatory_name
,grouper.ambulatory_operation_name
,grouper.childrens_name
,grouper.serviceline_division_name
,grouper.mc_operation_name
,grouper.inpatient_adult_name
,mdm.EPIC_DEPT_TYPE
,mdm.EPIC_SPCLTY
,mdm.FINANCE_COST_CODE
,mdm.PEOPLESOFT_NAME
,mdm.SERVICE_LINE
,mdm.SUB_SERVICE_LINE
,mdm.OPNL_SERVICE_NAME
,mdm.CORP_SERVICE_LINE
,mdm.RL_LOCATION [RL_LOCATION_BESAFE]
,COALESCE(mdm.LOC_ID,'0')  LOC_ID 
,COALESCE(mdm.REV_LOC_NAME,'Null') REV_LOC_NAME
,mdm.A2K3_NAME
,mdm.A2K3_CLINIC_CARE_AREA_DESCRIPTION
,mdm.AMB_PRACTICE_GROUP
,mdm.HS_AREA_ID
,COALESCE(REPLACE(mdm.HS_AREA_NAME,'upg','Null'),'Null') HS_AREA_NAME  --- some have null string
,mdm.TJC_FLAG
,mdm.NDNQI_NAME
,mdm.NHSN_NAME
,mdm.PRACTICE_GROUP_NAME
,mdm.PRESSGANEY_NAME
,mdm.DIVISION_DESC
,mdm.ADMIN_DESC
,mdm.BUSINESS_UNIT
,mdm.RPT_RUN_DT
,mdm.PFA_POD
,mdm.HUB
,mdm.PBB_POD
,mdm.PG_SURVEY_DESIGNATOR
,grouper.UPG_PRACTICE_FLAG
,grouper.UPG_PRACTICE_REGION_NAME
,grouper.UPG_PRACTICE_ID
,grouper.UPG_PRACTICE_NAME
,mdm.[HOSPITAL_CODE]
,mdm.[LOC_RPT_GRP_NINE_NAME]
,grouper.community_health_flag
,CASE WHEN mdmcurr.EPIC_DEPARTMENT_ID IS NULL THEN 1 ELSE 0 END AS deleted_flag

FROM
(
SELECT DISTINCT ids.EPIC_DEPARTMENT_ID, ids.sk_Ref_Clinical_Area_Map, ids.clinical_area_id, ids.clinical_area_name, ids.sk_Ref_Service_Map, ids.service_id, ids.[service_name], ids.organization_id, COALESCE(organization.organization_name,'Unmapped') AS organization_name
FROM
(
SELECT DISTINCT ca.EPIC_DEPARTMENT_ID, ca.sk_Ref_Clinical_Area_Map, ca.clinical_area_id, ca.clinical_area_name, ca.sk_Ref_Service_Map, COALESCE([service].service_id,0) AS service_id, COALESCE([service].[service_name], 'Unmapped') AS [service_name], COALESCE([service].organization_id,0) AS organization_id
FROM
(
SELECT DISTINCT mapping.EPIC_DEPARTMENT_ID, mapping.sk_Ref_Clinical_Area_Map, COALESCE(clinical_area.clinical_area_id,0) AS clinical_area_id, COALESCE(clinical_area.clinical_area_name,'Unmapped') AS clinical_area_name, COALESCE(clinical_area.sk_Ref_Service_Map,0) AS sk_Ref_Service_Map
FROM
(
SELECT DISTINCT mdm.EPIC_DEPARTMENT_ID, COALESCE(grouper.sk_Ref_Clinical_Area_Map,0) AS sk_Ref_Clinical_Area_Map
FROM
(
SELECT
	mdmhx.EPIC_DEPARTMENT_ID
FROM
(
SELECT
    EPIC_DEPARTMENT_ID,
	MDM_BATCH_ID,
	ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History
) mdmhx
WHERE mdmhx.seq =1
) mdm
LEFT JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers grouper
ON grouper.EPIC_DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID
) mapping
LEFT JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map clinical_area
ON mapping.sk_Ref_Clinical_Area_Map = clinical_area.sk_Ref_Clinical_Area_Map
) ca
LEFT JOIN DS_HSDM_App.Mapping.Ref_Service_Map [service]
ON ca.sk_Ref_Service_Map = [service].sk_Ref_Service_Map
) ids
LEFT JOIN DS_HSDM_App.Mapping.Ref_Organization_Map organization
ON organization.organization_id = ids.organization_id
) org
LEFT OUTER JOIN
(
SELECT
	mdmhx.EPIC_DEPARTMENT_ID,
    mdmhx.EPIC_DEPT_NAME,
    mdmhx.EPIC_EXT_NAME,
    mdmhx.EPIC_DEPT_TYPE,
    mdmhx.EPIC_SPCLTY,
    mdmhx.FINANCE_COST_CODE,
    mdmhx.PEOPLESOFT_NAME,
    mdmhx.SERVICE_LINE,
    mdmhx.SUB_SERVICE_LINE,
    mdmhx.OPNL_SERVICE_NAME,
    mdmhx.CORP_SERVICE_LINE,
    mdmhx.RL_LOCATION,
    mdmhx.LOC_ID,
    mdmhx.REV_LOC_NAME,
    mdmhx.A2K3_NAME,
    mdmhx.A2K3_CLINIC_CARE_AREA_DESCRIPTION,
    mdmhx.AMB_PRACTICE_GROUP,
    mdmhx.HS_AREA_ID,
    mdmhx.HS_AREA_NAME,
    mdmhx.TJC_FLAG,
    mdmhx.NDNQI_NAME,
    mdmhx.NHSN_NAME,
    mdmhx.PRACTICE_GROUP_NAME,
    mdmhx.PRESSGANEY_NAME,
    mdmhx.DIVISION_DESC,
    mdmhx.ADMIN_DESC,
    mdmhx.BUSINESS_UNIT,
    mdmhx.RPT_RUN_DT,
    mdmhx.PFA_POD,
    mdmhx.HUB,
    mdmhx.PBB_POD,
    mdmhx.PG_SURVEY_DESIGNATOR,
    mdmhx.HOSPITAL_CODE,
    mdmhx.LOC_RPT_GRP_NINE_NAME
FROM
(
SELECT
    EPIC_DEPARTMENT_ID,
    EPIC_DEPT_NAME,
    EPIC_EXT_NAME,
    EPIC_DEPT_TYPE,
    EPIC_SPCLTY,
    FINANCE_COST_CODE,
    PEOPLESOFT_NAME,
    SERVICE_LINE,
    SUB_SERVICE_LINE,
    OPNL_SERVICE_NAME,
    CORP_SERVICE_LINE,
    RL_LOCATION,
    LOC_ID,
    REV_LOC_NAME,
    A2K3_NAME,
    A2K3_CLINIC_CARE_AREA_DESCRIPTION,
    AMB_PRACTICE_GROUP,
    HS_AREA_ID,
    HS_AREA_NAME,
    TJC_FLAG,
    NDNQI_NAME,
    NHSN_NAME,
    PRACTICE_GROUP_NAME,
    PRESSGANEY_NAME,
    DIVISION_DESC,
    ADMIN_DESC,
    BUSINESS_UNIT,
	CAST(NULL AS DATETIME) AS RPT_RUN_DT,
    PFA_POD,
    HUB,
    PBB_POD,
    PG_SURVEY_DESIGNATOR,
    HOSPITAL_CODE,
    LOC_RPT_GRP_NINE_NAME,
	MDM_BATCH_ID,
	ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History
) mdmhx
WHERE mdmhx.seq =1
) mdm
ON mdm.EPIC_DEPARTMENT_ID = org.EPIC_DEPARTMENT_ID
LEFT JOIN
(
SELECT DISTINCT
	epic_department_id
FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master
) mdmcurr
ON mdmcurr.EPIC_DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID
LEFT JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers grouper
ON grouper.epic_department_id = org.EPIC_DEPARTMENT_ID
INNER JOIN @ClinicalArea clinicalarea
ON org.sk_Ref_Clinical_Area_Map = clinicalarea.sk_Ref_Clinical_Area_Map

LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
ON dep.DEPARTMENT_ID = org.EPIC_DEPARTMENT_ID

LEFT OUTER JOIN DS_HSDM_APP.Mapping.All_Departments_Corrected_Addresses [add]
ON [add].DEPARTMENT_ID = org.EPIC_DEPARTMENT_ID

ORDER BY org.organization_id
                  , org.service_id
				  , org.clinical_area_id
				  , mdm.EPIC_DEPARTMENT_ID;