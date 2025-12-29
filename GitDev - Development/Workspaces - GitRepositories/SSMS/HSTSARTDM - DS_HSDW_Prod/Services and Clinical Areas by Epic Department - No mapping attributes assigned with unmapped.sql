USE DS_HSDW_Prod

SELECT DISTINCT
 mdm.EPIC_DEPARTMENT_ID
,mdm.EPIC_DEPT_NAME
,grouper.epic_department_id AS mapping_grouper_epic_department_id
,grouper.sk_Ref_Clinical_Area_Map AS mapping_grouper_sk_Ref_Clinical_Area_Map
,grouper.ambulatory_flag
,grouper.community_health_flag
,org.grouper_organization_id AS grouper_organization_id
,org.grouper_organization_name AS grouper_organization_id
,org.grouper_service_id AS grouper_service_id
,org.grouper_service_name AS grouper_service_name
,org.grouper_clinical_area_id AS grouper_clinical_area_id
,org.grouper_clinical_area_name AS grouper_clinical_area_name
,org.reported_organization_id AS reported_organization_id
--,org.reported_organization_name AS reported_organization_name
,COALESCE(org.reported_organization_name,'Unmapped') AS reported_organization_name
,org.reported_service_id AS reported_service_id
--,org.reported_service_name AS reported_service_name
,COALESCE(org.reported_service_name,'Unmapped') AS reported_service_name
,org.reported_clinical_area_id AS reported_clinical_area_id
--,org.reported_clinical_area_name AS reported_clinical_area_name
,COALESCE(org.reported_clinical_area_name,'Unmapped') AS reported_clinical_area_name
,CASE WHEN mdmcurr.EPIC_DEPARTMENT_ID IS NULL THEN 1 ELSE 0 END AS deleted_flag

FROM
(
SELECT DISTINCT ids.EPIC_DEPARTMENT_ID, ids.grouper_sk_Ref_Clinical_Area_Map, ids.grouper_clinical_area_id, ids.grouper_clinical_area_name, ids.grouper_sk_Ref_Service_Map, ids.reported_clinical_area_id, ids.reported_clinical_area_name, ids.grouper_service_id, ids.grouper_service_name, ids.reported_service_id, ids.reported_service_name, ids.grouper_organization_id, grouper_organization.organization_name AS grouper_organization_name, ids.reported_organization_id, reported_organization.organization_name AS reported_organization_name
FROM
(
--SELECT DISTINCT ca.EPIC_DEPARTMENT_ID, ca.grouper_sk_Ref_Clinical_Area_Map, ca.grouper_clinical_area_id, ca.grouper_clinical_area_name, ca.grouper_sk_Ref_Service_Map, ca.reported_clinical_area_id, ca.reported_clinical_area_name, grouper_service.service_id AS grouper_service_id, grouper_service.service_name AS grouper_service_name, reported_service.service_id AS reported_service_id, reported_service.service_name AS reported_service_name, grouper_service. organization_id AS grouper_organization_id, COALESCE(grouper_service. organization_id,999) AS reported_organization_id
SELECT DISTINCT ca.EPIC_DEPARTMENT_ID, ca.grouper_sk_Ref_Clinical_Area_Map, ca.grouper_clinical_area_id, ca.grouper_clinical_area_name, ca.grouper_sk_Ref_Service_Map, ca.reported_clinical_area_id, ca.reported_clinical_area_name, grouper_service.service_id AS grouper_service_id, grouper_service.service_name AS grouper_service_name, reported_service.service_id AS reported_service_id, reported_service.service_name AS reported_service_name, grouper_service. organization_id AS grouper_organization_id, grouper_service. organization_id AS reported_organization_id
FROM
(
--SELECT DISTINCT mapping.EPIC_DEPARTMENT_ID, mapping.grouper_sk_Ref_Clinical_Area_Map, grouper_clinical_area.clinical_area_id AS grouper_clinical_area_id, grouper_clinical_area.clinical_area_name AS grouper_clinical_area_name, reported_clinical_area.clinical_area_id AS reported_clinical_area_id, reported_clinical_area.clinical_area_name AS reported_clinical_area_name, grouper_clinical_area.sk_Ref_Service_Map AS grouper_sk_Ref_Service_Map, COALESCE(grouper_clinical_area.sk_Ref_Service_Map, 43) AS reported_sk_Ref_Service_Map
SELECT DISTINCT mapping.EPIC_DEPARTMENT_ID, mapping.grouper_sk_Ref_Clinical_Area_Map, grouper_clinical_area.clinical_area_id AS grouper_clinical_area_id, grouper_clinical_area.clinical_area_name AS grouper_clinical_area_name, reported_clinical_area.clinical_area_id AS reported_clinical_area_id, reported_clinical_area.clinical_area_name AS reported_clinical_area_name, grouper_clinical_area.sk_Ref_Service_Map AS grouper_sk_Ref_Service_Map, grouper_clinical_area.sk_Ref_Service_Map AS reported_sk_Ref_Service_Map
FROM
(
--SELECT DISTINCT mdm.EPIC_DEPARTMENT_ID, grouper.sk_Ref_Clinical_Area_Map AS grouper_sk_Ref_Clinical_Area_Map, COALESCE(grouper.sk_Ref_Clinical_Area_Map,165) AS reported_sk_Ref_Clinical_Area_Map
SELECT DISTINCT mdm.EPIC_DEPARTMENT_ID, grouper.sk_Ref_Clinical_Area_Map AS grouper_sk_Ref_Clinical_Area_Map, grouper.sk_Ref_Clinical_Area_Map AS reported_sk_Ref_Clinical_Area_Map
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
--WHERE EPIC_DEPARTMENT_ID IN (10204015
--,10204016)
) mdmhx
WHERE mdmhx.seq =1
) mdm
LEFT JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers grouper
ON grouper.EPIC_DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID
) mapping
LEFT JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map grouper_clinical_area
ON mapping.grouper_sk_Ref_Clinical_Area_Map = grouper_clinical_area.sk_Ref_Clinical_Area_Map
LEFT JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map reported_clinical_area
ON mapping.reported_sk_Ref_Clinical_Area_Map = reported_clinical_area.sk_Ref_Clinical_Area_Map
) ca
LEFT JOIN DS_HSDM_App.Mapping.Ref_Service_Map grouper_service
ON ca.grouper_sk_Ref_Service_Map = grouper_service.sk_Ref_Service_Map
LEFT JOIN DS_HSDM_App.Mapping.Ref_Service_Map reported_service
ON ca.reported_sk_Ref_Service_Map = reported_service.sk_Ref_Service_Map
) ids
LEFT JOIN DS_HSDM_App.Mapping.Ref_Organization_Map grouper_organization
ON grouper_organization.organization_id = ids.grouper_organization_id
LEFT JOIN DS_HSDM_App.Mapping.Ref_Organization_Map reported_organization
ON reported_organization.organization_id = ids.reported_organization_id
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
--WHERE mdm.EPIC_DEPARTMENT_ID = 10210105
--WHERE org.grouper_organization_id = 999 OR org.reported_organization_id = 999
WHERE org.grouper_organization_id = 999 OR org.reported_organization_id = 999
              OR org.grouper_organization_id IS NULL OR org.reported_organization_id IS NULL
--INNER JOIN @ClinicalArea clinicalarea
--ON org.sk_Ref_Clinical_Area_Map = clinicalarea.sk_Ref_Clinical_Area_Map
--ORDER BY org.organization_id
--                  , org.service_id
--				  , org.clinical_area_id
--				  , mdm.EPIC_DEPARTMENT_ID;
--ORDER BY mdm.EPIC_DEPT_NAME
--                  , org.organization_id
--                  , org.service_id
--				  , org.clinical_area_id;
--ORDER BY mdm.EPIC_DEPT_NAME
--                  , org.reported_organization_id
--                  , org.reported_service_id
--				  , org.reported_clinical_area_id;
ORDER BY org.grouper_organization_id DESC
                  , org.reported_organization_id
				  , mdm.EPIC_DEPT_NAME
                  , org.reported_service_id
				  , org.reported_clinical_area_id;