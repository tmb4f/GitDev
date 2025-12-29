USE CLARITY_App

SELECT DISTINCT COALESCE(ids.organization_id,9999) AS organization_id, COALESCE(organization.organization_name,'Unmapped') AS organization_name
FROM
(
SELECT DISTINCT ca.sk_Ref_Service_Map, [service].organization_id AS organization_id
FROM
(
SELECT DISTINCT mapping.sk_Ref_Clinical_Area_Map, clinical_area.sk_Ref_Service_Map AS sk_Ref_Service_Map
FROM
(
SELECT DISTINCT mdm.EPIC_DEPARTMENT_ID, grouper.sk_Ref_Clinical_Area_Map AS sk_Ref_Clinical_Area_Map
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
FROM CLARITY_App.dbo.Ref_MDM_Location_Master_History
) mdmhx
WHERE mdmhx.seq =1
) mdm
LEFT JOIN CLARITY_App.Mapping.Epic_Dept_Groupers grouper
ON grouper.EPIC_DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID
) mapping
LEFT JOIN CLARITY_App.Mapping.Ref_Clinical_Area_Map clinical_area
ON mapping.sk_Ref_Clinical_Area_Map = clinical_area.sk_Ref_Clinical_Area_Map
) ca
LEFT JOIN CLARITY_App.Mapping.Ref_Service_Map [service]
ON ca.sk_Ref_Service_Map = [service].sk_Ref_Service_Map
) ids
LEFT JOIN CLARITY_App.Mapping.Ref_Organization_Map organization
ON organization.organization_id = ids.organization_id
WHERE 1=1
ORDER BY COALESCE(ids.organization_id,9999);