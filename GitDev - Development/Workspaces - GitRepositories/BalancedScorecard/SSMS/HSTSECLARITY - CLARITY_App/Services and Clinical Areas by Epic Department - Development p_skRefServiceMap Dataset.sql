USE CLARITY_App

DECLARE @Organization TABLE (
	[organization_id] [int] NOT NULL)

INSERT INTO @Organization
(
    organization_id
)
--SELECT Param AS organization_id FROM ETL.fn_ParmParse(@OrganizationId, ',')
VALUES
(1),
(9999)

SELECT DISTINCT COALESCE(ids.sk_Ref_Service_Map,9999) AS sk_Ref_Service_Map, COALESCE([service].[service_name],'Unmapped') AS [service_name]
FROM
(
	SELECT DISTINCT [organization].organization_id, [service].sk_Ref_Service_Map AS sk_Ref_Service_Map
	
	FROM
	(SELECT sk_Ref_Service_Map, service_name, organization_id FROM CLARITY_App.Mapping.Ref_Service_Map
	UNION ALL
	SELECT 9999 AS sk_Ref_Service_Map, 'Unmapped' AS service_name, 9999 AS organization_id
	) [service]
	INNER JOIN @Organization organization
	ON [service].organization_id = organization.organization_id
                WHERE [service].sk_Ref_Service_Map NOT IN (55,56,57)
) ids
LEFT OUTER JOIN CLARITY_App.Mapping.Ref_Service_Map [service]
ON [service].sk_Ref_Service_Map = ids.sk_Ref_Service_Map
ORDER BY 1,2;