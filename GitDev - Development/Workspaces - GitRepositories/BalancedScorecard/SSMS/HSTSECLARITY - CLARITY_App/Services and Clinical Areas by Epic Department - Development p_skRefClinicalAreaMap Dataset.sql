USE CLARITY_App

DECLARE @Service TABLE (
	[sk_Ref_Service_Map] INTEGER)

INSERT INTO @Service
(
    [sk_Ref_Service_Map]
)
--SELECT Param AS [sk_Ref_Service_Map] FROM ETL.fn_ParmParse(@in_skrefservicemap, ',')
VALUES
(9999),
(2)

SELECT DISTINCT COALESCE(ids.sk_Ref_Clinical_Area_Map,9999) AS sk_Ref_Clinical_Area_Map, COALESCE(clinical_area.clinical_area_name,'Unmapped') AS clinical_area_name
FROM
(
	SELECT DISTINCT [service].sk_Ref_Service_Map, clinical_area.sk_Ref_Clinical_Area_Map AS sk_Ref_Clinical_Area_Map
	
	FROM 	(SELECT sk_Ref_Clinical_Area_Map, clinical_area_name, sk_Ref_Service_Map FROM CLARITY_App.Mapping.Ref_Clinical_Area_Map
	UNION ALL
	SELECT 9999 AS sk_Ref_Clinical_Area_Map, 'Unmapped' AS clinical_area_name, 	9999 AS sk_Ref_Service_Map
	) clinical_area
	INNER JOIN @Service [service]
	ON clinical_area.sk_Ref_Service_Map = [service].sk_Ref_Service_Map
) ids
LEFT JOIN CLARITY_App.Mapping.Ref_Clinical_Area_Map clinical_area
ON ids.sk_Ref_Clinical_Area_Map = clinical_area.sk_Ref_Clinical_Area_Map
ORDER BY 1;