/******	Mapping
1.	Epic_Dept_Groupers
2.	Ref_Clinical_Area_Map
3.	Ref_Service_Map
3.	Ref_Organization_Map
******/
USE CLARITY_App

SELECT
	TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'Mapping'
ORDER BY TABLE_SCHEMA, TABLE_NAME
