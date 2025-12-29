USE CLARITY_App
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('tempdb..#RptgTable') IS NOT NULL
DROP TABLE #RptgTable

SELECT
	tabrptg.epic_department_id,
    tabrptg.epic_department_name,
	tabrptg.ambulatory_flag,
	tabrptg.community_health_flag
INTO #RptgTable

FROM
(
SELECT DISTINCT
       dep.DEPARTMENT_ID AS epic_department_id
      ,mdm.EPIC_DEPT_NAME AS epic_department_name
      ,mdm.EPIC_EXT_NAME AS epic_department_name_external
      ,mdm.service_line_id
      ,mdm.service_line
      ,mdm.sub_service_line_id
      ,mdm.sub_service_line
      ,mdm.opnl_service_id
      ,mdm.opnl_service_name
      ,mdm.corp_service_line_id
      ,mdm.corp_service_line
      ,mdm.hs_area_id
      ,mdm.hs_area_name
	  ,mdm.LOC_ID AS rev_location_id
	  ,mdm.REV_LOC_NAME AS rev_location
	  ,mdm.BUSINESS_UNIT
      ,mdm.upg_practice_flag
      ,mdm.upg_practice_region_id
      ,mdm.upg_practice_region_name
      ,mdm.upg_practice_id
      ,mdm.upg_practice_name

	  ,g.ambulatory_flag
	  ,o.organization_name
	  ,s.service_name
	  ,c.clinical_area_name

	  ,g.community_health_flag

FROM CLARITY..CLARITY_DEP dep
LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
LEFT OUTER JOIN
(
    SELECT ROW_NUMBER() OVER (PARTITION BY EPIC_DEPARTMENT_ID ORDER BY mdm_LM.MDM_BATCH_ID DESC) AS Seq
	      ,CAST(NULL AS VARCHAR(66)) AS POD_ID
	      ,PFA_POD
	      ,HUB_ID
	      ,HUB
          ,[EPIC_DEPARTMENT_ID]
          ,[EPIC_DEPT_NAME]
          ,[EPIC_EXT_NAME]
          ,[LOC_ID]
          ,[REV_LOC_NAME]
          ,service_line_id
          ,service_line
          ,sub_service_line_id
          ,sub_service_line
          ,opnl_service_id
          ,opnl_service_name
          ,corp_service_line_id
          ,corp_service_line
          ,hs_area_id
          ,hs_area_name
		  ,BUSINESS_UNIT
		  ,CAST(NULL AS INT) AS upg_practice_flag
		  ,CAST(NULL AS VARCHAR(254)) AS upg_practice_region_id
		  ,CAST(NULL AS VARCHAR(254)) AS upg_practice_region_name
		  ,CAST(NULL AS VARCHAR(66)) AS upg_practice_id
		  ,CAST(NULL AS VARCHAR(150)) AS upg_practice_name
	FROM
    (
        SELECT DISTINCT
	           PFA_POD
	          ,HUB_ID
	          ,HUB
              ,[EPIC_DEPARTMENT_ID]
              ,[EPIC_DEPT_NAME]
              ,[EPIC_EXT_NAME]
              ,[LOC_ID]
              ,[REV_LOC_NAME]
              ,service_line_id
              ,service_line
              ,sub_service_line_id
              ,sub_service_line
              ,opnl_service_id
              ,opnl_service_name
              ,corp_service_line_id
              ,corp_service_line
              ,hs_area_id
              ,hs_area_name
			  ,BUSINESS_UNIT
			  ,MDM_BATCH_ID
	    FROM CLARITY_App.Rptg.vwRef_MDM_Location_Master_History) mdm_LM
) AS mdm
ON (mdm.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
AND mdm.Seq = 1
)

				LEFT JOIN [CLARITY_App].[Mapping].[Epic_Dept_Groupers] g ON dep.DEPARTMENT_ID = g.epic_department_id
				LEFT JOIN [CLARITY_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
				LEFT JOIN [CLARITY_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
				LEFT JOIN [CLARITY_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id

WHERE
      excl.DEPARTMENT_ID IS NULL
) tabrptg

SELECT
	epic_department_id,
    epic_department_name,
    ambulatory_flag,
    community_health_flag
FROM #RptgTable
WHERE community_health_flag = 1
ORDER BY
	ambulatory_flag,
	epic_department_name

GO