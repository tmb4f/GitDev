USE CLARITY

IF OBJECT_ID('tempdb..#eta') IS NOT NULL
	DROP TABLE #eta

;WITH MDM_DEP
AS ( SELECT
         *
     FROM
         (
         SELECT -- one row per epic dept, with last designated hospital code
             t1.EPIC_DEPARTMENT_ID
           , t1.HOSPITAL_CODE
           , ROW_NUMBER() OVER ( PARTITION BY t1.EPIC_DEPARTMENT_ID ORDER BY t1.Update_Dtm DESC ) AS latest_upd
         FROM CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL_History t1
         WHERE
             ( t1.EPIC_DEPARTMENT_ID IS NOT NULL )
             --AND t1.HOSPITAL_CODE = 'UVA-MC'  -- exclude Community Hospitals
         ) AS mdm
     WHERE mdm.latest_upd = 1 )

/*
SELECT eta.[PAT_ENC_CSN_ID]
      ,[LINE]
      ,eta.[PAT_ID]
      ,eta.[PAT_ENC_DATE_REAL]
      ,eta.[CONTACT_DATE]
      ,eta.[CM_CT_OWNER_ID]
      ,[CARE_TEAMS_ID]
	  ,ptri.RECORD_NAME
	  ,ptri.RECORD_STATUS_C
	  ,enc.EFFECTIVE_DEPT_ID
	  ,dep.DEPARTMENT_NAME
	  ,hosp.HOSPITAL_CODE
  FROM [CLARITY].[dbo].[EPT_CARE_TEAMS] eta
	LEFT OUTER JOIN CLARITY.dbo.PROVTEAM_REC_INFO ptri
	  ON ptri.ID = eta.CARE_TEAMS_ID
	LEFT OUTER JOIN CLARITY.dbo.V_PAT_ENC enc
	  ON enc.PAT_ENC_CSN_ID = eta.PAT_ENC_CSN_ID
	LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
		ON dep.DEPARTMENT_ID = enc.EFFECTIVE_DEPT_ID
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
ON (mdm.EPIC_DEPARTMENT_ID = enc.EFFECTIVE_DEPT_ID)
AND mdm.Seq = 1
LEFT OUTER JOIN [CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group] hosp
	ON hosp.EPIC_DEPARTMENT_ID = enc.EFFECTIVE_DEPT_ID
*/
/*
	SELECT DISTINCT 
		-- pats.PAT_ID
		--,pats.PAT_ENC_CSN_ID
		 eta.PAT_ID
		,eta.PAT_ENC_CSN_ID
		,hsp.HOSP_DISCH_TIME
		,hsp.INPATIENT_DATA_ID
		,hsp.HOSP_SERV_C
		,zps.NAME AS HOSP_SERV_NAME
		,hsp.HSP_ACCOUNT_ID
		,hsp.DEPARTMENT_ID
		,mdm.EPIC_DEPT_NAME
		,hosp.HOSPITAL_CODE
		,eta.TEAM_AUDIT_ID
		--,eta.PRIMARYTEAM_AUDI_YN
		,eta.[PRIMARY_TEAM_?]
		,eta.RECORD_NAME
		--,ptri.RECORD_NAME
		--,ptri.RECORD_NAME + '_' + COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') AS RECORD_NAME
		,eta.TEAM_AUDIT_NAME
		,eta.TEAM_AUDIT_INSTANT
		,eta.LINE
		,eta.TEAM_ACTION_C
		,eta.TEAM_ACTION_NAME
		--,orlog.Pt_Class
		--,orlog.OR_SERVICE_NAME
	--INTO #eta
	FROM
	(
	SELECT
		team.PAT_ID,
        team.PAT_ENC_CSN_ID,
        team.TEAM_AUDIT_ID,
		--team.PRIMARYTEAM_AUDI_YN,
		team.[PRIMARY_TEAM_?],
		team.RECORD_NAME,
        team.TEAM_AUDIT_NAME,
        team.TEAM_AUDIT_INSTANT,
		team.LINE,
        team.TEAM_ACTION_C,
		team.TEAM_ACTION_NAME
	FROM
	(
	SELECT DISTINCT
		eta.PAT_ID,
		eta.PAT_ENC_CSN_ID,
		eta.TEAM_AUDIT_ID,
		eta.PRIMARYTEAM_AUDI_YN,
		ptri.RECORD_NAME,
		ptri.RECORD_NAME AS TEAM_AUDIT_NAME,
		--ptri.RECORD_NAME + '_' + COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') AS TEAM_AUDIT_NAME,
		COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') AS [PRIMARY_TEAM_?],
		eta.TEAM_AUDIT_INSTANT,
		eta.LINE,
		eta.TEAM_ACTION_C,
		zta.NAME AS TEAM_ACTION_NAME,
		ROW_NUMBER() OVER(PARTITION BY eta.PAT_ID, eta.PAT_ENC_CSN_ID, eta.TEAM_AUDIT_ID, CAST(eta.TEAM_AUDIT_INSTANT AS DATE) ORDER BY eta.TEAM_AUDIT_INSTANT DESC, ptri.RECORD_NAME + '_' + COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') DESC) AS seq
	FROM CLARITY..EPT_TEAM_AUDIT eta
	LEFT OUTER JOIN CLARITY.dbo.PROVTEAM_REC_INFO ptri
	  ON ptri.ID = eta.TEAM_AUDIT_ID
	LEFT OUTER JOIN CLARITY..ZC_TEAM_ACTION zta
	  ON zta.TEAM_ACTION_C = eta.TEAM_ACTION_C
    WHERE eta.TEAM_AUDIT_ID IS NOT NULL
    AND eta.TEAM_ACTION_C <> 2 -- Remove
	) team
	WHERE team.seq = 1
	) eta
	--INNER JOIN
	LEFT OUTER JOIN
	(
	SELECT DISTINCT
		PAT_ENC_CSN_ID
	   ,HOSP_DISCH_TIME
	   ,INPATIENT_DATA_ID
	   ,HOSP_SERV_C
	   ,HSP_ACCOUNT_ID
	   ,DEPARTMENT_ID
	FROM CLARITY..PAT_ENC_HSP
	) hsp
	ON eta.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
    LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_SERVICE zps
		ON zps.HOSP_SERV_C = hsp.HOSP_SERV_C
	--INNER JOIN #pats pats
	--	ON eta.PAT_ID =  pats.PAT_ID
	--	AND eta.PAT_ENC_CSN_ID = pats.PAT_ENC_CSN_ID
	--LEFT OUTER JOIN #AD_Events orlog
	--	ON orlog.PAT_ID = eta.PAT_ID
	--	AND orlog.PAT_ENC_CSN_ID = eta.PAT_ENC_CSN_ID
	--LEFT OUTER JOIN CLARITY.dbo.PROVTEAM_REC_INFO ptri
	--  ON ptri.ID = eta.TEAM_AUDIT_ID
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
ON (mdm.EPIC_DEPARTMENT_ID = hsp.DEPARTMENT_ID)
AND mdm.Seq = 1
--LEFT OUTER JOIN [CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group] hosp
--	ON hosp.EPIC_DEPARTMENT_ID = hsp.DEPARTMENT_ID
LEFT OUTER JOIN MDM_DEP hosp
	ON hosp.EPIC_DEPARTMENT_ID = hsp.DEPARTMENT_ID
	WHERE 1 = 1
	    --AND eta.PRIMARYTEAM_AUDI_YN = 'Y' -- Indicates whether this line of the team audit shows that the team was the primary team.
		----AND eta.TEAM_AUDIT_ID IN (55,101)  -- TRAUMA SURGERY, TRAUMA ICU
  --      AND eta.TEAM_AUDIT_ID IN (69,72,73,190) -- Green Surgery Team, Orange Surgery, EGS Team, Red Surgery Team
		--AND eta.TEAM_AUDIT_INSTANT >=  @start
		--AND eta.TEAM_AUDIT_INSTANT <= @end
		--AND eta.TEAM_AUDIT_ID IS NOT NULL
		AND hosp.HOSPITAL_CODE IS NOT NULL AND LEN(hosp.HOSPITAL_CODE) > 0 AND hosp.HOSPITAL_CODE NOT IN ('UVA-MC','UVA-SCR')
ORDER BY
	hosp.HOSPITAL_CODE,
	hsp.DEPARTMENT_ID,
	mdm.EPIC_DEPT_NAME,
	eta.PAT_ID,
	eta.PAT_ENC_CSN_ID,
	eta.LINE
*/

	SELECT DISTINCT 
		-- pats.PAT_ID
		--,pats.PAT_ENC_CSN_ID
		 eta.PAT_ID
		,eta.PAT_ENC_CSN_ID
		,hsp.HOSP_DISCH_TIME
		,hsp.INPATIENT_DATA_ID
		,hsp.HOSP_SERV_C
		,zps.NAME AS HOSP_SERV_NAME
		,hsp.HSP_ACCOUNT_ID
		,hsp.DEPARTMENT_ID
		,mdm.EPIC_DEPT_NAME
		,hosp.HOSPITAL_CODE
		,eta.TEAM_AUDIT_ID
		--,eta.PRIMARYTEAM_AUDI_YN
		,eta.[PRIMARY_TEAM_?]
		,eta.RECORD_NAME
		--,ptri.RECORD_NAME
		--,ptri.RECORD_NAME + '_' + COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') AS RECORD_NAME
		,eta.TEAM_AUDIT_NAME
		,eta.TEAM_AUDIT_INSTANT
		--,orlog.Pt_Class
		--,orlog.OR_SERVICE_NAME
	INTO #eta
	FROM
	(
	SELECT
		team.PAT_ID,
        team.PAT_ENC_CSN_ID,
        team.TEAM_AUDIT_ID,
		--team.PRIMARYTEAM_AUDI_YN,
		team.[PRIMARY_TEAM_?],
		team.RECORD_NAME,
        team.TEAM_AUDIT_NAME,
        team.TEAM_AUDIT_INSTANT,
        team.TEAM_ACTION_C
	FROM
	(
	SELECT DISTINCT
		eta.PAT_ID,
		eta.PAT_ENC_CSN_ID,
		eta.TEAM_AUDIT_ID,
		eta.PRIMARYTEAM_AUDI_YN,
		ptri.RECORD_NAME,
		ptri.RECORD_NAME AS TEAM_AUDIT_NAME,
		--ptri.RECORD_NAME + '_' + COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') AS TEAM_AUDIT_NAME,
		COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') AS [PRIMARY_TEAM_?],
		eta.TEAM_AUDIT_INSTANT,
		eta.LINE,
		eta.TEAM_ACTION_C,
		zta.NAME AS TEAM_ACTION_NAME,
		ROW_NUMBER() OVER(PARTITION BY eta.PAT_ID, eta.PAT_ENC_CSN_ID, eta.TEAM_AUDIT_ID, CAST(eta.TEAM_AUDIT_INSTANT AS DATE) ORDER BY eta.TEAM_AUDIT_INSTANT DESC, ptri.RECORD_NAME + '_' + COALESCE(eta.PRIMARYTEAM_AUDI_YN,'') DESC) AS seq
	FROM CLARITY..EPT_TEAM_AUDIT eta
	LEFT OUTER JOIN CLARITY.dbo.PROVTEAM_REC_INFO ptri
	  ON ptri.ID = eta.TEAM_AUDIT_ID
	LEFT OUTER JOIN CLARITY..ZC_TEAM_ACTION zta
	  ON zta.TEAM_ACTION_C = eta.TEAM_ACTION_C
    WHERE eta.TEAM_AUDIT_ID IS NOT NULL
    AND eta.TEAM_ACTION_C <> 2 -- Remove
	) team
	WHERE team.seq = 1
	) eta
	--INNER JOIN
	LEFT OUTER JOIN
	(
	SELECT DISTINCT
		PAT_ENC_CSN_ID
	   ,HOSP_DISCH_TIME
	   ,INPATIENT_DATA_ID
	   ,HOSP_SERV_C
	   ,HSP_ACCOUNT_ID
	   ,DEPARTMENT_ID
	FROM CLARITY..PAT_ENC_HSP
	) hsp
	ON eta.PAT_ENC_CSN_ID = hsp.PAT_ENC_CSN_ID
    LEFT OUTER JOIN CLARITY.dbo.ZC_PAT_SERVICE zps
		ON zps.HOSP_SERV_C = hsp.HOSP_SERV_C
	--INNER JOIN #pats pats
	--	ON eta.PAT_ID =  pats.PAT_ID
	--	AND eta.PAT_ENC_CSN_ID = pats.PAT_ENC_CSN_ID
	--LEFT OUTER JOIN #AD_Events orlog
	--	ON orlog.PAT_ID = eta.PAT_ID
	--	AND orlog.PAT_ENC_CSN_ID = eta.PAT_ENC_CSN_ID
	--LEFT OUTER JOIN CLARITY.dbo.PROVTEAM_REC_INFO ptri
	--  ON ptri.ID = eta.TEAM_AUDIT_ID
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
ON (mdm.EPIC_DEPARTMENT_ID = hsp.DEPARTMENT_ID)
AND mdm.Seq = 1
--LEFT OUTER JOIN [CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group] hosp
--	ON hosp.EPIC_DEPARTMENT_ID = hsp.DEPARTMENT_ID
LEFT OUTER JOIN MDM_DEP hosp
	ON hosp.EPIC_DEPARTMENT_ID = hsp.DEPARTMENT_ID
	WHERE 1 = 1
	    --AND eta.PRIMARYTEAM_AUDI_YN = 'Y' -- Indicates whether this line of the team audit shows that the team was the primary team.
		----AND eta.TEAM_AUDIT_ID IN (55,101)  -- TRAUMA SURGERY, TRAUMA ICU
  --      AND eta.TEAM_AUDIT_ID IN (69,72,73,190) -- Green Surgery Team, Orange Surgery, EGS Team, Red Surgery Team
		--AND eta.TEAM_AUDIT_INSTANT >=  @start
		--AND eta.TEAM_AUDIT_INSTANT <= @end
		--AND eta.TEAM_AUDIT_ID IS NOT NULL
		AND hosp.HOSPITAL_CODE IS NOT NULL AND LEN(hosp.HOSPITAL_CODE) > 0 AND hosp.HOSPITAL_CODE NOT IN ('UVA-MC','UVA-SCR')
    --ORDER BY pats.PAT_ID, pats.PAT_ENC_CSN_ID, eta.TEAM_AUDIT_ID, eta.TEAM_AUDIT_INSTANT
    --ORDER BY pats.PAT_ID, pats.PAT_ENC_CSN_ID, eta.TEAM_AUDIT_ID, ptri.RECORD_NAME + '_' + COALESCE(eta.PRIMARYTEAM_AUDI_YN,''), eta.TEAM_AUDIT_INSTANT
    --ORDER BY pats.PAT_ID, pats.PAT_ENC_CSN_ID, eta.TEAM_AUDIT_ID, eta.TEAM_AUDIT_NAME, eta.TEAM_AUDIT_INSTANT

	--SELECT
	--	*
	--FROM #eta eta
 --   ORDER BY eta.HOSPITAL_CODE, eta.PAT_ID, eta.PAT_ENC_CSN_ID, eta.TEAM_AUDIT_ID, eta.TEAM_AUDIT_NAME, eta.TEAM_AUDIT_INSTANT

	SELECT DISTINCT
		eta.DEPARTMENT_ID,
		eta.EPIC_DEPT_NAME,
		eta.HOSPITAL_CODE,
		eta.TEAM_AUDIT_ID,
		eta.TEAM_AUDIT_NAME,
		eta.[PRIMARY_TEAM_?]
	FROM #eta eta
    ORDER BY eta.HOSPITAL_CODE, eta.EPIC_DEPT_NAME, eta.TEAM_AUDIT_NAME