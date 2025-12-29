USE DS_HSDM_App
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
                 ,@enddate SMALLDATETIME

--SET @startdate = '7/1/2020 00:00 AM'
--SET @startdate = '7/1/2021 00:00 AM'
--SET @startdate = '10/1/2021 00:00 AM'
--SET @startdate = '7/1/2022 00:00 AM'
--SET @startdate = '12/1/2022 00:00 AM'
SET @startdate = '9/1/2024 00:00 AM'
--SET @enddate = '6/30/2021 11:59 PM'
--SET @enddate = '7/14/2021 11:59 PM'
--SET @enddate = '7/31/2022 11:59 PM'
--SET @enddate = '12/31/2022 11:59 PM'
SET @enddate = '9/30/2024 11:59 PM'

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
   EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

/*
PXO - Likelihood of Recommending (Practice)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_AmbOpt_CGCAHPS_LklihdRecommendPractice.sql
*/

IF OBJECT_ID('tempdb..#cgcahps ') IS NOT NULL
DROP TABLE #cgcahps

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

    SELECT DISTINCT
     --       CAST('Outpatient-CGCAHPS' AS VARCHAR(50)) AS event_type
		   --,CASE WHEN pm.VALUE IS NULL THEN 0
     --            ELSE 1
     --       END AS event_count		--count when the overall question has been answered
		    rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
     --      ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   --,pm.sk_Dim_PG_Question
		   --,pm.VARNAME AS PG_Question_Variable
		   --,pm.QUESTION_TEXT AS PG_Question_Text	
     --      ,rec.fmonth_num
     --      ,rec.fyear_name
     --      ,rec.fyear_num
     --      ,pm.MRN_int AS person_id		--patient
     --      ,pm.PAT_NAME AS person_name		--patient
     --      ,pm.BIRTH_DATE AS person_birth_date--patient
     --      ,pm.SEX AS person_gender
     --      ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
     --      ,rec.day_date AS report_date
     --      ,pm.service_line_id
     --      ,pm.service_line
     --      ,pm.sub_service_line_id
     --      ,pm.sub_service_line
     --      ,pm.opnl_service_id
     --      ,pm.opnl_service_name
     --      ,pm.hs_area_id
     --      ,pm.hs_area_name
		   --,pm.corp_service_line_id
		   --,pm.corp_service_line
		   --,pm.provider_id
		   --,pm.provider_name
		   --,pm.practice_group_id
		   --,pm.practice_group_name
		   --,pm.sk_Dim_Pt
     --      ,pm.sk_Fact_Pt_Acct
     --      ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
     --      ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
		   --,pm.pod_name
     --      ,pm.hub_id
		   --,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
     --      ,CASE WHEN pm.AGE<18 THEN 1
     --            ELSE 0
     --       END AS peds
     --      ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
     --            ELSE 0
     --       END AS transplant
		   --,pm.sk_Dim_Physcn
		   --,pm.BUSINESS_UNIT
	    --   ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
		   --,pm.Staff_Resource
		   --,pm.som_group_id
		   --,pm.som_group_name
		   --,pm.rev_location_id
		   --,pm.rev_location
		   --,pm.financial_division_id
		   --,pm.financial_division_name
		   --,pm.financial_sub_division_id
		   --,pm.financial_sub_division_name
		   --,pm.som_department_id
		   --,pm.som_department_name
		   --,pm.som_division_id -- int
		   --,pm.som_division_name
		   --,pm.som_hs_area_id
		   --,pm.som_hs_area_name
		   --,pm.upg_practice_flag
		   --,pm.upg_practice_region_id
		   --,pm.upg_practice_region_name
		   --,pm.upg_practice_id
		   --,pm.upg_practice_name
		   --,pm.F2F_Flag
		   --,pm.ENC_TYPE_C
		   --,pm.ENC_TYPE_TITLE
		   --,pm.Lip_Flag
		   --,pm.FINANCE_COST_CODE
		   --,pm.Prov_Based_Clinic
		   --,pm.Map_Type
		   --,CAST(CASE pm.VALUE
     --              WHEN 1 THEN 0
     --              WHEN 2 THEN 25
     --              WHEN 3 THEN 50
     --              WHEN 4 THEN 75
     --              WHEN 5 THEN 100
     --            END AS DECIMAL(10, 2)) AS weighted_score -- DECIMAL(10,2)

		   --,o.organization_id
		   --,o.organization_name
		   --,s.service_id
		   --,s.service_name
		   --,c.clinical_area_id
		   --,c.clinical_area_name
		   ,g.ambulatory_flag
		   --,g.community_health_flag
		   ,pm.Survey_Designator

    INTO #cgcahps

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
                   -- MDM
				,loc_master.service_line_id
				,loc_master.service_line
				,loc_master.sub_service_line_id
				,loc_master.sub_service_line
				,loc_master.opnl_service_id
				,loc_master.opnl_service_name
				,loc_master.corp_service_line_id
				,loc_master.corp_service_line
				,loc_master.hs_area_id
				,loc_master.hs_area_name
				,CAST(NULL AS INTEGER) AS practice_group_id
				,CAST(NULL AS VARCHAR(150)) AS practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,loc_master.EPIC_DEPT_NAME AS epic_department_name
				,loc_master.EPIC_EXT_NAME AS epic_department_name_external
				,loc_master.POD_ID AS pod_id
		        ,loc_master.pod_name
				,loc_master.HUB_ID AS hub_id
		        ,loc_master.hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn
				,resp.sk_Dim_Physcn
				,loc_master.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,loc_master.LOC_ID AS rev_location_id
				,loc_master.REV_LOC_NAME AS rev_location
				   -- SOM
				,TRY_CAST(prov.Financial_Division AS INT) AS financial_division_id
				,CASE WHEN prov.Financial_Division_Name <> 'na' THEN CAST(prov.Financial_Division_Name AS VARCHAR(150)) ELSE NULL END AS financial_division_name
				,TRY_CAST(prov.Financial_SubDivision AS INT) AS financial_sub_division_id
				,CASE WHEN prov.Financial_SubDivision_Name <> 'na' THEN CAST(prov.Financial_SubDivision_Name AS VARCHAR(150)) ELSE NULL END AS financial_sub_division_name
				,dvsn.som_group_id
				,dvsn.som_group_name
				,dvsn.Department_ID AS som_department_id
				,CAST(dvsn.Department AS VARCHAR(150)) AS som_department_name
				,CAST(dvsn.Org_Number AS INT) AS som_division_id
				,CAST(dvsn.Organization AS VARCHAR(150)) AS som_division_name
				,dvsn.som_hs_area_id
				,dvsn.som_hs_area_name
				,loc_master.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(loc_master.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(loc_master.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(loc_master.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(loc_master.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,loc_master.FINANCE_COST_CODE
				,dep.Prov_Based_Clinic
				,map.Map_Type
				,resp.Survey_Designator

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt	
		LEFT OUTER JOIN
		(
		SELECT
			sk_Dim_Physcn,
			NPI,
			PROV_ID,
			Prov_Nme,
			Prov_Typ,
			Staff_Resource,
			Financial_Division,
			Financial_Division_Name,
			Financial_SubDivision,
			Financial_SubDivision_Name,
			ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY Load_Dte DESC) AS ser_seq
		FROM [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc]
		) prov
		--provider table
		ON (prov.NPI = resp.NPI)
		AND prov.ser_seq = 1
        LEFT JOIN
		(
			SELECT sk_Dim_Physcn,
					UVaID,
					Service_Line,
					ProviderGroup
			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
			WHERE current_flag = 1
		) AS doc
			    ON doc.sk_Dim_Physcn = resp.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
		LEFT OUTER JOIN
		(
			SELECT ROW_NUMBER() OVER (PARTITION BY EPIC_DEPARTMENT_ID ORDER BY mdm_LM.MDM_BATCH_ID DESC) AS Seq
				  ,CAST(NULL AS VARCHAR(66)) AS POD_ID
				  ,PFA_POD AS pod_name
				  ,HUB_ID
				  ,HUB AS hub_name
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
				  ,mdm_LM.FINANCE_COST_CODE
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
					  ,FINANCE_COST_CODE
				FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History) mdm_LM
		) AS loc_master
		ON (loc_master.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID)
		AND loc_master.Seq = 1
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                ON map.Deptid = CAST(loc_master.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
				-- -------------------------------------
				-- SOM Financial Division Subdivision --
				-- -------------------------------------
		LEFT OUTER JOIN
		(
			SELECT
				Epic_Financial_Division_Code,
				Epic_Financial_Subdivision_Code,
				Department,
				Department_ID,
				Organization,
				Org_Number,
				som_group_id,
				som_group_name,
				som_hs_area_id,
				som_hs_area_name
			FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
			ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(prov.Financial_Division AS INT)
				AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(prov.Financial_SubDivision AS INT))

 		WHERE   1 = 1
		--AND resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
		AND resp.Svc_Cde='MD'
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	            --AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
	            AND SUBSTRING(resp.Survey_Designator,1,2) IN ('MD','MT','TP')
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhgr
	ON mdmhgr.EPIC_DEPARTMENT_ID = pm.epic_department_id

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

	SELECT event_date,
           event_id,
           epic_department_id,
           epic_department_name,
           epic_department_name_external,
           Survey_Designator,
		   ambulatory_flag

    --SELECT organization_id
	   --           ,organization_name
				--  ,service_id
	   --           ,service_name
				--  ,clinical_area_id
				--  ,clinical_area_name
				--  ,som_division_id
				--  ,som_division_name
				--  ,som_department_id
				--  ,som_department_name
				--  ,epic_department_id
	   --           ,epic_department_name
				--  ,weighted_score
				--  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS LikelihoodRecommendPracticeResponse
				--  ,ambulatory_flag
				--  ,community_health_flag
				--  ,Survey_Designator

	INTO #summary

	FROM #cgcahps

--SELECT
--	*
--FROM #summary
--WHERE ambulatory_flag = 1
--ORDER BY
--	event_date,
--	event_id

SELECT
    epic_department_id,
	epic_department_name,
	ambulatory_flag,
	COUNT(*) AS MDSurveysReceived
	FROM #summary
	GROUP BY
		epic_department_id,
		epic_department_name,
		ambulatory_flag
	ORDER BY
		epic_department_id,
		epic_department_name
/*
SELECT
    'PXO - Likelihood of Recommending (Practice)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(event_count) AS LikelihoodRecommendPracticeResponse
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_CGCAHPSRecommendProvOffice_Tiles tabrptg
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   event_date>=@locstartdate
            AND event_date<@locenddate
			AND ambulatory_flag = 1
			--AND (ambulatory_flag = 1 and community_health_flag = 0)
			--AND (ambulatory_flag = 1 or community_health_flag = 1)
*/
/*
PXO - Staff Worked Together (Practice)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_AmbOpt_MD_StaffWorkedTogether.sql
*/
/*
IF OBJECT_ID('tempdb..#md ') IS NOT NULL
DROP TABLE #md

IF OBJECT_ID('tempdb..#summary4 ') IS NOT NULL
DROP TABLE #summary4

    SELECT DISTINCT
            CAST('Outpatient-MD' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,pm.sk_Dim_PG_Question
		   ,pm.VARNAME AS PG_Question_Variable
		   ,pm.QUESTION_TEXT AS PG_Question_Text	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.SEX AS person_gender
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,pm.service_line_id
           ,pm.service_line
           ,pm.sub_service_line_id
           ,pm.sub_service_line
           ,pm.opnl_service_id
           ,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   ,pm.corp_service_line_id
		   ,pm.corp_service_line
		   ,pm.provider_id
		   ,pm.provider_name
		   ,pm.practice_group_id
		   ,pm.practice_group_name
		   ,pm.sk_Dim_Pt
           ,pm.sk_Fact_Pt_Acct
           ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,CAST(pm.pod_id AS VARCHAR(66)) AS pod_id
		   ,pm.pod_name
           ,pm.hub_id
		   ,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.sk_Dim_Physcn
		   ,pm.BUSINESS_UNIT
	       ,CAST(pm.Prov_Typ AS VARCHAR(66)) AS Prov_Typ
		   ,pm.Staff_Resource
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,pm.som_department_name
		   ,pm.som_division_id -- int
		   ,pm.som_division_name
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   ,pm.upg_practice_flag
		   ,pm.upg_practice_region_id
		   ,pm.upg_practice_region_name
		   ,pm.upg_practice_id
		   ,pm.upg_practice_name
		   ,pm.F2F_Flag
		   ,pm.ENC_TYPE_C
		   ,pm.ENC_TYPE_TITLE
		   ,pm.Lip_Flag
		   ,pm.FINANCE_COST_CODE
		   ,pm.Prov_Based_Clinic
		   ,pm.Map_Type
		   ,CAST(CASE pm.VALUE
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS weighted_score -- DECIMAL(10,2)

		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.ambulatory_flag
		   ,g.community_health_flag
		   ,pm.Survey_Designator
	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag

    INTO #md

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
                   -- MDM
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
				,mdm.practice_group_id
				,mdm.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,mdm.epic_department_name_external
				,mdm.POD_ID AS pod_id
		        ,mdm.PFA_POD AS pod_name
				,mdm.HUB_ID AS hub_id
		        ,mdm.HUB AS hub_name
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				,fpa.sk_Fact_Pt_Acct
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				,pat.BirthDate AS BIRTH_DATE
				,pat.SEX
				,resp.Load_Dtm
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
				      ELSE -999
				 END AS sk_Dim_Physcn
				,mdm.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,mdm.LOC_ID AS rev_location_id
				,mdm.REV_LOC_NAME AS rev_location
				   -- SOM
				,physcn.Clrt_Financial_Division AS financial_division_id
				,physcn.Clrt_Financial_Division_Name AS financial_division_name
				,physcn.Clrt_Financial_SubDivision AS	financial_sub_division_id
				,physcn.Clrt_Financial_SubDivision_Name AS financial_sub_division_name
				,physcn.SOM_Group_ID AS som_group_id
				,physcn.SOM_group AS som_group_name
				,physcn.SOM_department_id AS som_department_id
				,physcn.SOM_department AS	som_department_name
				,physcn.SOM_division_5 AS	som_division_id
				,physcn.SOM_division_name AS som_division_name
				,physcn.som_hs_area_id AS	som_hs_area_id
				,physcn.som_hs_area_name AS som_hs_area_name
				,mdm.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(mdm.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(mdm.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(mdm.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(mdm.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,mdm.FINANCE_COST_CODE
				,dep.Prov_Based_Clinic
				,map.Map_Type
				,resp.Survey_Designator

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt		
		  LEFT OUTER JOIN (SELECT PAT_ENC_CSN_ID
								, sk_Dim_Clrt_SERsrc
								, sk_Dim_Physcn
								, ROW_NUMBER() OVER (PARTITION BY sk_Fact_Pt_Enc_Clrt ORDER BY Atn_Beg_Dtm DESC, CASE
																												   WHEN Atn_End_Dtm = '1900-01-01' THEN GETDATE()
																												   ELSE Atn_End_Dtm
																												 END DESC) AS 'Atn_Seq'
						   FROM DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
						   WHERE Atn_End_Dtm = '1900-01-01' OR Atn_End_Dtm >= '1/1/2018 00:00:00') AS dschatn
			    ON (dschatn.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id) AND dschatn.Atn_Seq = 1	
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of -1 and 0 in SERsrc
        LEFT JOIN
		(
			SELECT sk_Dim_Physcn,
					UVaID,
					Service_Line,
					ProviderGroup
			FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
			WHERE current_flag = 1
		) AS doc
			    ON CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				        WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
						WHEN resp.sk_Dim_Physcn = -1 THEN -999
						WHEN resp.sk_Dim_Physcn = 0 THEN -999
						ELSE -999
				   END = doc.sk_Dim_Physcn		
		--LEFT OUTER JOIN
		--(
		--SELECT
		--	sk_Dim_Physcn,
		--	NPI,
		--	PROV_ID,
		--	Prov_Nme,
		--	Prov_Typ,
		--	Staff_Resource,
		--	Financial_Division,
		--	Financial_Division_Name,
		--	Financial_SubDivision,
		--	Financial_SubDivision_Name,
		--	ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY Load_Dte DESC) AS ser_seq
		--FROM [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc]
		--) prov
		----provider table
		--ON (prov.NPI = resp.NPI)
		--AND prov.ser_seq = 1
  --      LEFT JOIN
		--(
		--	SELECT sk_Dim_Physcn,
		--			UVaID,
		--			Service_Line,
		--			ProviderGroup
		--	FROM DS_HSDW_Prod.Rptg.vwDim_Physcn
		--	WHERE current_flag = 1
		--) AS doc
		--	    ON doc.sk_Dim_Physcn = resp.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN
		( --mdm history to replace vwRef_MDM_Location_Master_EpicSvc and vwRef_MDM_Location_Master
			SELECT DISTINCT
				   hx.max_dt
				  ,rmlmh.EPIC_DEPARTMENT_ID
				  ,rmlmh.EPIC_DEPT_NAME AS epic_department_name
				  ,rmlmh.EPIC_EXT_NAME  AS epic_department_name_external
				  ,rmlmh.SERVICE_LINE_ID
				  ,rmlmh.SERVICE_LINE
				  ,rmlmh.SUB_SERVICE_LINE_ID
				  ,rmlmh.SUB_SERVICE_LINE
				  ,rmlmh.HS_AREA_ID
				  ,rmlmh.HS_AREA_NAME
				  ,rmlmh.PRACTICE_GROUP_ID
				  ,rmlmh.PRACTICE_GROUP_NAME
				  ,rmlmh.OPNL_SERVICE_ID
				  ,rmlmh.OPNL_SERVICE_NAME
				  ,rmlmh.CORP_SERVICE_LINE_ID
				  ,rmlmh.CORP_SERVICE_LINE
				  ,rmlmh.LOC_ID
				  ,rmlmh.REV_LOC_NAME
				  ,rmlmh.POD_ID
				  ,rmlmh.PFA_POD
				  ,rmlmh.PBB_POD
				  ,rmlmh.HUB_ID
				  ,rmlmh.HUB
				  ,rmlmh.BUSINESS_UNIT
				  ,rmlmh.FINANCE_COST_CODE
				  ,rmlmh.UPG_PRACTICE_REGION_ID
				  ,rmlmh.UPG_PRACTICE_REGION_NAME
				  ,rmlmh.UPG_PRACTICE_ID
				  ,rmlmh.UPG_PRACTICE_NAME
				  ,rmlmh.UPG_PRACTICE_FLAG
			FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS rmlmh
				INNER JOIN
				( --hx--most recent batch date per dep id
					SELECT mdmhx.EPIC_DEPARTMENT_ID
						  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
					FROM DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History AS mdmhx
					GROUP BY mdmhx.EPIC_DEPARTMENT_ID
				)                                                 AS hx
					ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
					   AND rmlmh.BATCH_RUN_DT = hx.max_dt
		)                                                        AS mdm
			ON mdm.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
       -- LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
       --         ON dep.DEPARTMENT_ID = mdm.epic_department_id
       -- LEFT OUTER JOIN
       -- (
       --     SELECT DISTINCT
       --            EPIC_DEPARTMENT_ID,
       --            SERVICE_LINE,
				   --POD_ID,
       --            PFA_POD,
				   --HUB_ID,
       --            HUB,
			    --   BUSINESS_UNIT,
				   --LOC_ID,
				   --REV_LOC_NAME,
				   --UPG_PRACTICE_FLAG,
				   --UPG_PRACTICE_REGION_ID,
				   --UPG_PRACTICE_REGION_NAME,
				   --UPG_PRACTICE_ID,
				   --UPG_PRACTICE_NAME,
				   --FINANCE_COST_CODE
       --     FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
       -- ) AS loc_master
       --         ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                ON map.Deptid = CAST(mdm.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl
		        ON excl.DEPARTMENT_ID = dep.DEPARTMENT_ID
		LEFT OUTER JOIN Stage.Scheduled_Appointment appts
		        ON appts.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id

                -- -------------------------------------
                -- SOM Hierarchy--
                -- -------------------------------------
				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn
				    ON physcn.sk_Dim_Physcn = CASE
					                            WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
												WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
												WHEN resp.sk_Dim_Physcn = -1 THEN -999
												WHEN resp.sk_Dim_Physcn = 0 THEN -999
												ELSE -999
										      END
		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('2671') -- VARNAME: O15 - How well the staff worked together to care for you
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	            --AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
	            AND SUBSTRING(resp.Survey_Designator,1,2) IN ('MD','MT','TP')
	) AS pm
ON rec.day_date=pm.RECDATE
				-- -------------------------------------
				-- SOM Financial Division Subdivision --
				-- -------------------------------------
		--LEFT OUTER JOIN
		--(
		--	SELECT
		--		Epic_Financial_Division_Code,
		--		Epic_Financial_Subdivision_Code,
		--		Department,
		--		Department_ID,
		--		Organization,
		--		Org_Number,
		--		som_group_id,
		--		som_group_name,
		--		som_hs_area_id,
		--		som_hs_area_name
		--	FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
		--	ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(prov.Financial_Division AS INT)
		--		AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(prov.Financial_SubDivision AS INT))

-- 		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
--				AND resp.RECDATE>=@locstartdate
--				AND resp.RECDATE<@locenddate
--				--AND excl.DEPARTMENT_ID IS NULL
--			    AND pat.IS_VALID_PAT_YN = 'Y'
--			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
--	            AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
--	) AS pm
--ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN (
                     SELECT fpec.PAT_ENC_CSN_ID
                           ,txsurg.day_date AS transplant_surgery_dt
                           ,fpec.Adm_Dtm
                           ,fpec.sk_Fact_Pt_Enc_Clrt
                           ,fpec.sk_Fact_Pt_Acct
                           ,fpec.sk_Dim_Clrt_Pt
                           ,fpec.sk_Dim_Pt
                     FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.epic_department_id
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhgr
	ON mdmhgr.EPIC_DEPARTMENT_ID = pm.epic_department_id

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    SELECT organization_id
	              ,organization_name
				  ,service_id
	              ,service_name
				  ,clinical_area_id
				  ,clinical_area_name
				  ,som_division_id
				  ,som_division_name
				  ,som_department_id
				  ,som_department_name
				  ,epic_department_id
	              ,epic_department_name
				  ,weighted_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS StaffWorkedTogetherResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator

	INTO #summary4

	FROM #md

SELECT
    'PXO - Staff Worked Together (Practice)' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(StaffWorkedTogetherResponse) AS StaffWorkedTogetherResponse
	FROM #summary4
	WHERE StaffWorkedTogetherResponse = 1
	AND ambulatory_flag = 1
	--AND (ambulatory_flag = 1 and community_health_flag = 0)
	--AND (ambulatory_flag = 1 or community_health_flag = 1)

	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND SUBSTRING(Survey_Designator,1,2) IN ('MD','MT','TP')

SELECT
    'PXO - Staff Worked Together (Practice)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(event_count) AS StaffWorkedTogetherResponse
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_MDStaffWorkedTogether_Tiles tabrptg
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   event_date>=@locstartdate
            AND event_date<@locenddate
			AND g.ambulatory_flag = 1
			--AND (ambulatory_flag = 1 and community_health_flag = 0)
			--AND (ambulatory_flag = 1 or community_health_flag = 1)
*/
GO