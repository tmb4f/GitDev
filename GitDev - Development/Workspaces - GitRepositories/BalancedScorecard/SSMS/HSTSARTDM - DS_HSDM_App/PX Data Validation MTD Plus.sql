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
SET @startdate = '8/1/2023 00:00 AM'
--SET @enddate = '6/30/2021 11:59 PM'
--SET @enddate = '7/14/2021 11:59 PM'
--SET @enddate = '7/31/2022 11:59 PM'
--SET @enddate = '12/31/2022 11:59 PM'
SET @enddate = '8/31/2023 11:59 PM'

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

IF OBJECT_ID('tempdb..#respdept ') IS NOT NULL
DROP TABLE #respdept

IF OBJECT_ID('tempdb..#pgdept ') IS NOT NULL
DROP TABLE #pgdept

IF OBJECT_ID('tempdb..#cgcahps ') IS NOT NULL
DROP TABLE #cgcahps

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

IF OBJECT_ID('tempdb..#tabrptgfreq ') IS NOT NULL
DROP TABLE #tabrptgfreq

IF OBJECT_ID('tempdb..#pg ') IS NOT NULL
DROP TABLE #pg

IF OBJECT_ID('tempdb..#pgfreq ') IS NOT NULL
DROP TABLE #pgfreq

SELECT DISTINCT
	   resps.epic_department_id
	  ,resps.epic_department_name
	  ,resps.Responses
	  ,g.ambulatory_flag

  INTO #respdept

  FROM
	(
	SELECT
		resp.sk_Dim_Clrt_DEPt,
        resp.epic_department_id,
        resp.epic_department_name,
        resp.epic_department_name_external,
        COUNT(*) AS Responses
	FROM
	(
		SELECT DISTINCT
				 resp.sk_Dim_Clrt_DEPt
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,mdm.epic_department_name_external
				,resp.SURVEY_ID

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt	
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
 		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	            --AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
	            AND SUBSTRING(resp.Survey_Designator,1,2) IN ('MD','MT','TP')
) resp
GROUP BY
				 resp.sk_Dim_Clrt_DEPt
				,resp.epic_department_id
				,resp.epic_department_name
				,resp.epic_department_name_external
) resps	
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = resps.epic_department_id
	WHERE g.ambulatory_flag = 1

  --SELECT * FROM #respdept ORDER BY epic_department_name

SELECT DISTINCT
	   CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0)) AS epic_department_id
	  ,dep.Clrt_DEPt_Nme AS epic_department_name
	  ,CAST(pgf.Very_Poor_n AS INTEGER) + CAST(pgf.Poor_n AS INTEGER) + CAST(pgf.Fair_n AS INTEGER) + CAST(pgf.Good_n AS INTEGER) + CAST(pgf.Very_Good_n AS INTEGER) AS Responses
	  ,g.ambulatory_flag

  INTO #pgdept

  FROM [DS_HSDM_App].[Stage].[PGFusion_Query_Results] pgf
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
  ON ddte.day_date = CAST(SUBSTRING(Received_Date, 1, CHARINDEX('-',Received_Date) -2) AS DATE)
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
  ON dep.DEPARTMENT_ID = CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0))
  LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
  ON g.epic_department_id = CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0))
  WHERE Received_Date <> 'Total'
  AND IT_DEPT_ID <> 'Total'
  AND Questions = 'Likelihood of recommending'
  AND CAST(SUBSTRING(Received_Date, 1, CHARINDEX('-',Received_Date) -2) AS DATE) = '8/1/2023'
  AND ambulatory_flag = 1

  --SELECT * FROM #pgdept ORDER BY epic_department_name

  SELECT
	resp.epic_department_id,
    resp.epic_department_name,
    resp.Responses AS EDW_Responses,
	pgf.Responses AS PGF_Responses
  FROM #respdept resp
  LEFT OUTER JOIN #pgdept pgf
  ON pgf.epic_department_id = resp.epic_department_id
  ORDER BY resp.epic_department_name

  SELECT
	SUM(pex.EDW_Responses) AS EDW_Responses
   ,SUM(pex.PGF_Responses) AS EDW_Responses
  FROM
  (
  SELECT
	resp.epic_department_id,
    resp.epic_department_name,
    resp.Responses AS EDW_Responses,
	pgf.Responses AS PGF_Responses
  FROM #respdept resp
  LEFT OUTER JOIN #pgdept pgf
  ON pgf.epic_department_id = resp.epic_department_id
  ) pex

  SELECT
	resp.epic_department_id,
    resp.epic_department_name,
    resp.Responses AS EDW_Responses,
	pgf.Responses AS PGF_Responses
  FROM #respdept resp
  LEFT OUTER JOIN #pgdept pgf
  ON pgf.epic_department_id = resp.epic_department_id
  WHERE pgf.Responses IS NULL
  ORDER BY resp.epic_department_name

/*
SELECT DISTINCT
/*
       mdmhg.HOSPITAL_CODE
	  ,[IT_DEPT_ID]
	  ,TRIM('''' FROM IT_DEPT_ID) AS IT_DEPT_ID_TRIM
	  ,CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0)) AS IT_DEPT_ID_NUMERIC
	  ,CAST(pgf.Very_Poor_n AS INTEGER) + CAST(pgf.Poor_n AS INTEGER) + CAST(pgf.Fair_n AS INTEGER) + CAST(pgf.Good_n AS INTEGER) + CAST(pgf.Very_Good_n AS INTEGER) AS Total_n
	   ,g.epic_department_id AS groupers_epic_department_id
	  ,g.epic_department_name AS groupers_epic_department_name
	  ,mdmhg.EPIC_DEPARTMENT_ID AS mdmhg_epic_department_id
	  ,dep.DEPARTMENT_ID AS clrt_epic_department_id
	  ,dep.Clrt_DEPt_Nme AS clrt_epic_department_name
	  ,g.ambulatory_flag
	  ,resps.epic_department_id AS resps_epic_department_id
*/
	   resps.epic_department_id AS resps_epic_department_id
	  ,resps.epic_department_name AS resps_epic_department_name
	  ,resps.Responses

  INTO #pgdept

  FROM
	(
	SELECT
		resp.sk_Dim_Clrt_DEPt,
        resp.epic_department_id,
        resp.epic_department_name,
        resp.epic_department_name_external,
        COUNT(*) AS Responses
	FROM
	(
		SELECT DISTINCT
				 resp.sk_Dim_Clrt_DEPt
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				,mdm.epic_department_name_external
				,resp.SURVEY_ID

		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt	
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS mdm
                ON dep.DEPARTMENT_ID = mdm.epic_department_id
 		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
	            --AND SUBSTRING(resp.Survey_Designator,1,2) = 'MD'
	            AND SUBSTRING(resp.Survey_Designator,1,2) IN ('MD','MT','TP')
) resp
GROUP BY
				 resp.sk_Dim_Clrt_DEPt
				,resp.epic_department_id
				,resp.epic_department_name
				,resp.epic_department_name_external
) resps	
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = resps.epic_department_id
/*
  LEFT OUTER JOIN [DS_HSDM_App].[Stage].[PGFusion_Query_Results] pgf
  ON CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0)) = resps.epic_department_id
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
  ON ddte.day_date = CAST(SUBSTRING(Received_Date, 1, CHARINDEX('-',Received_Date) -2) AS DATE)
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0))

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhg
	ON mdmhg.EPIC_DEPARTMENT_ID = CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0))

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
	ON dep.DEPARTMENT_ID = CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0))
  WHERE Received_Date <> 'Total'
  AND IT_DEPT_ID <> 'Total'
  AND Questions = 'Likelihood of recommending'
  AND CAST(SUBSTRING(Received_Date, 1, CHARINDEX('-',Received_Date) -2) AS DATE) = '8/1/2023'
  --AND ambulatory_flag = 1

  --SELECT * FROM #pgdept ORDER BY HOSPITAL_CODE, epic_department_name
*/
  WHERE
  g.ambulatory_flag = 1

  SELECT * FROM #pgdept ORDER BY resps_epic_department_name
/*
  SELECT DISTINCT
	IT_DEPT_ID_NUMERIC AS Epic_Department_Id
   ,clrt_epic_department_name AS Epic_Department_Name
   --,CASE WHEN resps_epic_department_id IS NULL THEN 'N' ELSE 'Y' END AS [Has Likelihood of Recommending (Practice) Responses in EDW Response Table]
   ,resps_epic_department_id
   ,resps_epic_department_name
   ,Responses
   ,Total_n
   ,CASE WHEN groupers_epic_department_id IS NULL THEN 'N' ELSE 'Y' END AS [In Mapping Epic_Dept_Groupers]
   ,ambulatory_flag
   ,HOSPITAL_CODE AS [MDM Hospital Group Hospital Code]
  FROM #pgdept
  ORDER BY HOSPITAL_CODE, clrt_epic_department_name
*/
*/
    SELECT DISTINCT
            CAST('Outpatient-CGCAHPS' AS VARCHAR(50)) AS event_type
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

		   ,pm.PG_AcctNbr
		   ,pm.Pat_Enc_CSN_Id
		   ,pm.DISDATE

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
				,loc_master.POD_ID AS pod_id
		        ,loc_master.PFA_POD AS pod_name
				,loc_master.HUB_ID AS hub_id
		        ,loc_master.HUB AS hub_name
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
				,resp.PG_AcctNbr
				,resp.Pat_Enc_CSN_Id
				,resp.DISDATE

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
            SELECT DISTINCT
                   EPIC_DEPARTMENT_ID,
                   SERVICE_LINE,
				   POD_ID,
                   PFA_POD,
				   HUB_ID,
                   HUB,
			       BUSINESS_UNIT,
				   LOC_ID,
				   REV_LOC_NAME,
				   UPG_PRACTICE_FLAG,
				   UPG_PRACTICE_REGION_ID,
				   UPG_PRACTICE_REGION_NAME,
				   UPG_PRACTICE_ID,
				   UPG_PRACTICE_NAME,
				   FINANCE_COST_CODE
            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
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

 		WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
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
				  ,cgcahps.epic_department_id
	              ,cgcahps.epic_department_name
				  ,weighted_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS LikelihoodRecommendPracticeResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,event_id
				  ,Survey_Designator
				  ,PG_AcctNbr
				  ,Pat_Enc_CSN_Id
				  ,event_date
				  ,person_id
				  ,DISDATE
				  ,mdmhg.HOSPITAL_CODE

	INTO #summary

	FROM #cgcahps cgcahps
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhg
	ON mdmhg.EPIC_DEPARTMENT_ID = cgcahps.epic_department_id
	--INNER JOIN #pgdept pgdept
	--ON cgcahps.epic_department_id = pgdept.epic_department_id

SELECT
    'PXO - Likelihood of Recommending (Practice)' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(weighted_score) AS weighted_score,
	SUM(LikelihoodRecommendPracticeResponse) AS LikelihoodRecommendPracticeResponse,
	CAST(SUM(weighted_score) AS DECIMAL(10,2)) / CAST(SUM(LikelihoodRecommendPracticeResponse) AS DECIMAL(10,2)) AS Mean,
	GETDATE() AS RunTime
	FROM #summary
	WHERE LikelihoodRecommendPracticeResponse = 1
	AND ambulatory_flag = 1
	--AND (ambulatory_flag = 1 and community_health_flag = 0)
	--AND (ambulatory_flag = 1 or community_health_flag = 1)
	--AND HOSPITAL_CODE = 'UVA-MC'

	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND SUBSTRING(Survey_Designator,1,2) IN ('MD','MT','TP')

SELECT
    'PXO - Likelihood of Recommending (Practice)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(tabrptg.weighted_score) AS weighted_score,
	SUM(event_count) AS LikelihoodRecommendPracticeResponse,
	CAST(SUM(tabrptg.weighted_score) AS DECIMAL(10,2)) / CAST(SUM(event_count) AS DECIMAL(10,2)) AS Mean,
	GETDATE() AS RunTime
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_CGCAHPSRecommendProvOffice_Tiles tabrptg

	--LEFT OUTER JOIN #summary summary
	--ON summary.event_id = tabrptg.event_id

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhg
	ON mdmhg.EPIC_DEPARTMENT_ID = tabrptg.epic_department_id

	--INNER JOIN #pgdept pgdept
	--ON tabrptg.epic_department_id = pgdept.epic_department_id

    WHERE   tabrptg.event_date>=@locstartdate
            AND tabrptg.event_date<@locenddate
			AND g.ambulatory_flag = 1
			--AND (ambulatory_flag = 1 and community_health_flag = 0)
			--AND (ambulatory_flag = 1 or community_health_flag = 1)
			--AND mdmhg.HOSPITAL_CODE = 'UVA-MC'

	--AND SUBSTRING(summary.Survey_Designator,1,2) = 'MD'
	--AND SUBSTRING(summary.Survey_Designator,1,2) IN ('MD','MT','TP')

SELECT
    tabrptg.epic_department_id,
	tabrptg.epic_department_name,
	SUM(tabrptg.weighted_score) AS weighted_score,
	SUM(event_count) AS LikelihoodRecommendPracticeResponse,
	GETDATE() AS RunTime

INTO #tabrptgfreq

FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_CGCAHPSRecommendProvOffice_Tiles tabrptg

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhg
	ON mdmhg.EPIC_DEPARTMENT_ID = tabrptg.epic_department_id

	--LEFT OUTER JOIN #summary summary
	--ON summary.event_id = tabrptg.event_id

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id

	--INNER JOIN #pgdept pgdept
	--ON tabrptg.epic_department_id = pgdept.groupers_epic_department_id

    WHERE   tabrptg.event_date>=@locstartdate
            AND tabrptg.event_date<@locenddate
			AND g.ambulatory_flag = 1
			--AND (ambulatory_flag = 1 and community_health_flag = 0)
			--AND (ambulatory_flag = 1 or community_health_flag = 1)
			--AND mdmhg.HOSPITAL_CODE = 'UVA-MC'

	--AND SUBSTRING(summary.Survey_Designator,1,2) = 'MD'
	--AND SUBSTRING(summary.Survey_Designator,1,2) IN ('MD','MT','TP')

	GROUP BY
		tabrptg.epic_department_id,
		tabrptg.epic_department_name

SELECT [Service]
      ,[Received_Date]
	  ,SUBSTRING(Received_Date, 1, CHARINDEX('-',Received_Date) -2) AS Start_Received_Date_str
	  ,CAST(SUBSTRING(Received_Date, 1, CHARINDEX('-',Received_Date) -2) AS DATE) AS Start_Received_Date_dte
	  ,ddte.Fyear_num
	  ,ddte.fmonth_num
	  ,ddte.month_num
	  --,ddte.fmonth_name
	  ,ddte.month_name
      ,[IT_DEPT_ID]
	  ,TRIM('''' FROM IT_DEPT_ID) AS IT_DEPT_ID_TRIM
	  ,CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0)) AS IT_DEPT_ID_NUMERIC
	  ,g.epic_department_id
	  ,g.epic_department_name
      ,[Questions]
      ,[Very_Poor_n]
      ,[Poor_n]
      ,[Fair_n]
      ,[Good_n]
      ,[Very_Good_n]
	  ,ambulatory_flag
	  ,community_health_flag
	  ,CAST(CAST(pgf.Very_Poor_n AS DECIMAL(10,2)) * 0.00 AS DECIMAL(10,2)) AS Very_Poor_Total_Weighted_Score
	  ,CAST(CAST(pgf.Poor_n AS DECIMAL(10,2)) * 25.00 AS DECIMAL(10,2)) AS Poor_Total_Weighted_Score
	  ,CAST(CAST(pgf.Fair_n AS DECIMAL(10,2)) * 50.00 AS DECIMAL(10,2)) AS Fair_Total_Weighted_Score
	  ,CAST(CAST(pgf.Good_n AS DECIMAL(10,2)) * 75.00 AS DECIMAL(10,2)) AS Good_Total_Weighted_Score
	  ,CAST(CAST(pgf.Very_Good_n AS DECIMAL(10,2)) * 100.00 AS DECIMAL(10,2)) AS Very_Good_Total_Weighted_Score
	  ,CAST(pgf.Very_Poor_n AS DECIMAL(10,2)) + CAST(pgf.Poor_n AS DECIMAL(10,2)) + CAST(pgf.Fair_n AS DECIMAL(10,2)) + CAST(pgf.Good_n AS DECIMAL(10,2)) + CAST(pgf.Very_Good_n AS DECIMAL(10,2)) AS Total_n
	  ,mdmhg.HOSPITAL_CODE

  INTO #pg

  FROM [DS_HSDM_App].[Stage].[PGFusion_Query_Results] pgf
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Date ddte
  ON ddte.day_date = CAST(SUBSTRING(Received_Date, 1, CHARINDEX('-',Received_Date) -2) AS DATE)
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0))

	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_Hospital_Group mdmhg
	ON mdmhg.EPIC_DEPARTMENT_ID = CAST(TRIM('''' FROM IT_DEPT_ID) AS NUMERIC(18,0))
  WHERE Received_Date <> 'Total'
  AND IT_DEPT_ID <> 'Total'

  SELECT
    'PXO - Likelihood of Recommending (Practice)' AS Metric,
    'PGFusion' AS Source,
    'MTD' AS [Time Period],
	--SUM(Very_Poor_Total_Weighted_Score) AS Very_Poor_Total_Weighted_Score,
	--SUM(Poor_Total_Weighted_Score) AS Poor_Total_Weighted_Score,
	--SUM(Fair_Total_Weighted_Score) AS Fair_Total_Weighted_Score,
	--SUM(Good_Total_Weighted_Score) AS Good_Total_Weighted_Score,
	--SUM(Very_Good_Total_Weighted_Score) AS Very_Good_Total_Weighted_Score,
	SUM(Very_Poor_Total_Weighted_Score) +
	SUM(Poor_Total_Weighted_Score) +
	SUM(Fair_Total_Weighted_Score) +
	SUM(Good_Total_Weighted_Score) +
	SUM(Very_Good_Total_Weighted_Score) AS weighted_score,
	SUM(pg.Total_n) AS LikelihoodRecommendPracticeResponse,
	CAST(SUM(Very_Poor_Total_Weighted_Score) +
	SUM(Poor_Total_Weighted_Score) +
	SUM(Fair_Total_Weighted_Score) +
	SUM(Good_Total_Weighted_Score) +
	SUM(Very_Good_Total_Weighted_Score) AS DECIMAL(10,2)) / CAST(SUM(pg.Total_n) AS DECIMAL(10,2)) AS Mean,
	GETDATE() AS RunTime
  FROM
  (
  SELECT
	   Very_Poor_Total_Weighted_Score,
	   Poor_Total_Weighted_Score,
	   Fair_Total_Weighted_Score,
	   Good_Total_Weighted_Score,
       Very_Good_Total_Weighted_Score,
	   Total_n
  FROM #pg
  WHERE Questions = 'Likelihood of recommending'
  AND Start_Received_Date_dte = '8/1/2023'
  AND ambulatory_flag = 1
  --AND (ambulatory_flag = 1 or community_health_flag = 1)
) pg

  SELECT
    pg.epic_department_id,
	pg.epic_department_name,
	SUM(Very_Poor_Total_Weighted_Score) +
	SUM(Poor_Total_Weighted_Score) +
	SUM(Fair_Total_Weighted_Score) +
	SUM(Good_Total_Weighted_Score) +
	SUM(Very_Good_Total_Weighted_Score) AS weighted_score,
	SUM(pg.Total_n) AS LikelihoodRecommendPracticeResponse,
	GETDATE() AS RunTime

  INTO #pgfreq

  FROM
  (
  SELECT
       epic_department_id,
	   epic_department_name,
	   Very_Poor_Total_Weighted_Score,
	   Poor_Total_Weighted_Score,
	   Fair_Total_Weighted_Score,
	   Good_Total_Weighted_Score,
       Very_Good_Total_Weighted_Score,
	   Total_n
  FROM #pg
  WHERE Questions = 'Likelihood of recommending'
  AND Start_Received_Date_dte = '8/1/2023'
  --AND (ambulatory_flag = 1 or community_health_flag = 1)
  AND (ambulatory_flag = 1)
) pg
GROUP BY
	pg.epic_department_id,
	pg.epic_department_name

SELECT
	tabrptg.epic_department_id,
	tabrptg.epic_department_name,
	tabrptg.LikelihoodRecommendPracticeResponse AS tabrptg_response,
	pgf.pgf_responses,
	CAST(tabrptg.LikelihoodRecommendPracticeResponse AS NUMERIC(10,2)) - pgf.pgf_responses AS diff_responses
FROM #tabrptgfreq tabrptg
LEFT OUTER JOIN
(
SELECT
	epic_department_id,
	LikelihoodRecommendPracticeResponse AS pgf_responses
FROM #pgfreq
) pgf
ON pgf.epic_department_id = tabrptg.epic_department_id

WHERE CAST(tabrptg.LikelihoodRecommendPracticeResponse AS NUMERIC(10,2)) - pgf.pgf_responses IS NULL
OR (CAST(tabrptg.LikelihoodRecommendPracticeResponse AS NUMERIC(10,2)) - pgf.pgf_responses IS NOT NULL 
AND CAST(tabrptg.LikelihoodRecommendPracticeResponse AS NUMERIC(10,2)) - pgf.pgf_responses <> 0)

ORDER BY tabrptg.epic_department_id

/*
SELECT
    'PXO - Likelihood of Recommending (Practice)' AS Metric,
    'Stored Procedure' AS Source,
	epic_department_id AS Department_Id,
	epic_department_name AS Department_Name,
	event_id AS PG_Survey_Id,
	PG_AcctNbr AS AcctNbr,
	Pat_Enc_CSN_Id AS CSN,
	person_id AS MRN,
	DISDATE AS Encounter_Date,
	Survey_Designator,
	event_date AS Received_Date,
	weighted_score,
	GETDATE() AS RunTime
	FROM #summary
	WHERE LikelihoodRecommendPracticeResponse = 1
	AND ambulatory_flag = 1
	--AND (ambulatory_flag = 1 and community_health_flag = 0)
	--AND (ambulatory_flag = 1 or community_health_flag = 1)

	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND SUBSTRING(Survey_Designator,1,2) IN ('MD','MT','TP')
	--AND epic_department_id IN ( -- June 2023
	--10295005, -- CPSE CH CANC CTR
	--10348044  --	ZCSC PLASTIC SURGERY
	--)
	/*10228028	NRDG PULM ALLERGY
10242001	UVPC TELEMEDICINE
10276008	CPSS UVA OBGYN
10295005	CPSE CH CANC CTR
10348044	ZCSC PLASTIC SURGERY*/
/*	AND epic_department_id IN ( -- July 2023
	10228028, -- NRDG PULM ALLERGY
	10242001, -- UVPC TELEMEDICINE
	10276008, -- CPSS UVA OBGYN
	10295005, -- CPSE CH CANC CTR
	10348044  --ZCSC PLASTIC SURGERY*/
	AND epic_department_id IN ( -- August 2023
	10242001, -- UVPC TELEMEDICINE
	10276008, -- CPSS UVA OBGYN
	10295005, -- CPSE CH CANC CTR
	10348044  --ZCSC PLASTIC SURGERY
	)
	ORDER BY epic_department_id, event_id
*/

GO