USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL, 
        @enddate SMALLDATETIME = NULL

--SET @startdate = '7/1/2020 00:00 AM'
--SET @startdate = '12/1/2021 00:00 AM'
--SET @enddate = '12/31/2021 11:59 PM'
--SET @startdate = '2/1/2022 00:00 AM'
--SET @enddate = '2/28/2022 11:59 PM'
--SET @startdate = '7/1/2021 00:00 AM'
SET @startdate = '7/1/2023 00:00 AM'
--SET @startdate = '12/1/2022 00:00 AM'
--SET @enddate = '7/25/2023 11:59 PM'
--SET @enddate = '9/30/2023 11:59 PM'
--SET @startdate = '11/1/2023 00:00 AM'
SET @enddate = '11/30/2023 11:59 PM'

--CREATE PROCEDURE [ETL].[uspSrc_SvcLine_ER_OverallCareRating]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 

/*****************************************************************************************************************
WHAT:	Create procedure Rptg.uspSrc_SvcLine_ER_OverallCareRating
WHO :	Tom Burgan	
WHEN:	6/30/2022
WHY :	Calculate mean weighted score for "Overall rating of care received during your visit" question on ER survey (Metric Id: 556)
------------------------------------------------------------------------------------------------------------------
INFO:	
	INPUTS:	dbo.Fact_PressGaney_Responses	        
			dbo.Dim_Date
			dbo.Dim_PG_Question
			dbo.Fact_Pt_Acct
			dbo.Dim_Pt
			dbo.Dim_Physcn

	OUTPUTS: Data
   
------------------------------------------------------------------------------------------------------------------
MODS:
        06/30/2022--TMB--Create stored procedure
		07/18/2022--TMB--Add columns overall_responses and overall_weighted_score
		09/15/2022--TMB--Edit logic that filters events within a date range
		10/19/2022--TMB--Alter query to extract response detail at the survey level; use DM tables/views
******************************************************************************************************************/
    SET NOCOUNT ON;

	/*testing*/
    --DECLARE
    --    @startdate SMALLDATETIME = '7/1/2022'
    --   ,@enddate SMALLDATETIME = '10/31/2022';
	/*end testing*/

---------------------------------------------------
----get default Balanced Scorecard date range
IF  @startdate IS NULL
AND @enddate IS NULL
BEGIN 
    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT
                                                 ,@enddate OUTPUT;

    ---BDD 01/10/2019 for this proc, take it back another 6 months to the begin of the FY
	---  special (hopefully short term) reporting request
    SET @startdate = DATEADD(mm,-6,@startdate)

END 
----------------------------------------------------

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate

IF OBJECT_ID('tempdb..#ed ') IS NOT NULL
DROP TABLE #ed

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

    SELECT DISTINCT
            CAST('Emergency-Press Ganey' AS VARCHAR(50)) AS event_type
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

    INTO #ed

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				--,resp.sk_Dim_Clrt_DEPt
				,enc.sk_Dim_Clrt_DEPt
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
				,resp.sk_Dim_Physcn AS resp_sk_Dim_Physcn				,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
				      WHEN resp.sk_Dim_Physcn = -1 THEN -999
				      WHEN resp.sk_Dim_Physcn = 0 THEN -999
				      ELSE -999
				 END AS sk_Dim_Physcn
				,loc_master.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,loc_master.LOC_ID AS rev_location_id
				,loc_master.REV_LOC_NAME AS rev_location
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
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '326' -- Age question for ER
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt -- 20220725: Department key missing in response Fact table for some DS encs
				--ON dep.sk_Dim_Clrt_DEPt = enc.sk_Dim_Clrt_DEPt
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
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map --Missing in DMT>DS_HSDM_App
                ON map.Deptid = CAST(loc_master.FINANCE_COST_CODE AS INTEGER)
		LEFT OUTER JOIN Stage.AmbOpt_Excluded_Department excl  --Missing in DMT>DS_HSDM_App, is in app_DEV
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
		WHERE   resp.Svc_Cde='ER' AND resp.sk_Dim_PG_Question IN ('389') -- Overall rating of care received during your visit
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
				--AND excl.DEPARTMENT_ID IS NULL
			    AND pat.IS_VALID_PAT_YN = 'Y'
			    --AND appts.ENC_TYPE_C NOT IN ('2505','2506')
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

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    --ORDER BY rec.day_date;

	SELECT
		event_type,
        event_count,
        event_date,
        event_id,
        event_category,
        sk_Dim_PG_Question,
        PG_Question_Variable,
        PG_Question_Text,
        fmonth_num,
        FYear_name,
        Fyear_num,
        person_id,
        person_name,
        person_birth_date,
        person_gender,
        report_period,
        report_date,
        service_line_id,
        service_line,
        sub_service_line_id,
        sub_service_line,
        opnl_service_id,
        opnl_service_name,
        hs_area_id,
        hs_area_name,
        corp_service_line_id,
        corp_service_line,
        provider_id,
        provider_name,
        practice_group_id,
        practice_group_name,
        sk_Dim_Pt,
        sk_Fact_Pt_Acct,
        sk_Fact_Pt_Enc_Clrt,
        pod_id,
        pod_name,
        hub_id,
        hub_name,
        epic_department_id,
        epic_department_name,
        epic_department_name_external,
        peds,
        transplant,
        sk_Dim_Physcn,
        BUSINESS_UNIT,
        Prov_Typ,
        Staff_Resource,
        som_group_id,
        som_group_name,
        rev_location_id,
        rev_location,
        financial_division_id,
        financial_division_name,
        financial_sub_division_id,
        financial_sub_division_name,
        som_department_id,
        som_department_name,
        som_division_id,
        som_division_name,
        som_hs_area_id,
        som_hs_area_name,
        upg_practice_flag,
        upg_practice_region_id,
        upg_practice_region_name,
        upg_practice_id,
        upg_practice_name,
        F2F_Flag,
        ENC_TYPE_C,
        ENC_TYPE_TITLE,
        Lip_Flag,
        FINANCE_COST_CODE,
        Prov_Based_Clinic,
        Map_Type,
        weighted_score,
        organization_id,
        organization_name,
        service_id,
        service_name,
        clinical_area_id,
        clinical_area_name,
        ambulatory_flag,
		community_health_flag,
        Survey_Designator	
	FROM #ed
	ORDER BY epic_department_id
	        ,provider_name
	        ,event_date

--/* -- #summary
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
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS OverallRatingOfCareResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator
				  ,fmonth_num
				  ,Fyear_num

	INTO #summary

	FROM #ed

	--SELECT *
	--FROM #summary
	--WHERE OverallRatingOfCareResponse = 1
	----AND epic_department_name LIKE 'OCIR%'
	--ORDER BY epic_department_id
	--                   ,epic_department_name
	--				   ,weighted_score DESC

	SELECT
				  -- som_division_id
				  --,som_division_name
				  --,som_department_id
				  --,som_department_name
	     --         ,epic_department_name
	     --         ,epic_department_id
				  --,ambulatory_flag
/*
				   Fyear_num
				  ,fmonth_num
*/
				  --,SUM(weighted_score) AS weighted_score
				   SUM(weighted_score) AS weighted_score
				  --,SUM(OverallRatingOfCareResponse) AS OverallRatingOfCareResponse
				  ,SUM(OverallRatingOfCareResponse) AS Responses
	FROM #summary
	WHERE OverallRatingOfCareResponse = 1
	--AND ambulatory_flag = 1
	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND community_health_flag = 0
/*
	GROUP BY
				  -- som_division_id
				  --,som_division_name
				  --,som_department_id
				  --,som_department_name
	     --         ,epic_department_name
				  --,epic_department_id
				  --,ambulatory_flag
				   Fyear_num
				  ,fmonth_num
*/
/*
	ORDER BY
				  -- som_division_name
				  --,som_department_name
	     --         ,epic_department_name
				   Fyear_num
				  ,fmonth_num
*/

/*
	SELECT SUM(weighted_score) AS weighted_score
				  ,SUM(OverallRatingOfCareResponse) AS OverallRatingOfCareResponse
	FROM #summary
	WHERE OverallRatingOfCareResponse = 1
	--AND ambulatory_flag = 1
	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND community_health_flag = 0
*/
/*
SELECT SUM(weighted_score) AS weighted_score
				  ,SUM(event_count) AS OverallRatingOfCareResponse
FROM DS_HSDM_App.TabRptg.Dash_BalancedScorecard_ER_OverallCareRating_Tiles tabrptg
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   event_date>=@locstartdate
            AND event_date<@locenddate
	        AND community_health_flag = 0
*/
/*
	IF OBJECT_ID('tempdb..#surveys ') IS NOT NULL
	DROP TABLE #surveys;

	IF OBJECT_ID('tempdb..#weighted_scores ') IS NOT NULL
	DROP TABLE #weighted_scores;

	IF OBJECT_ID('tempdb..#section_averages ') IS NOT NULL
	DROP TABLE #section_averages;

	IF OBJECT_ID('tempdb..#overall ') IS NOT NULL
	DROP TABLE #overall;

	IF OBJECT_ID('tempdb..#RptgTemp ') IS NOT NULL
	DROP TABLE #RptgTemp;  

    SELECT
            p.SURVEY_ID
           ,p.RECDATE
           ,p.MRN_int
           ,p.NPINumber
           ,p.PAT_NAME
           ,p.AGE
           ,p.BIRTH_DATE
           ,p.F68 -- Overall rating of care received during your visit, Overall Assessment
		   --when relevant question is NULL set exclude flag
           ,CASE WHEN p.F68 IS NULL THEN 1
                 ELSE 0
            END AS exclude_flag
        INTO
            #surveys
        FROM
            (
             --pivoted
                     SELECT DISTINCT
                            pm.SURVEY_ID
                           ,pm.RECDATE
                           ,LEFT(pm.VALUE, 20) AS VALUE
                           ,qstn.VARNAME
                           ,fpa.MRN_int
                           ,dp.NPINumber
                           ,CONCAT(pat.PT_LNAME, ',', pat.PT_FNAME_MI) AS PAT_NAME
                           ,pat.BIRTH_DT AS BIRTH_DATE
                        FROM
                            DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses AS pm 
                        INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
                        ON  pm.sk_Dim_PG_Question = qstn.sk_Dim_PG_Question
                        INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
                        ON  pm.sk_Fact_Pt_Acct = fpa.sk_Fact_Pt_Acct
                        INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Pt AS pat
                        ON  fpa.sk_Dim_Pt = pat.sk_Dim_Pt
                        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn AS dp
                        ON  pm.sk_Dim_Physcn = dp.sk_Dim_Physcn
                        WHERE
                            pm.Svc_Cde = 'ER'
                            AND qstn.VARNAME IN ('F68', 'AGE')
                            AND pm.RECDATE >= @startdate
                            AND pm.RECDATE <= @enddate
            ) AS pivoted PIVOT ( MAX(VALUE) FOR VARNAME IN (F68, AGE) )

AS p;


--convert to weighted scores
    SELECT
            pe.SURVEY_ID
           ,pe.RECDATE
           ,pe.AGE
           --OVERALL
           ,CAST(CASE pe.F68
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS F68
        INTO
            #weighted_scores
        FROM
            #surveys AS pe
        WHERE
            pe.RECDATE >= @startdate
            AND pe.RECDATE <= @enddate
			--surveys with relevant question missing is excluded from the denominator
            AND pe.exclude_flag = 0;


--average each scores from each section
    SELECT
            ws.SURVEY_ID
           ,ws.RECDATE
           ,ws.AGE
           ,ws.F68
           ,(
             SELECT
                    AVG(FO.ovr)
                FROM
                    ( VALUES ( ws.F68) ) AS FO (ovr)
            ) AS SectionFO
           ,(
             SELECT
                    COUNT(FO.ovr)
                FROM
                    ( VALUES ( ws.F68) ) AS FO (ovr)
            ) AS SectionFODenom
           ,(
             SELECT
                    SUM(FO.ovr)
                FROM
                    ( VALUES ( ws.F68) ) AS FO (ovr)
            ) AS SectionFONumer
        INTO
            #section_averages
        FROM
            #weighted_scores AS ws
        ORDER BY
            ws.SURVEY_ID;

--overall mean of all section averages
--this is the method press ganey uses
    SELECT
            sa.SURVEY_ID
           ,sa.RECDATE
           ,dd.Fyear_num
           ,dd.FYear_name
           ,dd.fmonth_num
           ,dd.month_begin_date
           ,sa.AGE
           ,sa.F68
           ,sa.SectionFO
		   ,sa.SectionFODenom
		   ,sa.SectionFONumer
           ,(
             SELECT
                    AVG(total.ttl)
                FROM
                    ( VALUES ( sa.SectionFO) )
                    AS total (ttl)
            ) AS All_Sections
           ,(
             SELECT
                    COUNT(total.ttl)
                FROM
                    ( VALUES ( sa.SectionFO) )
                    AS total (ttl)
            ) AS All_Sections_Denom
           ,(
             SELECT
                    SUM(total.ttl)
                FROM
                    ( VALUES ( sa.SectionFO) )
                    AS total (ttl)
            ) AS All_Sections_Numer
        INTO
            #overall
        FROM
            #section_averages AS sa
        INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS dd
        ON  sa.RECDATE = dd.day_date
        ORDER BY
            sa.SURVEY_ID;
 

-----------------
--final count, averaging by month, fiscal year
    SELECT DISTINCT
            CAST('Emergency-Press Ganey' AS VARCHAR(50)) AS event_type
           ,date_dim.month_begin_date AS event_date
           ,CAST(NULL AS VARCHAR(25)) AS event_category
           ,o.event_count
           ,date_dim.fmonth_num
           ,date_dim.FYear_name
           ,date_dim.Fyear_num
           ,o.epic_department_id
           ,o.epic_department_name
           ,o.epic_department_name_external
           ,o.hs_area_id
           ,o.hs_area_name
           ,o.opnl_service_id
           ,o.opnl_service_name
           ,o.service_line
           ,o.service_line_id
           ,o.sub_service_line
           ,o.sub_service_line_id
           ,o.practice_group_id
           ,o.practice_group_name
           ,o.overall_mean
		   ,o.overall_responses
		   ,o.overall_weighted_score
        INTO
            #RptgTemp
        FROM
            DS_HSDW_Prod.Rptg.vwDim_Date AS date_dim
        LEFT OUTER JOIN (
                         SELECT DISTINCT
                                Fyear_num
                               ,month_begin_date
                               ,COUNT(SURVEY_ID) OVER (PARTITION BY Fyear_num ORDER BY month_begin_date) AS event_count
                               ,AVG(All_Sections) OVER (PARTITION BY Fyear_num ORDER BY month_begin_date) AS overall_mean
                               ,SUM(All_Sections_Denom) OVER (PARTITION BY Fyear_num ORDER BY month_begin_date) AS overall_responses
                               ,SUM(All_Sections_Numer) OVER (PARTITION BY Fyear_num ORDER BY month_begin_date) AS overall_weighted_score
                               ,loc.epic_department_id
                               ,loc.epic_department_name
                               ,loc.epic_department_name_external
                               ,loc.hs_area_id
                               ,loc.hs_area_name
                               ,loc.sub_service_line_id
                               ,loc.sub_service_line
                               ,loc.opnl_service_id
                               ,loc.opnl_service_name
                               ,loc.practice_group_id
                               ,loc.practice_group_name
                               ,loc.service_line_id
                               ,loc.service_line
                            FROM
                                #overall
                            LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS loc
                            ON  loc.epic_department_id = '10243026'
                        ) AS o
        ON  o.month_begin_date = date_dim.month_begin_date
        WHERE
            date_dim.day_date >= @startdate
            AND date_dim.day_date <= @enddate;

    --INSERT Stage.Balscore_Dash_ER_OverallCareRating
    --        (event_type
    --        ,event_date
    --        ,event_category
    --        ,fmonth_num
    --        ,FYear_name
    --        ,Fyear_num
    --        ,epic_department_id
    --        ,epic_department_name
    --        ,epic_department_name_external
    --        ,hs_area_id
    --        ,hs_area_name
    --        ,opnl_service_id
    --        ,opnl_service_name
    --        ,Service_Line
    --        ,Service_Line_ID
    --        ,Sub_Service_Line
    --        ,Sub_Service_Line_ID
    --        ,practice_group_id
    --        ,practice_group_name
    --        ,event_count
    --        ,overall_mean
	   --     ,overall_responses -- INTEGER
	   --     ,overall_weighted_score -- FLOAT
		  --  )
        SELECT DISTINCT
                i.event_type
               ,i.event_date
               ,i.event_category
               ,i.fmonth_num
               ,i.FYear_name
               ,i.Fyear_num
               ,i.epic_department_id
               ,i.epic_department_name
               ,i.epic_department_name_external
               ,i.hs_area_id
               ,i.hs_area_name
               ,i.opnl_service_id
               ,i.opnl_service_name
               ,i.service_line
               ,i.service_line_id
               ,i.sub_service_line
               ,i.sub_service_line_id
               ,i.practice_group_id
               ,i.practice_group_name
               ,i.event_count
               ,i.overall_mean
			   ,i.overall_responses
			   ,i.overall_weighted_score
            FROM
                #RptgTemp AS i;

        --SELECT DISTINCT
        --        i.event_type
        --       ,i.fmonth_num
        --       ,i.FYear_name
        --       ,i.Fyear_num
        --       ,i.event_date
        --       ,i.event_count
        --       ,i.overall_mean
        --    FROM
        --        #RptgTemp AS i
				
			--ORDER BY i.event_date;

SELECT event_date, event_count, overall_mean
FROM #RptgTemp
*/
GO


