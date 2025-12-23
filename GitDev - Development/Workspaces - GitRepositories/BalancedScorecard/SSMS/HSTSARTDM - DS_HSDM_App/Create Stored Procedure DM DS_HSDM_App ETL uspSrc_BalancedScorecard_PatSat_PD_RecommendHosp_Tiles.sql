USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [ETL].[uspSrc_BalancedScorecard_PatSat_PD_RecommendHosp_Tiles]
    (
     @startdate SMALLDATETIME=NULL
    ,@enddate SMALLDATETIME=NULL
    )
AS 

--DECLARE @startdate SMALLDATETIME = NULL, 
--        @enddate SMALLDATETIME = NULL

--SET @startdate = '7/1/2023 00:00 AM'
--SET @enddate = '6/30/2024 11:59 PM'

/**********************************************************************************************************************
WHAT: Balanced Scorecard Reporting:  IP Pediatric Likelihood Recommending Hospital (Metric Id 865)
WHO : Tom Burgan
WHEN: 05/20/2024
WHY : Press Ganey Targeted IP Pediatric (PD) results for survey question:
      "Likelihood of your recommending this hospital to others"
-----------------------------------------------------------------------------------------------------------------------
INFO:                
      INPUTS:   DS_HSDW_Prod.Rptg.vwDim_Date
					   DS_HSDW_Prod.dbo.Fact_PressGaney_Responses
					   DS_HSDW_Prod.Rptg.vwDim_PG_Question
					   DS_HSDW_Prod.Rptg.vwFact_Pt_Acct
					   DS_HSDW_Prod.Rptg.vwDim_Patient
					   DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
					   DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
					   DS_HSDW_Prod.Rptg.vwDim_Physcn
					   DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt
					   DS_HSDM_APP.Rptg.vwCLARITY_SER_OT_PROV_TYPE
					   DS_HSDW_Prod.dbo.Dim_Clrt_DEPt
					   DS_HSDW_Prod.dbo.Ref_MDM_Location_Master_History
					   DS_HSDM_APP.Rptg.UOS_Visit_Map_Epic
					   DS_HSDM_APP.Stage.AmbOpt_Excluded_Department
					   DS_HSDM_APP.Stage.Scheduled_Appointment
					   DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined
					   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt
					   DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt
                
      OUTPUTS:  ETL.uspSrc_BalancedScorecard_PatSat_PD_RecommendHosp_Tiles
--------------------------------------------------------------------------------------------------------------------------
MODS: 	
      07/09/2024 - TMB - created stored procedure

**************************************************************************************************************************************************************/
   
    SET NOCOUNT ON; 

---------------------------------------------------
 ----get default Balanced Scorecard date range
 IF @startdate IS NULL AND @enddate IS NULL
    EXEC etl.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT
----------------------------------------------------

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate

    SELECT DISTINCT
            CAST('IP Pediatric' AS VARCHAR(50)) AS event_type
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
		   ,CASE WHEN dc_phys.type_of_HSF_contract = 'UVACHMG Employed' THEN 1 ELSE 0 END  CH_Hosp_Based_YN
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
                 END AS DECIMAL(10, 2)) AS weighted_score
	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag
		   ,pm.Survey_Designator

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,resp.RECDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				--,resp.sk_Dim_Clrt_DEPt		-- 05/21/2024
				,enc.sk_Dim_Clrt_DEPt		-- 05/21/2024
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
		--LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Hsp_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        --ON ddte.date_key = enc.sk_Cont_Dte
		        ON ddte.date_key = enc.sk_Adm_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '2092' -- Age question for PD
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON dep.sk_Dim_Clrt_DEPt = enc.sk_Dim_Clrt_DEPt	-- 05/21/2024
				--ON dep.sk_Dim_Clrt_DEPt = resp.sk_Dim_Clrt_DEPt	-- 05/21/2024
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
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map --Missing in DMT>DS_HSDM_App
                ON map.Deptid = CAST(mdm.FINANCE_COST_CODE AS INTEGER)
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
		WHERE   resp.Svc_Cde='PD' AND resp.sk_Dim_PG_Question IN ('2347') -- Likelihood of your recommending this hospital to others
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
			    AND pat.IS_VALID_PAT_YN = 'Y'
	) AS pm
ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn           AS dc_phys ON pm.sk_Dim_Physcn = dc_phys.sk_Dim_Physcn
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

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    ORDER BY rec.day_date;

GO


