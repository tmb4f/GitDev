USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME
DECLARE @enddate SMALLDATETIME

SET @startdate = '7/1/2020 00:00 AM'
--SET @startdate = '3/15/2020 00:00 AM'
--SET @startdate = '4/1/2020 00:00 AM'
--SET @startdate = '12/1/2020 00:00 AM'
--SET @enddate = '3/31/2020 11:59 PM'
--SET @enddate = '3/14/2020 11:59 PM'
--SET @enddate = '10/31/2020 11:59 PM'
SET @enddate = '2/28/2021 11:59 PM'

-- =====================================================================================
-- Alter procedure uspSrc_BalancedScorecard_CGCAHPS_RecommendProvOffice
-- =====================================================================================

--ALTER PROCEDURE [ETL].[uspSrc_BalancedScorecard_CGCAHPS_RecommendProvOffice]
--    (
--     @startdate SMALLDATETIME=NULL
--    ,@enddate SMALLDATETIME=NULL
--    )
--AS 

/**********************************************************************************************************************
WHAT: Balanced Scorecard Reporting:  CGCAHPS Recommend Provider Office
WHO : Tom Burgan
WHEN: 7/16/2018
WHY : Press Ganey CGCAHPS results for survey question:
      "Would you recommend this provider's office to your family and friends?"
-----------------------------------------------------------------------------------------------------------------------
INFO:                  
      INPUTS:   DS_HSDW_Prod.Rptg.vwDim_Date
				DS_HSDW_Prod.dbo.Fact_PressGaney_Responses
				DS_HSDW_Prod.Rptg.vwDim_PG_Question
				DS_HSDW_Prod.Rptg.vwFact_Pt_Acct
				DS_HSDW_Prod.Rptg.vwDim_Patient
				DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Atn_Prov_All
                DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc
				DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt
                DS_HSDM_App.Rptg.vwCLARITY_SER_OT_PROV_TYPE
				DS_HSDW_Prod.dbo.Dim_Clrt_DEPt
				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                DS_HSDM_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv
				DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt
                
      OUTPUTS:  ETL.uspSrc_BalancedScorecard_CGCAHPS_RecommendProvOffice
--------------------------------------------------------------------------------------------------------------------------
MODS: 	
      07/16/2018 - TMB - create stored procedure
	  01/10/2020 - TMB - edit logic that assigns Epic Provider Id to a survey response
      03/31/2020 - TMB - add join to SOM Div/Subdiv view
**************************************************************************************************************************************************************/
   
    SET NOCOUNT ON; 

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

IF OBJECT_ID('tempdb..#cgcahps ') IS NOT NULL
DROP TABLE #cgcahps

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

IF OBJECT_ID('tempdb..#TabRptg ') IS NOT NULL
DROP TABLE #TabRptg

    SELECT DISTINCT
            --CAST('Outpatient-CGCAHPS' AS VARCHAR(50)) AS event_type
		   --,CASE WHEN pm.VALUE IS NULL THEN 0
		   CASE WHEN pm.VALUE IS NULL THEN 0
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
           --,pm.MRN_int AS person_id		--patient
           --,pm.PAT_NAME AS person_name		--patient
           --,pm.BIRTH_DATE AS person_birth_date--patient
           --,pm.SEX AS person_gender
           --,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           --,rec.day_date AS report_date
           --,pm.service_line_id
           --,pm.service_line
           --,pm.sub_service_line_id
           --,pm.sub_service_line
           --,pm.opnl_service_id
           --,pm.opnl_service_name
           ,pm.hs_area_id
           ,pm.hs_area_name
		   --,pm.corp_service_line_id
		   --,pm.corp_service_line
		   --,pm.provider_id
		   --,pm.provider_name
		   --,pm.practice_group_id
		   --,pm.practice_group_name
		   --,pm.sk_Dim_Pt
     --      ,pm.sk_Fact_Pt_Acct
     --      ,CAST(NULL AS INTEGER) AS sk_Fact_Pt_Enc_Clrt
           ,pm.pod_id
		   ,pm.pod_name
     --      ,pm.hub_id
		   --,pm.hub_name
           ,pm.epic_department_id
           ,pm.epic_department_name
     --      ,pm.epic_department_name_external
     --      ,CASE WHEN pm.AGE<18 THEN 1
     --            ELSE 0
     --       END AS peds
     --      ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
     --            ELSE 0
     --       END AS transplant
		   --,pm.sk_Dim_Physcn
		   ,CASE WHEN pm.sk_Dim_PG_Question IN ('805','809') AND pm.VALUE = 'Yes, definitely' THEN 1 -- Yes definitely, Yes somewhat, No scale questions
						ELSE CASE WHEN pm.sk_Dim_PG_Question IN ('707','721','731') AND pm.VALUE = 'Yes' THEN 1 -- Yes/No scale questions
						                     ELSE 0
						          END
			END AS TOP_BOX

    INTO #cgcahps

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,DISDATE
				,CAST(VALUE AS VARCHAR(500)) AS VALUE
				--,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
				,loc_master.POD_ID AS pod_id
		        ,loc_master.PFA_POD AS pod_name
				--,loc_master.HUB_ID AS hub_id
		  --      ,loc_master.HUB AS hub_name
                   -- MDM
				--,mdm.service_line_id
				--,mdm.service_line
				--,mdm.sub_service_line_id
				--,mdm.sub_service_line
				--,mdm.opnl_service_id
				--,mdm.opnl_service_name
				--,mdm.corp_service_line_id
				--,mdm.corp_service_line
				,mdm.hs_area_id
				,mdm.hs_area_name
				--,mdm.practice_group_id
				--,mdm.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdm.epic_department_name
				--,mdm.epic_department_name_external
				--,fpa.MRN_int
				--,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,qstn.QUESTION_TEXT
				--,fpa.sk_Fact_Pt_Acct
				--,prov.PROV_ID AS provider_id
				--,prov.Prov_Nme AS provider_name
				--,CAST(CONCAT(pat.LastName, ',', pat.FirstName + ' ' + RTRIM(COALESCE(CASE WHEN pat.MiddleName = 'Unknown' THEN NULL ELSE pat.MiddleName END,''))) AS VARCHAR(200)) AS PAT_NAME
				--,pat.BirthDate AS BIRTH_DATE
				--,pat.SEX
				--,resp.Load_Dtm
				--,CASE WHEN resp.[sk_Dim_Physcn] > 0 THEN resp.sk_Dim_Physcn
				--      WHEN dschatn.[sk_Dim_Physcn] > 0 THEN dschatn.sk_Dim_Physcn
				--      WHEN resp.sk_Dim_Physcn = -1 THEN -999
				--      WHEN resp.sk_Dim_Physcn = 0 THEN -999
				--      ELSE -999
				-- END AS sk_Dim_Physcn
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
				   UPG_PRACTICE_NAME
            FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
        ) AS loc_master
                ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID

                -- -------------------------------------
                -- SOM Financial Division Subdivision--
                -- -------------------------------------
				--LEFT OUTER JOIN
				--(
				--    SELECT
				--	    Epic_Financial_Division_Code,
    --                    Epic_Financial_Subdivision_Code,
    --                    Department,
    --                    Department_ID,
    --                    Organization,
    --                    Org_Number,
    --                    som_group_id,
    --                    som_group_name,
				--		som_hs_area_id,
				--		som_hs_area_name
				--	FROM Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) dvsn
				--    ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(prov.Financial_Division AS INT)
				--	    AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(prov.Financial_SubDivision AS INT))
		--WHERE   resp.Svc_Cde='MD' AND resp.sk_Dim_PG_Question IN ('805') -- Would you recommend this provider's office to your family and friends?
		--		AND resp.RECDATE>=@locstartdate
		--		AND resp.RECDATE<@locenddate
		WHERE   resp.Svc_Cde='MD'
		         AND qstn.sk_Dim_PG_Question IN (
				 809, --	CG_28CL, During your most recent visit, did clerks and receptionists at this provider's office treat you with courtesy and respect?; Yes definitely/Yes somewhat/No
				 707, --	ACO_02C, When you made this appointment for care you needed right away, did you get this appointment as soon as you thought you needed?; No/Yes
				 721, --	ACO_09C, During this visit, did you see this provider within 15 minutes of your appointment time?; No/Yes
				 731, --	ACO_14C, During this visit, did this provider have your medical records?; No/Yes
				 805  --	CG_26CL, Would you recommend this provider's office to your family and friends?; Yes definitely/Yes somewhat/No
				 )
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
	) AS pm
ON rec.day_date=pm.RECDATE
--ON rec.day_date=pm.DISDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    --LEFT OUTER JOIN (
    --                 SELECT fpec.PAT_ENC_CSN_ID
    --                       ,txsurg.day_date AS transplant_surgery_dt
    --                       ,fpec.Adm_Dtm
    --                       ,fpec.sk_Fact_Pt_Enc_Clrt
    --                       ,fpec.sk_Fact_Pt_Acct
    --                       ,fpec.sk_Dim_Clrt_Pt
    --                       ,fpec.sk_Dim_Pt
    --                 FROM   DS_HSDW_Prod.Rptg.VwFact_Pt_Trnsplnt_Clrt AS fptc
    --                 INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt AS fpec
    --                        ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
    --                 INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Date AS txsurg
    --                        ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
    --                 WHERE  txsurg.day_date BETWEEN fpec.Adm_Dtm AND fpec.Dsch_Dtm
    --                        AND txsurg.day_date<>'1900-01-01 00:00:00'
    --                ) AS tx
    --        ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
   
		-- ------------------------------------

    WHERE   rec.day_date>=@locstartdate
            AND rec.day_date<@locenddate

    --ORDER BY rec.day_date;

  -- Create index for temp table #cgcahps
  CREATE UNIQUE CLUSTERED INDEX IX_cgcahps ON #cgcahps ([pod_id], [epic_department_id], [PG_Question_Variable], [event_id])

	--SELECT 
 --          pod_id,
 --          pod_name,
 --          epic_department_id,
 --          epic_department_name,
 --          epic_department_name_external,
 --          PG_Question_Variable,
 --          sk_Dim_PG_Question,
 --          PG_Question_Text,
 --          event_category,
	--	   TOP_BOX,
	--	   event_type,
 --          event_count,
 --          event_date,
 --          event_id,
 --          fmonth_num,
 --          FYear_name,
 --          Fyear_num,
 --          person_id,
 --          person_name,
 --          person_birth_date,
 --          person_gender,
 --          report_period,
 --          report_date,
 --          service_line_id,
 --          service_line,
 --          sub_service_line_id,
 --          sub_service_line,
 --          opnl_service_id,
 --          opnl_service_name,
 --          hs_area_id,
 --          hs_area_name,
 --          corp_service_line_id,
 --          corp_service_line,
 --          provider_id,
 --          provider_name,
 --          practice_group_id,
 --          practice_group_name,
 --          sk_Dim_Pt,
 --          sk_Fact_Pt_Acct,
 --          sk_Fact_Pt_Enc_Clrt,
 --          hub_id,
 --          hub_name,
 --          peds,
 --          transplant,
 --          sk_Dim_Physcn

	--SELECT event_category,
 --          epic_department_id,
 --          epic_department_name,
 --          hs_area_id,
 --          hs_area_name,
	--       event_type,
 --          event_count,
 --          event_date,
 --          event_id,
 --          sk_Dim_PG_Question,
 --          PG_Question_Variable,
 --          PG_Question_Text,
 --          fmonth_num,
 --          FYear_name,
 --          Fyear_num,
 --          person_id,
 --          person_name,
 --          person_birth_date,
 --          person_gender,
 --          report_period,
 --          report_date,
 --          service_line_id,
 --          service_line,
 --          sub_service_line_id,
 --          sub_service_line,
 --          opnl_service_id,
 --          opnl_service_name,
 --          corp_service_line_id,
 --          corp_service_line,
 --          provider_id,
 --          provider_name,
 --          practice_group_id,
 --          practice_group_name,
 --          sk_Dim_Pt,
 --          sk_Fact_Pt_Acct,
 --          sk_Fact_Pt_Enc_Clrt,
 --          pod_id,
 --          pod_name,
 --          hub_id,
 --          hub_name,
 --          epic_department_name_external,
 --          peds,
 --          transplant,
 --          sk_Dim_Physcn
	--FROM #cgcahps
	--WHERE event_count = 1 AND hs_area_id = 1
	--WHERE event_count = 1
	--WHERE event_count = 1
	--AND ((hs_area_id = 1 AND epic_department_id NOT IN (10374001,10374002))
	--     OR (epic_department_id IN (10275003,10275005,10275006,10295005,10348004)))
	--ORDER BY pod_id
	--        ,epic_department_id
	--		,PG_Question_Variable
	--		,event_category
	--        ,event_date
	--ORDER BY epic_department_id
	--        ,event_category
	--        ,event_date

	--SELECT DISTINCT
 --          epic_department_id,
	--       event_category
	--FROM #cgcahps
	--WHERE event_count = 1 AND hs_area_id = 1
	----WHERE event_count = 1
	--ORDER BY epic_department_id
	--        ,event_category

	--SELECT DISTINCT
	--	sk_Dim_PG_Question
	--,   PG_Question_Variable
	--,   PG_Question_Text
	--,   event_category
	--FROM #cgcahps
	--ORDER BY PG_Question_Variable
	--        ,sk_Dim_PG_Question

	--SELECT 
 --          hs_area_name,
	--       service_line,
 --          epic_department_id,
 --          epic_department_name,
	--	   COUNT(*) [N]
	--FROM #cgcahps
	--WHERE event_count = 1 AND hs_area_id = 1
	--GROUP BY
 --          hs_area_name,
	--       service_line,
 --          epic_department_id,
 --          epic_department_name
	--ORDER BY
 --          hs_area_name,
	--       service_line,
 --          epic_department_id,
 --          epic_department_name
 
SELECT
       [Proc].pod_id
     , [Proc].pod_name
     , [Proc].epic_department_id
	 , [Proc].epic_department_name
	 , [Proc].PG_Question_Variable
	 , [Proc].PG_Question_Text
     --, SUM(CASE WHEN [Proc].event_category IN ('Yes, definitely') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN [Proc].event_count = 1 AND [Proc].TOP_BOX = 1 THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 --, CAST(
	 --  CAST(SUM(CASE WHEN [Proc].event_category IN ('Yes, definitely') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
  --     CAST(SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	 --  AS NUMERIC(6,4)) AS TOP_BOX_PCT

INTO #summary

FROM
(
SELECT pod_id
             , pod_name
			 , epic_department_id
			 , epic_department_name
			 , PG_Question_Variable
			 , PG_Question_Text
			 , event_count
			 , TOP_BOX
FROM #cgcahps
WHERE event_count = 1
--AND hs_area_id = 1
) [Proc]
GROUP BY [Proc].pod_id
                  , [Proc].pod_name
                  , [Proc].epic_department_id
				  , [Proc].epic_department_name
				  , [Proc].PG_Question_Variable
				  , [Proc].PG_Question_Text

SELECT *
FROM #summary
ORDER BY pod_id
                  , pod_name
                  , epic_department_id
				  , epic_department_name
				  , PG_Question_Variable
				  , PG_Question_Text

SELECT p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
       p.CG_26CL AS CG_26CL_Numerator,
       p.ACO_14C AS ACO_14C_Numerator,
       p.ACO_09C AS ACO_09C_Numerator,
       p.ACO_02C AS ACO_02C_Numerator,
       p.CG_28CL AS CG_28CL_Numerator
FROM
(
SELECT DISTINCT
	pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , Numerator
  , PG_Question_Variable
FROM #summary
) AS pivoted PIVOT ( MAX(Numerator) FOR PG_Question_Variable IN (CG_28CL, ACO_02C, ACO_09C, ACO_14C, CG_26CL) )
AS p;

SELECT p.pod_id,
       p.pod_name,
       p.epic_department_id,
       p.epic_department_name,
       p.CG_26CL AS CG_26CL_Denominator,
       p.ACO_14C AS ACO_14C_Denominator,
       p.ACO_09C AS ACO_09C_Denominator,
       p.ACO_02C AS ACO_02C_Denominator,
       p.CG_28CL AS CG_28CL_Denominator
FROM
(
SELECT DISTINCT
	pod_id
  , pod_name
  , epic_department_id
  , epic_department_name
  , Denominator
  , PG_Question_Variable
FROM #summary
) AS pivoted PIVOT ( MAX(Denominator) FOR PG_Question_Variable IN (CG_28CL, ACO_02C, ACO_09C, ACO_14C, CG_26CL) )
AS p;

--SELECT [event_type]
--      ,[event_count]
--      ,[event_date]
--      ,[event_id]
--      ,[event_category]
--      ,[epic_department_id]
--      ,[epic_department_name]
--      ,[epic_department_name_external]
--      ,[fmonth_num]
--      ,[fyear_num]
--      ,[fyear_name]
--      ,[report_period]
--      ,[report_date]
--      ,[peds]
--      ,[transplant]
--      ,[sk_Dim_Pt]
--      ,[sk_Fact_Pt_Acct]
--      ,[sk_Fact_Pt_Enc_Clrt]
--      ,[person_birth_date]
--      ,[person_gender]
--      ,[person_id]
--      ,[person_name]
--      ,[practice_group_id]
--      ,[practice_group_name]
--      ,[provider_id]
--      ,[provider_name]
--      ,[service_line_id]
--      ,[service_line]
--      ,[sub_service_line_id]
--      ,[sub_service_line]
--      ,[opnl_service_id]
--      ,[opnl_service_name]
--      ,[corp_service_line_id]
--      ,[corp_service_line_name]
--      ,[hs_area_id]
--      ,[hs_area_name]
--      ,[pod_id]
--      ,[pod_name]
--      ,[hub_id]
--      ,[hub_name]
--      ,[w_department_id]
--      ,[w_department_name]
--      ,[w_department_name_external]
--      ,[w_practice_group_id]
--      ,[w_practice_group_name]
--      ,[w_service_line_id]
--      ,[w_service_line_name]
--      ,[w_sub_service_line_id]
--      ,[w_sub_service_line_name]
--      ,[w_opnl_service_id]
--      ,[w_opnl_service_name]
--      ,[w_corp_service_line_id]
--      ,[w_corp_service_line_name]
--      ,[w_report_period]
--      ,[w_report_date]
--      ,[w_hs_area_id]
--      ,[w_hs_area_name]
--      ,[w_pod_id]
--      ,[w_pod_name]
--      ,[w_hub_id]
--      ,[w_hub_name]
--      ,[sk_Dim_PG_Question]
--      ,[PG_Question_Variable]
--      ,[PG_Question_Text]
--      ,[sk_Dim_Physcn]
--      ,[Load_Dtm]

--  INTO #TabRptg

--  FROM [DS_HSDM_App].[TabRptg].[Dash_BalancedScorecard_CGCAHPSRecommendProv_Tiles]
--  WHERE event_count = 1
--  AND hs_area_id = 1
--  AND event_date >= '7/1/2020 00:00 AM' AND event_date < '8/31/2020 11:59 PM'

--  SELECT *
--  FROM #TabRptg
--  --ORDER BY event_count
--  --       , event_category
--  --ORDER BY event_category
--  --ORDER BY event_date
--  ORDER BY event_category
--         , event_date

--SELECT SUM(CASE WHEN TabRptg.event_category IN ('Yes, definitely') THEN 1 ELSE 0 END) AS Numerator
--     , SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS Denominator
--	 , CAST(
--	   CAST(SUM(CASE WHEN TabRptg.event_category IN ('Yes, definitely') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
--       CAST(SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
--	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
--FROM
--(
--SELECT *
--FROM #TabRptg
--) TabRptg

GO


