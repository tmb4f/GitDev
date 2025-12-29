USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [ETL].[uspSrc_BalancedScorecard_PatSat_Telemedicine_OverallAssess_Tiles]
    (
     @startdate SMALLDATETIME=NULL
    ,@enddate SMALLDATETIME=NULL
    )
AS

--DECLARE
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL;

--SET @startdate = '3/1/2023 00:00 AM'
--SET @enddate = '3/31/2023 11:59 PM'

/**********************************************************************************************************************
WHAT: Corporate Service Lines Reporting:  Telemedicine Overall Assessment - Likelihood of your recommending our practice/video visit service to others
WHO : Tom Burgan
WHEN: 05/20/2020
WHY : Press Ganey Telemedicine results for overall assessment
-----------------------------------------------------------------------------------------------------------------------
INFO: 
                  
      OUTPUTS:  TabRptg.Dash_BalancedScorecard_PatSat_Telemedicine_OverallAssess_Tiles

-------------------------------------------------------------------------------------------------------------------------------------------------------------
MODS: 	05/20/2020:	Created stored procedure
				08/29/2022 - TAH - added field sk_fact_pt_enc_clrt
				03/13/2023 - TMB - added Telehealth_Mode_Name
				04/26/2023 - TMB - added app_flag
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
            CAST('Telemedicine-Press Ganey' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
		   ,rec.day_date AS event_date		--date survey received
		   ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,CAST(pm.VALUE AS VARCHAR(150)) AS event_category
		   ,CAST(CASE WHEN pm.VALUE=1  THEN 0
				      WHEN pm.VALUE=2  THEN 25
					  WHEN pm.VALUE=3  THEN 50
					  WHEN pm.VALUE=4  THEN 75
					  WHEN pm.VALUE=5  THEN 100		
				 END AS INT) 			   AS  event_score
		   ,pm.sk_Dim_PG_Question	
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,pm.PT_SEX AS person_gender
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
		   ,pm.sk_Fact_Pt_Enc_Clrt
           ,CAST(pm.EPIC_DEPARTMENT_ID AS NUMERIC(18, 0)) AS epic_department_id
           ,epic_department_name = pm.EPIC_DEPT_NAME
           ,epic_department_name_external = pm.EPIC_EXT_NAME
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,pm.Telehealth_Mode_Name
	       ,CASE WHEN pm.Prov_Typ IN ('NURSE PRACTITIONER','PHYSICIAN ASSISTANT','NURSE ANESTHETIST','CLINICAL NURSE SPECIALIST','GENETIC COUNSELOR','AUDIOLOGIST') THEN 1 ELSE 0 END AS app_flag -- INTEGER
    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
				 resp.SURVEY_ID
				,RECDATE
				,CAST(VALUE AS NVARCHAR(500)) AS VALUE
				,Resp_Age.AGE AS AGE
				,qstn.sk_Dim_PG_Question
				,resp.sk_Dim_Clrt_DEPt
				,loc_master.EPIC_DEPARTMENT_ID
				,loc_master.EPIC_DEPT_NAME
				,loc_master.EPIC_EXT_NAME
				,loc_master.HS_AREA_ID
				,loc_master.HS_AREA_NAME
				,loc_master.SERVICE_LINE_ID
				,loc_master.SERVICE_LINE
				,loc_master.SUB_SERVICE_LINE_ID
				,loc_master.SUB_SERVICE_LINE
				,loc_master.OPNL_SERVICE_ID
				,loc_master.OPNL_SERVICE_NAME
				,loc_master.CORP_SERVICE_LINE_ID
				,loc_master.CORP_SERVICE_LINE
				,loc_master.PRACTICE_GROUP_ID
				,loc_master.PRACTICE_GROUP_NAME
				,fpa.MRN_int
				,fpa.sk_Dim_Pt
				,qstn.VARNAME
				,fpa.sk_Fact_Pt_Acct
				,fpa.sk_Fact_Pt_Enc_Clrt
				,prov.PROV_ID AS provider_id
				,prov.Prov_Nme AS provider_name
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,CAST(CONCAT(pat.PT_LNAME, ',', pat.PT_FNAME_MI) AS VARCHAR(200)) AS PAT_NAME
				,pat.BIRTH_DT AS BIRTH_DATE
				,pat.PT_SEX
				,resp.Load_Dtm
				,tm.Telehealth_Mode_Name
		FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS resp
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
				ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
		INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
				ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
		INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Pt AS pat
				ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
		LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn AS dp
				ON resp.sk_Dim_Physcn=dp.sk_Dim_Physcn
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
				--provider table
				ON CASE WHEN dp.[sk_Dim_Physcn] IN ('0','-1') THEN '-999' ELSE dp.sk_Dim_Physcn END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of 0,-1 in SERsrc
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt] enc
		        ON enc.PAT_ENC_CSN_ID = resp.Pat_Enc_CSN_Id
		LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Date] ddte
		        ON ddte.date_key = enc.sk_Cont_Dte
	    LEFT OUTER JOIN Rptg.vwCLARITY_SER_OT_PROV_TYPE AS ptot
			    ON prov.PROV_ID = ptot.PROV_ID
				   AND ddte.day_date BETWEEN ptot.CONTACT_DATE AND ptot.EFF_TO_DATE
		LEFT OUTER JOIN
			(
				SELECT SURVEY_ID, CAST(MAX(VALUE) AS NVARCHAR(500)) AS AGE
				FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '784' -- Age question for Outpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN DS_HSDW_Prod.rptg.vwRef_MDM_location_master loc_master
				ON dep.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID
		LEFT OUTER JOIN DS_HSDM_App.TabRptg.Dash_Telemedicine_Encounters_Tiles tm
		ON tm.sk_Fact_Pt_Enc_Clrt = fpa.sk_Fact_Pt_Enc_Clrt
		WHERE   resp.Svc_Cde='MD' AND SUBSTRING(resp.Survey_Designator, 1, 2) = 'MT' AND resp.sk_Dim_PG_Question IN ('1333') -- Likelihood of your recommending our practice to others
				AND resp.RECDATE>=@locstartdate
				AND resp.RECDATE<@locenddate
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

    WHERE   rec.day_date>=@locstartdate
           AND rec.day_date<@locenddate;

GO


