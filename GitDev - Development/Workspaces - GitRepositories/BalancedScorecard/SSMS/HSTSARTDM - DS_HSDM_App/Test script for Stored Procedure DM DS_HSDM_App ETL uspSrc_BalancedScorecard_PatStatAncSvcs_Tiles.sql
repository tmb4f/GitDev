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
SET @startdate = '7/1/2022 00:00 AM'
SET @enddate = '7/31/2022 11:59 PM'

--ALTER PROCEDURE [ETL].[uspSrc_BalancedScorecard_PatStatAncSvcs_Tiles]
--    (
--     @startdate SMALLDATETIME=NULL
--    ,@enddate SMALLDATETIME=NULL
--    )
--AS 
/**********************************************************************************************************************
WHAT: Operational Service Lines Reporting:  Radiology and Therapies - OU (Ancillary Services)					
WHO : Chris Mitchell
WHEN: 8/21/2017
WHY : Press Ganey OU results for Radiology
-----------------------------------------------------------------------------------------------------------------------
INFO: 
                  
      OUTPUTS:  TabRptg.Dash.BalancedScorecard_PatStatAncSvcs_Tiles
-------------------------------------------------------------------------------------------------------------------------------------------------------------
MODS: 	8/21/17:  Calculate mean score, not top box
		8/21/17:  Remove score from event_category, create score column
		2/2/2018:  Remove RSS Grp / Loc reference, replace w/ MDM
		1/22/2019:  changed join to clrt_SERsrc to exclude 0 and -1
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

IF OBJECT_ID('tempdb..#ou ') IS NOT NULL
DROP TABLE #ou

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

IF OBJECT_ID('tempdb..#tabrptg ') IS NOT NULL
DROP TABLE #tabrptg

/* Response To Concern
WHERE (sk_dim_pg_question = '159' OR sk_dim_pg_question IS NULL) AND
(epic_department_id IN
			(
                            '10210007','10210008','10210009','10210011','10210018','10210023','10211015','10211016',
                            '10211017','10211020','10211021','10212020','10214001','10214005','10214006','10214007',
                            '10214008','10214010','10214012','10221003','10221004','10221005','10221006','10221008',
                            '10221009','10226001','10228008','10228019','10228021','10228024','10230003','10239007',
                            '10239014','10243015','10243016','10243017','10243018','10243019','10243021','10243025',
                            '10243030','10243084','10244020','10348016','10348017','10348018','10348019','10348020',
                            '10348034','10348042','10354001','10354003','10381001','10381002','10381003','10381004'
			)
    OR epic_department_id IS NULL)

   Concern for Privacy
WHERE (sk_dim_pg_question = '157' OR sk_dim_pg_question IS NULL)
AND (epic_department_id IN
(
    '10210007','10210008','10210009','10210011','10210018','10210023','10211015','10211016',
    '10211017','10211020','10211021','10212020','10214001','10214005','10214006','10214007',
    '10214008','10214010','10214012','10221003','10221004','10221005','10221006','10221008',
    '10221009','10226001','10228008','10228019','10228021','10228024','10230003','10239007',
    '10239014','10243015','10243016','10243017','10243018','10243019','10243021','10243025',
    '10243030','10243084','10244020','10348016','10348017','10348018','10348019','10348020',
    '10348034','10348042','10354001','10354003','10381001','10381002','10381003','10381004'			
)
    OR epic_department_id IS NULL)

Sensitivity to Needs
WHERE (sk_dim_pg_question = '158' OR sk_dim_pg_question IS NULL) AND
(epic_department_id IN
    (
        '10210007','10210008','10210009','10210011','10210018','10210023','10211015','10211016','10211017',
        '10211020','10211021','10212020','10214001','10214005','10214006','10214007','10214008','10214010','10214012',
        '10221003','10221004','10221005','10221006','10221008','10221009','10226001','10228008','10228019','10228021',
        '10228024','10230003','10239007','10239014','10243015','10243016','10243017','10243018','10243019','10243021',
        '10243025','10243030','10243084','10244020','10348016','10348017','10348018','10348019','10348020','10348034',
        '10348042','10354001','10354003','10381001','10381002','10381003','10381004'
			)
    OR epic_department_id IS NULL)
*/

    SELECT DISTINCT
            CAST('Ancillary Services' AS VARCHAR(50)) AS event_type
           ,CAST(NULL AS VARCHAR(150)) AS event_category

		   ,CAST(CASE WHEN PM.VALUE = 1 THEN 0
				 WHEN pm.VALUE = 2 THEN 25
				 WHEN pm.VALUE = 3 THEN 50
				 WHEN pm.VALUE = 4 THEN 75
				 WHEN pm.VALUE = 5 THEN 100
			END AS INT) AS Score
		   ,PM.sk_dim_pg_question
           ,rec.day_date AS event_date		--date survey received
           ,rec.fmonth_num
           ,rec.fyear_name
           ,rec.fyear_num
           ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,CAST(CASE WHEN pm.PT_SEX='F' THEN 'Female'
                      WHEN pm.PT_SEX='M' THEN 'Male'
                      ELSE NULL
                 END AS VARCHAR(6)) AS person_gender
           ,CAST(CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS INT) AS event_count		--count when the overall question has been answered
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,provider_id
           ,provider_name
			--handle UNIT of OBS, dividing between children and adult; handle when no unit is returned and set to medical center level
		   ,pm.service_line_id
		   ,pm.service_line
	       ,pm.sub_service_line_id
		   ,pm.sub_service_line
		   ,pm.opnl_service_id
		   ,pm.opnl_service_name
		   ,pm.hs_area_id
		   ,pm.hs_area_name
           ,pm.epic_department_id
           ,pm.epic_department_name
           ,pm.epic_department_name_external
           ,CAST(CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS SMALLINT) AS peds
           ,CAST(CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS SMALLINT) AS transplant
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

    INTO #ou

    FROM    DS_HSDW_Prod.Rptg.vwDim_Date AS rec
    LEFT OUTER JOIN
	(
		SELECT DISTINCT
                resp.SURVEY_ID
                ,RECDATE
                ,CAST(VALUE AS NVARCHAR(500)) AS VALUE
			    ,epic_department_id = CAST(mdm.EPIC_DEPARTMENT_ID AS NUMERIC(18, 0))
			    ,mdm.epic_department_name
			    ,mdm.epic_department_name_external
				,mdm.service_line_id
				,mdm.service_line
				,mdm.sub_service_line_id
				,mdm.sub_service_line
				,mdm.opnl_service_id
				,mdm.opnl_service_name
				,mdm.hs_area_id
				,mdm.hs_area_name
				,resp.sk_dim_pg_question
				,Resp_Age.AGE
                ,qstn.VARNAME
                ,fpa.MRN_int
                ,fpa.sk_Fact_Pt_Acct
                ,dp.NPINumber
                ,dp.sk_Dim_Physcn
                ,prov.PROV_ID AS provider_id
                ,dp.DisplayName AS provider_name
                ,dp.Service_Line AS Service_Line_md
                ,CAST(CONCAT(pat.PT_LNAME, ',', pat.PT_FNAME_MI) AS VARCHAR(200)) AS PAT_NAME
                ,pat.BIRTH_DT AS BIRTH_DATE
                ,pat.PT_SEX
				,resp.Load_Dtm
        FROM    DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses AS resp
        INNER JOIN DS_HSDW_Prod.Rptg.vwDim_PG_Question AS qstn
                ON resp.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
        INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Acct AS fpa
                ON resp.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
        INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Pt AS pat
                ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn AS dp
                ON resp.sk_Dim_Physcn=dp.sk_Dim_Physcn
        LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwDim_Clrt_SERsrc] prov
                ON CASE WHEN dp.[sk_Dim_Physcn] IN ('0','-1') THEN '-999' ELSE dp.sk_Dim_Physcn END = prov.[sk_Dim_Physcn] -- multiple sk_dim_phys of 0,-1 in SERsrc
		LEFT OUTER JOIN
		(
			SELECT SURVEY_ID, CAST(MAX(VALUE) AS VARCHAR(500)) AS AGE
			FROM DS_HSDW_Prod.Rptg.vwFact_PressGaney_Responses
			WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = 145 -- Age question for OU
			GROUP BY SURVEY_ID
		) Resp_Age
		ON Resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN
		DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		INNER JOIN
		DS_HSDW_Prod.rptg.vwRef_MDM_Location_Master_EpicSvc mdm
			ON dep.DEPARTMENT_ID = mdm.EPIC_DEPARTMENT_ID

		WHERE   resp.Svc_Cde='OU' AND resp.sk_Dim_PG_Question IN (157, 158, 159,   ---radiology concern for privacy, sensiv. to needs, response to concerns/complaints
		                                                          148,149,150,151,152,1075) ---Therapies (test and treat)
                AND RECDATE>=@locstartdate
                AND RECDATE< @locenddate
	) pm
	ON rec.day_date=pm.RECDATE

		-- -------------------------------------
		-- Identify transplant encounter
		-- -------------------------------------
    LEFT OUTER JOIN
	(
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

    WHERE rec.day_date>=@locstartdate
          AND rec.day_date<@locenddate;

--SELECT *
--FROM #ou
--WHERE
--(epic_department_id IN
--			(
--                            '10210007','10210008','10210009','10210011','10210018','10210023','10211015','10211016',
--                            '10211017','10211020','10211021','10212020','10214001','10214005','10214006','10214007',
--                            '10214008','10214010','10214012','10221003','10221004','10221005','10221006','10221008',
--                            '10221009','10226001','10228008','10228019','10228021','10228024','10230003','10239007',
--                            '10239014','10243015','10243016','10243017','10243018','10243019','10243021','10243025',
--                            '10243030','10243084','10244020','10348016','10348017','10348018','10348019','10348020',
--                            '10348034','10348042','10354001','10354003','10381001','10381002','10381003','10381004'
--			)
--)
--ORDER BY sk_Dim_PG_Question
--                  , report_date

--/* -- #summary
    SELECT organization_id
	              ,organization_name
				  ,service_id
	              ,service_name
				  ,clinical_area_id
				  ,clinical_area_name
				  ,epic_department_id
	              ,epic_department_name
				  ,weighted_score
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS AncTestTreatmentResponse
				  ,ambulatory_flag
				  ,sk_Dim_PG_Question
				  ,peds
				  ,transplant

	INTO #summary

	FROM #ou
	WHERE
	(epic_department_id IN
				(
								'10210007','10210008','10210009','10210011','10210018','10210023','10211015','10211016',
								'10211017','10211020','10211021','10212020','10214001','10214005','10214006','10214007',
								'10214008','10214010','10214012','10221003','10221004','10221005','10221006','10221008',
								'10221009','10226001','10228008','10228019','10228021','10228024','10230003','10239007',
								'10239014','10243015','10243016','10243017','10243018','10243019','10243021','10243025',
								'10243030','10243084','10244020','10348016','10348017','10348018','10348019','10348020',
								'10348034','10348042','10354001','10354003','10381001','10381002','10381003','10381004'
				)
	)

	--SELECT *
	--FROM #summary
	--WHERE AncTestTreatmentResponse = 1
	----AND epic_department_name LIKE 'OCIR%'
	--ORDER BY epic_department_id
	--                   ,epic_department_name
	--				   ,weighted_score DESC
--*/
--/*
	SELECT
				   epic_department_name
	              ,epic_department_id
				  ,ambulatory_flag
				  ,SUM(weighted_score) AS weighted_score
				  --,SUM(AncTestTreatmentResponse) AS AncTestTreatmentResponse
				  ,SUM(AncTestTreatmentResponse) AS Responses
	FROM #summary
	WHERE AncTestTreatmentResponse = 1
	--AND ambulatory_flag = 1
	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND organization_id = 5 -- Medical Center Operations
	AND sk_Dim_PG_Question = 157 -- Concern for Privacy
	--AND sk_Dim_PG_Question = 158 -- Sensitivity to Needs
	--AND sk_Dim_PG_Question = 159 -- Response to Concern
	AND peds = 0
	AND transplant = 0
	GROUP BY
				   epic_department_name
				  ,epic_department_id
				  ,ambulatory_flag
	ORDER BY
				   epic_department_name
--*/
--/*
	SELECT SUM(weighted_score) AS weighted_score
				  ,SUM(AncTestTreatmentResponse) AS AncTestTreatmentResponse
	FROM #summary
	WHERE AncTestTreatmentResponse = 1
	--AND ambulatory_flag = 1
	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	AND organization_id = 5 -- Medical Center Operations
	AND sk_Dim_PG_Question = 157 -- Concern for Privacy
	--AND sk_Dim_PG_Question = 158 -- Sensitivity to Needs
	--AND sk_Dim_PG_Question = 159 -- Response to Concern
	AND peds = 0
	AND transplant = 0
--*/

	SELECT sk_Dash_BalancedScorecard_PatStatAncSvcs_Tiles,
           event_type,
           event_count,
           event_date,
           event_id,
           event_category,
           epic_department_id,
           epic_department_name,
           epic_department_name_external,
           fmonth_num,
           fyear_num,
           fyear_name,
           report_period,
           report_date,
           peds,
           transplant,
           sk_Dim_Pt,
           person_birth_date,
           person_gender,
           person_id,
           person_name,
           practice_group_id,
           practice_group_name,
           provider_id,
           provider_name,
           service_line_id,
           service_line,
           sub_service_line_id,
           sub_service_line,
           opnl_service_id,
           opnl_service_name,
           hs_area_id,
           hs_area_name,
           w_department_id,
           w_department_name,
           w_department_name_external,
           w_opnl_service_id,
           w_opnl_service_name,
           w_practice_group_id,
           w_practice_group_name,
           w_service_line_id,
           w_service_line_name,
           w_sub_service_line_id,
           w_sub_service_line_name,
           w_report_period,
           w_report_date,
           w_hs_area_id,
           w_hs_area_name,
           Score,
           sk_Dim_PG_Question,
           Load_Dtm

	INTO #tabrptg

	FROM 	[DS_HSDM_App].[TabRptg].[Dash_BalancedScorecard_PatStatAncSvcs_Tiles]
	WHERE
	(epic_department_id IN
				(
								'10210007','10210008','10210009','10210011','10210018','10210023','10211015','10211016',
								'10211017','10211020','10211021','10212020','10214001','10214005','10214006','10214007',
								'10214008','10214010','10214012','10221003','10221004','10221005','10221006','10221008',
								'10221009','10226001','10228008','10228019','10228021','10228024','10230003','10239007',
								'10239014','10243015','10243016','10243017','10243018','10243019','10243021','10243025',
								'10243030','10243084','10244020','10348016','10348017','10348018','10348019','10348020',
								'10348034','10348042','10354001','10354003','10381001','10381002','10381003','10381004'
				)
	)

    AND CAST(event_date AS SMALLDATETIME) >= @locstartdate
          AND CAST(event_date AS SMALLDATETIME) < @locenddate
	--AND event_date BETWEEN '7/1/2022 00:00 AM' AND '7/31/2022 11:59 PM'
--/*
	SELECT SUM(Score) AS weighted_score
				  ,SUM(event_count) AS AncTestTreatmentResponse
	FROM #tabrptg
	WHERE event_count  = 1
	--AND ambulatory_flag = 1
	--AND SUBSTRING(Survey_Designator,1,2) = 'MD'
	--AND organization_id = 5 -- Medical Center Operations
	AND sk_Dim_PG_Question = 157 -- Concern for Privacy
	AND peds = 0
	AND transplant = 0
--*/

GO


