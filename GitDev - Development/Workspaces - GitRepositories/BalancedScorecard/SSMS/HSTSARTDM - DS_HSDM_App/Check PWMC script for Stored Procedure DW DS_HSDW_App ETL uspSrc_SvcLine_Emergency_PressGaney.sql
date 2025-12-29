USE [DS_HSDW_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [ETL].[uspSrc_SvcLine_Emergency_PressGaney]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
--    )
--AS 

/*****************************************************************************************************************
WHAT:	Create procedure Rptg.uspSrc_SvcLine_Emergency_PressGaney
WHO :	Dayna Monaghan	
WHEN:	2/26/2016
WHY :	Calculate overall mean for emergency
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
MODS:	create 3/16/2016
		03/04/2016--DRM--change to pull from dim_date and left outer join to event data to return historical data
		03/14/2016--DRM--comment out custom questions
		03/15/2016--DRM--use the fact table instead of the pivoted table in staging
		03/24/2016--DRM--change monthly totals to running totals for the fiscal year
		08/26/2016--DRM--Adding time to date parameters was incorrect, changing CAST
		03/14/2017--DRM--Refactor for standard columns

		03/15/2017--BDD--Changed date handling to conform to other SSIS ETL processes
		03/15/2017--BDD--Added CASTS to ensure consistent datatypes on placeholder and calculated columns
        03/15/2017  BDD - With all the temp tables in this proc it is not conducive to an SSIS Data Flow. I have set it up
					to insert the data to a staging table which will then be moved over to the dashboard tiles table via SSIS. 
					!!!!Important!!! This kludge assumes that the stage table will be truncated by an SSIS SQL command prior
					to the proc running. 
		06/05/2017--DRM--change to use MDM mapping instead of bscm mapping table
		01/22/2018--DRM--Adjust which questions to pull; exclude surveys when all relevant questions are NULL; arrange questions
						 by sections and refine calculation
		02/23/2021--TMB--Adjust which questions to pull; arrange questions by sections
		02/24/2021--TMB--Adjust which questions to pull
		07/29/2022--TAH--Added two new fields sk_dim_pt and sk_fact_pt_enc_clrt
		11/05/2024--TMB--Edited logic to include all UVa Health System emergency departments
		11/06/2024--TMB--Corrected event count and mean score calculation
******************************************************************************************************************/
    SET NOCOUNT ON;

	/*testing*/
    DECLARE
    --    @startdate SMALLDATETIME = '7/1/2016'
    --   ,@enddate SMALLDATETIME = '1/1/2018';
       -- @startdate SMALLDATETIME = NULL
       --,@enddate SMALLDATETIME = NULL;
        @startdate SMALLDATETIME = '7/1/2025'
       ,@enddate SMALLDATETIME = '11/1/2025';

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

    SELECT
            p.SURVEY_ID
           ,p.RECDATE
           ,p.MRN_int
           ,p.NPINumber
           ,p.PAT_NAME
           ,p.AGE
           ,p.BIRTH_DATE
           ,p.A5 -- Comfort of the waiting area, Arrival
           ,p.A4 -- Waiting time before you were brought to the treatment area, Arrival
           ,p.B1 -- Courtesy of the nurses, Nurses
           ,p.B76 -- Degree to which the nurses took the time to listen to you, Nurses
           ,p.B95 -- Nurses' responses to your questions/ concerns, Nurses
           ,p.B3 -- Nurses' attention to your needs, Nurses
           ,p.B5 -- Nurses' concern for your privacy, Nurses
           ,p.C110 -- How well the doctors included you in decisions about your treatment, Doctors
           ,p.C2 -- Courtesy of the doctor, Doctors
           ,p.C75 -- Degree to which the doctor took the time to listen to you, Doctors
           ,p.C5 -- Doctor's concern to keep you informed about your treatment, Doctors
           ,p.C4 -- Doctor's concern for your comfort while treating you, Doctors
           ,p.F68 -- Overall rating of care received during your visit, Overall Assessment
           ,p.F81 -- Degree to which the hospital staff worked together as a team, Overall Assessment
           ,p.F2 -- Degree to which staff cared about you as a person, Overall Assessment
           ,p.F4 -- Likelihood of your recommending our emergency department to others, Overall Assessment
		   --when all relevant questions are NULL set exclude flag
           ,CASE WHEN p.A5 IS NULL
                      AND p.A4 IS NULL
                      AND p.B1 IS NULL
                      AND p.B76 IS NULL
                      AND p.B95 IS NULL
                      AND p.B3 IS NULL
                      AND p.B5 IS NULL
                      AND p.C110 IS NULL
                      AND p.C2 IS NULL
                      AND p.C75 IS NULL
                      AND p.C5 IS NULL
                      AND p.C4 IS NULL
                      AND p.F68 IS NULL
                      AND p.F81 IS NULL
                      AND p.F2 IS NULL
                      AND p.F4 IS NULL THEN 1
                 ELSE 0
            END AS exclude_flag
			, p.sk_Dim_Pt
			, p.sk_Fact_Pt_Enc_Clrt
			, p.sk_Dim_Clrt_DEPt
		    , p.DEPARTMENT_ID
			, p.Clrt_DEPt_Nme
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
						   , pm.sk_Dim_Pt
						   , fpa.sk_Fact_Pt_Enc_Clrt
						   , pm.sk_Dim_Clrt_DEPt
						   , dep.DEPARTMENT_ID
						   , dep.Clrt_DEPt_Nme
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
						LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep
							ON dep.sk_Dim_Clrt_DEPt = pm.sk_Dim_Clrt_DEPt
                        WHERE
                            pm.Svc_Cde = 'ER'
                            AND qstn.VARNAME IN ('A5', 'A4', 'B1', 'B76', 'B95', 'B3', 'B5', 'C110', 'C2', 'C75', 'C5', 'C4', 'F68', 'F81', 'F2', 'F4', 'AGE')
												 --exclude custom questions: 'C17', 'F69', 'F118', 'F119', 'F28', 'F81', 'F184'
												 --exclude custom questions: 'B4','C1','C7','E1','E2'; include standard questions: 'B95','F81'; move 'F2' from
												 --      section 'Personal Issues' to 'Overall Assessment' (2/23/21)
												 --include standard question 'C110'; exclude custom questions 'A86', 'A87', 'D2', 'D52', 'D3', 'D4','D65', 'E3', 'A28', 'A2', 'A3',
												 --      i.e. exclude questions in survey sections Personal/Insurance Information, Tests, and Family or Friends (2/24/21)
                            AND pm.RECDATE >= @startdate
                            AND pm.RECDATE < @enddate
            ) AS pivoted PIVOT ( MAX(VALUE) FOR VARNAME IN (A5, A4, B1, B76, B95, B3, B5, C110, C2, C75, C5, C4, F68, F81, F2, F4, AGE) )

AS p;

--SELECT
--    Clrt_DEPt_Nme,
--	SURVEY_ID,
--    RECDATE,
--    MRN_int,
--    NPINumber,
--    PAT_NAME,
--    AGE,
--    BIRTH_DATE,
--    A5,
--    A4,
--    B1,
--    B76,
--    B95,
--    B3,
--    B5,
--    C110,
--    C2,
--    C75,
--    C5,
--    C4,
--    F68,
--    F81,
--    F2,
--    F4,
--    exclude_flag,
--    sk_Dim_Pt,
--    sk_Fact_Pt_Enc_Clrt,
--    sk_Dim_Clrt_DEPt,
--    DEPARTMENT_ID
--FROM #surveys
--ORDER BY
--	Clrt_DEPt_Nme, RECDATE

--convert to weighted scores
    SELECT
            pe.SURVEY_ID
           ,pe.RECDATE
           ,pe.AGE
           --ARRIVAL
           ,CAST(CASE pe.A5
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS A5
           ,CAST(CASE pe.A4
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS A4
			--NURSES
           ,CAST(CASE pe.B1
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS B1
           ,CAST(CASE pe.B76
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS B76
           ,CAST(CASE pe.B95
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS B95
           ,CAST(CASE pe.B3
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS B3
           ,CAST(CASE pe.B5
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS B5
			--DOCTORS
           ,CAST(CASE pe.C110
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS C110
           ,CAST(CASE pe.C2
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS C2
           ,CAST(CASE pe.C75
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS C75
           ,CAST(CASE pe.C5
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS C5
           ,CAST(CASE pe.C4
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS C4
			--OVERALL
           ,CAST(CASE pe.F68
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS F68
           ,CAST(CASE pe.F81
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS F81
           ,CAST(CASE pe.F2
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS F2
           ,CAST(CASE pe.F4
                   WHEN 1 THEN 0
                   WHEN 2 THEN 25
                   WHEN 3 THEN 50
                   WHEN 4 THEN 75
                   WHEN 5 THEN 100
                 END AS DECIMAL(10, 2)) AS F4
			, pe.sk_Dim_Pt
			, pe.sk_Fact_Pt_Enc_Clrt
			, pe.sk_Dim_Clrt_DEPt
		    , pe.DEPARTMENT_ID
			, pe.Clrt_DEPt_Nme
        INTO
            #weighted_scores
        FROM
            #surveys AS pe
        WHERE
            pe.RECDATE >= @startdate
            AND pe.RECDATE < @enddate
			--surveys with all relevant questions missing are excluded from the denominator
            AND pe.exclude_flag = 0;

--average each scores from each section
    SELECT
            ws.SURVEY_ID
           ,ws.RECDATE
           ,ws.AGE
           ,ws.A5
           ,ws.A4
           ,(
             SELECT
                    AVG(A.arr)
                FROM
                    ( VALUES ( ws.A5), ( ws.A4) ) AS A (arr)
            ) AS SectionA
           ,ws.B1
           ,ws.B76
           ,ws.B95
           ,ws.B3
           ,ws.B5
           ,(
             SELECT
                    AVG(B.rn)
                FROM
                    ( VALUES ( ws.B1), ( ws.B76), ( ws.B95), ( ws.B3), ( ws.B5) ) AS B (rn)
            ) AS SectionB
           ,ws.C110
           ,ws.C2
           ,ws.C75
           ,ws.C5
           ,ws.C4
           ,(
             SELECT
                    AVG(C.md)
                FROM
                    ( VALUES ( ws.C110), ( ws.C2), ( ws.C75), ( ws.C5), ( ws.C4) ) AS C (md)
            ) AS SectionC
           ,ws.F68
           ,ws.F81
           ,ws.F2
           ,ws.F4
           ,(
             SELECT
                    AVG(FO.ovr)
                FROM
                    ( VALUES ( ws.F68), ( ws.F81), ( ws.F2), ( ws.F4) ) AS FO (ovr)
            ) AS SectionFO
			, ws.sk_Dim_Pt
			, ws.sk_Fact_Pt_Enc_Clrt
			, ws.sk_Dim_Clrt_DEPt
			, ws.DEPARTMENT_ID
			, ws.Clrt_DEPt_Nme
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
           ,sa.A5
           ,sa.A4
           ,sa.SectionA
           ,sa.B1
           ,sa.B76
           ,sa.B95
           ,sa.B3
           ,sa.B5
           ,sa.SectionB
           ,sa.C110
           ,sa.C2
           ,sa.C75
           ,sa.C5
           ,sa.C4
           ,sa.SectionC
           ,sa.F68
           ,sa.F81
           ,sa.F2
           ,sa.F4
           ,sa.SectionFO
           ,(
             SELECT
                    AVG(total.ttl)
                FROM
                    ( VALUES ( sa.SectionA), ( sa.SectionB), ( sa.SectionC), ( sa.SectionFO) )
                    AS total (ttl)
            ) AS All_Sections
			, sa.sk_Dim_Pt
			, sa.sk_Fact_Pt_Enc_Clrt
			, sa.sk_Dim_Clrt_DEPt
		    , sa.DEPARTMENT_ID
			, sa.Clrt_DEPt_Nme
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
		   ,o.RECDATE
           ,date_dim.month_begin_date AS event_date
           ,CAST(NULL AS VARCHAR(25)) AS event_category
           ,o.event_count
           ,date_dim.fmonth_num
           ,date_dim.FYear_name
           ,date_dim.Fyear_num
		   ,o.DEPARTMENT_ID AS epic_department_id
           ,o.Clrt_DEPt_Nme AS epic_department_name
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
		   ,o.organization_name
		   ,o.service_name
		   ,o.clinical_area_name
           ,o.overall_mean
		   ,o.sk_Dim_Pt
		   ,o.sk_Fact_Pt_Enc_Clrt
		   ,o.sk_Dim_Clrt_DEPt
        INTO
            #RptgTemp
        FROM
            DS_HSDW_Prod.Rptg.vwDim_Date AS date_dim
        LEFT OUTER JOIN (
                         SELECT DISTINCT
							    Clrt_DEPt_Nme
							   ,RECDATE
                               ,Fyear_num
                               ,month_begin_date
                               ,COUNT(SURVEY_ID) OVER (PARTITION BY Clrt_DEPt_Nme, Fyear_num ORDER BY month_begin_date) AS event_count
                               ,AVG(All_Sections) OVER (PARTITION BY Clrt_DEPt_Nme, Fyear_num ORDER BY month_begin_date) AS overall_mean
                               ,loc.EPIC_EXT_NAME AS epic_department_name_external
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
							   ,o.organization_name
							   ,s.service_name
							   ,c.clinical_area_name
							   , sk_Dim_Pt
							   , sk_Fact_Pt_Enc_Clrt
							   , sk_Dim_Clrt_DEPt
						       , DEPARTMENT_ID
                            FROM
                                #overall
                            LEFT OUTER JOIN
							(
							SELECT DISTINCT
									   rmlmh.EPIC_DEPARTMENT_ID
									  ,rmlmh.EPIC_EXT_NAME
									  ,rmlmh.SERVICE_LINE_ID
									  ,rmlmh.SERVICE_LINE
									  ,rmlmh.SUB_SERVICE_LINE_ID
									  ,rmlmh.SUB_SERVICE_LINE
									  ,rmlmh.OPNL_SERVICE_ID
									  ,rmlmh.OPNL_SERVICE_NAME
									  ,rmlmh.CORP_SERVICE_LINE_ID
									  ,rmlmh.CORP_SERVICE_LINE
									  ,rmlmh.FINANCE_COST_CODE
									  ,rmlmh.HS_AREA_ID
									  ,rmlmh.HS_AREA_NAME
									  ,rmlmh.POD_ID
									  ,rmlmh.PFA_POD
									  ,rmlmh.HUB_ID
									  ,rmlmh.HUB
									  ,rmlmh.LOC_ID
									  ,rmlmh.REV_LOC_NAME
									  ,rmlmh.PRACTICE_GROUP_ID
									  ,rmlmh.PRACTICE_GROUP_NAME
								FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History AS rmlmh
									INNER JOIN
									( --hx--most recent batch date per dep id
										SELECT mdmhx.EPIC_DEPARTMENT_ID
											  ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
										FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History AS mdmhx
										GROUP BY mdmhx.EPIC_DEPARTMENT_ID
									)                                                 AS hx
										ON hx.EPIC_DEPARTMENT_ID = rmlmh.EPIC_DEPARTMENT_ID
										   AND rmlmh.BATCH_RUN_DT = hx.max_dt
							) loc
							ON loc.EPIC_DEPARTMENT_ID = #overall.DEPARTMENT_ID

							LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
							ON g.epic_department_id = #overall.DEPARTMENT_ID
							LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
							ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
							LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
							ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
							LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
							ON o.organization_id = s.organization_id

                        ) AS o
        ON  o.month_begin_date = date_dim.month_begin_date
        WHERE
            date_dim.day_date >= @startdate
            AND date_dim.day_date < @enddate
			--AND o.clinical_area_name = 'PWMC-Emergency Services';
			AND o.clinical_area_name IN ('HYMC-Emergency Services','PWMC-Emergency Services','Culpeper-Emergency Services');
--doing the work of the wrapper stored proc to meet Monday 3/13 due date

   -- INSERT Stage.Balscore_Dash_Emergency_PressGaney
   --         (event_type
   --         ,event_date
   --         ,event_category
   --         ,fmonth_num
   --         ,FYear_name
   --         ,Fyear_num
   --         ,epic_department_id
   --         ,epic_department_name
   --         ,epic_department_name_external
   --         ,hs_area_id
   --         ,hs_area_name
   --         ,opnl_service_id
   --         ,opnl_service_name
   --         ,Service_Line
   --         ,Service_Line_ID
   --         ,Sub_Service_Line
   --         ,Sub_Service_Line_ID
   --         ,practice_group_id
   --         ,practice_group_name
   --         ,event_count
   --         ,overall_mean
			--,sk_Dim_Pt
		 --   ,sk_Fact_Pt_Enc_Clrt
		 --   )
SELECT DISTINCT
                i.event_type
               ,i.epic_department_id
               ,i.epic_department_name
               ,i.epic_department_name_external
               ,i.event_date
               ,i.event_category
			   ,i.RECDATE
               ,i.fmonth_num
               ,i.FYear_name
               ,i.Fyear_num
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
			   ,i.organization_name
			   ,i.service_name
			   ,i.clinical_area_name
               ,i.event_count
               ,i.overall_mean
			   ,i.sk_Dim_Pt
			   ,i.sk_Fact_Pt_Enc_Clrt
            FROM
                #RptgTemp AS i--;
			ORDER BY
				i.epic_department_id,
				i.event_date

        SELECT DISTINCT
                i.event_type
			   ,i.organization_name
			   ,i.service_name
			   ,i.clinical_area_name
			   ,i.epic_department_id
			   ,i.epic_department_name
               ,i.fmonth_num
               ,i.FYear_name
               ,i.Fyear_num
               ,i.event_date
               ,i.event_count
               ,i.overall_mean
            FROM
                #RptgTemp AS i
				
			--ORDER BY i.epic_department_id, i.event_date;				
			ORDER BY i.organization_name, i.service_name, i.clinical_area_name, i.epic_department_id, i.event_date;

/****************************************************************************************************************************************************************************************************/
/*			
SELECT DISTINCT
	   [event_type]
      ,[event_count]
      ,[event_date]
      --,[event_id]
      --,[event_category]
      ,[epic_department_id]
      ,[epic_department_name]
      ,[epic_department_name_external]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      --,[report_period]
      --,[report_date]
      --,[peds]
      --,[transplant]
      --,[age_flag]
      --,[sk_Dim_Pt]
      --,[person_birth_date]
      --,[person_gender]
      --,[person_id]
      --,[person_name]
      --,[practice_group_id]
      --,[practice_group_name]
      --,[provider_id]
      --,[provider_name]
      --,[service_line_id]
      --,[service_line]
      --,[sub_service_line_id]
      --,[sub_service_line]
      --,[opnl_service_id]
      --,[opnl_service_name]
      --,[hs_area_id]
      ,[hs_area_name]
      --,[w_department_id]
      --,[w_department_name]
      --,[w_department_name_external]
      --,[w_opnl_service_id]
      --,[w_opnl_service_name]
      --,[w_practice_group_id]
      --,[w_practice_group_name]
      --,[w_service_line_id]
      --,[w_service_line_name]
      --,[w_sub_service_line_id]
      --,[w_sub_service_line_name]
      --,[w_report_period]
      --,[w_report_date]
      --,[w_hs_area_id]
      --,[w_hs_area_name]
      ,[overall_mean]
      --,[Load_Dtm]
      --,[w_serviceline_division_flag]
      --,[w_serviceline_division_id]
      --,[w_serviceline_division_name]
      --,[w_mc_operation_flag]
      --,[w_mc_operation_id]
      --,[w_mc_operation_name]
      --,[w_post_acute_flag]
      --,[w_ambulatory_operation_flag]
      --,[w_ambulatory_operation_id]
      --,[w_ambulatory_operation_name]
      --,[w_inpatient_adult_flag]
      --,[w_inpatient_adult_id]
      --,[w_inpatient_adult_name]
      --,[w_childrens_flag]
      --,[w_childrens_id]
      --,[w_childrens_name]
      --,[sk_Fact_Pt_Acct]
      --,[sk_Fact_Pt_Enc_Clrt]
  FROM [DS_HSDM_APP].[TabRptg].[Dash_BalancedScorecard_Emergency_PressGaney_Tiles]
  WHERE 1 = 1
  AND fyear_num = 2026
  AND epic_department_name = 'UVHE EMERGENCY DEPT'
  ORDER BY
	event_date
*/
GO


