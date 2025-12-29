USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--ALTER PROCEDURE [ETL].[uspSrc_SvcLine_Inpatient_HCAHPS]
--    (
--     @startdate SMALLDATETIME=NULL
--    ,@enddate SMALLDATETIME=NULL
--    )
--AS 

DECLARE @startdate SMALLDATETIME = NULL, 
        @enddate SMALLDATETIME = NULL

--SET @startdate = '11/1/2023 00:00 AM'
--SET @enddate = '11/30/2023 11:59 PM'
SET @startdate = '1/1/2018 00:00 AM'
SET @enddate = '3/18/2024 11:59 PM'

/**********************************************************************************************************************
WHAT: Create procedure Rptg.uspSrc_SvcLine_Inpatient_HCAHPS
WHO : Dayna Monaghan 
WHEN: 2/9/2016
WHY : Survey results for service_code=MD
-----------------------------------------------------------------------------------------------------------------------
INFO: 
      INPUTS:	dbo.Dim_Date
				dbo.Fact_PressGaney_Responses
				dbo.Dim_PG_Question
				dbo.Svc_Line_Map_Physician_Roster
                  
      OUTPUTS:  Rptg.uspSrc_SvcLine_Inpatient_HCAHPS

-------------------------------------------------------------------------------------------------------------------------------------------------------------
MODS: 	03/04/2016--DRM--Removed joins to staging tables; rewrote to use the fact and dimension tables for press ganey
		03/08/2016--DRM--Table name changed from Fact_PressGainey_Responses to Fact_PressGaney_Responses
		03/09/2016--DRM--Aliased the service line column to match the other balanced scorecard stored procedures
		03/09/2016--DRM--Returning only the left 20 characters of the VALUE column to avoid Tableau issues
		03/14/2016--DRM--Use sk_dim_physcn from survey (not account) for service line mapping
		08/26/2016--DRM--Adding time to date parameters was incorrect, changing CAST
		09/05/2016--DRM--Add transplant flag; map via Dim_Physican not roster table
		12/21/2016--DRM--Change to join by unit rather than physician for service line;
						 exclude adjusted surveys
		03/07/2017--AEH2Q--Refactor columns for Balanced Scorecard wrapper.
					Add fmonth_num,fyear_name,fyear_num
					Add Balanced Scorecard coulmns epic_department_id, epic_department_name, epic_department_name_external, service_line, service_line_id
					Add sub_service_line_id, sub_service_line, opnl_service_line_id, opnl_service_name, hs_area_id, hs_area_name
					Add person_gender, provider_id, provider_name
					Use CAST to specific length of varchar variables
		09/06/2017--DRM--Correct join to dim_physcn to handle -1 or 0 values
		03/08/2017 - BDD - changed date handling to eliminate strings
		03/08/2017 - BDD - refactored to eliminate temp table usage so that this can be handled in SSIS
		04/14/2017--DRM--corrected CASE statement handling UNIT; set NULL UNIT to medical center level; refined join logic for service line mapping
		05/18/2017--DRM--use Chris Mitchell's CASE statement for Unit; check for unit name changes for update press ganey surveys
		05/22/2017 - BDD - changed point of origin from the DW server to the DM server
		09/08/2017--DRM--Add in logic to handle an sk_dim_physcn of -1; discontinue filtering out sk_fact_pt_acct = -1
		10/05/2017--DRM--Updated proc to use MDM for all mapping joins
		11/07/2017--DRM--Joins change for PG update
		04/12/2018 -MAli A -- add logic for updated/new views Rptg.vwRef_Crosswalk_HSEntity_Prov and Rptg.vwRef_SOM_Hierarchy
		05/15/2019 -MAli A- edit logic to resolve issue resulting from multiple primary, active wd jobs for a provider;
                         add place-holder columns for w_som_hs_area_id (SMALLINT) and w_som_hs_area_name (VARCHAR(150))
		09/12/2019 -TMB--Add sk_Dim_Pt to extract
		07/29/2022 --TAH--Added new field sk_fact_pt_enc_clrt
	    09/26/2023 - GPS - Added CH Hospitalist Based Flag YN
		02/19/2024 - TMB - Replace joins to vwRef_MDM_Location_Master and vwRef_MDM_Location_Master_EpicSvc to
								join to vwRef_MDM_Location_Master_History
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

--DECLARE @startdate SMALLDATETIME = '9/1/2023'
--DECLARE @enddate   SMALLDATETIME = '9/10/2023'

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate
----------------------------------------------------

IF OBJECT_ID('tempdb..#hcahps ') IS NOT NULL
DROP TABLE #hcahps

IF OBJECT_ID('tempdb..#TabRptg ') IS NOT NULL
DROP TABLE #TabRptg

IF OBJECT_ID('tempdb..#summary ') IS NOT NULL
DROP TABLE #summary

    SELECT DISTINCT
            CAST('Inpatient-HCAHPS' AS VARCHAR(50)) AS event_type		--this is the service code for inpatient-HCAHPS
           ,pm.CMS_23 AS event_category	--overall question, will count 9's and 10's -- Rate hospital 0-10
		   -- Using any number from 0 to 10, where 0 is the worst hospital possible and 10 is the best hospital possible, what number would you use to rate this hospital?
           ,rec.day_date AS event_date		--date survey received
           ,CAST(COALESCE(pm.epic_department_name,pm.UNIT) AS VARCHAR(254)) AS UNIT
           ,rec.fmonth_num
           ,rec.FYear_name
           ,rec.Fyear_num
		   ,rec.month_num
		   ,rec.month_name
		   ,rec.year_num
           ,pm.SURVEY_ID AS event_id			--including the survey id to distinguish multiple surveys received by same patient on same day
           ,pm.MRN_int AS person_id		--patient
           ,pm.PAT_NAME AS person_name		--patient
           ,pm.BIRTH_DATE AS person_birth_date--patient
           ,CAST(CASE WHEN pm.PT_SEX='F' THEN 'Female'
                      WHEN pm.PT_SEX='M' THEN 'Male'
                      ELSE NULL
                 END AS VARCHAR(6)) AS person_gender
           ,CASE WHEN pm.CMS_23 IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,provider_id
           ,provider_name
		   ,CASE WHEN dc_phys.type_of_HSF_contract = 'UVACHMG Employed' THEN 1 ELSE 0 END  CH_Hosp_Based_YN
			--handle UNIT of OBS, dividing between children and adult; handle when no unit is returned and set to medical center level
           ,service_line_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.Service_Line_ID
                                   WHEN pm.epic_department_name IS NULL AND pm.UNIT='No Unit' THEN NULL
                                   ELSE COALESCE(pm.service_line_id, bscm.SERVICE_LINE_ID)
                              END
           ,service_line = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.Service_Line
                                WHEN pm.epic_department_name IS NULL AND pm.UNIT='No Unit' THEN NULL
                                ELSE COALESCE(pm.service_line,bscm.Service_Line)
                           END
           ,sub_service_line_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS'
                                            AND AGE<18 THEN 1
                                       WHEN pm.epic_department_name IS NULL AND pm.UNIT='obs'
                                            AND AGE>=18 THEN 3
                                       WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN NULL
                                       ELSE COALESCE(pm.sub_service_line_id,bscm.Sub_Service_Line_ID)
                                  END
           ,sub_service_line = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS'
                                         AND AGE<18 THEN 'Children'
                                    WHEN pm.epic_department_name IS NULL AND pm.UNIT='obs'
                                         AND AGE>=18 THEN 'Women'
                                    WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN NULL
                                    ELSE COALESCE(pm.sub_service_line,bscm.Sub_Service_Line)
                               END
           ,opnl_service_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT<>'OBS' THEN bscm.opnl_service_id
                                   ELSE COALESCE(pm.opnl_service_id,NULL)
                              END
           ,opnl_service_name = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT<>'OBS' THEN bscm.opnl_service_name
                                     ELSE COALESCE(pm.opnl_service_name,NULL)
                                END
           ,hs_area_id = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.hs_area_id
                              WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN 1
                              ELSE COALESCE(pm.hs_area_id,bscm.hs_area_id)
                         END
           ,hs_area_name = CASE WHEN pm.epic_department_name IS NULL AND pm.UNIT='OBS' THEN sl.hs_area_name
                                WHEN pm.epic_department_name IS NULL AND pm.UNIT='no unit' THEN 'Medical Center'
                                ELSE COALESCE(pm.hs_area_name,bscm.hs_area_name)
                           END
			
			--Add SOM grouping  04/12/2019  Mali A. 
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
		   ,pm.som_division_id
		   ,pm.som_division_name
		   --Add SOM Grouping 05/15/2019 -Mali A.
		    ,pm.som_division_5
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   --Add sk_Dim_Pt 09/12/2019 -TMB
		   ,pm.sk_Dim_Pt -- INTEGER
		   , pm.sk_Fact_Pt_Enc_Clrt
		   ------
		   ,CAST(COALESCE(pm.epic_department_id,bscm.EPIC_DEPARTMENT_ID) AS NUMERIC(18, 0)) AS epic_department_id
           ,epic_department_name = COALESCE(pm.epic_department_name,department.Clrt_DEPt_Nme)
           ,epic_department_name_external = COALESCE(pm.epic_department_name_external,department.Clrt_DEPt_Ext_Nme)
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
		   ,o.organization_id
		   ,o.organization_name
		   ,s.service_id
		   ,s.service_name
		   ,c.clinical_area_id
		   ,c.clinical_area_name
		   ,g.ambulatory_flag
		   ,g.community_health_flag
		   ,pm.Survey_Designator

	INTO #hcahps

    FROM    DS_HSDW_Prod.dbo.Dim_Date AS rec
    
	LEFT OUTER JOIN (
                     --pm
SELECT  unitemp.SURVEY_ID
           ,unitemp.RECDATE
		   ,unitemp.Load_Dtm
           ,unitemp.MRN_int
           ,unitemp.sk_Fact_Pt_Acct
           ,unitemp.NPINumber
           ,unitemp.sk_Dim_Physcn
           ,unitemp.provider_id
           ,unitemp.provider_name
           ,unitemp.Service_Line_md
           ,unitemp.PAT_NAME
           ,unitemp.CMS_23
           ,unitemp.AGE
		   ,CASE WHEN unitemp.unit=pg_update.update_unit THEN unitemp.unit 
		   WHEN pg_update.update_unit IS NOT NULL 
		   THEN pg_update.update_unit 
		   ELSE unitemp.unit END AS UNIT
		   ,dept.DEPARTMENT_ID
           ,unitemp.ADJSAMP
           ,unitemp.BIRTH_DATE
           ,unitemp.PT_SEX
		   ,NULL AS som_group_id
		   ,NULL AS som_group_name
		   ,mdmhst.LOC_ID AS rev_location_id
			,mdmhst.REV_LOC_NAME AS rev_location
		    ,uwd.Clrt_Financial_Division AS financial_division_id
			,uwd.Clrt_Financial_Division_Name AS financial_division_name
			,uwd.Clrt_Financial_SubDivision AS financial_sub_division_id
			,uwd.Clrt_Financial_SubDivision_Name financial_sub_division_name
			,CAST(uwd.SOM_Department_ID AS INT) AS som_department_id
			,CAST(uwd.SOM_Department AS VARCHAR(150)) AS som_department_name
			,CAST(uwd.SOM_Division_ID AS INT) AS som_division_id
			,CAST(uwd.SOM_Division_Name AS VARCHAR(150)) AS som_division_name
		-- Add 05/15/2018 Mali A. 
			,CAST(uwd.SOM_division_5 AS VARCHAR(150)) AS som_division_5

  			    ,CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS SMALLINT) ELSE CAST(3 AS SMALLINT) END AS som_hs_area_id
				,CASE WHEN uwd.SOM_Group_ID IS NULL THEN CAST(NULL AS VARCHAR(150)) ELSE CAST('School of Medicine' AS VARCHAR(150)) END AS som_hs_area_name
		-- Add 09/12/2019 TMB
		    ,unitemp.sk_Dim_Pt
			, unitemp.sk_Fact_Pt_Enc_Clrt
			,unitemp.Survey_Designator

			,mdmhst.EPIC_DEPARTMENT_ID
			,mdmhst.EPIC_DEPT_NAME AS epic_department_name
			,mdmhst.EPIC_EXT_NAME AS epic_department_name_external
			,mdmhst.SERVICE_LINE_ID
			,mdmhst.SERVICE_LINE
			,mdmhst.SUB_SERVICE_LINE_ID
			,mdmhst.SUB_SERVICE_LINE
			,mdmhst.OPNL_SERVICE_ID
			,mdmhst.OPNL_SERVICE_NAME
			,mdmhst.HS_AREA_ID
			,mdmhst.HS_AREA_NAME
			,mdmhst.PRESSGANEY_NAME

    FROM    (
             --unitemp
                     SELECT SURVEY_ID
                           ,RECDATE
                           ,MRN_int
                           ,sk_Fact_Pt_Acct
                           ,NPINumber
                           ,sk_Dim_Physcn
                           ,provider_id
                           ,provider_name
                           ,Service_Line_md
						   ,p.Load_Dtm
                           ,PAT_NAME
                           ,[CMS_23]
                           ,[AGE]
						   --,p.UNIT
                           ,CAST(CASE [UNIT]
                                   WHEN 'ADMT (Closed)' THEN 'Other'
                                   WHEN 'ER' THEN 'Other'
                                   WHEN 'ADMT' THEN 'Other'
                                   WHEN 'MSIC' THEN 'Other'
                                   WHEN 'MSICU' THEN 'Other'
                                   WHEN 'NNICU' THEN 'NNIC'
                                   ELSE [UNIT]
                                 END AS VARCHAR(8)) AS UNIT
                           ,p.sk_Dim_Clrt_DEPt
						   ,[ADJSAMP]
                           ,BIRTH_DATE
                           ,PT_SEX
						   ,p.sk_Dim_Pt
						   ,p.sk_Fact_Pt_Enc_Clrt
						   ,p.Survey_Designator
                     FROM   (
                             --pivoted
												SELECT DISTINCT
                                                        pm.SURVEY_ID
                                                       ,RECDATE
                                                       ,LEFT(VALUE, 20) AS VALUE
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
													   ,pm.sk_Dim_Clrt_DEPt
													   ,pm.Load_Dtm
													   ,pm.sk_Dim_Pt
													   ,fptec.sk_Fact_Pt_Enc_Clrt
													   ,pm.Survey_Designator
                                                FROM    DS_HSDW_Prod.dbo.Fact_PressGaney_Responses AS pm
                                                INNER JOIN DS_HSDW_Prod.dbo.Dim_PG_Question AS qstn
                                                        ON pm.sk_Dim_PG_Question=qstn.sk_Dim_PG_Question
                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Acct AS fpa
                                                        ON pm.sk_Fact_Pt_Acct=fpa.sk_Fact_Pt_Acct
                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Pt AS pat
                                                        ON fpa.sk_Dim_Pt=pat.sk_Dim_Pt
                                                LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Physcn AS dp
                                                        ON CASE WHEN pm.sk_Dim_Physcn IN (0,-1) THEN -999 ELSE pm.sk_Dim_Physcn END=dp.sk_Dim_Physcn
                                                LEFT OUTER JOIN [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc] prov
                                                        --provider table
                                                            ON dp.[sk_Dim_Physcn]=prov.[sk_Dim_Physcn]
												LEFT JOIN DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt fptec
														ON pm.Pat_Enc_CSN_Id=fptec.PAT_ENC_CSN_ID
                                                WHERE   pm.Svc_Cde='IN'
                                                        AND qstn.VARNAME IN ('CMS_23', 'AGE', 'UNIT', 'ADJSAMP')
                                                        AND pm.RECDATE >= @locstartdate
                                                        AND pm.RECDATE <  @locenddate
                            ) AS pivoted PIVOT ( MAX(VALUE) FOR VARNAME IN ([CMS_23], [AGE], [UNIT], [ADJSAMP]) ) AS p
            ) unitemp
    LEFT OUTER JOIN (
                     SELECT DISTINCT
                            sk_Fact_Pt_Acct
						    ,LAST_VALUE(loc.PRESSGANEY_NAME) OVER (PARTITION BY sk_Fact_Pt_Acct ORDER BY LOAD_DATE, LOAD_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS update_unit

                     FROM   [DS_HSDM_Ext_OutPuts].[DwStage].[Press_Ganey_DW_Submission] 

					 INNER JOIN ds_hsdw_prod.Rptg.vwRef_MDM_Location_Master_A2K3 AS a2unit ON a2unit.A2K3_NAME=Press_Ganey_DW_Submission.NURSTA
					 INNER JOIN DS_HSDW_Prod.rptg.vwRef_MDM_Location_Master AS loc ON a2unit.epic_department_id = loc.EPIC_DEPARTMENT_ID
                    ) AS pg_update
            ON unitemp.sk_Fact_Pt_Acct=pg_update.sk_Fact_Pt_Acct
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt AS dept
			ON unitemp.sk_Dim_Clrt_DEPt = dept.sk_Dim_Clrt_DEPt
	LEFT OUTER JOIN
		(
			SELECT
				history.MDM_BATCH_ID,
                history.EPIC_DEPARTMENT_ID,
                history.EPIC_DEPT_NAME,
				history.EPIC_EXT_NAME,
                history.SERVICE_LINE_ID,
                history.SERVICE_LINE,
                history.SUB_SERVICE_LINE_ID,
                history.SUB_SERVICE_LINE,
                history.LOC_ID,
                history.REV_LOC_NAME,
                history.HS_AREA_ID,
                history.HS_AREA_NAME,
                history.OPNL_SERVICE_ID,
                history.OPNL_SERVICE_NAME,
                history.PRESSGANEY_NAME
			FROM
			(
				SELECT
					MDM_BATCH_ID,
                    EPIC_DEPARTMENT_ID,
                    EPIC_DEPT_NAME,
					EPIC_EXT_NAME,
                    SERVICE_LINE_ID,
                    SERVICE_LINE,
                    SUB_SERVICE_LINE_ID,
                    SUB_SERVICE_LINE,
                    LOC_ID,
                    REV_LOC_NAME,
                    HS_AREA_ID,
                    HS_AREA_NAME,
                    OPNL_SERVICE_ID,
                    OPNL_SERVICE_NAME,
                    PRESSGANEY_NAME
				   ,ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
				FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
			) history
			WHERE history.seq = 1
		) mdmhst
		ON mdmhst.EPIC_DEPARTMENT_ID = dept.DEPARTMENT_ID
           
	LEFT OUTER JOIN
	     (
					SELECT DISTINCT
					    wd.sk_Dim_Physcn,
						wd.PROV_ID,
             			wd.Clrt_Financial_Division,
			    		wd.Clrt_Financial_Division_Name,
						wd.Clrt_Financial_SubDivision, 
					    wd.Clrt_Financial_SubDivision_Name,
					    wd.wd_Dept_Code,
					    wd.SOM_Group_ID,
					    wd.SOM_Group,
						wd.SOM_department_id,
					    wd.SOM_department,
						wd.SOM_division_id,
						wd.SOM_division_name,
						wd.SOM_division_5
					FROM
					(
					    SELECT
						    cwlk.sk_Dim_Physcn,
							cwlk.PROV_ID,
             			    cwlk.Clrt_Financial_Division,
			    		    cwlk.Clrt_Financial_Division_Name,
						    cwlk.Clrt_Financial_SubDivision, 
							cwlk.Clrt_Financial_SubDivision_Name,
							cwlk.wd_Dept_Code,
							som.SOM_Group_ID,
							som.SOM_Group,
							som.SOM_department_id,
							som.SOM_department,
							som.SOM_division_id,
							som.SOM_division_name,
							som.SOM_division_5,
							ROW_NUMBER() OVER (PARTITION BY cwlk.sk_Dim_Physcn ORDER BY som.som_group_id ASC) AS [SOMSeq]
						FROM Rptg.vwRef_Crosswalk_HSEntity_Prov AS cwlk
						    LEFT OUTER JOIN (SELECT DISTINCT
							                     SOM_Group_ID,
												 SOM_Group,
												 SOM_department_id,
												 SOM_department,
												 SOM_division_id,
												 SOM_division_name,
												 SOM_division_5
						                     FROM Rptg.vwRef_SOM_Hierarchy
						                    ) AS som
						        ON cwlk.wd_Dept_Code = som.SOM_division_5
					    WHERE cwlk.wd_Is_Primary_Job = 1
                              AND cwlk.wd_Is_Position_Active = 1
					) AS wd
					WHERE wd.SOMSeq = 1
				) AS uwd
					 ON uwd.sk_Dim_Physcn = unitemp.sk_Dim_Physcn      
		  ) AS pm
            ON rec.day_date=pm.RECDATE
					   --AND	ADJSAMP<>'Not Included'  --remove adjusted internet surveys
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
                     FROM   DS_HSDW_Prod.dbo.Fact_Pt_Trnsplnt_Clrt AS fptc
                     INNER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt AS fpec
                            ON fptc.sk_Dim_Clrt_Pt=fpec.sk_Dim_Clrt_Pt
                     INNER JOIN DS_HSDW_Prod.dbo.Dim_Date AS txsurg
                            ON fptc.sk_Tx_Surg_Dt=txsurg.date_key
                     WHERE  txsurg.day_date BETWEEN CAST(fpec.Adm_Dtm AS DATE) AND fpec.Dsch_Dtm
                            AND txsurg.day_date<>'1900-01-01 00:00:00'
                    ) AS tx
            ON pm.sk_Fact_Pt_Acct=tx.sk_Fact_Pt_Acct
    LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master AS bscm
            ON pm.UNIT=bscm.PRESSGANEY_NAME
    LEFT OUTER JOIN DS_HSDW_Prod.Anlys.Ref_Service_Line AS sl
            ON (CASE WHEN pm.UNIT='OBS' THEN 'Womens and Childrens'
                     ELSE NULL
                END)=sl.Service_Line
		-- ------------------------------------
    LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt AS department
            ON bscm.EPIC_DEPARTMENT_ID=department.DEPARTMENT_ID
   
		-- ------------------------------------

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.DEPARTMENT_ID
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id
   
		-- ------------------------------------
	
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn           AS dc_phys ON pm.sk_Dim_Physcn = dc_phys.sk_Dim_Physcn

    WHERE   rec.day_date >= @locstartdate
        AND rec.day_date <  @locenddate
		AND CASE WHEN pm.CMS_23 IS NULL THEN 0
		                 ELSE 1
		            END = 1;
/*
SELECT SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 , CAST(
	   CAST(SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #hcahps
WHERE event_count = 1
--AND epic_department_id = 10243037
--AND hs_area_id = 1
AND community_health_flag = 0
) [Proc]
*/
SELECT
       [Proc].month_num,
	   [Proc].month_name,
	   [Proc].year_num,
	   [Proc].epic_department_id,
	   [Proc].epic_department_name,
       SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 --, CAST(
	 --  CAST(SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
  --     CAST(SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	 --  AS NUMERIC(6,4)) AS TOP_BOX_PCT

INTO #summary

FROM
(
SELECT
	--event_type,
    event_category,
 --   event_date,
 --   UNIT,
 --   fmonth_num,
 --   FYear_name,
 --   Fyear_num,
	month_num,
	month_name,
	year_num,
    --event_id,
    --person_id,
    --person_name,
    --person_birth_date,
    --person_gender,
    event_count,
    --report_period,
    --report_date,
    --provider_id,
    --provider_name,
    --CH_Hosp_Based_YN,
    --service_line_id,
    --service_line,
    --sub_service_line_id,
    --sub_service_line,
    --opnl_service_id,
    --opnl_service_name,
    --hs_area_id,
    --hs_area_name,
    --som_group_id,
    --som_group_name,
    --rev_location_id,
    --rev_location,
    --financial_division_id,
    --financial_division_name,
    --financial_sub_division_id,
    --financial_sub_division_name,
    --som_department_id,
    --som_department_name,
    --som_division_id,
    --som_division_name,
    --som_division_5,
    --som_hs_area_id,
    --som_hs_area_name,
    --sk_Dim_Pt,
    --sk_Fact_Pt_Enc_Clrt,
    epic_department_id,
    epic_department_name--,
    --epic_department_name_external,
    --peds,
    --transplant,
    --organization_id,
    --organization_name,
    --service_id,
    --service_name,
    --clinical_area_id,
    --clinical_area_name,
    --ambulatory_flag,
    --community_health_flag
FROM #hcahps
WHERE event_count = 1
--AND epic_department_id = 10243037
--AND hs_area_id = 1
AND community_health_flag = 0
) [Proc]
GROUP BY
	[Proc].month_num,
	[Proc].month_name,
	[Proc].year_num,
	[Proc].epic_department_id,
	[Proc].epic_department_name
--ORDER BY
--	[Proc].month_num,
--	[Proc].month_name,
--	[Proc].year_num,
--	[Proc].epic_department_id,
--	[Proc].epic_department_name

SELECT
	month_num,
    month_name,
    year_num,
    epic_department_id,
    epic_department_name,
    Numerator,
    Denominator
FROM #summary
ORDER BY
	year_num, 
	month_num,
	month_name,
	epic_department_id,
	epic_department_name
/*
SELECT event_type,
       event_count,
       event_date,
       event_id,
       event_category,
       tabrptg.epic_department_id,
       tabrptg.epic_department_name,
       tabrptg.epic_department_name_external,
       fmonth_num,
       fyear_num,
       fyear_name,
       report_period,
       report_date,
       peds,
       transplant,
       age_flag,
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
       corp_service_line_id,
       corp_service_line_name,
       hs_area_id,
       hs_area_name,
       Unit,
       w_department_id,
       w_department_name,
       w_department_name_external,
       w_opnl_service_id,
       w_opnl_service_name,
       w_corp_service_line_id,
       w_corp_service_line_name,
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
       tabrptg.Load_Dtm,
       orig_transplant,
       orig_peds,
       financial_division_id,
       financial_division_name,
       financial_sub_division_id,
       financial_sub_division_name,
       rev_location_id,
       rev_location,
       som_group_id,
       som_group_name,
       som_department_id,
       som_department_name,
       som_division_id,
       som_division_name,
       w_financial_division_id,
       w_financial_division_name,
       w_financial_sub_division_id,
       w_financial_sub_division_name,
       w_rev_location_id,
       w_rev_location,
       w_som_group_id,
       w_som_group_name,
       w_som_department_id,
       w_som_department_name,
       w_som_division_id,
       w_som_division_name,
       som_division_5,
       w_som_hs_area_id,
       w_som_hs_area_name,
       w_serviceline_division_flag,
       w_serviceline_division_id,
       w_serviceline_division_name,
       w_mc_operation_flag,
       w_mc_operation_id,
       w_mc_operation_name,
       w_post_acute_flag,
       w_ambulatory_operation_flag,
       w_ambulatory_operation_id,
       w_ambulatory_operation_name,
       w_inpatient_adult_flag,
       w_inpatient_adult_id,
       w_inpatient_adult_name,
       w_childrens_flag,
       w_childrens_id,
       w_childrens_name,
       sk_Fact_Pt_Acct,
       sk_Fact_Pt_Enc_Clrt,
	   g.community_health_flag

INTO #TabRptg

FROM [DS_HSDM_App].[TabRptg].[Dash_BalancedScorecard_HCAHPS_Tiles] tabrptg
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id
WHERE  event_date >= @locstartdate AND event_date <= @locenddate
  --AND event_count = 1

SELECT SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 , CAST(
	   CAST(SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #TabRptg
WHERE event_count = 1
--AND epic_department_id = 10243037
--AND hs_area_id = 1
AND community_health_flag = 0
) TabRptg
*/
GO


