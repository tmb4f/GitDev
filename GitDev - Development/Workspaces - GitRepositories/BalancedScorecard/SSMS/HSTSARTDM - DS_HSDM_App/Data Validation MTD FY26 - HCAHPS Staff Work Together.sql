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
--SET @startdate = '12/1/2024 00:00 AM'
SET @startdate = '7/1/2025 00:00 AM'
--SET @enddate = '6/30/2021 11:59 PM'
--SET @enddate = '7/14/2021 11:59 PM'
--SET @enddate = '7/31/2022 11:59 PM'
--SET @enddate = '12/31/2022 11:59 PM'
--SET @enddate = '12/31/2024 11:59 PM'
SET @enddate = '7/31/2025 11:59 PM'

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
PXO - Overall Rating HCAHPS

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_SvcLine_Inpatient_HCAHPS.sql
*/

IF OBJECT_ID('tempdb..#Proc ') IS NOT NULL
DROP TABLE #Proc

IF OBJECT_ID('tempdb..#TabRptg3 ') IS NOT NULL
DROP TABLE #TabRptg3

    SELECT DISTINCT
            CAST('Inpatient-HCAHPS' AS VARCHAR(50)) AS event_type		--this is the service code for inpatient-HCAHPS
           ,pm.CMS_23 AS event_category	--overall question, will count 9's and 10's
           ,rec.day_date AS event_date		--date survey received
           ,CAST(COALESCE(pm.epic_department_name,pm.UNIT) AS VARCHAR(254)) AS UNIT
           ,rec.fmonth_num
           ,rec.FYear_name
           ,rec.Fyear_num
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
		   ,g.community_health_flag

    INTO #Proc

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
	
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Physcn           AS dc_phys ON pm.sk_Dim_Physcn = dc_phys.sk_Dim_Physcn

	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = pm.DEPARTMENT_ID
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Clinical_Area_Map c
	ON c.sk_Ref_Clinical_Area_Map = g.sk_Ref_Clinical_Area_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Service_Map s
	ON s.sk_Ref_Service_Map = c.sk_Ref_Service_Map
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Ref_Organization_Map o
	ON o.organization_id = s.organization_id

    WHERE   rec.day_date >= @locstartdate
        AND rec.day_date <  @locenddate
		AND CASE WHEN pm.CMS_23 IS NULL THEN 0
		                 ELSE 1
		            END = 1;

SELECT
       'PXO - Overall Rating HCAHPS' AS Metric
	 , 'Stored Procedure' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 , CAST(
	   CAST(SUM(CASE WHEN [Proc].event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN [Proc].event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #Proc
WHERE event_count = 1
AND community_health_flag = 0
) [Proc]

SELECT [event_type]
      ,[event_count]
      ,[event_date]
      ,[event_id]
      ,[event_category]
      ,tabrptg.[epic_department_id]
      ,tabrptg.[epic_department_name]
      ,[epic_department_name_external]
      ,[fmonth_num]
      ,[fyear_num]
      ,[fyear_name]
      ,[report_period]
      ,[report_date]
      ,[peds]
      ,[transplant]
      ,[age_flag]
      ,[sk_Dim_Pt]
      ,[person_birth_date]
      ,[person_gender]
      ,[person_id]
      ,[person_name]
      ,[practice_group_id]
      ,[practice_group_name]
      ,[provider_id]
      ,[provider_name]
      ,[service_line_id]
      ,[service_line]
      ,[sub_service_line_id]
      ,[sub_service_line]
      ,[opnl_service_id]
      ,[opnl_service_name]
      ,[corp_service_line_id]
      ,[corp_service_line_name]
      ,[hs_area_id]
      ,[hs_area_name]
      ,[Unit]
      ,[w_department_id]
      ,[w_department_name]
      ,[w_department_name_external]
      ,[w_opnl_service_id]
      ,[w_opnl_service_name]
      ,[w_corp_service_line_id]
      ,[w_corp_service_line_name]
      ,[w_practice_group_id]
      ,[w_practice_group_name]
      ,[w_service_line_id]
      ,[w_service_line_name]
      ,[w_sub_service_line_id]
      ,[w_sub_service_line_name]
      ,[w_report_period]
      ,[w_report_date]
      ,[w_hs_area_id]
      ,[w_hs_area_name]
      ,tabrptg.[Load_Dtm]
      ,[orig_transplant]
      ,[orig_peds]
      ,[financial_division_id]
      ,[financial_division_name]
      ,[financial_sub_division_id]
      ,[financial_sub_division_name]
      ,[rev_location_id]
      ,[rev_location]
      ,[som_group_id]
      ,[som_group_name]
      ,[som_department_id]
      ,[som_department_name]
      ,[som_division_id]
      ,[som_division_name]
      ,[w_financial_division_id]
      ,[w_financial_division_name]
      ,[w_financial_sub_division_id]
      ,[w_financial_sub_division_name]
      ,[w_rev_location_id]
      ,[w_rev_location]
      ,[w_som_group_id]
      ,[w_som_group_name]
      ,[w_som_department_id]
      ,[w_som_department_name]
      ,[w_som_division_id]
      ,[w_som_division_name]
      ,[som_division_5]
      ,[w_som_hs_area_id]
      ,[w_som_hs_area_name]

  INTO #TabRptg3

  FROM [DS_HSDM_App].[TabRptg].[Dash_BalancedScorecard_HCAHPS_Tiles] tabrptg
	LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
	ON g.epic_department_id = tabrptg.epic_department_id
  WHERE  event_date >= @locstartdate AND event_date <= @locenddate
  --AND event_count = 1
  AND g.community_health_flag = 0

SELECT
       'PXO - Overall Rating HCAHPS' AS Metric
	 , 'Tab Table' AS Source
	 , 'MTD' AS [Time Period]
	 , SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS Numerator
     , SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS Denominator
	 , CAST(
	   CAST(SUM(CASE WHEN TabRptg.event_category IN ('9','10-Best possible') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN TabRptg.event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	   AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #TabRptg3
WHERE event_count = 1
) TabRptg

/*
PXO - Staff Worked Together (Adult Inpatient)

C:\Users\tmb4f\source\Workspaces\Tom Burgan - My Files\Development\GitRepositories\AmbulatoryOptimization\SSMS\HSTSARTDM - DS_HSDM_App\Test script for Stored Procedure HSTSARTDM DS_HSDM_App ETL uspSrc_AmbOpt_IN_StaffWorkedTogether.sql
*/

IF OBJECT_ID('tempdb..#in ') IS NOT NULL
DROP TABLE #in

IF OBJECT_ID('tempdb..#summary5 ') IS NOT NULL
DROP TABLE #summary5

    SELECT DISTINCT
            CAST('HCAHPS' AS VARCHAR(50)) AS event_type
		   ,CASE WHEN pm.VALUE IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the HCAHPS question has been answered
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
		   ,CAST(NULL AS DECIMAL(10, 2)) AS weighted_score
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

    INTO #in

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
				,mdmhst.service_line_id
				,mdmhst.service_line
				,mdmhst.sub_service_line_id
				,mdmhst.sub_service_line
				,mdmhst.opnl_service_id
				,mdmhst.opnl_service_name
				,mdmhst.corp_service_line_id
				,mdmhst.corp_service_line
				,mdmhst.hs_area_id
				,mdmhst.hs_area_name
				,mdmhst.practice_group_id
				,mdmhst.practice_group_name
				,dep.DEPARTMENT_ID AS epic_department_id
				,mdmhst.epic_department_name
				,mdmhst.epic_department_name_external
				,mdmhst.POD_ID AS pod_id
		        ,mdmhst.PFA_POD AS pod_name
				,mdmhst.HUB_ID AS hub_id
		        ,mdmhst.HUB AS hub_name
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
				,mdmhst.BUSINESS_UNIT
				,COALESCE(ptot.PROV_TYPE_OT_NAME, prov.Prov_Typ, NULL) AS Prov_Typ
				,prov.Staff_Resource
				,mdmhst.LOC_ID AS rev_location_id
				,mdmhst.REV_LOC_NAME AS rev_location
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
				,upg.UPG_PRACTICE_FLAG AS upg_practice_flag
				,CAST(upg.UPG_PRACTICE_REGION_ID AS INTEGER) AS upg_practice_region_id
				,CAST(upg.UPG_PRACTICE_REGION_NAME AS VARCHAR(150)) AS upg_practice_region_name
				,CAST(upg.UPG_PRACTICE_ID AS INTEGER) AS upg_practice_id
				,CAST(upg.UPG_PRACTICE_NAME AS VARCHAR(150)) AS upg_practice_name
				,appts.F2F_Flag
				,appts.ENC_TYPE_C
				,appts.ENC_TYPE_TITLE
	            ,CASE WHEN (doc.ProviderGroup = 'Clin Staff') THEN 1 ELSE 0 END AS Lip_Flag
	            ,mdmhst.FINANCE_COST_CODE
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
				WHERE sk_Fact_Pt_Acct > 0 AND sk_Dim_PG_Question = '4' -- Age question for Inpatient
				GROUP BY SURVEY_ID
			) Resp_Age
				ON resp.SURVEY_ID = Resp_Age.SURVEY_ID
		LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
				ON resp.sk_Dim_Clrt_DEPt = dep.sk_Dim_Clrt_DEPt
		LEFT OUTER JOIN
			(
				SELECT
					history.MDM_BATCH_ID,
					history.EPIC_DEPARTMENT_ID,
					history.EPIC_DEPT_NAME AS epic_department_name,
					history.EPIC_EXT_NAME AS epic_department_name_external,
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
					history.PRESSGANEY_NAME,
					history.FINANCE_COST_CODE,
					history.CORP_SERVICE_LINE_ID,
					history.CORP_SERVICE_LINE,
					history.PRACTICE_GROUP_ID,
					history.PRACTICE_GROUP_NAME,
					history.POD_ID,
					history.PFA_POD,
					history.HUB_ID,
					history.	HUB,
					history.BUSINESS_UNIT
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
						PRESSGANEY_NAME,
						FINANCE_COST_CODE,
						CORP_SERVICE_LINE_ID,
						CORP_SERVICE_LINE,
						PRACTICE_GROUP_ID,
						PRACTICE_GROUP_NAME,
						POD_ID,
						PFA_POD,
						HUB_ID,
						HUB,
						BUSINESS_UNIT,
						ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
					FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
				) history
				WHERE history.seq = 1
			) mdmhst
			ON mdmhst.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID
        LEFT OUTER JOIN Rptg.UOS_Visit_Map_Epic map
                ON map.Deptid = CAST(mdmhst.FINANCE_COST_CODE AS INTEGER)
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

                -- -------------------------------------
                -- Department UPG Practice--
                -- -------------------------------------
				LEFT OUTER JOIN Rptg.vwClarity_DEP_UPG upg
				    ON upg.DEPARTMENT_ID = dep.DEPARTMENT_ID

		WHERE   resp.Svc_Cde='IN'
				AND (resp.sk_Dim_PG_Question = '3834' AND resp.RECDATE > '12/31/2024') -- VARNAME: CMS_47 - During this hospital stay, how often did doctors, nurses and other hospital staff work well together to care for you?
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

    SELECT HS_AREA_ID
	              ,organization_id
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
				  ,event_category
				  ,CASE WHEN event_count = 1 THEN 1 ELSE 0 END AS StaffWorkedTogetherResponse
				  ,ambulatory_flag
				  ,community_health_flag
				  ,Survey_Designator

	INTO #summary5

	FROM #in

SELECT
    'PXO - Staff Worked Together (Adult Inpatient)' AS Metric,
    'Stored Procedure' AS Source,
    'MTD' AS [Time Period],
	SUM(CASE WHEN [summary5].event_category IN ('Always') THEN 1 ELSE 0 END) AS Numerator,
	SUM(StaffWorkedTogetherResponse) AS StaffWorkedTogetherResponse,
	CAST(
	   CAST(SUM(CASE WHEN [summary5].event_category IN ('Always') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN [summary5].StaffWorkedTogetherResponse = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM
(
SELECT *
FROM #summary5
WHERE StaffWorkedTogetherResponse = 1
--AND community_health_flag = 0
AND HS_AREA_ID = 1
) [summary5]

SELECT
    'PXO - Staff Worked Together (Adult Inpatient)' AS Metric,
    'Tab Table' AS Source,
    'MTD' AS [Time Period],
	SUM(CASE WHEN tabrptg.event_category IN ('Always') THEN 1 ELSE 0 END) AS Numerator,
	SUM(event_count) AS StaffWorkedTogetherResponse,
	CAST(
	   CAST(SUM(CASE WHEN tabrptg.event_category IN ('Always') THEN 1 ELSE 0 END) AS NUMERIC(9,2)) /
       CAST(SUM(CASE WHEN tabrptg.event_count = 1 THEN 1 ELSE 0 END) AS NUMERIC(9,2))
	AS NUMERIC(6,4)) AS TOP_BOX_PCT
FROM DS_HSDM_App.TabRptg.Dash_AmbOpt_HCAHPSStaffWorkTogether_Tiles tabrptg
LEFT OUTER JOIN DS_HSDM_App.Mapping.Epic_Dept_Groupers g
ON g.epic_department_id = tabrptg.epic_department_id

    WHERE   tabrptg.event_date>=@locstartdate
            AND tabrptg.event_date<@locenddate
	        --AND g.community_health_flag = 0
			AND tabrptg.hs_area_id = 1

GO