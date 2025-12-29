USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate SMALLDATETIME = NULL

--SET @StartDate = '10/1/2017 00:00:00'
--SET @StartDate = '4/16/2019 00:00:00'
SET @startdate = '7/1/2019 00:00:00'
--SET @EndDate = '6/30/2019 00:00:00'
SET @enddate = '3/31/2020 00:00:00'

--ALTER PROCEDURE [ETL].[uspSrc_SvcLine_Inpatient_CHCAHPS]
--    (
--     @startdate SMALLDATETIME=NULL
--    ,@enddate SMALLDATETIME=NULL
--    )
--AS 
/**********************************************************************************************************************
WHAT: Create procedure ETL.uspSrc_SvcLine_Inpatient_CHCAHPS
WHO : Tom Burgan 
WHEN: 3/30/2020
WHY : Survey results for service_code=PD
-----------------------------------------------------------------------------------------------------------------------
INFO: 
      INPUTS:	DS_HSDM_App.ETL.usp_Get_Dash_Dates_BalancedScorecard
	            DS_HSDW_Prod.dbo.Dim_Date
				DS_HSDW_Prod.dbo.Fact_PressGaney_Responses
				DS_HSDW_Prod.dbo.Dim_PG_Question
				DS_HSDW_Prod.dbo.Fact_Pt_Acct
				DS_HSDW_Prod.dbo.Dim_Pt
				DS_HSDW_Prod.dbo.Dim_Physcn
				DS_HSDW_Prod.dbo.Dim_Clrt_SERsrc
				DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt
				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                DS_HSDM_App.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv
				DS_HSDW_Prod.dbo.Fact_Pt_Trnsplnt_Clrt
				DS_HSDW_Prod.dbo.Fact_Pt_Enc_Clrt
				DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc
                  
      OUTPUTS:  ETL.uspSrc_SvcLine_Inpatient_CHCAHPS

-------------------------------------------------------------------------------------------------------------------------------------------------------------
MODS: 	03/30/2020--TMB--Create stored procedure
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

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
SET @locstartdate = @startdate
SET @locenddate   = @enddate
----------------------------------------------------

    SELECT DISTINCT
            CAST('Inpatient-CHCAHPS' AS VARCHAR(50)) AS event_type		--this is the service code for inpatient-CHCAHPS
           ,pm.CH_48 AS event_category	--overall question, will count 9's and 10's
           ,rec.day_date AS event_date		--date survey received
           ,CAST(COALESCE(loc.epic_department_name,pm.UNIT) AS VARCHAR(254)) AS UNIT
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
           ,CASE WHEN pm.CH_48 IS NULL THEN 0
                 ELSE 1
            END AS event_count		--count when the overall question has been answered
           ,CAST(LEFT(DATENAME(MM, rec.day_date), 3)+' '+CAST(DAY(rec.day_date) AS VARCHAR(2)) AS VARCHAR(10)) AS report_period
           ,rec.day_date AS report_date
           ,provider_id
           ,provider_name
           ,11 AS service_line_id
           ,'Womens and Childrens' AS service_line
           ,1 AS sub_service_line_id
           ,'Children' AS sub_service_line
           ,COALESCE(loc.opnl_service_id,bscm.opnl_service_id) AS opnl_service_id
           ,COALESCE(loc.opnl_service_name,bscm.opnl_service_name) AS opnl_service_name
           ,COALESCE(loc.hs_area_id,bscm.hs_area_id) AS hs_area_id
           ,COALESCE(loc.hs_area_name,bscm.hs_area_name) AS hs_area_name
		   ,pm.som_group_id
		   ,pm.som_group_name
		   ,pm.rev_location_id
		   ,pm.rev_location
		   ,pm.financial_division_id
		   ,pm.financial_division_name
		   ,pm.financial_sub_division_id
		   ,pm.financial_sub_division_name
		   ,pm.som_department_id
		   ,CAST(pm.som_department_name AS VARCHAR(150)) AS som_department_name
		   ,pm.som_division_id
		   ,CAST(pm.som_division_name AS VARCHAR(150)) AS som_division_name
		   ,pm.som_hs_area_id
		   ,pm.som_hs_area_name
		   ,pm.sk_Dim_Pt
		   ------
		   ,CAST(COALESCE(loc.epic_department_id,bscm.EPIC_DEPARTMENT_ID) AS NUMERIC(18, 0)) AS epic_department_id
           ,epic_department_name = COALESCE(loc.epic_department_name,department.Clrt_DEPt_Nme)
           ,epic_department_name_external = COALESCE(loc.epic_department_name_external,department.Clrt_DEPt_Ext_Nme)
           ,CASE WHEN pm.AGE<18 THEN 1
                 ELSE 0
            END AS peds
           ,CASE WHEN tx.PAT_ENC_CSN_ID IS NOT NULL THEN 1
                 ELSE 0
            END AS transplant
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
       ,unitemp.CH_48
       ,unitemp.AGE
	   ,unitemp.unit AS UNIT
	   ,dept.DEPARTMENT_ID
       ,unitemp.ADJSAMP
       ,unitemp.BIRTH_DATE
       ,unitemp.PT_SEX
	   ,dvsn.som_group_id
	   ,dvsn.som_group_name
	   ,loc_master.LOC_ID AS rev_location_id
	   ,loc_master.REV_LOC_NAME AS rev_location
	   ,TRY_CAST(unitemp.Financial_Division AS INT) AS financial_division_id
	   ,CASE WHEN unitemp.Financial_Division_Name <> 'na' THEN CAST(unitemp.Financial_Division_Name AS VARCHAR(150)) ELSE NULL END AS financial_division_name
	   ,TRY_CAST(unitemp.Financial_SubDivision AS INT) AS financial_sub_division_id
	   ,CASE WHEN unitemp.Financial_SubDivision_Name <> 'na' THEN CAST(unitemp.Financial_SubDivision_Name AS VARCHAR(150)) ELSE NULL END AS financial_sub_division_name
	   ,dvsn.Department_ID AS som_department_id
	   ,dvsn.Department AS som_department_name
	   ,dvsn.Org_Number AS som_division_id
	   ,dvsn.Organization AS som_division_name
	   ,dvsn.som_hs_area_id
	   ,dvsn.som_hs_area_name
	   ,unitemp.sk_Dim_Pt
	   ,unitemp.PG_DESG

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
                           ,[CH_48]
                           ,[AGE]
                           ,CAST(COALESCE(UNIT,'Null Unit') AS VARCHAR(8)) AS UNIT
                           ,p.sk_Dim_Clrt_DEPt
						   ,[ADJSAMP]
                           ,BIRTH_DATE
                           ,PT_SEX
						   ,p.sk_Dim_Pt
						   ,p.PG_DESG
						   ,p.Financial_Division
						   ,p.Financial_Division_Name
						   ,p.Financial_SubDivision
						   ,p.Financial_SubDivision_Name
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
													   ,SUBSTRING(pm.Survey_Designator,1,2) AS PG_DESG
													   ,prov.Financial_Division
													   ,prov.Financial_Division_Name
													   ,prov.Financial_SubDivision
													   ,prov.Financial_SubDivision_Name
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
                                                WHERE   pm.Svc_Cde='PD'
                                                        AND qstn.VARNAME IN ('CH_48', 'AGE', 'UNIT', 'ADJSAMP')
                                                        AND pm.RECDATE >= @locstartdate
                                                        AND pm.RECDATE <  @locenddate
                            ) AS pivoted PIVOT ( MAX(VALUE) FOR VARNAME IN ([CH_48], [AGE], [UNIT], [ADJSAMP]) ) AS p
            ) unitemp
	LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt AS dept
			ON unitemp.sk_Dim_Clrt_DEPt = dept.sk_Dim_Clrt_DEPt
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
                ON dept.DEPARTMENT_ID = loc_master.EPIC_DEPARTMENT_ID

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
		ON (CAST(dvsn.Epic_Financial_Division_Code AS INTEGER) = TRY_CAST(unitemp.Financial_Division AS INT)
			AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INTEGER) = TRY_CAST(unitemp.Financial_SubDivision AS INT))     
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
   LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc AS loc
			ON pm.DEPARTMENT_ID=loc.epic_department_id
		-- ------------------------------------
    LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt AS department
            ON bscm.EPIC_DEPARTMENT_ID=department.DEPARTMENT_ID
	
    WHERE   rec.day_date >= @locstartdate
        AND rec.day_date <  @locenddate
		AND ((pm.PG_DESG IS NULL) OR (pm.PG_DESG = 'PC'))

	ORDER BY event_category DESC
	       , event_date;
GO


