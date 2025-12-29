USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--ALTER  PROC [ETL].[uspSrc_CEO_DV_UVA_ALL_MainORIMRI_Detail_v2]
--AS
/*******************************************************************************************
WHAT:	[Rptg].[uspSrc_CEO_DV_MainORIMRI_Detail_v2]
WHO :	Katherine Couvillon	
WHEN:	1/15/16
WHY :	CEO Dashboard Query
   	
------------------------------------------------------------------------------------------
MODS: 2/18/2016 - BDD - split into separate stored procs for SSIS package 
	  7/06/2017 - AAO - Phase 2 updates
	  10/4/2017 - Richard Rickles - Consultant - modifying selection criteria, tables and log source to include Procedure Logs in addition to Surgical logs
				  Created an initial temp table to hold log IDs and CSNs for filtering and joining data in main query.
	  07/02/2020 - AAO - Service Area Filter
	  02/08/2023 - Gian Simone - Added Additional COlumns needed for Data Portal Integration
--------------------------------------------------------------------------------------------------------------------
--------------SEARH UL### TO FIND AREAS OF CODE THAT WERE UPDATED RELATED TO UL###----------------------------------
--------------------------------------------------------------------------------------------------------------------
      04/11/2023--UL001--Chris Meshes - UPDATED CODE FOR CH HOSPITAL INCLUSION
      06/26/2023--UL002--YIING-HARN CHUANG - UPDATED MAIN OR LOCATION BY USING HARD CODED LOC ID FOR OPTIMIAZATION 
                                             LOC ABBR IS NOT VALID AFTER JUNE 2023 UPDATE
      09/21/2023--UL003--YIING-HARN CHUANG - ADDED PRIOR FISCAL YEAR DATA
      10/16/2023--UL004--YIING-HARN CHUANG - UPDATED vwRef_MDM_Location_Master_Hospital_Group TO 
                                           vwRef_MDM_Location_Master_Hospital_Group_All FOR MAPPING ALL DEPARTMENTS  
      12/06/2023--UL005--YIING-HARN CHUANG - ADDED BASE PATIENT CLASS 
      07/25/2024--UL006--YIING-HARN CHUANG - Exclude GI procedures
*******************************************************************************************/

SET NOCOUNT ON 

DECLARE @STARTDATE SMALLDATETIME
DECLARE @ENDDATE SMALLDATETIME

--SET @STARTDATE = '7/1/2022'
--SET @ENDDATE = GETDATE()--'1/31/2023'

--UL003 START - CHANGE DATE LOGIC
        -- DECLARE @currdate SMALLDATETIME
        -- SET @currdate = DATEADD(DAY, -1, CONVERT(CHAR(10),GETDATE(),101)) 

        -- DECLARE @fystartyr INTEGER

        -- IF MONTH(@currdate) > 6
        -- 	SET @fystartyr = YEAR(@currdate)
        -- ELSE 
        -- 	SET @fystartyr = YEAR(@currdate) - 1

        -- /* SET DATE VARIABLES */
        -- IF @STARTDATE IS NULL
        --     /* LAST 7 DAYS */
        -- 	SET @STARTDATE = CAST('7/1/' + CAST(@fystartyr AS VARCHAR(4)) AS SMALLDATETIME)

        -- IF @ENDDATE  IS NULL
        -- 	/*  BEGINNING OF TODAY */
        -- 	SET @ENDDATE = CAST(DATEADD(DAY, 1, @currdate) AS SMALLDATETIME)

                
EXEC ETL.usp_Get_Dash_Dates_DailyVolumes @startdate OUTPUT, @enddate OUTPUT 
--UL003 END

DECLARE @enddate_plus1 SMALLDATETIME
SET @enddate_plus1 = DATEADD(d,1,@ENDDATE)


 ;WITH mdm AS (
	SELECT DISTINCT
		EPIC_DEPARTMENT_ID
		,SERVICE_LINE_ID
		,SERVICE_LINE
		,SUB_SERVICE_LINE_ID
		,SUB_SERVICE_LINE
		,OPNL_SERVICE_ID
		,OPNL_SERVICE_NAME
		,CORP_SERVICE_LINE_ID
		,CORP_SERVICE_LINE 
		,FINANCE_COST_CODE
		,HS_AREA_ID
		,HS_AREA_NAME
		,POD_ID
		,PFA_POD
		,HUB_ID
		,HUB
		,ROW_NUMBER() OVER (PARTITION BY EPIC_DEPARTMENT_ID ORDER BY hs_area_id DESC) seq
	
	FROM CLARITY_App.Rptg.vwRef_MDM_Location_Master
)

, MDM_DEP AS (			
	SELECT DISTINCT		
		mdm.EPIC_DEPARTMENT_ID	
		,mdm.HOSPITAL_CODE	
		,mdm.DE_HOSPITAL_CODE	
		,mdm.HOSPITAL_GROUP	
	FROM	CLARITY_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_All AS mdm	--UL004 CHANGED TO MDM ALL VERSION
	WHERE	mdm.EPIC_DEPARTMENT_ID IS NOT NULL	
)	
/*
SELECT
	CAST(A.ACCTNBR_INT AS NUMERIC(18, 0))														AS AcctNbr_Int
	,CAST(A.PAT_MRN_ID AS VARCHAR(102))															AS pat_mrn_id
	,CAST(A.UNIQUE_ID AS NUMERIC(18, 0))														AS Unique_ID
	,CAST(A.SURGERY_DATE AS DATE)																AS UOS_DTTM
	,CAST('Operating Rooms' AS VARCHAR(50))														AS DEPARTMENT
	,CAST(A.UOS AS VARCHAR(100))																AS UOS
	--UL001 REPLACED CASE STATEMENT FOR COST CODE FOR BUDGET ALIGNMENT
	,CASE 
		WHEN a.hospital_code = 'UVA-MC' THEN CAST('2039' AS VARCHAR(30))
		WHEN a.hospital_code = 'UVA-PW' THEN CAST('3137' AS VARCHAR(30))
		WHEN a.hospital_code = 'UVA-HM' THEN CAST('3337' AS VARCHAR(30))
		WHEN a.hospital_code = 'UVA-CP' THEN CAST('3537' AS VARCHAR(30))
	ELSE NULL
	END AS Cost_Code
	--,CAST(CASE 
	--		WHEN A.DEPARTMENT = 'MAIN_OR' THEN '2039'  
	--		WHEN A.DEPARTMENT = 'OPSC' THEN '2660'
	--		WHEN A.DEPARTMENT = 'IMRI' THEN '2378'
	--		WHEN A.DEPARTMENT = 'OFF_SITE' THEN '14'
	--		WHEN A.DEPARTMENT = 'GUOR' THEN '2885'
	--		ELSE '' END AS VARCHAR(30)
	--	)																						AS COST_CODE
	--UL001 END
	,CAST(A.AGE_AT_SERVICE AS DECIMAL(5,2))														AS AGE_AT_SERVICE
	,CAST(A.PAT_ENC_CSN_ID AS NUMERIC(18, 0))													AS pat_enc_csn_id
	,CAST(A.pat_id AS VARCHAR(18))																AS pat_id
	,CAST('CEO Daily Volumes Main OR Details' AS VARCHAR(50))									AS event_type
	,CAST(CASE WHEN a.LOG_ID IS NOT NULL THEN 1 ELSE 0 END AS INT)								AS event_count
	,CAST(A.SURGERY_DATE AS DATETIME)															AS event_date
	,CAST(A.UNIQUE_ID AS BIGINT)																AS event_id
	,CAST(NULL AS VARCHAR(25))																	AS event_category
	,CAST(a.department_id AS NUMERIC(18,0))														AS epic_department_id
	,CAST(a.department_name AS VARCHAR(255))													AS epic_department_name
	,CAST(a.EXTERNAL_NAME AS VARCHAR(255))														AS epic_department_name_external
	,a.fmonth_num
	,a.Fyear_num
	,a.FYear_name
	,report_period = CAST(LEFT(DATENAME(MM, a.month_begin_date), 3) + ' ' + CAST(DAY(a.month_begin_date) AS VARCHAR(2)) AS VARCHAR(10))
	,report_date = CAST(CAST(a.month_begin_date AS DATE) AS SMALLDATETIME)
	,CAST(NULL AS SMALLINT)																		AS peds
	,CAST(NULL AS SMALLINT)																		AS transplant
	,CAST(NULL AS INT)																			AS sk_dim_pt
	,CAST(a.birth_date AS DATETIME)																AS person_birth_date
	,CAST(a.gender AS VARCHAR(255))																AS person_gender
	,TRY_CAST(a.pat_mrn_id AS INT)																	AS person_id
	,CAST(a.pat_name AS VARCHAR(200))															AS person_name
	,CAST(NULL AS INT)																			AS practice_group_id
	,CAST(NULL AS VARCHAR(150))																	AS practice_group_name
	,CAST(a.prov_id AS VARCHAR(18))																AS provider_id
	,CAST(a.prov_name AS VARCHAR(200))															AS provider_name
	,CAST(a.SERVICE_LINE_ID AS INT)																AS service_line_id
	,CAST(a.service_line AS VARCHAR(150))														AS service_line
	,CAST(a.sub_service_line_id AS INT)															AS sub_service_line_id
	,CAST(a.sub_service_line AS VARCHAR(150))													AS sub_service_line
	,CAST(a.opnl_service_id AS INT)																AS opnl_service_id
	,CAST(a.opnl_service_name	AS VARCHAR(150))												AS opnl_service_name
	,CAST(a.corp_service_line_id AS INT)														AS corp_service_id
	,CAST(a.corp_service_line AS VARCHAR(150))													AS corp_service_name
	,CAST(a.hs_area_id AS SMALLINT)																AS hs_area_id
	,CAST(a.hs_area_name AS VARCHAR(150))														AS hs_area_name
	,CAST(a.HOSPITAL_CODE	AS VARCHAR(150))									AS hospital_code
	, CAST(a.POD_ID AS VARCHAR(100))												AS pod_id
	, CAST(a.PFA_POD AS VARCHAR(100))											AS pod_name
	, CAST(a.HUB_ID AS VARCHAR(100))												AS hub_id
	, CAST(a.HUB AS VARCHAR(100))												AS hub_name
	, a.prov_service_line_id														AS prov_service_line_id
	, a.prov_service_line														AS prov_service_line
	, a.prov_hs_area_id															AS prov_hs_area_id
	, a.prov_hs_area_name														AS prov_hs_area_name
	, a.sk_dim_physcn															AS sk_dim_physcn
	, TRY_CAST(a.Financial_Division AS INT)																						AS financial_division_id
    , CASE WHEN a.Financial_Division_Name <> 'na' THEN CAST(a.Financial_Division_Name AS VARCHAR(150)) ELSE NULL END			AS financial_division_name
	, TRY_CAST(a.Financial_SubDivision AS INT)																					AS financial_sub_division_id
	, CASE WHEN a.Financial_SubDivision_Name <> 'na' THEN CAST(a.Financial_SubDivision_Name AS VARCHAR(150)) ELSE NULL END	AS financial_sub_division_name
	, a.LOC_ID																	AS rev_location_id
	, a.LOC_NAME																	AS rev_location
	, a.som_group_id																AS som_group_id
	, a.som_group_name															AS som_group_name
	, a.som_department_id														AS som_department_id
	, CAST(a.som_department_name AS VARCHAR(150))								AS som_department_name
	, CAST(a.som_division_id AS INT)												AS som_division_id
	, CAST(a.som_division_name AS VARCHAR(150))									AS som_division_name
  , CAST(a.accout_patient_class AS VARCHAR(50) )  AS Patient_Class --UL005 ADDED PATIENT CLASS
FROM
	(
*/
SELECT
	A.ACCTNBR_INT,
    A.PAT_ENC_CSN_ID,
    A.UNIQUE_ID,
	A.TRACKING_TIME_IN,
	A.TRACKING_TIME_OUT,
	A.Seq,
    A.PAT_MRN_ID,
    A.PAT_ID,
    A.PAT_NAME,
    A.BIRTH_DATE,
    A.gender,
    A.LOC_ID,
    A.LOC_NAME,
	A.Surg_Date,
    A.DEPARTMENT_ID,
    A.DEPARTMENT_NAME,
    A.EXTERNAL_NAME,
    A.fmonth_num,
    A.Fyear_num,
    A.FYear_name,
    A.month_begin_date,
    A.PROV_ID,
    A.PROV_NAME,
    A.AGE_AT_SERVICE,
    A.SURGERY_DATE,
    A.UOS,
    A.IN_ROOM,
    A.SERVICE_LINE_ID,
    A.SERVICE_LINE,
    A.SUB_SERVICE_LINE_ID,
    A.SUB_SERVICE_LINE,
    A.OPNL_SERVICE_ID,
    A.OPNL_SERVICE_NAME,
    A.CORP_SERVICE_LINE_ID,
    A.CORP_SERVICE_LINE,
    A.HS_AREA_ID,
    A.HS_AREA_NAME,
    A.HOSPITAL_CODE,
    A.POD_ID,
    A.PFA_POD,
    A.HUB_ID,
    A.HUB,
    A.prov_service_line_id,
    A.prov_service_line,
    A.prov_hs_area_id,
    A.prov_hs_area_name,
    A.sk_dim_physcn,
    A.Financial_Division,
    A.Financial_Division_Name,
    A.Financial_SubDivision,
    A.Financial_SubDivision_Name,
    A.som_group_id,
    A.som_group_name,
    A.som_department_id,
    A.som_department_name,
    A.som_division_id,
    A.som_division_name,
    A.accout_patient_class,
    A.Seq
FROM
	(
		SELECT       
			HSP.HSP_ACCOUNT_ID AS ACCTNBR_INT
			,LNK.PAT_ENC_CSN_ID 
			,ORL.LOG_ID AS UNIQUE_ID
			,ort.TRACKING_TIME_IN
			,ort.TRACKING_TIME_OUT
			,ROW_NUMBER() OVER (PARTITION BY hsp.HSP_ACCOUNT_ID, orl.LOG_ID ORDER BY ort.TRACKING_TIME_IN DESC) "Seq"
			,PAT.PAT_MRN_ID
			,pat.PAT_ID
		--UL002 START - CHANGE LOCATION COLUMNS SOURCING FROM LOC TO LOGLIST
            ,LOGLIST.LOC_ID
		    ,LOGLIST.LOC_NAME
			,LOGLIST.Surg_Date
        --UL002 END
			,pat.PAT_NAME
			,pat.BIRTH_DATE
			,zcs.NAME	gender
			,dep.DEPARTMENT_ID
			,dep.DEPARTMENT_NAME
			,dep.EXTERNAL_NAME
			,dd.fmonth_num
			,dd.Fyear_num
			,dd.FYear_name
			,dd.month_begin_date
			,ser.PROV_ID
			,ser.PROV_NAME
			,ROUND(DATEDIFF(DAY, PAT.BIRTH_DATE, ORL.SURGERY_DATE)/365.0,2) AGE_AT_SERVICE
			,ORL.SURGERY_DATE
			--UL001 START - REMOVED DUE TO DEPRECATION OF CASE STATEMENT THAT USES THIS SECTION
			--,CASE	
			--	WHEN LOC.LOC_NAME  = 'UVHE Main OR' THEN 'Main_OR'
			--	WHEN LOC.LOC_NAME LIKE '%IMRI%' THEN 'IMRI'
			--	WHEN LOC.LOC_NAME LIKE '%Gen%Op%' THEN 'GUOR'
			--	WHEN LOC.LOC_NAME LIKE '%Ou%pa%Su%Ce%' THEN 'OPSC'
			--	WHEN LOC.LOC_NAME LIKE '%EP%LABS%' THEN 'Off_Site'
			--	WHEN LOC.LOC_NAME LIKE '%W%C%' THEN 'Off_Site'
			--	WHEN LOC.LOC_NAME LIKE '%Anes%Ser%' THEN 'Off_Site'
			--	ELSE LOC.LOC_NAME
			--	END AS DEPARTMENT
			--UL001 END
			,CASE WHEN LOGLIST.LOG_TYPE_C = 0 THEN 'SURGERY' ELSE 'PROCEDURE' END UOS
			,CASE WHEN ORT.TRACKING_EVENT_C = 60 THEN ORT.TRACKING_TIME_IN  END AS IN_ROOM
			,map.SERVICE_LINE_ID 								
			,map.service_line 								
			,map.sub_service_line_id 								
			,map.sub_service_line								
			,map.opnl_service_id							
			,map.opnl_service_name								
			,map.corp_service_line_id								
			,map.corp_service_line							
			,map.hs_area_id 							
			,map.hs_area_name
			,MDM_DEP.HOSPITAL_CODE
		,Map.POD_ID
		,Map.PFA_POD
		,Map.HUB_ID
		,Map.HUB
		,physsvc.Service_Line_ID	AS prov_service_line_id
		,physsvc.Service_Line		AS prov_service_line
		,physsvc.hs_area_id			AS prov_hs_area_id
		,physsvc.hs_area_name		AS prov_hs_area_name
		,physsvc.sk_Dim_Physcn		AS sk_dim_physcn
		,vwDim_Clrt_SERsrc.Financial_Division
		,vwDim_Clrt_SERsrc.Financial_Division_Name
		,vwDim_Clrt_SERsrc.Financial_SubDivision
		,vwDim_Clrt_SERsrc.Financial_SubDivision_Name
		,dvsn.som_group_id		
		,dvsn.som_group_name
		,dvsn.Department_ID som_department_id
		,dvsn.Department som_department_name
		,dvsn.Org_Number som_division_id
		,dvsn.Organization som_division_name
    ,BCLS.name accout_patient_class --UL005 ADDED PATIENT CLASS
		FROM
			(SELECT l.LOG_ID
				,COALESCE(a.OR_LINK_CSN, a.PAT_ENC_CSN_ID) OR_CSN_ID  /* THIS COALESCES THE SURGERY CSN AND THE PROCEDURE CSN TO CREATE A REFERENCE CSN USED IN MAIN QUERY */
				,l.LOG_TYPE_C
                --UL002 ADDED location columns 
                ,l.LOC_ID
                ,loc.LOC_NAME
     ---bdd 12/12/2024 additions for use in outer query
	            ,CAST(l.SURGERY_DATE AS DATE) AS Surg_Date
				,l.PRIMARY_PHYS_ID
     ---end of 12/12/2024 additions
			FROM CLARITY..OR_LOG l 
				LEFT OUTER JOIN CLARITY..PAT_OR_ADM_LINK a ON l.LOG_ID = a.OR_CASELOG_ID
				LEFT OUTER JOIN CLARITY..CLARITY_LOC LOC ON L.LOC_ID = LOC.LOC_ID
                LEFT OUTER JOIN CLARITY..OR_CASE_ALL_PROC aproc ON l.CASE_ID = aproc.OR_CASE_ID AND aproc.line = 1 --UL006
			    LEFT OUTER JOIN CLARITY..OR_LOC orc ON orc.LOC_ID = L.LOC_ID
			    LEFT OUTER JOIN CLARITY..CLARITY_DEP DEP ON dep.DEPARTMENT_ID = orc.OR_DEPARTMENT_ID
			WHERE 1=1 
				AND l.SURGERY_DATE >= @STARTDATE 

				   ---BDD 3/15/2023 altered below to avoid using function in where clause
				---AND l.SURGERY_DATE < DATEADD(d,1,@ENDDATE)
				AND l.SURGERY_DATE < @enddate_plus1
				AND l.STATUS_C NOT IN (4,6)       /* NOT VOIDED, NOT CANCELED */
				
                --UL002 START - MODIFIED LOCATION PULL TO USE HARD CODED LOCATION IDs
                    --UL001 START - MODIFIED LOCATION PULL TO BE DYNAMIC FOR MAIN OR'S
                    -- AND loc.LOCATION_ABBR = 'MAIN'
                    --AND l.LOC_ID IN (SELECT L.LOC_ID FROM CLARITY..CLARITY_LOC L WHERE ( L.LOC_NAME  = 'UVHE Main OR'  OR  L.LOC_NAME LIKE '%IMRI%' OR L.LOC_NAME LIKE '%Gen%Op%'))) LOGLIST
                    --UL001 END
                AND
				(l.LOC_ID IN (
				1071024300	/*UVHE Main OR*/
				,1071029500	/*CPSA OR*/
				,1071038800	/*PWMC OR*/
				,1071074300	/*HYMC OR*/)
                --UL002 END
				OR
				dep.DEPARTMENT_NAME LIKE '%OPSC%')
			--UL001 END 
                AND aproc.OR_PROC_ID NOT IN ('1070002516', '1070007167', '10711017868', '1071110004', '1071110163', '1071110213',
                                        '107444003', '1078090530', '1078090532', '1078090533', '1078090535', '1078090536',
                                        '1078090543', '56') --UL006 GI PROCEDURES
			) LOGLIST

			LEFT OUTER JOIN CLARITY..OR_LOG AS ORL ON LOGLIST.LOG_ID = ORL.LOG_ID

			LEFT OUTER JOIN CLARITY..PAT_OR_ADM_LINK LNK ON LOGLIST.LOG_ID = LNK.OR_CASELOG_ID

			LEFT OUTER JOIN CLARITY..PAT_ENC AS ENC ON LOGLIST.OR_CSN_ID = ENC.PAT_ENC_CSN_ID 
			LEFT OUTER JOIN CLARITY..PAT_ENC_HSP AS ENC_HSP ON LOGLIST.OR_CSN_ID = ENC_HSP.PAT_ENC_CSN_ID 
			--LEFT OUTER JOIN CLARITY..CLARITY_LOC AS LOC ON ORL.LOC_ID = LOC.LOC_ID --UL002 REMOVED FOR OPTIMIZATION
			LEFT OUTER JOIN CLARITY..PATIENT AS PAT ON PAT.PAT_ID = ENC.PAT_ID
			--LEFT OUTER JOIN CLARITY..IDENTITY_ID AS ID ON ID.PAT_ID = PAT.PAT_ID --UL001 REMOVED PER TOM BURGAN FOR CH GO-LIVE
			
--			LEFT OUTER JOIN CLARITY..OR_LOG_CASE_TIMES AS ORT ON ORT.LOG_ID = orl.LOG_ID AND ORT.TRACKING_EVENT_C = 60                        ---BDD 12/12/2024
			LEFT OUTER JOIN CLARITY..OR_LOG_CASE_TIMES AS ORT ON ORT.LOG_ID = LOGLIST.LOG_ID AND ORT.TRACKING_EVENT_C = 60
			
			LEFT OUTER JOIN CLARITY..zc_sex zcs ON zcs.RCPT_MEM_SEX_C = pat.SEX_C
			
--			LEFT OUTER JOIN CLARITY..OR_LOC orc ON orc.LOC_ID = orl.LOC_ID
			LEFT OUTER JOIN CLARITY..OR_LOC orc ON orc.LOC_ID = LOGLIST.LOC_ID

			--LEFT OUTER JOIN CLARITY..clarity_dep dep ON dep.DEPARTMENT_ID = orc.OR_DEPARTMENT_ID
			LEFT OUTER JOIN CLARITY..clarity_dep dep ON dep.DEPARTMENT_ID = ENC_HSP.DEPARTMENT_ID
			
---			LEFT OUTER JOIN CLARITY..clarity_ser ser ON ser.PROV_ID = orl.PRIMARY_PHYS_ID                                 ---BDD 12/12/2024
			LEFT OUTER JOIN CLARITY..clarity_ser ser ON ser.PROV_ID = LOGLIST.PRIMARY_PHYS_ID

---			LEFT OUTER JOIN clarity_app.rptg.vwDim_Date dd ON dd.day_date = CAST(orl.SURGERY_DATE AS DATE)
			LEFT OUTER JOIN clarity_app.rptg.vwDim_Date dd ON dd.day_date = LOGLIST.Surg_Date

			LEFT OUTER JOIN mdm AS map ON map.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID AND map.seq = 1
			LEFT OUTER JOIN 
				(
				SELECT 
					HAR.HSP_ACCOUNT_ID,
					HAR.PRIM_ENC_CSN_ID,
					har.acct_class_ha_c,
          har.ACCT_BASECLS_HA_C --UL005 ADDED PATIENT CLASS
				FROM
					CLARITY..HSP_ACCOUNT HAR
					INNER JOIN CLARITY..HSP_ACCT_SBO SBO ON HAR.HSP_ACCOUNT_ID = SBO.HSP_ACCOUNT_ID 
					LEFT JOIN CLARITY..HSP_ACCT_TYPE TYP ON HAR.HSP_ACCOUNT_ID = TYP.HSP_ACCOUNT_ID 
				WHERE
					SBO.SBO_HAR_TYPE_C = '0'
					AND (TYP.HAR_TYPE_C <> '5' OR TYP.HAR_TYPE_C IS NULL)
				) AS HSP ON ENC.HSP_ACCOUNT_ID = HSP.HSP_ACCOUNT_ID
      LEFT JOIN CLARITY..ZC_ACCT_BASECLS_HA BCLS ON HSP.ACCT_BASECLS_HA_C = BCLS.ACCT_BASECLS_HA_C --UL005 ADDED PATIENT CLASS
			LEFT OUTER JOIN MDM_DEP ON MDM_DEP.EPIC_DEPARTMENT_ID = dep.DEPARTMENT_ID 
		  LEFT OUTER JOIN clarity_app.Rptg.vwDim_Clrt_SERsrc ON vwDim_Clrt_SERsrc.PROV_ID = ser.PROV_ID 
		  LEFT OUTER JOIN
						(SELECT 
						sk_Dim_Physcn,
						Service_Line_ID,
						Service_Line,
						hs_area_id,
						hs_area_name
				    FROM clarity_app.Rptg.vwRef_Physcn_Combined) AS physsvc ON physsvc.sk_Dim_Physcn = vwDim_Clrt_SERsrc.sk_Dim_Physcn
		LEFT OUTER JOIN
					(SELECT
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
					FROM clarity_app.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv) AS dvsn ON (CAST(dvsn.Epic_Financial_Division_Code AS INT) = TRY_CAST(vwDim_Clrt_SERsrc.Financial_Division AS INT)
																				AND CAST(dvsn.Epic_Financial_Subdivision_Code AS INT) = TRY_CAST(vwDim_Clrt_SERsrc.Financial_SubDivision AS INT))
	
		WHERE
			(hsp.acct_class_ha_c <> '123' OR hsp.acct_class_ha_c IS NULL)
			AND
			DEP.SERV_AREA_ID = 10 -- 20211014
		--AND id.IDENTITY_TYPE_ID = 14  --UL001 REMOVED PER TOM BURGAN FOR CH GO-LIVE

	) AS A
	--ORDER BY
	--	A.ACCTNBR_INT, A.UNIQUE_ID, A.TRACKING_TIME_IN, A.Seq

WHERE
	A.Seq = 1 
	AND 
	A.IN_ROOM IS NOT NULL

--ORDER BY 1
	ORDER BY
		A.ACCTNBR_INT, A.UNIQUE_ID, A.TRACKING_TIME_IN, A.Seq

GO


