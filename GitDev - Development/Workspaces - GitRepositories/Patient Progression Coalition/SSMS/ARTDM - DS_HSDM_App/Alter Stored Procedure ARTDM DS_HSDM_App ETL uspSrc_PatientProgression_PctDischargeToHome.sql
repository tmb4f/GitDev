USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [ETL].[uspSrc_PatientProgression_PctDischargeToHome]
AS

/*********************************************************************************************************************************************
WHAT: Create procedure 
WHO : Zachary Daniels
WHEN: 1/10/2024
WHY : Patient Progression % Discharge to Home
MODS: 
	03/11/2024 -- Tom B Update logic to select latest MDM information from history table
	05/02/2024 -- Tom B Add Provider Team to extract

---monthly metric
************************************************************************************************************************************************/

SET NOCOUNT ON;
DECLARE @startdate SMALLDATETIME = NULL
       ,@enddate   SMALLDATETIME = NULL;
--SET @startdate = '07/1/2023'
--SET @enddate = GETDATE();

--get default Balanced Scorecard date range
IF @startdate IS NULL
   AND @enddate IS NULL
    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT
                                                 ,@enddate OUTPUT;

IF @startdate < '07/01/2023'
   SET @startdate = '07/01/2023'

SELECT DISTINCT
       'Discharge_Disposition'             AS event_type
      ,CAST(CASE
                WHEN main.Dsch_Dtm IS NOT NULL
                THEN 1
                ELSE 0
            END AS INT)                  AS event_count
      ,CAST(D.day_date AS SMALLDATETIME) AS event_date
      ,CAST(NULL AS INT)                 AS event_id
      ,CAST(NULL AS VARCHAR(50))         AS event_category
      ,main.PAT_ENC_CSN_ID
      ,main.AcctNbr_int                  AS HAR
	  ,main.sk_Dim_Clrt_Disch_Disp       AS Disch_Disp
	  ,main.Clrt_Disch_Disp_Ttle		 AS Disch_Disp_name
	  ,main.DISCH_DISP_C
	  ,CAST(CASE
			WHEN main.DISCH_DISP_C IN ( '1', '6', '81', '86') 
			THEN 1
			ELSE 0
		END AS INT)						Discharge_to_home_flag
	  ,main.ADMIT_SOURCE_C
      ,main.Adm_Dtm
      ,main.Dsch_Dtm
      ,main.adt_pt_cls
      ,main.BirthDate                    AS person_birth_date
      ,main.Sex                          AS person_gender
      ,main.sk_Dim_Pt                    AS person_id
      ,main.PAT_NAME                     AS person_name
      ,main.AGE
      ,main.Clrt_DEPt_Nme                AS Department
      ,main.Clrt_Hsptl_Svc_Descr         AS Service
      ,main.Hospital_Name
      ,main.epic_department_id
      ,main.epic_department_name
      ,main.epic_department_name_external
      ,main.PROV_ID                      AS provider_id
      ,main.Prov_Nme                     AS provider_name
      ,main.Prov_Typ                     AS prov_type
      ,CAST(CASE
                WHEN main.Prov_Typ IN ( 'NURSE PRACTITIONER', 'PHYSICIAN ASSISTANT', 'NURSE ANESTHETIST', 'CLINICAL NURSE SPECIALIST', 'GENETIC COUNSELOR'
                                       ,'AUDIOLOGIST'
                                      )
                THEN 1
                ELSE 0
            END AS INT)                  app_flag
      ,TRY_CAST(main.Financial_Division AS INT)         AS financial_division_id
      ,main.Financial_Division_Name
      ,TRY_CAST(main.Financial_SubDivision AS INT)        AS financial_sub_division_id
      ,main.Financial_SubDivision_Name   AS financial_sub_division_name
      ,main.som_hs_area_id
      ,main.som_hs_area_name
      ,main.som_group_id
      ,main.som_group_name
      ,main.Department_ID                AS som_department_id
      ,main.Department                   AS som_department_name
      ,main.Org_Number                   AS som_division_id
      ,main.Organization                 AS som_division_name
      ,D.fmonth_num                      AS fmonth_num
      ,D.Fyear_num                       AS fyear_num
      ,D.FYear_name                      AS fyear_name
      ,main.hs_area_id
      ,main.hs_area_name
      ,CAST(CASE
                WHEN main.AGE < 18
                THEN 1
                ELSE 0
            END AS SMALLINT)             AS peds_flag
	  ,main.Provider_Team -- VARCHAR(254)

FROM DS_HSDW_Prod.Rptg.vwDim_Date D
    LEFT OUTER JOIN
    ( --main
        SELECT A.sk_Disch_Dte
              ,A.PAT_ENC_CSN_ID
              ,E.AcctNbr_int
			  ,E.sk_Dim_Clrt_Disch_Disp
			  ,DD.Clrt_Disch_Disp_Ttle
			  ,DD.DISCH_DISP_C
			  ,AC.ADMIT_SOURCE_C
              ,A.Adm_Dtm
              ,A.Dsch_Dtm
              ,E.adt_pt_cls
              ,B.IS_VALID_PAT_YN
              ,B.BirthDate
              ,B.Sex
              ,H.sk_Dim_Pt
              ,H.PAT_NAME
              ,CASE
                   WHEN DATEADD(YEAR, DATEDIFF(YEAR, B.BirthDate, A.Dsch_Dtm), B.BirthDate) > A.Dsch_Dtm
                   THEN DATEDIFF(YEAR, B.BirthDate, A.Dsch_Dtm) - 1
                   ELSE DATEDIFF(YEAR, B.BirthDate, A.Dsch_Dtm)
               END                                                                        AS AGE
              ,F.Clrt_DEPt_Nme
              ,S.Clrt_Hsptl_Svc_Descr
              ,F.Hospital_Name
              ,mdmhst.epic_department_id
              ,mdmhst.epic_department_name
              ,mdmhst.epic_department_name_external
              ,I.PROV_ID
              ,I.Prov_Nme
              ,I.Prov_Typ
              ,I.Financial_Division
              ,I.Financial_Division_Name
              ,I.Financial_SubDivision
              ,I.Financial_SubDivision_Name
              ,SOM.som_hs_area_id
              ,SOM.som_hs_area_name
              ,SOM.som_group_id
              ,SOM.som_group_name
              ,SOM.Department_ID
              ,SOM.Department
              ,SOM.Org_Number
              ,SOM.Organization
              ,mdmhst.hs_area_id
              ,mdmhst.hs_area_name
			  ,prov_team_dc.RECORD_NAME AS Provider_Team
             
        FROM  DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt          A 
			LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_HospitalAccount      AS HAR 
                ON HAR.HSP_ACCOUNT_ID = A.AcctNbr_int
            LEFT JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt            E
                ON A.PAT_ENC_CSN_ID = E.PAT_ENC_CSN_ID
            LEFT JOIN DS_HSDW_Prod.Rptg.vwDim_Patient                     B
                ON E.sk_Dim_pt = B.sk_Dim_Pt
            LEFT JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_DEPt                   F
                ON E.sk_Dim_Clrt_DEPt = F.sk_Dim_Clrt_DEPt
            LEFT JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_Hsptl_Svc              S
                ON E.sk_Dim_Clrt_Hsptl_Svc = S.sk_Dim_Clrt_Hsptl_Svc
			LEFT OUTER JOIN
				(
					SELECT
						G.MDM_BATCH_ID,
						G.EPIC_DEPARTMENT_ID,
						G.EPIC_DEPT_NAME AS epic_department_name,
						G.EPIC_EXT_NAME AS epic_department_name_external,
						G.HS_AREA_ID,
						G.HS_AREA_NAME
					FROM
					(
						SELECT
							MDM_BATCH_ID,
							EPIC_DEPARTMENT_ID,
							EPIC_DEPT_NAME,
							EPIC_EXT_NAME,
							HS_AREA_ID,
							HS_AREA_NAME,
							ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
						FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
					) G
					WHERE G.seq = 1
				) mdmhst
				ON mdmhst.EPIC_DEPARTMENT_ID = F.DEPARTMENT_ID
            LEFT JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_Pt                     H
                ON E.sk_Dim_pt = H.sk_Dim_Pt
            LEFT JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc                 I
                ON E.sk_Adm_SERsrc = I.sk_Dim_Clrt_SERsrc
            LEFT OUTER JOIN DS_HSDM_APP.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv   AS SOM
                ON I.Financial_SubDivision = SOM.Epic_Financial_Subdivision_Code
			LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_Disch_Disp           DD
				ON E.sk_Dim_Clrt_Disch_Disp = DD.sk_Dim_Clrt_Disch_Disp
			LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_Admt_Chrcstc         AC
				ON E.sk_Dim_Clrt_Admt_Chrcstc = AC.sk_Dim_Clrt_Admt_Chrcstc
			OUTER APPLY
			( --prov_team_dc--prov team at discharge
				SELECT ppt_dc.PAT_ENC_CSN_ID
					  ,ppt_dc.LINE
					  ,ppt_dc.ID
					  ,ppt_dc.RECORD_NAME
					  ,ppt_dc.TEAM_AUDIT_ID
					  ,ppt_dc.TEAM_ACTION_C
					  ,ppt_dc.PRIMARYTEAM_AUDI_YN
					  ,ppt_dc.TEAMAUDIT_USER_ID
					  ,ppt_dc.TEAM_AUDIT_INSTANT
					  ,ppt_dc.NEXT_AUDIT_INSTANT
				FROM
				( --ppt_dc
					SELECT prteam.PAT_ENC_CSN_ID
						  --,prteam.LINE
						  ,prteam.Team_LINE AS LINE
						  ,team.ID
						  ,team.DISPLAY_NAME AS RECORD_NAME
						  ,prteam.ID AS TEAM_AUDIT_ID
						  ,prteam.Team_Action AS TEAM_ACTION_C
						  ,prteam.PRIMARYTEAM_AUDI_YN
						  ,prteam.sk_Dim_Clrt_EMPlye AS TEAMAUDIT_USER_ID
						  ,prteam.TEAM_AUDIT_INSTANT
						  ,LEAD(prteam.TEAM_AUDIT_INSTANT) OVER (PARTITION BY prteam.PAT_ENC_CSN_ID
																 ORDER BY prteam.TEAM_AUDIT_INSTANT
																) AS NEXT_AUDIT_INSTANT
					FROM DS_HSDW_Prod.Rptg.vwFact_Clrt_EPT_TEAM_AUDIT              AS prteam
						INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_EMPlye       AS prov_emp
							ON prteam.sk_Dim_Clrt_EMPlye = prov_emp.sk_Dim_Clrt_EMPlye
						INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc       AS prov_ser
							ON prov_emp.EMPlye_PROV_ID = prov_ser.PROV_ID
						INNER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_Prov_Team AS team
							ON prteam.sk_Dim_Clrt_Prov_Team = team.sk_Dim_Clrt_Prov_Team
					WHERE prteam.PRIMARYTEAM_AUDI_YN = 'Y' --primary prov team
						  AND prteam.PAT_ENC_CSN_ID = A.PAT_ENC_CSN_ID
				) AS ppt_dc
				WHERE A.Dsch_Dtm
				BETWEEN TEAM_AUDIT_INSTANT AND COALESCE(NEXT_AUDIT_INSTANT, GETDATE())
			) AS prov_team_dc
        WHERE A.Dsch_Dtm >= @startdate
              AND A.Dsch_Dtm < @enddate
             
              AND E.adt_pt_cls = 'Inpatient'
              AND B.IS_VALID_PAT_YN = 'Y'
			  AND mdmhst.MDM_BATCH_ID >= 2324
			  AND DD.DISCH_DISP_C NOT IN ('7','20','21','30','40','41','42','50','62','87')
			  AND AC.ADMIT_SOURCE_C NOT IN ('5','8')
			  AND mdmhst.HS_AREA_ID <> '0'
    )                             AS main
        ON main.sk_Disch_Dte = D.date_key
WHERE D.day_date >= @startdate
      AND D.day_date < @enddate
	  
ORDER BY CAST(D.day_date AS SMALLDATETIME);

GO


