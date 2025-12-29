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
--SET @EndDate = '6/30/2019 00:00:00'

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;

DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
		
SET @locstartdate = @startdate
SET @locenddate   = @enddate

-------------------------------------------------------------------------------

SELECT DISTINCT

 --flags	
		CAST('Hospice_of_the_Piedmont_Placement' AS VARCHAR(50))				'event_type'
	   ,CAST(dmdt.day_date AS DATETIME)						'event_date'
	   ,CASE WHEN hspc.[Final_Provider_Status]='accept'
						AND [Final_Hospital_Status]='booked' THEN '1'
			 ELSE '0'
		END													'event_count'
			
--patient info
       ,hspc.PAT_ENC_CSN_ID
	   ,patient.PAT_ID
	   ,patient.Name										'person_name'
       ,MRN													'person_id'			--MRN
       ,CAST(hspc.Patient_DOB AS DATETIME)					'person_birth_date'
	   ,patient.Sex											'person_gender'
	   ,patient.sk_Dim_Pt									'sk_Dim_Pt'
	   ,CAST(CASE WHEN FLOOR((CAST (COALESCE(hspc.Referral_Dttm,
									   day_date) AS INTEGER)
					   - CAST(CAST(patient.BirthDate AS DATETIME) AS INTEGER))
					   / 365.25) < 18 THEN 1
			      ELSE 0
		    END AS SMALLINT)								'peds'
	   ,CAST(CASE WHEN tx.pat_enc_csn_id IS NOT NULL THEN 1
				  ELSE 0
			 END AS SMALLINT)                               'transplant'         

--dates/times
	   ,CAST(LEFT(DATENAME(MM, dmdt.day_date), 3) +
	    ' ' + CAST(DAY(dmdt.day_date) AS VARCHAR(2))
		AS VARCHAR(10))										'report_period'
	   ,CAST(CAST(dmdt.day_date AS DATE) AS SMALLDATETIME)	'report_date'
	   ,dmdt.fmonth_num
	   ,dmdt.fmonth_name
	   ,dmdt.fyear_num
	   ,dmdt.fyear_name
	   ,CAST(NULL AS VARCHAR(150))							'event_category'

--Provider/scheduler info
       ,ser.PROV_ID											'provider_id'
       ,ser.Prov_Nme										'provider_name'
       ,physcn.Service_Line									'prov_service_line'  --service line

 --fac-org info
	   ,dep.DEPARTMENT_ID									'epic_department_id'
       ,dep.Clrt_DEPt_Nme									'epic_department_name'
       ,locsvc.epic_department_name_external				'epic_department_name_external'
    --   ,CAST(NULL AS INT)									'pod_id'
	   --,CAST(NULL AS VARCHAR(100))							'pod_name'                -- pod
    --   ,CAST(NULL AS INT)									'hub_id' -- hub
	   --,CAST(NULL AS VARCHAR(100))							'hub_name'
    --   ,CAST(NULL AS VARCHAR(150))							'practice_group_name' 
    --   ,CAST(NULL AS INT)									'practice_group_id' 
       ,locsvc.service_line_id								'service_line_id' 
       ,locsvc.service_line									'service_line'
      -- ,CAST(CASE WHEN FLOOR((CAST (COALESCE(hspc.Referral_Dttm,
						--				     day_date) AS INTEGER)
						--      - CAST(CAST(patient.BirthDate AS DATETIME) AS INTEGER))
						--     / 365.25) < 18 THEN 1
			   --   ELSE NULL
		    --END AS INT)										'sub_service_line_id'
      -- ,CAST(CASE WHEN FLOOR((CAST (COALESCE(hspc.Referral_Dttm,
						--				     day_date) AS INTEGER)
						--      - CAST(CAST(patient.BirthDate AS DATETIME) AS INTEGER))
						--     / 365.25) < 18 THEN 'Children'
			   --   ELSE NULL
		    --END AS VARCHAR(150))							'sub_service_line'
	   ,locsvc.opnl_service_id								'opnl_service_id'
	   ,locsvc.opnl_service_name							'opnl_service_name'
	   ,locsvc.corp_service_line_id							'corp_service_line_id'
	   ,locsvc.corp_service_line							'corp_service_line_name'
	   ,locsvc.hs_area_id									'hs_area_id'
	   ,locsvc.hs_area_name									'hs_area_name'

	   ,mdmloc.LOC_ID										'rev_location_id'
	   ,mdmloc.REV_LOC_NAME									'rev_location'
	
	   ,physcn.Clrt_Financial_Division						'financial_division_id'
	   ,physcn.Clrt_Financial_Division_Name					'financial_division_name'
	   ,physcn.Clrt_Financial_SubDivision					'financial_sub_division_id'
	   ,physcn.Clrt_Financial_SubDivision_Name				'financial_sub_division_name'
	   ,physcn.SOM_Group_ID									'som_group_id'
	   ,physcn.SOM_group									'som_group_name'
	   ,physcn.SOM_department_id							'som_department_id'
	   ,physcn.SOM_department								'som_department_name'
	   ,physcn.SOM_division_5								'som_division_id'
	   ,physcn.SOM_division_name							'som_division_name'
	
	   ,physcn.som_hs_area_id								'som_hs_area_id'
	   ,physcn.som_hs_area_name								'som_hs_area_name'
	   
 --placement info
	   ,hspc.AcctNbr_int
	   ,hspc.Patient_Class
	   ,hspc.Admitting_Physician
	   ,hspc.Discharge_Disposition
	   ,hspc.Placement_Type
	   ,hspc.Level_Of_Care
	   ,hspc.Admit_Type
	   ,hspc.Admission_Date
	   ,hspc.Est_Discharge_Date
	   ,hspc.Discharge_Date
	   ,hspc.Delay_Reason
	   ,hspc.LOS
	   ,hspc.Provider_Name									'Placement_Provider_Name'
	   ,hspc.Referral_Dttm
	   ,hspc.Response_Dttm
	   ,hspc.Accept_Dttm
	   ,hspc.Booked_Dttm
	   ,hspc.Final_Provider_Status
	   ,hspc.Final_Hospital_Status
	   ,hspc.Referral_Made_Flag
	   ,hspc.Booking_Made_Flag
	   ,hspc.Standard_Decline_Reason
	   ,hspc.Unit
	   ,hspc.Hospital_Service
	   ,hspc.CMS_Number
	   ,CASE WHEN Final_Hospital_Status = 'booked' THEN CAST(ROUND(CAST(DATEDIFF(HOUR, Referral_Dttm, Booked_Dttm) AS NUMERIC(7,2))/24.0,1) AS NUMERIC(4,1)) ELSE NULL END AS ReferralToBookedDays

FROM DS_HSDW_Prod.dbo.Dim_Date dmdt
		LEFT OUTER JOIN  (SELECT	   hscdc.[PAT_ENC_CSN_ID]
		                              ,hscdc.[AcctNbr_int]
									  ,[MRN]
									  ,[Patient_Last_Name]
									  ,[Patient_First_Name]
									  ,[Patient_DOB]
									  ,[Patient_Class]
									  ,[Admitting_Physician]
									  ,[Discharge_Disposition]
									  ,[Placement_Type]
									  ,[Level_of_Care]
									  ,[Admit_Type]
									  ,[Admission_Date]
									  ,[Est_Discharge_Date]
									  ,[Discharge_Date]
									  ,[Delay_Reason]
									  ,[LOS]
									  ,[Provider_Name]
									  ,[Referral_Dttm]
									  ,[Response_Dttm]
									  ,[Accept_Dttm]
									  ,[Booked_Dttm]
									  ,[Final_Provider_Status]
									  ,[Final_Hospital_Status]
									  ,[Referral_Made_Flag]
									  ,[Booking_Made_Flag]
									  ,[Standard_Decline_Reason]
									  ,[Unit]
									  ,[Hospital_Service]
									  ,[CMS_Number]
									  ,patient.sk_Dim_Pt
									  ,peh.sk_Fact_Pt_Enc_Clrt
									  ,peh.sk_Dim_Clrt_DEPt
									  ,atn.sk_Dim_Clrt_SERsrc
									 
					      FROM [DS_HSDM_App].[Rptg].[vwCuraspan_by_discharge] hscdc
					      LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient patient ON patient.MRN_int = hscdc.MRN
					      LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt AS peh ON hscdc.PAT_ENC_CSN_ID=peh.PAT_ENC_CSN_ID
						  LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Atn_Prov_All] atn ON (atn.sk_Fact_Pt_Enc_Clrt = peh.sk_Fact_Pt_Enc_Clrt) AND atn.inv_seq = 1
					      WHERE 1=1
					      AND CAST([Referral_Dttm] AS SMALLDATETIME) >= @locstartdate
					      AND CAST([Referral_Dttm] AS SMALLDATETIME) <  @locenddate
						  AND (hscdc.Provider_Name = 'Hospice of the Piedmont')
						  UNION ALL
						  SELECT	   peh.[PAT_ENC_CSN_ID] AS PAT_ENC_CSN_ID
									  ,cbd.[Patient Account Number] AS AcctNbr_int
									  ,CAST(cbd.[Patient MRN] AS INT) AS MRN
									  ,cbd.[Patient Last Name] AS Patient_Last_Name
									  ,cbd.[Patient First Name] AS Patient_First_Name
									  ,cbd.[Patient Date of Birth] AS Patient_DOB
									  ,cbd.[Patient Class] AS Patient_Class
									  ,cbd.[Attending Physician] AS Admitting_Physician
									  ,cbd.[Discharge Disposition] AS Discharge_Disposition
									  ,cbd.[Placement Type] AS Placement_Type
									  ,cbd.[Level of Care] AS Level_Of_Care
									  ,cbd.[Admit Type] AS Admit_Type
									  ,cbd.[Admission Date] AS Admission_Date
									  ,cbd.[Estimated Discharge Date] AS Est_Discharge_Date
									  ,cbd.[Discharged Date] AS Discharge_Date
									  ,CASE WHEN cbd.[Delay Reason] <> '' THEN cbd.[Delay Reason] ELSE NULL END AS Delay_Reason
									  ,cbd.[Length of Stay] AS LOS
									  ,CASE WHEN cbd.[Provider Name]  <> '' THEN cbd.[Provider Name] ELSE NULL END AS Provider_Name
									  ,CASE WHEN cbd.[Referral Date] <> '' THEN  CAST(cbd.[Referral Date] AS DATETIME) + CAST(cbd.[Referral Time(EST)] AS DATETIME)  ELSE NULL END AS Referral_Dttm
									  ,CASE WHEN cbd.[Response Date] <> '' THEN  CAST(cbd.[Response Date] AS DATETIME) + CAST(cbd.[Response Time(EST)] AS DATETIME)  ELSE NULL END AS Response_Dttm
									  ,CASE WHEN cbd.[Accept Date] <> '' THEN  CAST(cbd.[Accept Date] AS DATETIME) + CAST(cbd.[Accept Time(EST)] AS DATETIME)  ELSE NULL END AS Accept_Dttm
									  ,CASE WHEN cbd.[Booked Date] <> '' THEN  CAST(cbd.[Booked Date] AS DATETIME) + CAST(cbd.[Booked Time(EST)] AS DATETIME)  ELSE NULL END AS Booked_Dttm
									  ,CASE WHEN cbd.[Final Provider Status] <> '' THEN cbd.[Final Provider Status] ELSE NULL END AS Final_Provider_Status
									  ,CASE WHEN cbd.[Final Hospital Status] <> '' THEN cbd.[Final Hospital Status] ELSE NULL END AS Final_Hospital_Status
									  ,cbd.[Referral Made Flag?] AS Referral_Made_Flag
									  ,cbd.[Booking Made Flag?] AS Booking_Made_Flag
									  ,CASE WHEN cbd.[Standard Decline Reason] <> '' THEN cbd.[Standard Decline Reason] ELSE NULL END AS Standard_Decline_Reason
									  ,cbd.Unit
									  ,cbd.[Hospital Service] AS Hospital_Service
									  ,CASE WHEN cbd.[CMS Number] <> '' THEN cbd.[CMS Number] ELSE NULL END AS CMS_Number
									  ,patient.sk_Dim_Pt
									  ,peh.sk_Fact_Pt_Enc_Clrt
									  ,peh.sk_Dim_Clrt_DEPt
									  ,atn.sk_Dim_Clrt_SERsrc

                          FROM   CMS.Curaspan_by_discharge_History AS cbd
                          LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt AS peh ON cbd.[Patient Account Number]=peh.AcctNbr_int
					      LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient patient ON patient.MRN_int = CAST(cbd.[Patient MRN] AS INT)
						  LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Atn_Prov_All] atn ON (atn.sk_Fact_Pt_Enc_Clrt = peh.sk_Fact_Pt_Enc_Clrt) AND atn.inv_seq = 1
						  WHERE 1=1
						  AND LEN(cbd.[Patient Account Number]) < 12
						  AND CAST(CASE WHEN cbd.[Referral Date] <> '' THEN  CAST(cbd.[Referral Date] AS DATETIME) + CAST(cbd.[Referral Time(EST)] AS DATETIME)  ELSE NULL END AS SMALLDATETIME) >= @locstartdate
						  AND CAST(CASE WHEN cbd.[Referral Date] <> '' THEN  CAST(cbd.[Referral Date] AS DATETIME) + CAST(cbd.[Referral Time(EST)] AS DATETIME)  ELSE NULL END AS SMALLDATETIME) <  @locenddate
						  AND (CASE WHEN cbd.[Provider Name]  <> '' THEN cbd.[Provider Name] ELSE NULL END = 'Hospice of the Piedmont')
						  UNION ALL
						  SELECT	   cbd.[Patient Account Number] AS PAT_ENC_CSN_ID
									  ,peh.AcctNbr_int
									  ,CAST(cbd.[Patient MRN] AS INT) AS MRN
									  ,cbd.[Patient Last Name] AS Patient_Last_Name
									  ,cbd.[Patient First Name] AS Patient_First_Name
									  ,cbd.[Patient Date of Birth] AS Patient_DOB
									  ,cbd.[Patient Class] AS Patient_Class
									  ,cbd.[Attending Physician] AS Admitting_Physician
									  ,cbd.[Discharge Disposition] AS Discharge_Disposition
									  ,cbd.[Placement Type] AS Placement_Type
									  ,cbd.[Level of Care] AS Level_Of_Care
									  ,cbd.[Admit Type] AS Admit_Type
									  ,cbd.[Admission Date] AS Admission_Date
									  ,cbd.[Estimated Discharge Date] AS Est_Discharge_Date
									  ,cbd.[Discharged Date] AS Discharge_Date
									  ,CASE WHEN cbd.[Delay Reason] <> '' THEN cbd.[Delay Reason] ELSE NULL END AS Delay_Reason
									  ,cbd.[Length of Stay] AS LOS
									  ,CASE WHEN cbd.[Provider Name]  <> '' THEN cbd.[Provider Name] ELSE NULL END AS Provider_Name
									  ,CASE WHEN cbd.[Referral Date] <> '' THEN  CAST(cbd.[Referral Date] AS DATETIME) + CAST(cbd.[Referral Time(EST)] AS DATETIME)  ELSE NULL END AS Referral_Dttm
									  ,CASE WHEN cbd.[Response Date] <> '' THEN  CAST(cbd.[Response Date] AS DATETIME) + CAST(cbd.[Response Time(EST)] AS DATETIME)  ELSE NULL END AS Response_Dttm
									  ,CASE WHEN cbd.[Accept Date] <> '' THEN  CAST(cbd.[Accept Date] AS DATETIME) + CAST(cbd.[Accept Time(EST)] AS DATETIME)  ELSE NULL END AS Accept_Dttm
									  ,CASE WHEN cbd.[Booked Date] <> '' THEN  CAST(cbd.[Booked Date] AS DATETIME) + CAST(cbd.[Booked Time(EST)] AS DATETIME)  ELSE NULL END AS Booked_Dttm
									  ,CASE WHEN cbd.[Final Provider Status] <> '' THEN cbd.[Final Provider Status] ELSE NULL END AS Final_Provider_Status
									  ,CASE WHEN cbd.[Final Hospital Status] <> '' THEN cbd.[Final Hospital Status] ELSE NULL END AS Final_Hospital_Status
									  ,cbd.[Referral Made Flag?] AS Referral_Made_Flag
									  ,cbd.[Booking Made Flag?] AS Booking_Made_Flag
									  ,CASE WHEN cbd.[Standard Decline Reason] <> '' THEN cbd.[Standard Decline Reason] ELSE NULL END AS Standard_Decline_Reason
									  ,cbd.Unit
									  ,cbd.[Hospital Service] AS Hospital_Service
									  ,CASE WHEN cbd.[CMS Number] <> '' THEN cbd.[CMS Number] ELSE NULL END AS CMS_Number
									  ,patient.sk_Dim_Pt
									  ,peh.sk_Fact_Pt_Enc_Clrt
									  ,peh.sk_Dim_Clrt_DEPt
									  ,atn.sk_Dim_Clrt_SERsrc

                          FROM   CMS.Curaspan_by_discharge_History AS cbd
                          LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Hsp_Clrt AS peh ON cbd.[Patient Account Number]=peh.PAT_ENC_CSN_ID
					      LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient patient ON patient.MRN_int = CAST(cbd.[Patient MRN] AS INT)
						  LEFT OUTER JOIN [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Atn_Prov_All] atn ON (atn.sk_Fact_Pt_Enc_Clrt = peh.sk_Fact_Pt_Enc_Clrt) AND atn.inv_seq = 1
						  WHERE 1=1
						  AND LEN(cbd.[Patient Account Number]) >= 12
						  AND CAST(CASE WHEN cbd.[Referral Date] <> '' THEN  CAST(cbd.[Referral Date] AS DATETIME) + CAST(cbd.[Referral Time(EST)] AS DATETIME)  ELSE NULL END AS SMALLDATETIME) >= @locstartdate
						  AND CAST(CASE WHEN cbd.[Referral Date] <> '' THEN  CAST(cbd.[Referral Date] AS DATETIME) + CAST(cbd.[Referral Time(EST)] AS DATETIME)  ELSE NULL END AS SMALLDATETIME) <  @locenddate
						  AND (CASE WHEN cbd.[Provider Name]  <> '' THEN cbd.[Provider Name] ELSE NULL END = 'Hospice of the Piedmont')
						 ) hspc													    ON CAST(dmdt.day_date AS DATE)=CAST(hspc.[Referral_Dttm] AS DATE)				
		LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Patient patient						ON patient.sk_Dim_Pt = hspc.sk_Dim_Pt					--patient name (combined) and sex
		LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwDim_Clrt_SERsrc ser						ON ser.sk_Dim_Clrt_SERsrc = hspc.sk_Dim_Clrt_SERsrc
		LEFT OUTER JOIN Ds_HSDW_Prod.Rptg.vwDim_Clrt_DEPt dep						ON dep.sk_Dim_Clrt_DEPt = hspc.sk_Dim_Clrt_DEPt
        LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_EpicSvc locsvc	ON locsvc.epic_department_id = dep.DEPARTMENT_ID 

	-- Identify transplant encounter--

        LEFT OUTER JOIN (SELECT DISTINCT
							btd.pat_enc_csn_id
					       ,btd.Event_Transplanted AS 'transplant_surgery_dt'
					       ,btd.hosp_admsn_time AS 'Adm_Dtm'
					       ,enc.sk_Fact_Pt_Enc_Clrt
					       ,enc.sk_Fact_Pt_Acct
					       ,enc.sk_Dim_Clrt_Pt
					       ,enc.sk_Dim_Pt
						   ,enc.AcctNbr_int
					     FROM DS_HSDM_Prod.Rptg.Big6_Transplant_Datamart btd
					     INNER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt enc ON btd.pat_enc_csn_id = enc.PAT_ENC_CSN_ID
                         WHERE (
                                btd.TX_Episode_Phase = 'transplanted'
                                AND    btd.TX_Stat_Dt >= @locstartdate
                                AND    btd.TX_Stat_Dt < @locenddate
                                )
                                AND btd.TX_GroupedPhaseStatus = 'TX-ADMIT'
                        ) AS tx														ON hspc.AcctNbr_int = tx.AcctNbr_int

                LEFT OUTER JOIN
                (
                    SELECT DISTINCT
                        EPIC_DEPARTMENT_ID,
                        SERVICE_LINE,
                        PFA_POD,
                        HUB,
						BUSINESS_UNIT,
						LOC_ID,
						REV_LOC_NAME
                    FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master
                ) AS mdmloc															ON dep.DEPARTMENT_ID = mdmloc.EPIC_DEPARTMENT_ID

				LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwRef_Physcn_Combined physcn		ON physcn.sk_Dim_Physcn = ser.sk_Dim_Physcn
WHERE 1=1
AND dmdt.day_date >= @locstartdate
AND dmdt.day_date <  @locenddate
AND CASE WHEN hspc.[Final_Provider_Status]='accept'
						AND [Final_Hospital_Status]='booked' THEN '1'
			 ELSE '0'
		END = '1'
		
ORDER BY  event_date

GO


