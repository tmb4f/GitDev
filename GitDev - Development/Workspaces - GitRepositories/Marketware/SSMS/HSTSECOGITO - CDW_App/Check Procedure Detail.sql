USE [CDW_App]
--USE [CDW]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET NOCOUNT ON;

---parameters

--DECLARE @StartDate AS SMALLDATETIME = NULL
--       ,@EndDate AS SMALLDATETIME = NULL;

DECLARE @StartDate AS BIGINT = NULL
       ,@EndDate AS BIGINT = NULL;

--For Testing
    --SET @startDate = 20250701;
    --SET @endDate = 20251231;
    SET @startDate = 20250101;
    SET @endDate = 20251231;

--IF @StartDate IS NULL
--   AND @EndDate IS NULL
--BEGIN

--    EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @StartDate OUTPUT, @EndDate OUTPUT;

--END;

--DECLARE @locstartdate SMALLDATETIME,
--        @locenddate SMALLDATETIME

DECLARE @locstartdate BIGINT,
        @locenddate BIGINT

SET @locstartdate = @startdate
SET @locenddate   = @enddate

	IF OBJECT_ID('tempdb..#pre_mdm ') IS NOT NULL
		DROP TABLE #pre_mdm
	IF OBJECT_ID('tempdb..#ref ') IS NOT NULL
		DROP TABLE #ref
	IF OBJECT_ID('tempdb..#refpts ') IS NOT NULL
		DROP TABLE #refpts
	IF OBJECT_ID('tempdb..#sample ') IS NOT NULL
		DROP TABLE #sample
	IF OBJECT_ID('tempdb..#encs ') IS NOT NULL
		DROP TABLE #encs
	--IF OBJECT_ID('tempdb..#temp_procedures ') IS NOT NULL
	--	DROP TABLE #temp_procedures
	IF OBJECT_ID('tempdb..#billed_account ') IS NOT NULL
		DROP TABLE #billed_account
	IF OBJECT_ID('tempdb..#billed_amount ') IS NOT NULL
		DROP TABLE #billed_amount
	IF OBJECT_ID('tempdb..#billed_amount_2 ') IS NOT NULL
		DROP TABLE #billed_amount_2
	IF OBJECT_ID('tempdb..#billed_amount_total ') IS NOT NULL
		DROP TABLE #billed_amount_total
	IF OBJECT_ID('tempdb..#Rptg ') IS NOT NULL
		DROP TABLE #Rptg

-------------------------
--Getting Dept from EDW
-------------------------
--DROP TABLE IF EXISTS #pre_mdm;
--/*
SELECT DISTINCT
       rmlmh.EPIC_DEPARTMENT_ID
      ,hx.max_dt
      ,rmlmh.EPIC_DEPT_NAME
      ,rmlmh.EPIC_EXT_NAME
      ,rmlmh.EPIC_DEPT_TYPE
      ,rmlmh.HS_AREA_ID
      ,rmlmh.HS_AREA_NAME
      ,rmlmh.HOSPITAL_CODE
      ,rmlmh.EPIC_SPCLTY
INTO #pre_mdm
FROM Rptg.vwRef_MDM_Location_Master_History AS rmlmh
    --[CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_History] AS rmlmh
    INNER JOIN ( --hx--most recent batch date per dep id
                   SELECT mdmhx.EPIC_DEPARTMENT_ID
                         ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
                   FROM CDW_App.Rptg.vwRef_MDM_Location_Master_History AS mdmhx
                   --[CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_History] AS mdmhx

                   GROUP BY mdmhx.EPIC_DEPARTMENT_ID) AS hx
        ON hx.EPIC_DEPARTMENT_ID  = rmlmh.EPIC_DEPARTMENT_ID
           AND rmlmh.BATCH_RUN_DT = hx.max_dt
ORDER BY
	rmlmh.EPIC_DEPARTMENT_ID

 --Create index for temp table #pre_mdm
	CREATE CLUSTERED INDEX IX_mdm ON #pre_mdm (EPIC_DEPARTMENT_ID)
--*/
    --------------------------------------------------
	-- Temp Table:  ref
    --------------------------------------------------
	--;
	--WITH ref_cte AS
	--(
    SELECT ref.PatientDurableKey,
           --ref.CreationInstant,
           ref.CreationDateKey,
		   ref.rflseq,
		   ref.PAT_ID,
		   ref.person_id,
		   ref.sk_Dim_Pt

    INTO #ref

    FROM
    (
        SELECT DISTINCT
               -- Surrogate keys used later for joining to Dim tables.
               ref.PatientDurableKey,
               --ref.CreationInstant,			-- event_date
               ref.CreationDateKey,			-- event_date
		       CAST(pat.PatientEpicId AS VARCHAR(18)) AS [PAT_ID],
			   --TRY_CAST(pat.PrimaryMrn AS INT) AS [person_id],
			   pat.PrimaryMrn AS [person_id],
			   --ROW_NUMBER() OVER(PARTITION BY ref.PatientDurableKey ORDER BY ref.CreationInstant) AS rflseq--,
			   ROW_NUMBER() OVER(PARTITION BY ref.PatientDurableKey ORDER BY ref.CreationDateKey) AS rflseq,
			   dpat.sk_Dim_Pt

		FROM [CDW].[FullAccess].[ReferralFact] ref

        INNER JOIN [CDW].[FullAccess].[PatientDim] pat
            ON ref.PatientDurableKey = pat.DurableKey
                AND pat.IsCurrent = 1
                AND pat.IsValid = 1
                AND pat.IsHistoricalPatient = 0
                AND pat.Test = 0
                AND pat.DurableKey <> 15282261 /* ANONYMOUS HIM ONLY,REG IN ERROR TESTPATIENTS MRN 4233223 */

		LEFT OUTER JOIN
		(
		SELECT 
			MRN_display,
			sk_Dim_Pt
		FROM [CDW_App].dbo.Dim_Patient
		) dpat
			ON dpat.MRN_display = pat.PrimaryMrn 

        WHERE 1 = 1

              --AND map.HasErrors = 0

              --AND map.Count = 1

              AND ref.Count = 1

              --------------------------------------------------
              -- Referral criteria
              --------------------------------------------------

              --AND ref.CreationInstant >= @StartDate
              --AND ref.CreationInstant < @EndDate

              AND ref.CreationDateKey >= @StartDate
              AND ref.CreationDateKey < @EndDate

              --AND CAST(ref.CreationInstant AS SMALLDATETIME) >= @locstartdate
              --AND CAST(ref.CreationInstant AS SMALLDATETIME) < @locenddate

			  --AND ref.Class IN ( 'Internal', 'Incoming' )
			  AND ref.Class = 'Incoming'

			  AND (ref.NumberOfCompletedVisits_X IS NOT NULL AND ref.NumberOfCompletedVisits_X > 0)

			  AND ref.TYPE NOT IN ('Home Health Care', 'Home Health Pharmacy', 'Home Health Visits', 'Insurance Referral', 'Lab', 'NonTOC Order', 'PFA Appointment Request', 'Treatment Plan')

    ) ref
	--),

	ORDER BY
		ref.PatientDurableKey,
		ref.rflseq

 --Create index for temp table #ref
	CREATE CLUSTERED INDEX IX_ref ON #ref (PatientDurableKey, rflseq)

		--SELECT
		--	*
		--FROM #ref
		--ORDER BY
		--    referral_type,
		--    ReferralType,
		--	CreationInstant,
		--	PatientDurableKey

		--SELECT DISTINCT
		--	referral_type,
		--	ReferralType
		--FROM #ref
		--ORDER BY
		--    referral_type,
		--    ReferralType
--/*
 --   refpts_cte AS
	--(
	SELECT --DISTINCT
		PatientDurableKey,
		PAT_ID,
		person_id,
		--CreationInstant -- Referral entry date
		CreationDateKey, -- Referral entry date
		sk_Dim_Pt
	INTO #refpts
	FROM #ref
	--FROM ref_cte
	WHERE rflseq = 1 -- Earliest referral entry date for a patient
	--)
	ORDER BY
		PatientDurableKey,
		--CreationInstant
		CreationDateKey

	 --Create index for temp table #refpts

	--CREATE CLUSTERED INDEX IX_refpts ON #refpts (PatientDurableKey, CreationInstant)
	CREATE CLUSTERED INDEX IX_refpts ON #refpts (PatientDurableKey, CreationDateKey)

	--SELECT
	--	*
	----FROM refpts_cte
	--FROM #refpts
	--ORDER BY PatientDurableKey

	SELECT
		PatientDurableKey,
        PAT_ID,
        person_id,
        CreationDateKey,
		sk_Dim_Pt
	INTO #sample
	FROM #refpts
	--FROM #refpts TABLESAMPLE (50 PERCENT)
	--FROM #refpts TABLESAMPLE (20 PERCENT)
	--FROM #refpts TABLESAMPLE (10 PERCENT)
	--FROM #refpts TABLESAMPLE (5 PERCENT)
	ORDER BY
		PatientDurableKey,
		--CreationInstant
		CreationDateKey

	 --Create index for temp table #sample

	CREATE CLUSTERED INDEX IX_sample ON #sample (PatientDurableKey, CreationDateKey)

	SELECT
		enc.PatientDurableKey,
		refs.CreationDateKey,
		refs.person_id,
		refs.sk_Dim_Pt,
		enc.EncounterKey,
		enc.PatientClass,
		--enc.DateKey AS ContactDateKey,
		CASE WHEN enc.DateKey < 1 THEN NULL ELSE enc.DateKey END AS ContactDateKey,
		CASE WHEN enc.AdmissionDateKey < 1 THEN NULL ELSE enc.AdmissionDateKey END AS AdmissionDateKey,
		--enc.DischargeDateKey AS DischDateKey,
		CASE WHEN enc.DischargeDateKey < 1 THEN NULL ELSE enc.DischargeDateKey END AS DischargeDateKey,
		--COALESCE(DischargeDateKey, enc.DateKey) AS DischargeDateKey,
		enc.EncounterEpicCsn,
		enc.DepartmentKey,
		enc.ProviderKey,
		pd.PrimaryCareProviderKey,
		dept.DepartmentEpicId
	INTO #encs
	FROM CDW.FullAccess.EncounterFact AS enc
  --  INNER JOIN #refpts refs ON enc.PatientDurableKey = refs.PatientDurableKey
	 --   AND enc.DateKey > refs.CreationDateKey
		----AND (enc.DateKey - refs.CreationDateKey) BETWEEN 1 AND 15
    INNER JOIN #sample refs ON enc.PatientDurableKey = refs.PatientDurableKey
	    AND enc.DateKey > refs.CreationDateKey
	LEFT OUTER JOIN
	(
	SELECT
		pd.PatientKey,
		pd.PrimaryCareProviderDurableKey,
		pd.PrimaryCareProviderKey,
		pd.StartDate,
		pd.EndDate,
		pd.IsCurrent
	FROM CDW.FullAccess.PatientDim pd
	) pd ON pd.PatientKey = enc.PatientKey
			AND enc.DATE BETWEEN pd.StartDate AND pd.EndDate
    LEFT OUTER JOIN CDW.fullaccess.DepartmentDim AS dept
        ON dept.DepartmentKey = enc.DepartmentKey
	WHERE 1 = 1
		AND enc.[Date] <= GETDATE()
		AND enc.IsDerivedFromBilling = 0
		AND enc.DerivedEncounterStatus <>'Invalid'
		AND enc.EncounterKey > 0
	ORDER BY
		enc.PatientDurableKey,
		enc.EncounterKey

	 --Create index for temp table #encs

	CREATE CLUSTERED INDEX IX_encs ON #encs (EncounterKey, PatientDurableKey)

	--SELECT
	--	*
	--FROM #encs
	--ORDER BY
	--	PatientDurableKey,
	--	EncounterKey

	--SELECT DISTINCT
	--	PatientClass,
	--	ContactDateKey,
	--	AdmissionDateKey,
	--	DischargeDateKey
	--FROM #encs
	--ORDER BY
	--	PatientClass,
	--	ContactDateKey

	SELECT
	    enc.PatientDurableKey,
		enc.EncounterKey,
		baf.PrimaryEncounterKey,
		btf.BillingTransactionKey,
		btf.HospitalBillingTransactionEpicId,
		btf.ProfessionalBillingTransactionEpicId,
		btf.MatchingTransactionEpicId,
		baf.BillingAccountKey,
		baf.AccountEpicId,
		baf.BillingAccountType,
		baf.FinancialClass,
		baf.PrimaryCoverageKey,
		btf.TransactionType,
		btf.ChargeAmount,
		btf.BillingProcedureCode,
		btf.BillingProviderKey,
		btf.ReferringProviderKey,
		bpd.CptCode,
		bpd.HcpcsCode,
		bpd.AdaCode,
		bpd.AsaCode,
		bpd.OtherCodeType,
		bpd.OtherCode,
		bpd.BillingProcedureKey,
		bpd.Name AS BillingProcedureName,
		bpd.PatientFriendlyName,
		bpd.Type AS BillingProcedureType,
		bpd.Category AS BillingProcedureCat,
		bpd.BillingCategory,
		cd.BenefitPlanName,
		cd.PayorName
	INTO #billed_account
	FROM #encs enc
	INNER JOIN cdw.fullaccess.billingaccountfact baf ON enc.EncounterKey = baf.PrimaryEncounterKey
	INNER JOIN
	(
	SELECT
		btf.TransactionType,
		btf.ChargeAmount,
		btf.BillingProcedureCode,
		btf.BillingProviderKey,
		btf.BillingAccountKey,
		btf.BillingProcedureKey,
		btf.BillingTransactionKey,
		btf.ReferringProviderDurableKey,
		btf.ReferringProviderKey,
		btf.HospitalBillingTransactionEpicId,
		btf.ProfessionalBillingTransactionEpicId,
		btf.MatchingTransactionEpicId
	FROM cdw.fullaccess.BillingTransactionFact btf
	WHERE 1 = 1
	AND btf.TransactionType = 'Charge'
	AND btf.IsInactive = 0 -- active only
	) btf ON baf.BillingAccountKey= btf.BillingAccountKey
	INNER JOIN cdw.FullAccess.BillingProcedureDim bpd ON btf.BillingProcedureKey = bpd.BillingProcedureKey
	INNER JOIN
	(
	SELECT
		cd.CoverageKey,
		--cd.CoverageEpicId,
		--cd.CoverageFinancialClass,
		--cd.CoverageType,
		--cd.BenefitPlanEpicId,
		cd.PayorName,
		cd.BenefitPlanName--,
		--cd.BenefitPlanType
	FROM CDW.FullAccess.CoverageDim cd
	WHERE 1 = 1
	AND cd._IsDeleted = 0
	) cd ON baf.PrimaryCoverageKey = cd.CoverageKey
	ORDER BY 
	    baf.PrimaryEncounterKey,
		baf.BillingAccountKey,
		btf.BillingTransactionKey

	 --Create index for temp table #billed_account

	CREATE CLUSTERED INDEX IX_billed_account ON #billed_account (PrimaryEncounterKey, BillingAccountKey, BillingTransactionKey)

	--SELECT
	--	*
	--FROM #billed_account
	--ORDER BY
	--	PatientDurableKey,
	--	EncounterKey,
	--	BillingAccountKey,
	--	TransactionType,
	--	BillingProcedureCode
	--ORDER BY
	--    PrimaryEncounterKey,
	--	BillingAccountKey,
	--	BillingTransactionKey
	
	SELECT
		enc.PatientDurableKey,
		enc.person_id,
		enc.sk_Dim_Pt,
		--refs.CreationInstant,
		enc.CreationDateKey,
		enc.PatientClass,
		--enc.Date AS ContactDate,
		enc.ContactDateKey,
		enc.AdmissionDateKey,
		enc.DischargeDateKey,
		enc.EncounterKey,
		enc.EncounterEpicCsn,
		enc.DepartmentKey,
		--CAST(enc.DepartmentEpicId AS NUMERIC(18,0)) AS DepartmentEpicId,
		TRY_CAST(enc.DepartmentEpicId AS NUMERIC(18,0)) AS DepartmentEpicId,
		baf.PrimaryEncounterKey,
		--baf.BillingTransactionKey,
		--baf.HospitalBillingTransactionEpicId,
		--baf.ProfessionalBillingTransactionEpicId,
		--baf.MatchingTransactionEpicId,
		COALESCE(baf.HospitalBillingTransactionEpicId, baf.ProfessionalBillingTransactionEpicId) AS BillingTransactionEpicId,
		baf.BillingAccountKey,
		baf.AccountEpicId,
		baf.BillingAccountType,
		baf.FinancialClass,
		baf.PrimaryCoverageKey,
		baf.TransactionType,
		baf.ChargeAmount,
		baf.BillingProcedureCode,
		baf.CptCode,
		baf.HcpcsCode,
		baf.AdaCode,
		baf.AsaCode,
		baf.OtherCodeType,
		baf.OtherCode,
		baf.BillingProcedureKey,
		baf.BillingProcedureName,
		baf.PatientFriendlyName,
		baf.BillingProcedureType,
		baf.BillingProcedureCat,
		baf.BillingCategory,
		baf.PayorName,
		baf.BenefitPlanName,
		enc.ProviderKey,
		baf.BillingProviderKey,
		baf.ReferringProviderKey,
		enc.PrimaryCareProviderKey
	INTO #billed_amount
	FROM #encs enc
	INNER JOIN #billed_account baf
		ON enc.EncounterKey = baf.PrimaryEncounterKey
	--FROM #billed_account baf
	--LEFT OUTER JOIN #encs enc
	--	ON enc.EncounterKey = baf.PrimaryEncounterKey

	SELECT
		baf.PatientDurableKey,
        baf.EncounterKey,
        baf.BillingAccountKey,
  --      baf.BillingTransactionKey,
		--baf.HospitalBillingTransactionEpicId,
		--baf.ProfessionalBillingTransactionEpicId,
		--baf.MatchingTransactionEpicId,
		baf.BillingTransactionEpicId,
        baf.CreationDateKey,
		baf.person_id,
		baf.sk_Dim_Pt,
        baf.PatientClass,
        baf.ContactDateKey,
		--baf.AdmissionDateKey,
		COALESCE(baf.AdmissionDateKey, baf.DischargeDateKey) AS AdmissionDateKey,
        --baf.DischargeDateKey,
		COALESCE(baf.DischargeDateKey, baf.ContactDateKey) AS DischargeDateKey,
        baf.EncounterEpicCsn,
        baf.DepartmentKey,
		mdm.EPIC_DEPT_NAME,
		mdm.EPIC_SPCLTY,
        baf.PrimaryEncounterKey,
        baf.AccountEpicId,
        baf.BillingAccountType,
        baf.FinancialClass,
        baf.PrimaryCoverageKey,
        baf.TransactionType,
        baf.ChargeAmount,
        baf.BillingProcedureCode,
        baf.CptCode,
        baf.HcpcsCode,
        baf.AdaCode,
        baf.AsaCode,
        baf.OtherCodeType,
        baf.OtherCode,
        baf.BillingProcedureKey,
        baf.BillingProcedureName,
        baf.PatientFriendlyName,
        baf.BillingProcedureType,
        baf.BillingProcedureCat,
        baf.BillingCategory,
        baf.PayorName,
        baf.BenefitPlanName,
        baf.ProviderKey,
		encser.Npi AS ProviderNPI,
        baf.BillingProviderKey,
		ser.Npi AS BillingProviderNPI,
        baf.ReferringProviderKey,
		refser.Npi AS ReferringProviderNPI,
        baf.PrimaryCareProviderKey,
		pcpser.Npi AS PrimaryCareProviderNPI
	INTO #billed_amount_2
	FROM #billed_amount baf
	LEFT OUTER JOIN cdw.FullAccess.ProviderDim ser ON ser.ProviderKey = baf.BillingProviderKey 
	LEFT OUTER JOIN cdw.FullAccess.ProviderDim encser ON encser.ProviderKey = baf.ProviderKey
	LEFT OUTER JOIN cdw.FullAccess.ProviderDim refser ON refser.ProviderKey = baf.ReferringProviderKey
	LEFT OUTER JOIN cdw.FullAccess.ProviderDim pcpser ON pcpser.ProviderKey = baf.PrimaryCareProviderKey
	LEFT OUTER JOIN #pre_mdm mdm ON mdm.EPIC_DEPARTMENT_ID = baf.DepartmentEpicId
	--ORDER BY
	--	PatientDurableKey,
	--	EncounterKey,
	--	BillingAccountKey,
	--	TransactionType,
	--	BillingProcedureCode
	--ORDER BY
	--	PatientDurableKey,
	--	EncounterKey,
	--	BillingAccountKey,
	--	BillingTransactionKey

	--SELECT
	--	PatientDurableKey,
 --       EncounterKey,
 --       BillingAccountKey,
 --       --BillingTransactionKey,
 --       --HospitalBillingTransactionEpicId,
 --       --ProfessionalBillingTransactionEpicId,
 --       --MatchingTransactionEpicId,
	--	BillingTransactionEpicId,
 --       CreationDateKey,
 --       person_id,
 --       PatientClass,
 --       ContactDateKey,
 --       AdmissionDateKey,
 --       DischargeDateKey,
 --       EncounterEpicCsn,
 --       DepartmentKey,
 --       EPIC_DEPT_NAME,
 --       EPIC_SPCLTY,
 --       PrimaryEncounterKey,
 --       AccountEpicId,
 --       BillingAccountType,
 --       FinancialClass,
 --       PrimaryCoverageKey,
 --       TransactionType,
 --       ChargeAmount,
 --       BillingProcedureCode,
 --       CptCode,
 --       HcpcsCode,
 --       AdaCode,
 --       AsaCode,
 --       OtherCodeType,
 --       OtherCode,
 --       BillingProcedureKey,
 --       BillingProcedureName,
 --       PatientFriendlyName,
 --       BillingProcedureType,
 --       BillingProcedureCat,
 --       BillingCategory,
 --       PayorName,
 --       BenefitPlanName,
 --       ProviderKey,
 --       ProviderNPI,
 --       BillingProviderKey,
 --       BillingProviderNPI,
 --       ReferringProviderKey,
 --       ReferringProviderNPI,
 --       PrimaryCareProviderKey,
 --       PrimaryCareProviderNPI
	--FROM #billed_amount_2
	--ORDER BY
	--	PatientDurableKey,
	--	EncounterKey,
	--	BillingAccountKey,
	--	--BillingTransactionKey
	--	BillingTransactionEpicId
		
	SELECT
	    --btf.BillingTransactionKey AS UniqueID,
	    btf.BillingTransactionEpicId AS UniqueID,
		btf.EncounterEpicCsn AS EncounterID,
		--btf.DischargeDate,
		--btf.DischargeDateKey,
		at.DateValue AS AdmitDate,
		dt.DateValue AS DischargeDate,
		--btf.person_id AS UniquePatientID,
		btf.person_id,
		btf.sk_Dim_Pt AS UniquePatientID,
		btf.EPIC_DEPT_NAME AS Facility,
		btf.EPIC_SPCLTY AS ServiceLine,
		btf.PatientClass AS ServiceType,
		--btf.BillingProcedureCode,
		CASE
			WHEN btf.BillingProcedureCode = btf.CptCode THEN btf.BillingProcedureCode
			WHEN btf.BillingProcedureCode = btf.HcpcsCode THEN btf.BillingProcedureCode
			WHEN btf.OtherCodeType <> '*Not Applicable' THEN btf.OtherCode
			ELSE NULL
		END AS ProcedureCode,
		CASE
			WHEN btf.BillingProcedureCode = btf.CptCode THEN 'CptCode'
			WHEN btf.BillingProcedureCode = btf.HcpcsCode THEN 'HcpcsCode'
			WHEN btf.OtherCodeType <> '*Not Applicable' THEN 'Custom'
			ELSE NULL
		END AS ProcedureCodeType,
		btf.BillingProcedureName AS ProcedureDecsription,
		btf.FinancialClass,
		btf.PayorName AS Payer,
		--btf.ChargeAmount AS Revenue,
		CAST(ROUND(btf.ChargeAmount,0) AS INTEGER) AS ChargeAmount,
		CASE
			WHEN (LEN(btf.ReferringProviderNPI) > 0  AND btf.ReferringProviderNPI NOT LIKE '*%') THEN btf.ReferringProviderNPI
			WHEN (btf.PrimaryCareProviderNPI IS NOT NULL AND LEN(btf.PrimaryCareProviderNPI) > 0 AND btf.PrimaryCareProviderNPI NOT LIKE '*%') THEN btf.PrimaryCareProviderNPI
			WHEN (LEN(btf.ProviderNPI) > 0  AND btf.ProviderNPI NOT LIKE '*%') THEN btf.ProviderNPI
			WHEN (LEN(btf.BillingProviderNPI) > 0 AND btf.BillingProviderNPI NOT LIKE '*%') THEN btf.BillingProviderNPI
			ELSE NULL
		END AS ReferringID,
		CASE
			WHEN (LEN(btf.BillingProviderNPI) > 0 AND btf.BillingProviderNPI NOT LIKE '*%') THEN btf.BillingProviderNPI
			WHEN (LEN(btf.ProviderNPI) > 0  AND btf.ProviderNPI NOT LIKE '*%') THEN btf.ProviderNPI
			WHEN (LEN(btf.ReferringProviderNPI) > 0  AND btf.ReferringProviderNPI NOT LIKE '*%') THEN btf.ReferringProviderNPI
			WHEN (btf.PrimaryCareProviderNPI IS NOT NULL AND LEN(btf.PrimaryCareProviderNPI) > 0 AND btf.PrimaryCareProviderNPI NOT LIKE '*%') THEN btf.PrimaryCareProviderNPI
			ELSE NULL
		END AS AttendingID,
		--btf.PatientDurableKey,
	 --   btf.CreationInstant,
	 --   btf.ContactDate,
	 --   btf.EncounterKey,
	 --   btf.DepartmentKey,
	 --   btf.DepartmentEpicId,
		--mdm.EPIC_DEPT_NAME,
	 --   btf.PrimaryEncounterKey,
	 --   btf.BillingTransactionKey,
	 --   btf.BillingAccountKey,
	 --   btf.AccountEpicId,
	 --   btf.BillingAccountType,
		--btf.PrimaryCoverageKey,
		--btf.CoverageKey,
		--btf.CoverageEpicId,
		--btf.CoverageFinancialClass,
		--btf.CoverageType,
		--btf.BenefitPlanEpicId,
		--btf.BenefitPlanName,
		--btf.BenefitPlanType,
	 --   btf.TransactionType,
	 --   btf.ChargeAmount,
		--btf.BillingProcedureKey,
	 --   btf.CptCode,
	 --   btf.HcpcsCode,
		--btf.AdaCode,
		--btf.AsaCode,
		--btf.OtherCodeType,
		--btf.OtherCode,
	 --   btf.PatientFriendlyName,
	 --   btf.BillingProcedureType,
	 --   btf.BillingProcedureCat,
	 --   btf.BillingCategory,
		btf.BillingProviderNPI,
		btf.ProviderNPI,
		btf.ReferringProviderNPI,
		btf.PrimaryCareProviderNPI
	INTO #billed_amount_total
	FROM #billed_amount_2 btf
	--LEFT OUTER JOIN #pre_mdm mdm ON mdm.EPIC_DEPARTMENT_ID = btf.DepartmentEpicId
	INNER JOIN CDW.FullAccess.DateDim at
		ON btf.AdmissionDateKey = at.DateKey
	INNER JOIN CDW.FullAccess.DateDim dt
		ON btf.DischargeDateKey = dt.DateKey

	WHERE 1 = 1
	AND btf.ChargeAmount > 0

SELECT
    btf.UniqueID,
	btf.EncounterID,
	btf.AdmitDate,
    btf.DischargeDate,
	btf.person_id,
    btf.UniquePatientID,
	btf.Facility,
    btf.ServiceLine,
    btf.ServiceType,
    --btf.BillingProcedureCode,
    btf.ProcedureCode,
    btf.ProcedureCodeType,
    btf.ProcedureDecsription,
    btf.FinancialClass,
    btf.Payer,
	COUNT(*)  AS Volume,
    SUM(btf.ChargeAmount) AS Revenue,
    btf.ReferringID,
    btf.AttendingID
INTO #Rptg
FROM #billed_amount_total btf
WHERE 1 = 1
AND btf.UniquePatientID IS NOT NULL
GROUP BY
    btf.UniqueID,
	btf.EncounterID,
	btf.AdmitDate,
    btf.DischargeDate,
	btf.person_id,
    btf.UniquePatientID,
	btf.Facility,
    btf.ServiceLine,
    btf.ServiceType,
    --btf.BillingProcedureCode,
    btf.ProcedureCode,
    btf.ProcedureCodeType,
    btf.ProcedureDecsription,
    btf.FinancialClass,
    btf.Payer,
    btf.ReferringID,
    btf.AttendingID
--ORDER BY
--	btf.EncounterID,
--    btf.DischargeDate,
--    btf.UniquePatientID,
--    btf.ServiceLine,
--    btf.ServiceType,
--    --btf.BillingProcedureCode,
--    btf.ProcedureCode,
--    btf.ProcedureCodeType,
--    btf.ProcedureDecsription,
--    btf.FinancialClass,
--    btf.Payer,
--    btf.ReferringID,
--    btf.AttendingID
--ORDER BY
--    btf.UniquePatientID,
--	btf.EncounterID,
--    btf.DischargeDate,
--    btf.ServiceLine,
--    btf.ServiceType,
--    --btf.BillingProcedureCode,
--	btf.UniqueID,
--    btf.ProcedureCode,
--    btf.ProcedureCodeType,
--    btf.ProcedureDecsription,
--    btf.FinancialClass,
--    btf.Payer,
--    btf.ReferringID,
--    btf.AttendingID

SELECT
    CAST(UniqueID AS VARCHAR(200)) AS UniqueID,
    CAST(EncounterID AS VARCHAR(100)) AS EncounterID,
	CAST(
	   CAST(MONTH(AdmitDate) AS VARCHAR(2)) + '/' +
       CAST(DAY(AdmitDate) AS VARCHAR(2)) + '/' +
       CAST(YEAR(AdmitDate) AS VARCHAR(4))
	   AS VARCHAR(10)) AS AdmitDate,
	CAST(
	   CAST(MONTH(DischargeDate) AS VARCHAR(2)) + '/' +
       CAST(DAY(DischargeDate) AS VARCHAR(2)) + '/' +
       CAST(YEAR(DischargeDate) AS VARCHAR(4))
	   AS VARCHAR(10)) AS DischargeDate,
    CAST(UniquePatientID AS VARCHAR(100)) AS UniquePatientID,
    CAST(Facility AS VARCHAR(200)) AS Facility,
    CAST(ServiceLine AS VARCHAR(250)) AS ServiceLine,
    CAST(ServiceType AS VARCHAR(150)) AS ServiceType,
    CAST(ProcedureCode AS VARCHAR(100)) AS ProcedureCode,
    CAST(ProcedureCodeType AS VARCHAR(200)) AS ProcedureCodeType,
    CAST(ProcedureDecsription AS VARCHAR(100)) AS ProcedureDecsription,
    CAST(FinancialClass AS VARCHAR(50)) AS FinancialClass,
    CAST(Payer AS VARCHAR(150)) AS Payer,
    CAST(Volume AS VARCHAR(20)) AS Volume,
    CAST(Revenue AS VARCHAR(20)) AS Revenue,
    CAST(ReferringID AS VARCHAR(100)) AS ReferringID,
    CAST(AttendingID AS VARCHAR(100)) AS AttendingID 
FROM #Rptg
ORDER BY
    UniqueID,
	person_id,
    UniquePatientID,
	EncounterID,
    DischargeDate,
    ServiceLine,
    ServiceType,
    ProcedureCode,
    ProcedureCodeType,
    ProcedureDecsription,
    FinancialClass,
    Payer,
    ReferringID,
    AttendingID

SELECT
    'UniqueID|EncounterID|AdmitDate|DischargeDate|UniquePatientID|Facility|ServiceLine|ServiceType|ProcedureCode|ProcedureCodeType|' +
	'ProcedureDecsription|FinancialClass|Payer|Volume|Revenue|ReferringID|AttendingID' AS RptgRow
UNION ALL
SELECT
    CAST(UniqueID AS VARCHAR(200)) + '/' +
    CAST(EncounterID AS VARCHAR(100)) + '/' +
	CAST(
	   CAST(MONTH(AdmitDate) AS VARCHAR(2)) + '/' +
       CAST(DAY(AdmitDate) AS VARCHAR(2)) + '/' +
       CAST(YEAR(AdmitDate) AS VARCHAR(4))
	   AS VARCHAR(10)) + '|' +
	CAST(
	   CAST(MONTH(DischargeDate) AS VARCHAR(2)) + '/' +
       CAST(DAY(DischargeDate) AS VARCHAR(2)) + '/' +
       CAST(YEAR(DischargeDate) AS VARCHAR(4))
	   AS VARCHAR(10)) + '|' +
    CAST(UniquePatientID AS VARCHAR(100)) + '|' +
    CAST(Facility AS VARCHAR(200)) + '|' +
    CAST(ServiceLine AS VARCHAR(250)) + '|' +
    CAST(ServiceType AS VARCHAR(150)) + '|' +
    CAST(ProcedureCode AS VARCHAR(100)) + '|' +
    CAST(ProcedureCodeType AS VARCHAR(200)) + '|' +
    CAST(ProcedureDecsription AS VARCHAR(100)) + '|' +
    CAST(FinancialClass AS VARCHAR(50)) + '|' +
    CAST(Payer AS VARCHAR(150)) + '|' +
    CAST(Volume AS VARCHAR(20)) + '|' +
    CAST(Revenue AS VARCHAR(20)) + '|' +
    CAST(ReferringID AS VARCHAR(100)) + '|' +
    CAST(AttendingID AS VARCHAR(100)) AS RptgRow
FROM #Rptg

--*/
/*
-------------------------
-- Procedures
-------------------------
--DROP TABLE IF EXISTS #temp_procedures;
SELECT pd.ProcedureKey
      ,pd.DurableKey
      ,pd.codeset
      ,pd.CptCode
      ,pd.Code
      ,pd.Name ProcName
	  ,pd.Category
	  ,pd.PrimaryValueSetName
	  ,pd.PrimaryValueSetDetailName
INTO #temp_procedures
FROM CDW.FullAccess.ProcedureDim AS pd
WHERE 1                = 1
      AND LEN(Category) > 0
      AND Category NOT IN ('*Deleted','*Not Applicable','*Unknown','*Unspecified')
      --AND Category NOT LIKE '%ORDERABLES%'

      --AND Category LIKE 'PR %'

      --AND ((LEN(PrimaryValueSetName) > 0) AND (PrimaryValueSetName <> 'Medicine Services and Procedures'))
      --AND ((LEN(PrimaryValueSetDetailName) > 0) AND (PrimaryValueSetDetailName LIKE '%Procedures%') AND (PrimaryValueSetDetailName NOT LIKE 'Pathology and Laboratory%'))
      ;
*/
/*
SELECT btf.BillingTransactionKey,
baf.BillingAccountKey, baf.PrimaryEncounterKey, baf.AccountEpicId, baf.BillingAccountType, 
btf.TransactionType, btf.BillingProcedureCode, bpd.Name, bpd.CptCode, bpd.HcpcsCode, btf.BillingProviderKey, ser.ProviderEpicId, ser.Name
--SELECT *
FROM cdw.fullaccess.billingaccountfact baf
INNER JOIN cdw.fullaccess.BillingTransactionFact btf ON baf.BillingAccountKey= btf.BillingAccountKey
inner JOIN cdw.FullAccess.BillingProcedureDim bpd ON btf.BillingProcedureKey = bpd.BillingProcedureKey
INNER JOIN cdw.FullAccess.ProviderDim ser ON btf.BillingProviderKey = ser.ProviderKey
--WHERE baf.accountepicid IN ( 14037482783, 13018900372)
WHERE baf.accountepicid = 14038835820
AND btf.TransactionType = 'Charge'
AND btf.IsInactive = 0 -- active only
*/
/* -- Start
SELECT
	refs.PatientDurableKey,
	refs.person_id,
	refs.CreationInstant,
	enc.PatientClass,
	enc.Date AS ContactDate,
	CAST(COALESCE(CAST(enc.DischargeInstant AS DATETIME), enc.Date) AS DATE) AS DischargeDate,
	enc.EncounterKey,
	enc.EncounterEpicCsn,
	enc.DepartmentKey,
	--dept.DepartmentEpicId,
	CAST(dept.DepartmentEpicId AS NUMERIC(18,0)) AS DepartmentEpicId,
	--mdm.EPIC_DEPT_NAME,
	--mdm.EPIC_SPCLTY,
	baf.PrimaryEncounterKey,
	btf.BillingTransactionKey,
	baf.BillingAccountKey,
	--baf.PrimaryEncounterKey,
	baf.AccountEpicId,
	baf.BillingAccountType,
	baf.FinancialClass,
	baf.PrimaryCoverageKey,
	btf.TransactionType,
	btf.ChargeAmount,
	btf.BillingProcedureCode,
	bpd.CptCode,
	bpd.HcpcsCode,
	bpd.AdaCode,
	bpd.AsaCode,
	bpd.OtherCodeType,
	bpd.OtherCode,
	bpd.BillingProcedureKey,
	bpd.Name AS BillingProcedureName,
	bpd.PatientFriendlyName,
	bpd.Type AS BillingProcedureType,
	bpd.Category AS BillingProcedureCat,
	bpd.BillingCategory,
	--cd.CoverageKey,
 --   cd.CoverageEpicId,
 --   cd.CoverageFinancialClass,
 --   cd.CoverageType,
 --   cd.BenefitPlanEpicId,
    cd.PayorName,
    cd.BenefitPlanName,
 --   cd.BenefitPlanType,
	--btf.BillingProviderKey,
	--ser.ProviderEpicId AS BillingProviderEpicId,
	pdsd.Npi AS BillingProviderNPI,
	--ser.Name AS BillingProviderName,
	--encser.ProviderKey AS EncounterProviderKey,
	--encser.ProviderEpicId AS EncounterProviderEpicId,
	encpdsd.Npi AS EncounterProviderNPI,
	--encser.Name AS EncounterProviderName,
	--refser.ProviderEpicId AS ReferringProviderEpicId,
	refpdsd.Npi AS ReferringProviderNPI,
	--refser.NAME AS ReferringProviderName,
	--pd.PrimaryCareProviderDurableKey,
	--pd.StartDate,
	--pd.EndDate,
	--pd.IsCurrent,
	--pcpser.ProviderEpicId AS PCPProviderEpicId,
	pcppdsd.Npi AS PCPProviderNPI--,
	--pcpser.NAME AS PCPProviderName
--SELECT *
INTO #billed_amount
FROM CDW.FullAccess.EncounterFact AS enc
--INNER JOIN CDW.FullAccess.PatientDim pd ON enc.PatientDurableKey = pd.PatientKey
--/*
--INNER JOIN
LEFT OUTER JOIN
(
SELECT
	pd.PatientKey,
	pd.PrimaryCareProviderDurableKey,
	pd.PrimaryCareProviderKey,
	pd.StartDate,
	pd.EndDate,
	pd.IsCurrent
FROM CDW.FullAccess.PatientDim pd
--) pd ON enc.PatientDurableKey = pd.PatientKey
--		AND enc.DATE BETWEEN pd.StartDate AND pd.EndDate
) pd ON pd.PatientKey = enc.PatientKey
		AND enc.DATE BETWEEN pd.StartDate AND pd.EndDate
--*/
INNER JOIN #refpts refs ON enc.PatientDurableKey = refs.PatientDurableKey
	AND enc.[Date] >= refs.CreationInstant
	--AND DATEDIFF(DAY, refs.CreationInstant, enc.[Date]) BETWEEN 0 AND 7
	--AND DATEDIFF(DAY, refs.CreationInstant, enc.[Date]) BETWEEN 0 AND 5
--INNER JOIN refpts_cte refs ON enc.PatientDurableKey = refs.PatientDurableKey
--	AND enc.[Date] >= refs.CreationInstant
--INNER JOIN refpts_cte refs ON enc.PatientDurableKey = refs.PatientDurableKey
--	AND DATEDIFF(DAY, refs.CreationInstant, enc.[Date]) BETWEEN 0 AND 17
INNER JOIN cdw.fullaccess.billingaccountfact baf ON enc.EncounterKey = baf.PrimaryEncounterKey
--INNER JOIN cdw.fullaccess.BillingTransactionFact btf ON baf.BillingAccountKey= btf.BillingAccountKey
INNER JOIN
(
SELECT
	btf.TransactionType,
	btf.ChargeAmount,
	btf.BillingProcedureCode,
	btf.BillingProviderKey,
	btf.BillingAccountKey,
	btf.BillingProcedureKey,
	btf.BillingTransactionKey,
	btf.ReferringProviderDurableKey,
	btf.ReferringProviderKey
FROM cdw.fullaccess.BillingTransactionFact btf
WHERE 1 = 1
AND btf.TransactionType = 'Charge'
AND btf.IsInactive = 0 -- active only
) btf ON baf.BillingAccountKey= btf.BillingAccountKey
INNER JOIN cdw.FullAccess.BillingProcedureDim bpd ON btf.BillingProcedureKey = bpd.BillingProcedureKey
--/*
INNER JOIN
(
SELECT
	cd.CoverageKey,
	--cd.CoverageEpicId,
	--cd.CoverageFinancialClass,
	--cd.CoverageType,
	--cd.BenefitPlanEpicId,
	cd.PayorName,
	cd.BenefitPlanName--,
	--cd.BenefitPlanType
FROM CDW.FullAccess.CoverageDim cd
WHERE 1 = 1
AND cd._IsDeleted = 0
) cd ON baf.PrimaryCoverageKey = cd.CoverageKey
--*/
--INNER JOIN cdw.FullAccess.ProviderDim ser ON btf.BillingProviderKey = ser.ProviderKey
--INNER JOIN cdw.FullAccess.ProviderDim encser ON enc.ProviderKey = encser.ProviderKey
LEFT OUTER JOIN cdw.FullAccess.ProviderDim ser ON ser.ProviderKey = btf.BillingProviderKey 
LEFT OUTER JOIN cdw.FullAccess.ProviderDim encser ON encser.ProviderKey = enc.ProviderKey
--LEFT OUTER JOIN cdw.FullAccess.ProviderDim refser ON refser.DurableKey = btf.ReferringProviderDurableKey
LEFT OUTER JOIN cdw.FullAccess.ProviderDim refser ON refser.ProviderKey = btf.ReferringProviderKey
--LEFT OUTER JOIN cdw.FullAccess.ProviderDim pcpser ON pcpser.DurableKey = pd.PrimaryCareProviderDurableKey
LEFT OUTER JOIN cdw.FullAccess.ProviderDim pcpser ON pcpser.ProviderKey = pd.PrimaryCareProviderKey
--/*
LEFT OUTER JOIN
(
SELECT
	ProviderEpicId,
	Npi
FROM cdw.dbo. ProviderDimSourceData
WHERE 1 = 1
AND IsCurrent = 1
) pdsd ON pdsd.ProviderEpicId = ser.ProviderEpicId
--*/
--/*
LEFT OUTER JOIN
(
SELECT
	ProviderEpicId,
	Npi
FROM cdw.dbo. ProviderDimSourceData
WHERE 1 = 1
AND IsCurrent = 1
) encpdsd ON encpdsd.ProviderEpicId = encser.ProviderEpicId
--/*
LEFT OUTER JOIN
(
SELECT
	ProviderEpicId,
	Npi
FROM cdw.dbo. ProviderDimSourceData
WHERE 1 = 1
AND IsCurrent = 1
) refpdsd ON refpdsd.ProviderEpicId = refser.ProviderEpicId
--*/
--/*
LEFT OUTER JOIN
(
SELECT
	ProviderEpicId,
	Npi
FROM cdw.dbo. ProviderDimSourceData
WHERE 1 = 1
AND IsCurrent = 1
) pcppdsd ON pcppdsd.ProviderEpicId = pcpser.ProviderEpicId
--*/
--*/
        --INNER JOIN CDW.fullaccess.DepartmentDim AS dept
        --    ON enc.DepartmentKey           = dept.DepartmentKey
        --INNER JOIN #pre_mdm mdm
        --    ON dept.DepartmentEpicId       = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
        LEFT OUTER JOIN CDW.fullaccess.DepartmentDim AS dept
            ON dept.DepartmentKey = enc.DepartmentKey
        --LEFT OUTER JOIN #pre_mdm mdm
        --    ON CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID) = dept.DepartmentEpicId
ORDER BY
	CAST(dept.DepartmentEpicId AS NUMERIC(18,0))

 --Create index for temp table #billed_amount
	CREATE CLUSTERED INDEX IX_billed_amount ON #billed_amount (DepartmentEpicId)

--SELECT
--	btf.PatientDurableKey,
--    btf.person_id,
--    btf.CreationInstant,
--    btf.PatientClass,
--    btf.ContactDate,
--    btf.DischargeDate,
--    btf.EncounterKey,
--    btf.EncounterEpicCsn,
--    btf.DepartmentKey,
--    btf.DepartmentEpicId,
--	mdm.EPIC_DEPT_NAME,
--	mdm.EPIC_SPCLTY,
--    btf.PrimaryEncounterKey,
--    btf.BillingTransactionKey,
--    btf.BillingAccountKey,
--    btf.AccountEpicId,
--    btf.BillingAccountType,
--    btf.TransactionType,
--    btf.ChargeAmount,
--    btf.BillingProcedureCode,
--    btf.CptCode,
--    btf.HcpcsCode,
--    btf.BillingProcedureName,
--    btf.PatientFriendlyName,
--    btf.BillingProcedureType,
--    btf.BillingProcedureCat,
--    btf.BillingCategory,
--    btf.BillingProviderNPI,
--    btf.EncounterProviderNPI,
--    btf.ReferringProviderNPI,
--    btf.PCPProviderNPI

SELECT
    btf.EncounterEpicCsn AS EncounterID,
    btf.DischargeDate,
    btf.person_id AS UniquePatientID,
	mdm.EPIC_SPCLTY AS ServiceLine,
    btf.PatientClass AS ServiceType,
    --btf.BillingProcedureCode,
    CASE
		WHEN btf.BillingProcedureCode = btf.CptCode THEN btf.BillingProcedureCode
		WHEN btf.BillingProcedureCode = btf.HcpcsCode THEN btf.BillingProcedureCode
		WHEN btf.OtherCodeType <> '*Not Applicable' THEN btf.OtherCode
		ELSE NULL
	END AS ProcedureCode,
    CASE
		WHEN btf.BillingProcedureCode = btf.CptCode THEN 'CptCode'
		WHEN btf.BillingProcedureCode = btf.HcpcsCode THEN 'HcpcsCode'
		WHEN btf.OtherCodeType <> '*Not Applicable' THEN 'Custom'
		ELSE NULL
	END AS ProcedureCodeType,
    btf.BillingProcedureName AS ProcedureDecsription,
	btf.FinancialClass,
	btf.PayorName AS Payer,
	--btf.ChargeAmount AS Revenue,
	CAST(ROUND(btf.ChargeAmount,0) AS INTEGER) AS ChargeAmount,
	CASE
		WHEN (LEN(btf.ReferringProviderNPI) > 0  AND btf.ReferringProviderNPI NOT LIKE '*%') THEN btf.ReferringProviderNPI
		WHEN (btf.PCPProviderNPI IS NOT NULL AND LEN(btf.PCPProviderNPI) > 0 AND btf.PCPProviderNPI NOT LIKE '*%') THEN btf.PCPProviderNPI
		WHEN (LEN(btf.EncounterProviderNPI) > 0  AND btf.EncounterProviderNPI NOT LIKE '*%') THEN btf.EncounterProviderNPI
		WHEN (LEN(btf.BillingProviderNPI) > 0 AND btf.BillingProviderNPI NOT LIKE '*%') THEN btf.BillingProviderNPI
		ELSE NULL
	END AS ReferringID,
	CASE
		WHEN (LEN(btf.BillingProviderNPI) > 0 AND btf.BillingProviderNPI NOT LIKE '*%') THEN btf.BillingProviderNPI
		WHEN (LEN(btf.EncounterProviderNPI) > 0  AND btf.EncounterProviderNPI NOT LIKE '*%') THEN btf.EncounterProviderNPI
		WHEN (LEN(btf.ReferringProviderNPI) > 0  AND btf.ReferringProviderNPI NOT LIKE '*%') THEN btf.ReferringProviderNPI
		WHEN (btf.PCPProviderNPI IS NOT NULL AND LEN(btf.PCPProviderNPI) > 0 AND btf.PCPProviderNPI NOT LIKE '*%') THEN btf.PCPProviderNPI
		ELSE NULL
	END AS AttendingID,
	--btf.PatientDurableKey,
 --   btf.CreationInstant,
 --   btf.ContactDate,
 --   btf.EncounterKey,
 --   btf.DepartmentKey,
 --   btf.DepartmentEpicId,
	--mdm.EPIC_DEPT_NAME,
 --   btf.PrimaryEncounterKey,
 --   btf.BillingTransactionKey,
 --   btf.BillingAccountKey,
 --   btf.AccountEpicId,
 --   btf.BillingAccountType,
	--btf.PrimaryCoverageKey,
	--btf.CoverageKey,
	--btf.CoverageEpicId,
	--btf.CoverageFinancialClass,
	--btf.CoverageType,
	--btf.BenefitPlanEpicId,
	--btf.BenefitPlanName,
	--btf.BenefitPlanType,
 --   btf.TransactionType,
 --   btf.ChargeAmount,
	--btf.BillingProcedureKey,
 --   btf.CptCode,
 --   btf.HcpcsCode,
	--btf.AdaCode,
	--btf.AsaCode,
	--btf.OtherCodeType,
	--btf.OtherCode,
 --   btf.PatientFriendlyName,
 --   btf.BillingProcedureType,
 --   btf.BillingProcedureCat,
 --   btf.BillingCategory,
    btf.BillingProviderNPI,
    btf.EncounterProviderNPI,
    btf.ReferringProviderNPI,
    btf.PCPProviderNPI
INTO #billed_amount_total
FROM #billed_amount btf
LEFT OUTER JOIN #pre_mdm mdm ON mdm.EPIC_DEPARTMENT_ID = btf.DepartmentEpicId
WHERE 1 = 1
AND btf.ChargeAmount > 0
/*
SELECT
	*
FROM #billed_amount_total btf
--ORDER BY
--	PatientDurableKey,
--	ContactDate,
--	EncounterEpicCsn,
--	AccountEpicId,
--	BillingTransactionKey
--ORDER BY
--    EncounterKey,
--	PatientDurableKey,
--	ContactDate,
--	EncounterEpicCsn,
--	AccountEpicId,
--	BillingTransactionKey
ORDER BY
	btf.EncounterID,
    btf.DischargeDate,
    btf.UniquePatientID,
    btf.ServiceLine,
    btf.ServiceType,
    --btf.BillingProcedureCode,
    btf.ProcedureCode,
    btf.ProcedureCodeType,
    btf.ProcedureDecsription,
    btf.FinancialClass,
    btf.Payer,
    btf.ReferringID,
    btf.AttendingID
*/
SELECT
	btf.EncounterID,
    btf.DischargeDate,
    btf.UniquePatientID,
    btf.ServiceLine,
    btf.ServiceType,
    --btf.BillingProcedureCode,
    btf.ProcedureCode,
    btf.ProcedureCodeType,
    btf.ProcedureDecsription,
    btf.FinancialClass,
    btf.Payer,
    SUM(btf.ChargeAmount) AS Revenue,
    btf.ReferringID,
    btf.AttendingID
FROM #billed_amount_total btf
GROUP BY
	btf.EncounterID,
    btf.DischargeDate,
    btf.UniquePatientID,
    btf.ServiceLine,
    btf.ServiceType,
    --btf.BillingProcedureCode,
    btf.ProcedureCode,
    btf.ProcedureCodeType,
    btf.ProcedureDecsription,
    btf.FinancialClass,
    btf.Payer,
    btf.ReferringID,
    btf.AttendingID
ORDER BY
	btf.EncounterID,
    btf.DischargeDate,
    btf.UniquePatientID,
    btf.ServiceLine,
    btf.ServiceType,
    --btf.BillingProcedureCode,
    btf.ProcedureCode,
    btf.ProcedureCodeType,
    btf.ProcedureDecsription,
    btf.FinancialClass,
    btf.Payer,
    btf.ReferringID,
    btf.AttendingID
	
*/ -- END

GO


