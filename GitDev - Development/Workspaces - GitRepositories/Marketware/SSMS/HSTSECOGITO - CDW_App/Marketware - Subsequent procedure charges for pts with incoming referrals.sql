USE [CDW_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET NOCOUNT ON;

---parameters


DECLARE @StartDate AS SMALLDATETIME = NULL
       ,@EndDate AS SMALLDATETIME = NULL;

--For Testing
    --set @startDate = '07/01/2024';
    --set @endDate  = '3/1/2025';

IF @StartDate IS NULL
   AND @EndDate IS NULL
BEGIN

    EXEC CDW_App.ETL.usp_Get_Dash_Dates_BalancedScorecard @StartDate OUTPUT, @EndDate OUTPUT;

END;

	IF OBJECT_ID('tempdb..#pre_mdm ') IS NOT NULL
		DROP TABLE #pre_mdm
	IF OBJECT_ID('tempdb..#ref ') IS NOT NULL
		DROP TABLE #ref
	IF OBJECT_ID('tempdb..#refpts ') IS NOT NULL
		DROP TABLE #refpts
	IF OBJECT_ID('tempdb..#temp_procedures ') IS NOT NULL
		DROP TABLE #temp_procedures
	IF OBJECT_ID('tempdb..#surgical_procedures ') IS NOT NULL
		DROP TABLE #surgical_procedures
	IF OBJECT_ID('tempdb..#coded_procedures ') IS NOT NULL
		DROP TABLE #coded_procedures
	IF OBJECT_ID('tempdb..#billed_procedures ') IS NOT NULL
		DROP TABLE #billed_procedures
	IF OBJECT_ID('tempdb..#performed_procedures ') IS NOT NULL
		DROP TABLE #performed_procedures
	IF OBJECT_ID('tempdb..#diagnostic_or_treatment_measures ') IS NOT NULL
		DROP TABLE #diagnostic_or_treatment_measures
	--IF OBJECT_ID('tempdb..#billed_total_counts ') IS NOT NULL
	--	DROP TABLE #billed_total_counts
	--IF OBJECT_ID('tempdb..#billed_total_charges ') IS NOT NULL
	--	DROP TABLE #billed_total_charges
	IF OBJECT_ID('tempdb..#billed_amount ') IS NOT NULL
		DROP TABLE #billed_amount

-------------------------
--Getting Dept from EDW
-------------------------
--DROP TABLE IF EXISTS #pre_mdm;
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
FROM CDW_App.Rptg.vwRef_MDM_Location_Master_History AS rmlmh
    --[CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_History] AS rmlmh
    INNER JOIN ( --hx--most recent batch date per dep id
                   SELECT mdmhx.EPIC_DEPARTMENT_ID
                         ,MAX(mdmhx.BATCH_RUN_DT) AS max_dt
                   FROM CDW_App.Rptg.vwRef_MDM_Location_Master_History AS mdmhx
                   --[CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_History] AS mdmhx

                   GROUP BY mdmhx.EPIC_DEPARTMENT_ID) AS hx
        ON hx.EPIC_DEPARTMENT_ID  = rmlmh.EPIC_DEPARTMENT_ID
           AND rmlmh.BATCH_RUN_DT = hx.max_dt;

    --------------------------------------------------
	-- Temp Table:  ref
    --------------------------------------------------
    SELECT ref.PatientDurableKey,
           ref.ReferralEpicId,
           ref.CreationInstant,
           ref.referral_type,
		   ref.ReferralType,
           ref.Class,
           ref.referral_status,
           ref.referral_status_reason,
           ref.referral_priority,
           ref.GeneratedByOrder_YesNo,
           ref.FirstEncounterInstant,
           ref.CreationToFirstEncounterDate,
		   ref.rflseq,
		   ref.PAT_ID,
		   ref.person_id

    INTO #ref

    FROM
    (
        SELECT DISTINCT
               -- Surrogate keys used later for joining to Dim tables.
               ref.PatientDurableKey,
               ref.ReferralEpicId,			-- event_id
               ref.CreationInstant,			-- event_date
               ref.Type AS [referral_type],
			   auth.ReferralType,
               ref.Class,					-- event_category
               ref.Status AS [referral_status],
               ref.StatusReason AS [referral_status_reason],
               ref.Priority AS [referral_priority],
               ref.GeneratedByOrder_YesNo,
               ref.FirstEncounterInstant,
               ref.CreationToFirstEncounterDate,
			   -- Patient
			   CAST(pat.BirthDate AS DATETIME) AS [person_birth_date],
			   CAST(pat.Sex AS VARCHAR(255)) AS [person_gender],
		       CAST(pat.PatientEpicId AS VARCHAR(18)) AS [PAT_ID],
			   TRY_CAST(pat.PrimaryMrn AS INT) AS [person_id],
			   CAST(pat.Name AS VARCHAR(200)) AS [person_name],
			   ROW_NUMBER() OVER(PARTITION BY auth.PatientDurableKey ORDER BY ref.CreationInstant) AS rflseq

		FROM [CDW].[FullAccess].[ReferralFact] ref

		LEFT OUTER JOIN CDW.FullAccess.AuthorizationFact auth
			ON auth.ReferralKey = ref.ReferralKey
				AND auth.Count = 1

        INNER JOIN [CDW].[FullAccess].[PatientDim] pat
            ON ref.PatientDurableKey = pat.DurableKey
                AND pat.IsCurrent = 1
                AND pat.IsValid = 1
                AND pat.IsHistoricalPatient = 0
                AND pat.Test = 0
                AND pat.DurableKey <> 15282261 /* ANONYMOUS HIM ONLY,REG IN ERROR TESTPATIENTS MRN 4233223 */

        WHERE 1 = 1

              AND ref.Count = 1

              --------------------------------------------------
              -- Referral criteria
              --------------------------------------------------

              AND ref.CreationInstant >= @StartDate
              AND ref.CreationInstant < @EndDate

			  AND ref.Class = 'Incoming'

			  AND (ref.NumberOfCompletedVisits_X IS NOT NULL AND ref.NumberOfCompletedVisits_X > 0)

			  AND ref.TYPE NOT IN ('Home Health Care', 'Home Health Pharmacy', 'Home Health Visits', 'Insurance Referral', 'Lab', 'NonTOC Order', 'PFA Appointment Request', 'Treatment Plan')

    ) ref

	ORDER BY
		ref.PatientDurableKey,
		ref.rflseq

-- Create index for temp table #ref
	CREATE CLUSTERED INDEX IX_ref ON #ref (PatientDurableKey, rflseq)

	SELECT --DISTINCT
		PatientDurableKey,
		PAT_ID,
		person_id,
		CreationInstant -- Referral entry date
	INTO #refpts
	FROM #ref
	WHERE rflseq = 1 -- Earliest referral entry date for a patient
	ORDER BY
		PatientDurableKey

	-- Create index for temp table #refpts

	CREATE CLUSTERED INDEX IX_refpts ON #refpts (PatientDurableKey)

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

--DROP TABLE IF EXISTS #surgical_procedures;
/**  surgical procedures **/
SELECT DISTINCT
       'CPT' AS CodeSet
      ,br.Code
      ,br.Name
      ,spef.SurgeryDateKey
      ,spef.PatientDurableKey
      ,spef.HospitalEncounterKey
      ,enc_hsp.EncounterEpicCsn AS HospitalEncounterEpicCsn
      ,spef.SurgeryEncounterKey
      ,enc_surgery.EncounterEpicCsn AS SurgeryEncounterEpicCsn
      ,dept.RoomName
      ,dept.DepartmentName
      ,dept.DepartmentEpicId DepartmentID
      ,spef.PrimarySurgeonDurableKey
      ,prov.ProviderEpicId ProviderID
      ,enc_hsp.EncounterKey
      ,enc_hsp.PrimaryProcedureKey
      ,enc_hsp.PrimaryProcedureDurableKey
	  ,pr.CodeSet AS PrimaryCodeSet
	  ,pr.Code AS PrimaryCode
	  ,pr.CptCode AS PrimaryCptCode
	  ,pr.NAME AS PrimaryName
	  ,prs.Category
	  ,prs.PrimaryValueSetName
	  ,prs.PrimaryValueSetDetailName
INTO #surgical_procedures
FROM CDW.FullAccess.SurgicalProcedureEventFact AS spef
    INNER JOIN CDW.FullAccess.SurgicalProcedureCodeBridge AS br
        ON spef.ProcedureCodeComboKey = br.SurgicalProcedureCodeComboKey
    INNER JOIN #temp_procedures prs
        ON spef.ProcedureKey = prs.ProcedureKey
    INNER JOIN CDW.FullAccess.SurgicalCaseFact AS scf
        ON spef.SurgicalCaseKey       = scf.SurgicalCaseKey
    INNER JOIN #refpts AS rfls
        ON spef.PatientDurableKey = rfls.PatientDurableKey
		AND scf.ProcedureCompleteInstant > rfls.CreationInstant
        INNER JOIN CDW.FullAccess.DurationDim AS age
        ON spef.AgeKey                = age.DurationKey
    INNER JOIN CDW.FullAccess.SourceDim AS sd
        ON spef.SourceKey             = sd.SourceKey
    INNER JOIN CDW.FullAccess.DepartmentDim AS dept
        ON spef.OperatingRoomKey      = dept.DepartmentKey
    INNER JOIN CDW.FullAccess.DateDim AS surgery_dte
        ON spef.SurgeryDateKey        = surgery_dte.DateKey
    INNER JOIN CDW.FullAccess.EncounterFact AS enc_hsp
        ON spef.HospitalEncounterKey  = enc_hsp.EncounterKey
    INNER JOIN CDW.FullAccess.EncounterFact AS enc_surgery
        ON spef.SurgeryEncounterKey   = enc_surgery.EncounterKey
    LEFT JOIN CDW.FullAccess.PatientDim pt
        ON pt.DurableKey = enc_hsp.PatientDurableKey
    INNER JOIN CDW_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL AS mdm
        ON dept.DepartmentEpicId      = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
    INNER JOIN CDW.FullAccess.ProviderDim AS prov
        ON prov.DurableKey            = spef.PrimarySurgeonDurableKey
           AND prov.IsCurrent         = 1
           AND prov.DurableKey        > 0
	LEFT OUTER JOIN CDW.FullAccess.ProcedureDim pr
		ON pr.ProcedureKey = enc_hsp.PrimaryProcedureKey
WHERE 1                             = 1
      AND mdm.DE_HOSPITAL_CODE      = 'UVA-MC'
      AND spef.Count              = 1
      AND spef.Performed            = 1
      AND scf.Count               = 1
      AND scf.Canceled              = 0
      AND scf.ProcedureNotPerformed = 0
      AND NOT EXISTS (SELECT enc_hsp.DerivedEncounterStatus INTERSECT SELECT N'Invalid')
      AND enc_hsp.Type NOT LIKE '%erroneous%'

/* performed procedures */
--DROP TABLE IF EXISTS #performed_procedures;

SELECT DISTINCT
       tcd.StandardName AS CodeSet
      ,tcd.Concept AS Code
      ,procd.Name
      ,pef.ProcedureStartDateKey
      ,pef.PatientDurableKey
      ,enc.EncounterEpicCsn
      ,enc.EncounterKey
      ,dept.DepartmentEpicId DepartmentID
      ,pef.PerformingProviderDurableKey
      ,prov.ProviderEpicId ProviderID
	  ,pef.ProcedureKey
	  ,pef.ProcedureDurableKey
      ,enc.PrimaryProcedureKey
      ,enc.PrimaryProcedureDurableKey
	  ,pr.CodeSet AS PrimaryCodeSet
	  ,pr.Code AS PrimaryCode
	  ,pr.CptCode AS PrimaryCptCode
	  ,pr.NAME AS PrimaryName
      ,pt.PrimaryMrn
      ,pef.ProcedureEventKey
	  ,prs.Category
	  ,prs.PrimaryValueSetName
	  ,prs.PrimaryValueSetDetailName
INTO #performed_procedures
FROM CDW.FullAccess.ProcedureEventFact AS pef
    INNER JOIN CDW.FullAccess.ProcedureDim AS procd
        ON pef.ProcedureDurableKey     = procd.DurableKey
           AND procd.IsCurrent         = 1
    INNER JOIN #temp_procedures prs
        ON pef.ProcedureKey = prs.ProcedureKey
    INNER JOIN CDW.FullAccess.ProcedureMappingDim AS pmd
        ON procd.DurableKey            = pmd.ProcedureDurableKey
           AND pmd.IsCurrent           = 1
    INNER JOIN CDW.FullAccess.TerminologyConceptDim AS tcd
        ON pmd.TerminologyConceptKey   = tcd.TerminologyConceptKey
    INNER JOIN CDW.FullAccess.EncounterFact AS enc
        ON pef.EncounterKey            = enc.EncounterKey
    LEFT JOIN CDW.FullAccess.PatientDim pt
        ON pt.DurableKey = enc.PatientDurableKey
	--LEFT OUTER JOIN CDW.FullAccess.BillingAccountFact baf
	--	ON enc.EncounterKey = baf.PrimaryEncounterKey
	INNER JOIN #refpts AS rfls
		ON pef.PatientDurableKey = rfls.PatientDurableKey
		AND enc.AdmissionInstant > rfls.CreationInstant
    INNER JOIN CDW.FullAccess.DepartmentDim AS dept
        ON enc.AttributedDepartmentKey = dept.DepartmentKey
    INNER JOIN CDW_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL AS mdm
        ON dept.DepartmentEpicId       = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
    INNER JOIN CDW.FullAccess.ProcedureEventSourceBridge AS br
        ON pef.SourceComboKey          = br.ProcedureEventSourceComboKey
    INNER JOIN CDW.FullAccess.ProviderDim AS prov
        ON prov.DurableKey             = pef.PerformingProviderDurableKey
           AND prov.IsCurrent          = 1
           AND prov.DurableKey         > 0
	LEFT OUTER JOIN CDW.FullAccess.ProcedureDim pr
		ON pr.ProcedureKey = enc.PrimaryProcedureKey
WHERE 1                        = 1
      AND mdm.DE_HOSPITAL_CODE = 'UVA-MC'
      AND pef.Count          = 1
      AND NOT EXISTS (SELECT enc.DerivedEncounterStatus INTERSECT SELECT N'Invalid')
      ;


/* combine all procedures */
--DROP TABLE IF EXISTS #diagnostic_or_treatment_measures;

SELECT DISTINCT
       ua.CodeSet
      ,ua.Code
      ,ua.Name
      ,dd.Year
      ,ua.DateKey
      ,ua.PatientDurableKey
      ,pat.primaryMrn
      ,ua.DepartmentID
      ,ua.ProviderID
      ,ua.EncounterKey
	  ,ua.PrimaryCodeSet
	  ,ua.PrimaryCode
	  ,ua.PrimaryCptCode
	  ,ua.PrimaryName
	  ,ua.Category
	  ,ua.PrimaryValueSetName
	  ,ua.PrimaryValueSetDetailName
INTO #diagnostic_or_treatment_measures
FROM (   SELECT DISTINCT
                sp.CodeSet
               ,sp.Code
               ,sp.Name
               ,sp.SurgeryDateKey AS DateKey
               ,sp.PatientDurableKey
               ,sp.DepartmentID
               ,sp.ProviderID
               ,sp.EncounterKey
			   ,sp.PrimaryCodeSet
			   ,sp.PrimaryCode
			   ,sp.PrimaryCptCode
			   ,sp.PrimaryName
			   ,sp.Category
			   ,sp.PrimaryValueSetName
			   ,sp.PrimaryValueSetDetailName
         FROM #surgical_procedures AS sp
         UNION ALL
         SELECT DISTINCT
		        pp.CodeSet
               ,pp.Code
               ,pp.Name
               ,pp.ProcedureStartDateKey
               ,pp.PatientDurableKey
               ,pp.DepartmentID
               ,pp.ProviderID
               ,pp.EncounterKey
			   ,pp.PrimaryCodeSet
			   ,pp.PrimaryCode
			   ,pp.PrimaryCptCode
			   ,pp.PrimaryName
			   ,pp.Category
			   ,pp.PrimaryValueSetName
			   ,pp.PrimaryValueSetDetailName
         FROM #performed_procedures AS pp) AS ua
    INNER JOIN CDW.FullAccess.DateDim AS dd
        ON ua.DateKey      = dd.DateKey
    INNER JOIN CDW.FullAccess.PatientDim AS pat
        ON pat.DurableKey  = ua.PatientDurableKey
           AND pat.IsValid = 1;

SELECT
	bllng.PrimaryEncounterKey,
    bllng.AccountBalanceType,
    bllng.TotalChargeAmount,
    bllng.AccountEpicId,
    bllng.BillingAccountKey,
    bllng.CPTCode,
    bllng.ServiceDateKey,
    --bllng._LastUpdatedInstant,
    bllng.BilledAmt
INTO #billed_amount
FROM
(
SELECT DISTINCT
    baf.PrimaryEncounterKey,
    baf.AccountBalanceType,
    baf.TotalChargeAmount,
    baf.AccountEpicId,
	fctx.BillingAccountKey,
	fctx.CPTCode,
	fctx.ServiceDateKey,
	fctx._LastUpdatedInstant,
	fctx.BilledAmt,
	ROW_NUMBER() OVER(PARTITION BY fctx.BillingAccountKey, CPTCode, ServiceDateKey ORDER BY fctx._LastUpdatedInstant DESC) AS updtrn
FROM
(
SELECT DISTINCT
	BillingAccountKey,
	CPTCode,
	ServiceDateKey,
	_LastUpdatedInstant,
	BilledAmt
FROM [CDW].[FullAccess].[BillingTransactionLineLevelFactX]
WHERE 1 = 1
AND CPTCode IS NOT NULL
AND LEN(CPTCode) > 0
AND BilledAmt > 0.0
)fctx
LEFT OUTER JOIN CDW.FullAccess.BillingAccountFact baf
	ON baf.BillingAccountKey = fctx.BillingAccountKey
INNER JOIN 
(
SELECT DISTINCT
	EncounterKey,
	Code
FROM #diagnostic_or_treatment_measures
WHERE LEN(Code) > 0
) prs
	ON baf.PrimaryEncounterKey = prs.EncounterKey
	AND fctx.CPTCode = prs.Code
) bllng
WHERE bllng.updtrn = 1

SELECT
    prs.PatientDurableKey,
    prs.EncounterKey,
    prs.DateKey,
    prs.Code,
    prs.CodeSet,
	bllg.CPTCode,
	bllg.BilledAmt,
    prs.Name,
    prs.Year,
    prs.PrimaryMrn,
    prs.DepartmentID,
    prs.ProviderID,
    prs.PrimaryCodeSet,
    prs.PrimaryCode,
    prs.PrimaryCptCode,
    prs.PrimaryName,
    prs.Category,
    prs.PrimaryValueSetName,
    prs.PrimaryValueSetDetailName,
    bllg.PrimaryEncounterKey,
    bllg.AccountBalanceType,
    bllg.TotalChargeAmount,
    bllg.AccountEpicId,
	bllg.BillingAccountKey--,
FROM #diagnostic_or_treatment_measures prs
LEFT OUTER JOIN #billed_amount bllg
	ON bllg.PrimaryEncounterKey = prs.EncounterKey
	AND prs.Code = bllg.CPTCode
ORDER BY
    PatientDurableKey,
	prs.EncounterKey,
    DateKey,
	prs.Code
GO


