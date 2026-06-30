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
	IF OBJECT_ID('tempdb..#billed_total_counts ') IS NOT NULL
		DROP TABLE #billed_total_counts
	IF OBJECT_ID('tempdb..#billed_total_charges ') IS NOT NULL
		DROP TABLE #billed_total_charges

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
           --ref.AgeKey,
           --ref.DepartmentKey,
           --ref.PrimaryVisitProviderDurableKey,
           ref.ReferralEpicId,
           ref.CreationInstant,
           ref.referral_type,
		   ref.ReferralType,
           ref.Class,
           ref.referral_status,
           ref.referral_status_reason,
           ref.referral_priority,
           ref.GeneratedByOrder_YesNo,
           --ref.ProcedureOrderEpicId,
           --ref.proc_code,
           --ref.proc_name,
           --ref.VisitTypeEpicId,
           --ref.VisitType,
           --ref.EncounterEpicCsn,
           --ref.encounter_type,
           --ref.AppointmentSerialNumber,
           --ref.AppointmentInstant,
           --ref.AppointmentStatus,
           --ref.RescheduledVisitKey,
           --ref.CancellationInstant,
           --ref.ReasonAppointmentCanceled,
           --ref.CancellationInitiator,
           ref.FirstEncounterInstant,
           ref.CreationToFirstEncounterDate,
           --ref.appt_order
		   ref.rflseq,
		   ref.PAT_ID,
		   ref.person_id

    INTO #ref

    FROM
    (
        SELECT DISTINCT
               -- Surrogate keys used later for joining to Dim tables.
               ref.PatientDurableKey,
               --vst.AgeKey,
               --vst.DepartmentKey,
               --vst.PrimaryVisitProviderDurableKey,

               ref.ReferralEpicId,			-- event_id
               ref.CreationInstant,			-- event_date
               ref.Type AS [referral_type],
			   auth.ReferralType,
               ref.Class,					-- event_category
               ref.Status AS [referral_status],
               ref.StatusReason AS [referral_status_reason],
               ref.Priority AS [referral_priority],
               ref.GeneratedByOrder_YesNo,
			   --pof.ProcedureOrderEpicId,
      --         prc.Code AS [proc_code],
      --         prc.Name AS [proc_name],
			   -- Used to later identify Consult Visits.
               --vst.VisitTypeEpicId,
               --vst.VisitType,
               --vst.EncounterEpicCsn,
               --vst.EncounterType AS [encounter_type],

               --vst.AppointmentSerialNumber,
               --vst.AppointmentInstant,
               --vst.AppointmentStatus,
               -- Non-canceled appointment rescheduled from this visit.
               --vst.RescheduledVisitKey,
               --vst.CancellationInstant,
               --vst.ReasonAppointmentCanceled,
               --vst.CancellationInitiator,

               ref.FirstEncounterInstant,
               ref.CreationToFirstEncounterDate,
			   -- Patient
			   CAST(pat.BirthDate AS DATETIME) AS [person_birth_date],
			   CAST(pat.Sex AS VARCHAR(255)) AS [person_gender],
		       CAST(pat.PatientEpicId AS VARCHAR(18)) AS [PAT_ID],
			   TRY_CAST(pat.PrimaryMrn AS INT) AS [person_id],
			   CAST(pat.Name AS VARCHAR(200)) AS [person_name],

               --ROW_NUMBER() OVER (PARTITION BY ref.ReferralKey
               --                   ORDER BY vst.AppointmentSerialNumber ASC,
               --                            vst.RescheduledVisitKey DESC,
               --                            vst.CancellationInstant ASC
               --                  ) AS [appt_order]
			   ROW_NUMBER() OVER(PARTITION BY auth.PatientDurableKey ORDER BY ref.CreationInstant) AS rflseq

		FROM [CDW].[FullAccess].[ReferralFact] ref
/*
        FROM [CDW].[FullAccess].[VisitReferralMappingFact] map

            INNER JOIN [CDW].[FullAccess].[VisitFact] vst
                ON map.VisitKey = vst.VisitKey
                   AND vst.Count = 1

            INNER JOIN [CDW].[FullAccess].[ReferralFact] ref
                ON map.ReferralKey = ref.ReferralKey
                   AND ref.Count = 1

            -- Use LEFT OUTER JOIN since not all referrals are generated by a procedure order.
            LEFT OUTER JOIN [CDW].[FullAccess].[ProcedureOrderFact] pof
                ON ref.GeneratedByOrderKey = pof.ProcedureOrderKey
                   AND pof.Count = 1

            LEFT OUTER JOIN [CDW].[FullAccess].[ProcedureDim] prc
                ON ref.PrimaryProcedureDurableKey = prc.DurableKey
                   AND prc.IsCurrent = 1

*/

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

        --INNER JOIN [CDW].[FullAccess].[DurationDim] age
        --    ON ref.AgeKey = age.DurationKey

        WHERE 1 = 1

              --AND map.HasErrors = 0

              --AND map.Count = 1

              AND ref.Count = 1

              --------------------------------------------------
              -- Referral criteria
              --------------------------------------------------

              AND ref.CreationInstant >= @StartDate
              AND ref.CreationInstant < @EndDate

			  --AND ref.Class IN ( 'Internal', 'Incoming' )
			  AND ref.Class = 'Incoming'

              --AND
              --(
              --    -- Results are combined from both the Referred To Department Specialty and the Referred To Provider Specialty.
              --    ref.ReferredToSpecialty = 'Radiation Oncology'
              --    -- Added to make certain that all referrals to Radiation Oncology are included.
              --    OR ref.ReferredToDepartmentSpecialty = 'Radiation Oncology'
              --    OR ref.ReferredToProviderSpecialty = 'Radiation Oncology'
              --)

              -- Referral Status
              --AND ref.Status IN ( 'Authorized', 'Closed', 'Completed', 'Pending Review' )

              -- The LinkedToAnEncounter column stores a 1 value once the referral is linked to a scheduled, non-canceled encounter.
              -- It does not require that the encounter actually occur. This indicates that the patient and the organization made progress
              -- with the referral and continuing care.
              --AND ref.LinkedToAnEncounter = 1

			  --AND
			  --(

				 -- ref.PrimaryProcedureDurableKey    = 37413		-- RADIATION ONCOLOGY EVALUATION (RAD3) [40241] - RADIATION ONCOLOGY ORDERABLES
				 -- OR ref.PrimaryProcedureDurableKey = 20513		-- AMB REFERRAL TO RADIATION ONCOLOGY (REF95) [231] - OUTPATIENT REF ORDERABLES UVA
				 -- OR ref.PrimaryProcedureDurableKey = 163894	-- RADIATION THERAPY (SHX1077062) [209846] - GENERIC SURGICAL HISTORY
				 -- OR ref.PrimaryProcedureDurableKey = -1		-- *Unspecified
			  --)

              --------------------------------------------------
              -- Consult Visit criteria
              --------------------------------------------------

              --AND
              --(
              --    -- Emily Couric Cancer Center (ECCC)
              --    vst.VisitTypeEpicId    = '11701282' -- RADONC CONS - GAMMA, SECONDARY [11701282]
              --    OR vst.VisitTypeEpicId = '11701279' -- RADONC CONS - GYN,GU,PONC,SARC [11701279]
              --    OR vst.VisitTypeEpicId = '11701281' -- RADONC CONS - HEADNECK, SKIN CA [11701281]
              --    OR vst.VisitTypeEpicId = '11701280' -- RADONC CONSULT - CNS AND LUNG [11701280]
              --    OR vst.VisitTypeEpicId = '11701283' -- RADONC CONSULT - GENERAL [11701283]
              --    OR vst.VisitTypeEpicId = '11701278' -- RADONC CONSULT - GI AND BREAST [11701278]
              --    OR vst.VisitTypeEpicId = '11703517' -- RADONC CONSULT EXTENDED [11703517]

              --    -- Moser Radiation Therapy Center
              --    OR vst.VisitTypeEpicId = '11701286' -- MOSER RONC CON-CNS/LUNG [11701286]
              --    OR vst.VisitTypeEpicId = '11701285' -- MOSER RONC CON-GYN,GU,PONC,SAR [11701285]
              --    OR vst.VisitTypeEpicId = '11701287' -- MOSER RONC CON-HEADNECK/SKIN [11701287]
              --    OR vst.VisitTypeEpicId = '11701284' -- MOSER RONC CONSULT - GI/BREAST [11701284]
              --    OR vst.VisitTypeEpicId = '11701289' -- MOSER RONC CONSULT - RAD ONC [11701289]
              --    OR vst.VisitTypeEpicId = '11701288' -- MOSER RONC-GAMMA,SECONDARY [11701288]
              --)

              --AND vst.ReasonAppointmentCanceled <> 'Error'

			  AND (ref.NumberOfCompletedVisits_X IS NOT NULL AND ref.NumberOfCompletedVisits_X > 0)

			  AND ref.TYPE NOT IN ('Home Health Care', 'Home Health Pharmacy', 'Home Health Visits', 'Insurance Referral', 'Lab', 'NonTOC Order', 'PFA Appointment Request', 'Treatment Plan')

    ) ref

    WHERE 1 = 1

        --AND ref.appt_order = 1;

	ORDER BY
		ref.PatientDurableKey,
		ref.rflseq

-- Create index for temp table #ref
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
INTO #temp_procedures
FROM CDW.FullAccess.ProcedureDim AS pd
WHERE 1                = 1
      AND LEN(Category) > 0
      AND Category NOT IN ('*Deleted','*Not Applicable','*Unknown','*Unspecified')
      AND Category NOT LIKE '%ORDERABLES%'

      AND Category LIKE 'PR %'

      AND ((LEN(PrimaryValueSetName) > 0) AND (PrimaryValueSetName <> 'Medicine Services and Procedures'))
      AND ((LEN(PrimaryValueSetDetailName) > 0) AND (PrimaryValueSetDetailName LIKE '%Procedures%') AND (PrimaryValueSetDetailName NOT LIKE 'Pathology and Laboratory%'))
      ;
/*
-------------------------
-- Intravitreal Injection Orders with Macular Degeneration Dx (Denominator)
-------------------------
--DROP TABLE IF EXISTS #tempIntraVitInjections;
SELECT pd.ProcedureKey
      ,pd.DurableKey
      ,pd.codeset
      ,pd.CptCode
      ,pd.Code
      ,pd.Name ProcName
INTO #tempIntraVitInjections
FROM CDW.FullAccess.ProcedureDim AS pd
WHERE 1                = 1
      AND pd.CodeSet   = 'CPT(R)'
      AND pd.CptCode   = N'67028' --intravitreal injections
      AND pd.IsCurrent = 1;


-- like N'H35.32_1' --active wet AMD
--DROP TABLE IF EXISTS #tempDenom;
;WITH CTE
AS (SELECT pat.PrimaryMrn
          ,pat.Name PatientName
          ,pat.Sex PatientSex
          ,dx.Name
          ,dtd.NameAndCode
          ,def.Type
          ,pat.DurableKey
          ,dept.DepartmentEpicId
          ,dept.DepartmentName
          ,mdm.EPIC_DEPARTMENT_ID
          ,mdm.HOSPITAL_CODE
          ,def.StartDateKey
    FROM CDW.fullaccess.DiagnosisEventFact AS def
        INNER JOIN CDW.fullaccess.DiagnosisDim AS dx
            ON def.DiagnosisKey            = dx.DiagnosisKey
        INNER JOIN CDW.fullaccess.DiagnosisTerminologyDim AS dtd
            ON dx.DiagnosisKey             = dtd.DiagnosisKey
        INNER JOIN CDW.fullaccess.PatientDim AS pat
            ON def.PatientDurableKey       = pat.DurableKey
               AND pat.IsCurrent           = 1
               AND pat.IsValid             = 1
               AND pat.IsHistoricalPatient = 0
               AND pat.DurableKey          <> 15282261 /* ANONYMOUS HIM ONLY,REG IN ERROR TESTPATIENTS MRN 4233223 */

        INNER JOIN CDW.FullAccess.DiagnosisEventSourceBridge AS br
            ON def.SourceComboKey          = br.DiagnosisEventSourceComboKey
        INNER JOIN CDW.fullaccess.DepartmentDim AS dept
            ON def.DepartmentKey           = dept.DepartmentKey
        INNER JOIN CDW_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL AS mdm
            ON dept.DepartmentEpicId       = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
    WHERE 1                                  = 1
          AND def.Count                      = 1
          AND dtd.Type                     = N'ICD-10-CM'
          AND dtd.Value LIKE (N'H35.32_1') --active wet AMD
          AND def.StartDateKey               >= CONVERT(CHAR(8), @StartDate, 112)
          AND def.StartDateKey               <= CONVERT(CHAR(8), @EndDate, 112)
          AND def._HasSourceClaimBasedEvents = 0
          AND br.SourceName                  = N'Clarity - Epic'
          AND mdm.DE_HOSPITAL_CODE           = 'UVA-MC')

SELECT DISTINCT
       CTE.PrimaryMrn
      ,CTE.PatientName
      ,CTE.PatientSex
      ,CTE.Name
      ,CTE.NameAndCode
      ,CTE.Type
      ,CTE.DurableKey
      ,CTE.DepartmentEpicId
      ,CTE.DepartmentName
      ,CTE.EPIC_DEPARTMENT_ID
      ,CTE.HOSPITAL_CODE
      ,CTE.StartDateKey
INTO #tempDenom
FROM CTE;
*/

--Intravitreal Injections
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
      --,baf.PrimaryEncounterKey
      --,baf.AccountEpicId
--, trim(map.Workbook_Sheet)     AS Workbook_Sheet
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
	--LEFT OUTER JOIN CDW.FullAccess.BillingAccountFact baf
	--	ON enc_hsp.EncounterKey = baf.PrimaryEncounterKey
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
      --AND pt.PrimaryMrn = '0706301'
      AND spef.PatientDurableKey = 226079
	  AND spef.SurgeryDateKey = 20251211
      ;

SELECT
	*
FROM #surgical_procedures
WHERE 1 = 1
--AND PatientDurableKey = 16234
--AND SurgeryDateKey = 20251120


/* coded procedures */
--DROP TABLE IF EXISTS #coded_procedures;
/*
SELECT ptd.CodeSet
      ,ptd.Code
      ,ptd.Name
      ,cpf.ProcedureDateKey
      ,baf.PatientDurableKey
      ,baf.AccountEpicId
      ,baf.PrimaryEncounterKey
      ,cpf.PerformingProviderDurableKey
      ,prov.ProviderEpicId ProviderID
      ,dept.DepartmentEpicId DepartmentID
      ,dept.DepartmentName
--, trim(map.Workbook_Sheet) AS Workbook_Sheet
INTO #coded_procedures
FROM CDW.FullAccess.CodedProcedureFact AS cpf
    INNER JOIN CDW.FullAccess.ProcedureTerminologyDim AS ptd
        ON cpf.ProcedureTerminologyKey = ptd.ProcedureTerminologyKey
    INNER JOIN CDW.FullAccess.BillingAccountFact AS baf
        ON cpf.BillingAccountKey       = baf.BillingAccountKey
	INNER JOIN CDW.FullAccess.EncounterFact AS ef
		ON baf.PrimaryEncounterKey  = ef.EncounterKey
    INNER JOIN CDW.FullAccess.PatientDim AS pat
        ON baf.PatientDurableKey       = pat.DurableKey
           AND pat.IsCurrent           = 1
           AND pat.IsValid             = 1
           AND pat.IsHistoricalPatient = 0
    INNER JOIN #refpts AS rfls
        ON baf.PatientDurableKey = rfls.PatientDurableKey
		AND ef.AdmissionInstant > rfls.CreationInstant
    INNER JOIN CDW.FullAccess.DepartmentDim AS dept
        ON baf.DepartmentKey           = dept.DepartmentKey
    INNER JOIN CDW_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL AS mdm
        ON dept.DepartmentEpicId       = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
    INNER JOIN CDW.FullAccess.ProviderDim AS prov
        ON prov.DurableKey             = cpf.PerformingProviderDurableKey
           AND prov.IsCurrent          = 1
           AND prov.DurableKey         > 0

-- INNER JOIN #tempDenom     AS ghd
--     ON pat.DurableKey              = ghd.DurableKey
WHERE cpf.Count              = 1
      AND baf.Count          = 1
      AND mdm.DE_HOSPITAL_CODE = 'UVA-MC';
*/
/*
/* billed procedures */
--DROP TABLE IF EXISTS #billed_procedures;

SELECT DISTINCT
       ua.CodeSet
      ,ua.CptCode
      ,ua.Name
      ,ua.ServiceDateKey
      ,ua.EncounterKey
      ,ua.PatientDurableKey
      ,ua.BillingAccountKey
      ,ua.AccountEpicId
      ,ua.BillingProcedureQuantity
      ,ua.DepartmentID
      ,ua.AttendingProviderDurableKey
      ,ua.ProviderID
INTO #billed_procedures
FROM (/* CPT */
         SELECT 'CPT' AS CodeSet
               ,bpd.cptcode
               ,bpd.Name
               ,btf.ServiceDateKey
               ,btf.EncounterKey
               ,btf.PatientDurableKey
               ,baf.BillingAccountKey
               ,baf.AccountEpicId
               ,btf.BillingProcedureQuantity
               ,dept.DepartmentEpicId DepartmentID
               ,baf.AttendingProviderDurableKey
               ,prov.ProviderEpicId ProviderID
         --, trim(map.Workbook_Sheet) AS Workbook_Sheet
         FROM CDW.FullAccess.BillingTransactionFact AS btf
             INNER JOIN CDW.FullAccess.BillingProcedureDim AS bpd
                 ON btf.BillingProcedureDurableKey = bpd.DurableKey
                    AND bpd.IsCurrent              = 1
             INNER JOIN CDW.FullAccess.BillingAccountFact AS baf
                 ON btf.BillingAccountKey          = baf.BillingAccountKey
             INNER JOIN CDW.fullaccess.encounterfact AS ef
                 ON ef.encounterkey                = btf.encounterkey
			INNER JOIN #refpts AS rfls
				ON btf.PatientDurableKey = rfls.PatientDurableKey
				AND ef.AdmissionInstant > rfls.CreationInstant
             INNER JOIN CDW.FullAccess.DepartmentDim AS dept
                 ON ef.DepartmentKey               = dept.DepartmentKey
             INNER JOIN CDW_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL AS mdm
                 ON dept.DepartmentEpicId          = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
             INNER JOIN CDW.FullAccess.ProviderDim AS prov
                 ON prov.DurableKey                = baf.AttendingProviderDurableKey
                    AND prov.IsCurrent             = 1
                    AND prov.DurableKey            > 0
             INNER JOIN CDW.FullAccess.PatientDim AS pat
                 ON pat.DurableKey                 = btf.PatientDurableKey

         --  INNER JOIN #tempDenom     AS ghd
         --      ON btf.PatientDurableKey       = ghd.DurableKey



         WHERE 1                                = 1
               AND btf.ReportingTransactionType = N'Charge'
               AND btf.Count                  = 1
               AND baf.Count                  = 1
               AND mdm.DE_HOSPITAL_CODE         = 'UVA-MC'

--AND trim(map.Status) IN ( 'New', 'No change' )
) AS ua;
*/

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
      --,baf.PrimaryEncounterKey
      --,baf.AccountEpicId
      ,pt.PrimaryMrn
      ,pef.ProcedureEventKey
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
      --AND trim(map.[Type])          = tcd.StandardName
      AND NOT EXISTS (SELECT enc.DerivedEncounterStatus INTERSECT SELECT N'Invalid')
--AND enc.[Type] NOT LIKE '%erroneous%'
      --AND pt.PrimaryMrn = '0706301'
      AND pef.PatientDurableKey = 226079
	  AND pef.ProcedureStartDateKey = 20251211
      ;

SELECT
	*
FROM #performed_procedures
WHERE 1 = 1
--AND PatientDurableKey = 16234
--AND ProcedureStartDateKey = 20251120


/* combine all procedures */
--DROP TABLE IF EXISTS #diagnostic_or_treatment_measures;
/*
;WITH billed_total_counts
AS (SELECT bp.CodeSet
          ,bp.CptCode
          ,bp.Name
          ,bp.ServiceDateKey
          ,bp.PatientDurableKey
          ,bp.EncounterKey
          -- , bp.Workbook_Sheet
          ,bp.BillingProcedureQuantity
          ,bp.DepartmentID
          ,bp.ProviderID
          ,SUM(bp.BillingProcedureQuantity) OVER (PARTITION BY
                                                      -- bp.Workbook_Sheet
                                                      bp.PatientDurableKey
                                                     ,bp.CptCode
                                                     ,bp.ServiceDateKey) AS BilledTotalQuantity
    FROM #billed_procedures AS bp)
*/
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
INTO #diagnostic_or_treatment_measures
FROM (   SELECT sp.CodeSet
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
         FROM #surgical_procedures AS sp
         UNION ALL
         /*
         SELECT cp.CodeSet
               ,cp.Code
               ,cp.Name
               ,cp.ProcedureDateKey
               ,cp.PatientDurableKey
               ,cp.DepartmentID
               ,cp.ProviderID
         FROM #coded_procedures AS cp
         UNION ALL
         */
         /*
         SELECT btc.CodeSet
               ,btc.CptCode
               ,btc.Name
               ,btc.ServiceDateKey
               ,btc.PatientDurableKey
               ,btc.DepartmentID
               ,btc.ProviderID
         FROM billed_total_counts AS btc
         WHERE btc.BilledTotalQuantity > 0
         UNION ALL
         */
         SELECT pp.CodeSet
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
         FROM #performed_procedures AS pp) AS ua
    INNER JOIN CDW.FullAccess.DateDim AS dd
        ON ua.DateKey      = dd.DateKey
    INNER JOIN CDW.FullAccess.PatientDim AS pat
        ON pat.DurableKey  = ua.PatientDurableKey
           AND pat.IsValid = 1;

SELECT
    *
FROM #diagnostic_or_treatment_measures

/*
--H44.0 or H44.1) --endophthalmitis 
--DROP TABLE IF EXISTS #tempInfections;
;WITH CTE
AS (SELECT pat.PrimaryMrn
          ,dx.Name
          ,dtd.NameAndCode
          ,def.Type
          ,pat.PatientDurableKey
          ,pat.DateKey InjectionDate
          ,def.StartDateKey InfDxDate
    FROM CDW.fullaccess.DiagnosisEventFact AS def
        INNER JOIN CDW.fullaccess.DiagnosisDim AS dx
            ON def.DiagnosisKey      = dx.DiagnosisKey
        INNER JOIN CDW.fullaccess.DiagnosisTerminologyDim AS dtd
            ON dx.DiagnosisKey       = dtd.DiagnosisKey

        -- INNER JOIN cdw.fullaccess.PatientDim                            AS pat
        --     ON def.PatientDurableKey       = pat.DurableKey
        --        AND pat.IsCurrent           = 1
        --        AND pat.IsValid             = 1
        --        AND pat.IsHistoricalPatient = 0
        --        AND pat.DurableKey <> 15282261 /* ANONYMOUS HIM ONLY,REG IN ERROR TESTPATIENTS MRN 4233223 */
        INNER JOIN #diagnostic_or_treatment_measures AS pat
            ON def.PatientDurableKey = pat.PatientDurableKey
        INNER JOIN CDW.FullAccess.DiagnosisEventSourceBridge AS br
            ON def.SourceComboKey    = br.DiagnosisEventSourceComboKey
        INNER JOIN CDW.fullaccess.DepartmentDim AS dept
            ON def.DepartmentKey     = dept.DepartmentKey
        INNER JOIN CDW_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL AS mdm
            ON dept.DepartmentEpicId = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
    WHERE 1                                  = 1
          AND def.Count                      = 1
          AND dtd.Type                     = N'ICD-10-CM'
          AND (   dtd.Value LIKE N'H44.0%'
                  OR dtd.Value LIKE N'H44.1%')
          AND def.StartDateKey               >= CONVERT(CHAR(8), @StartDate, 112)
          AND def.StartDateKey               <= CONVERT(CHAR(8), @EndDate, 112)
          AND def._HasSourceClaimBasedEvents = 0
          AND br.SourceName                  = N'Clarity - Epic'
          AND mdm.DE_HOSPITAL_CODE           = 'UVA-MC')
SELECT DISTINCT
       CTE.PrimaryMrn
      ,CTE.Name
      ,CTE.NameAndCode
      ,CTE.Type
      ,CTE.PatientDurableKey
      ,CTE.InjectionDate
      ,CTE.InfDxDate
      ,DATEDIFF(DAY, CONVERT(DATE, CAST(CTE.InjectionDate AS CHAR(8)), 112), CONVERT(DATE, CAST(CTE.InfDxDate AS CHAR(8)), 112)) AS DaysBetween
INTO #tempInfections
FROM CTE;
*/

--SELECT
--    baf.*
--    --baf.PrimaryEncounterKey,
--    --baf.TotalChargeAmount,
--    --baf.AccountEpicId
--FROM CDW.FullAccess.BillingAccountFact baf
--WHERE 1 = 1
--AND baf.PrimaryEncounterKey = 266835662
--ORDER BY
--    PatientDurableKey

SELECT
    prs.*,
    baf.PrimaryEncounterKey,
    baf.AccountBalanceType,
    baf.TotalChargeAmount,
    baf.AccountEpicId
FROM #diagnostic_or_treatment_measures prs
	LEFT OUTER JOIN CDW.FullAccess.BillingAccountFact baf
		ON prs.EncounterKey = baf.PrimaryEncounterKey
WHERE LEN(Code) > 0
ORDER BY
    PatientDurableKey,
    DateKey

SELECT
    prs.*,
    baf.PrimaryEncounterKey,
    baf.AccountBalanceType,
    baf.TotalChargeAmount,
    baf.AccountEpicId,
	baf.BillingAccountKey,
	btf.Line,
	btf.CPTCode,
	btf.BilledAmt
FROM #diagnostic_or_treatment_measures prs
	LEFT OUTER JOIN CDW.FullAccess.BillingAccountFact baf
		ON prs.EncounterKey = baf.PrimaryEncounterKey
	LEFT OUTER JOIN CDW.FullAccess.BillingTransactionLineLevelFactX btf
		ON btf.BillingAccountKey = baf.BillingAccountKey
		AND prs.Code = btf.CPTCode
WHERE LEN(Code) > 0
--AND prs.PatientDurableKey = 226079
--AND prs.DateKey = 20251211
ORDER BY
    PatientDurableKey,
    DateKey,
	btf.Line

SELECT DISTINCT
    --btf.BillingTransactionLineLevelKey,
    --btf.BillingTransactionKey,
    --btf.Line,
    btf.BillingAccountKey,
    btf.RevenueCode,
    btf.RevenueCodeName,
    btf.PostDateKey,
    btf.PostDateInstant,
    btf.ServiceAreaId,
    btf.CPTCode,
    --btf.Modifier,
    btf.ServiceDateKey,
    btf.BilledAmt--,
    --btf.AllowedAmt,
    --btf.NotAllowedAmt,
    --btf.DeductibleAmt,
    --btf.CoinsuranceAmt,
    --btf.CopayAmt,
    --btf.NonCoveredAmt,
    --btf.PostedAmt,
    --btf.AdjustmentAmt,
    --btf.ReasonCodes,
    --btf.Actions,
    --btf.ControlNumber,
    --btf.Quantity,
    --btf.Count,
    --btf._CreationInstant,
    --btf._LastUpdatedInstant,
    --btf._IsInferred,
    --btf._IsDeleted,
    --btf._HasSourceClarity,
    --btf._DeletedFromSourceClarity
FROM #diagnostic_or_treatment_measures prs
	LEFT OUTER JOIN CDW.FullAccess.BillingAccountFact baf
		ON prs.EncounterKey = baf.PrimaryEncounterKey
	LEFT OUTER JOIN CDW.FullAccess.BillingTransactionLineLevelFactX btf
		ON btf.BillingAccountKey = baf.BillingAccountKey
WHERE LEN(Code) > 0
--AND prs.PatientDurableKey = 226079
--AND prs.DateKey = 20251211
--ORDER BY
--    btf.BillingTransactionKey,
--	btf.Line
ORDER BY
    btf.CPTCode


/*
--Final Query
--DROP TABLE IF EXISTS #finalquery;
SELECT DISTINCT
       t.PrimaryMrn
      ,pat.Name PatientName
      ,pat.Sex PatientSex
      ,t.DepartmentID
      ,DepartmentName
      ,dept.DepartmentSpecialty
      ,t.ProviderID
      ,t.DateKey InjectionDate
      ,i.InfDxDate
      ,t.Code
      ,CASE
           WHEN i.DaysBetween < 0
           THEN 0
           ELSE i.DaysBetween
       END DaysBetween
      ,1 Denominator
      ,CASE
           WHEN i.InfDxDate >= i.InjectionDate
                AND (   i.DaysBetween >= 0
                        AND i.DaysBetween <= 14)
           THEN 1
           ELSE 0
       END Numerator
INTO #finalquery
FROM #diagnostic_or_treatment_measures AS t
    LEFT JOIN #tempInfections AS i
        ON i.PrimaryMrn          = t.PrimaryMrn
           AND i.InjectionDate   = t.DateKey
    INNER JOIN CDW.FullAccess.PatientDim AS pat
        ON pat.PrimaryMrn        = t.PrimaryMrn
    INNER JOIN CDW.FullAccess.DepartmentDim AS dept
        ON dept.DepartmentEpicId = t.DepartmentID
    INNER JOIN CDW_App.Rptg.vwRef_MDM_Location_Master_Hospital_Group_ALL AS mdm
        ON dept.DepartmentEpicId = CONVERT(NVARCHAR(18), mdm.EPIC_DEPARTMENT_ID)
WHERE mdm.DE_HOSPITAL_CODE = 'UVA-MC';




------------------------
--Main Query for Stored Proc
------------------------
SELECT DISTINCT

    -- Event columns.
       CAST('DAR_Opth_IntravitrealInj_Inf_Rate' AS VARCHAR(50)) AS event_type
                           -- ,CASE
                           --     WHEN main.InfDxDate IS NOT NULL THEN
                           --         CAST(1 AS INT)
                           --     ELSE
                           --         CAST(0 AS INT)
                           -- END AS [event_count]
      ,COALESCE(main.Numerator,0) AS  event_count
      ,CASE
           WHEN main.Numerator = 1
                AND main.InfDxDate IS NOT NULL
           THEN CAST(CAST(main.InfDxDate AS VARCHAR) AS DATE)
		   ELSE dt.day_date
       END AS event_date --make this regular date not dttm
      ,CASE
           WHEN main.Numerator = 1
                AND main.InfDxDate IS NOT NULL
           THEN HASHBYTES('sha2_256', CONCAT(main.person_id, main.InjectionDate, main.InfDxDate, main.Code))
       END AS event_id
      ,CASE
           WHEN main.Numerator = 1
                AND main.InfDxDate IS NOT NULL
           THEN 'Numerator'
       END AS event_category

                           -- Date columns.
      ,dt.fmonth_num
      ,dt.Fyear_num
      ,dt.FYear_name


                           -- Main
      ,main.person_gender
      ,main.person_id
      ,main.person_name
      ,main.EPIC_DEPARTMENT_ID
      ,main.epic_department_name
      ,main.epic_department_name_external
      ,main.provider_id
      ,main.provider_name
      ,main.provider_type
      ,main.financial_division_id
      ,main.financial_division_name
      ,main.financial_sub_division_id
      ,main.financial_sub_division_name
      ,main.HS_AREA_ID
      ,main.HS_AREA_NAME
      ,main.som_hs_area_id
      ,main.som_hs_area_name
      ,main.som_group_id
      ,main.som_group_name
      ,main.som_department_id
      ,main.som_department_name
      ,main.som_division_id
      ,main.som_division_name
      ,main.HOSPITAL_CODE
                           --following are main fields for tableau formulas and metrics
      ,main.Denominator
      ,main.Numerator
      ,main.Code
      ,main.InjectionDate
      ,main.InfDxDate
      ,main.DaysBetween
FROM CDW_App..Dim_Date AS dt
    LEFT OUTER JOIN ( -- Main
                        SELECT DISTINCT
                               t.PrimaryMrn person_id
                              ,t.PatientName person_name
                              ,t.PatientSex person_gender
                              ,t.Code
                              ,t.DepartmentID
                              ,t.ProviderID
                              ,t.InjectionDate
                              ,t.InfDxDate
                              ,t.Denominator
                              ,t.Numerator
                              ,t.DaysBetween
                              -- Epic Department (Performing Department)
                              ,mdm.EPIC_DEPARTMENT_ID
                              ,mdm.EPIC_DEPT_NAME AS epic_department_name
                              ,mdm.EPIC_EXT_NAME AS epic_department_name_external
                              -- Hospital Area
                              -- Based on Performing Department.
                              ,mdm.HS_AREA_ID
                              ,mdm.HS_AREA_NAME
                              ,mdm.HOSPITAL_CODE

                              -- Provider (Finalizing Provider)
                              --,TRY_CAST(t.ProviderID AS INT) AS [provider_id]
                              ,t.ProviderID provider_id
                              ,CAST(COALESCE(fin_prov.Name, '*Unspecified') AS VARCHAR(50)) AS provider_name
                              ,fin_prov.Type provider_type

                              -- Financial Division
                              -- Based on Finalizing Provider.
                              ,TRY_CAST(fin.Financial_Division AS INT) AS financial_division_id
                              ,CAST(fin.Financial_Division_Name AS VARCHAR(150)) AS financial_division_name
                              ,TRY_CAST(fin.Financial_SubDivision AS INT) AS financial_sub_division_id
                              ,CAST(fin.Financial_SubDivision_Name AS VARCHAR(150)) AS financial_sub_division_name

                              -- School of Medicine (SOM)
                              -- Based on Finalizing Provider.
                              -- Per 09/19/2024 meeting with Kaitlin Dorrier & Nick Harvey, don't limit SOM columns to Hospital Code = "UVA-MC".
                              -- For Radiology DAR / SOM, filtering will be done in the Tableau workbook.
                              ,som.som_hs_area_id
                              ,som.som_hs_area_name
                              ,som.som_group_id
                              ,som.som_group_name
                              ,som.Department_ID AS som_department_id
                              ,CAST(som.Department AS VARCHAR(150)) AS som_department_name
                              ,TRY_CAST(som.Org_Number AS INT) AS som_division_id
                              ,CAST(som.Organization AS VARCHAR(150)) AS som_division_name
                        FROM #finalquery AS t
                            LEFT JOIN #pre_mdm AS mdm
                                ON t.DEPARTMENTID                      = mdm.EPIC_DEPARTMENT_ID
                            LEFT JOIN CDW.FullAccess.ProviderDim AS fin_prov
                                ON t.ProviderID                        = fin_prov.ProviderEpicId
                                   AND fin_prov.IsCurrent              = 1
                                   AND fin_prov.ProviderKey            > 0
                            LEFT JOIN CDW_App_Dev.Rptg.vwDim_Clrt_SERsrc AS fin
                                ON fin_prov.ProviderEpicId             = fin.PROV_ID
                            LEFT JOIN CDW_App_Dev.Rptg.vwRef_OracleOrg_to_EpicFinancialSubdiv AS som
                                ON som.Epic_Financial_Subdivision_Code = fin.Financial_SubDivision
    -- LEFT JOIN [CLARITY_App].[Rptg].[vwDim_Clrt_SERsrc] fin ON fin_prov.ProviderEpicId = fin.PROV_ID
    -- LEFT JOIN [CLARITY_App].[Rptg].[vwRef_OracleOrg_to_EpicFinancialSubdiv] som ON fin.Financial_SubDivision = som.Epic_Financial_Subdivision_Code
    ) AS main
        ON main.InjectionDate = dt.date_key
WHERE dt.date_key     >= CONVERT(CHAR(8), @StartDate, 112)
      AND dt.date_key < CONVERT(CHAR(8), @EndDate, 112);
--and main.hs_area_id=1 --UVAMC
*/

GO


