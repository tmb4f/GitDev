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
SET @enddate = '12/31/2019 00:00:00'

--ALTER PROCEDURE [ETL].[uspSrc_SvcLine_Inpatient_HCAHPS]
--    (
--     @startdate SMALLDATETIME=NULL
--    ,@enddate SMALLDATETIME=NULL
--    )
--AS 
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

DECLARE @Pod TABLE (PodName VARCHAR(100))

INSERT INTO @Pod
(
    PodName
)
VALUES
--('Cancer'),
--('Musculoskeletal'),
--('Primary Care'),
--('Surgical Procedural Specialties'),
--('Transplant'),
--('Medical Specialties'),
--('Radiology'),
--('Heart and Vascular Center'),
--('Neurosciences and Psychiatry'),
--('Women''s and Children''s'),
--('CPG'),
--('UVA Community Cancer POD'),
--('Digestive Health'),
--('Ophthalmology'),
--('Community Medicine')
--('Medical Specialties')
('Digestive Health')
;

DECLARE @ServiceLine TABLE (ServiceLineName VARCHAR(150))

INSERT INTO @ServiceLine
(
    ServiceLineName
)
VALUES
--('Digestive Health'),
--('Heart and Vascular'),
--('Medical Subspecialties'),
--('Musculoskeletal'),
--('Neurosciences and Behavioral Health'),
--('Oncology'),
--('Ophthalmology'),
--('Primary Care'),
--('Surgical Subspecialties'),
--('Transplant'),
--('Womens and Childrens')
--('Medical Subspecialties')
--('Digestive Health')
('Womens and Childrens')
;

DECLARE @Department TABLE (DepartmentId NUMERIC(18,0))

INSERT INTO @Department
(
    DepartmentId
)
VALUES
-- (10210006)
--,(10210040)
--,(10210041)
--,(10211006)
--,(10214011)
--,(10214014)
--,(10217003)
--,(10239017)
--,(10239018)
--,(10239019)
--,(10239020)
--,(10241001)
--,(10242007)
--,(10242049)
--,(10243003)
--,(10244004)
--,(10348014)
--,(10354006)
--,(10354013)
--,(10354014)
--,(10354015)
--,(10354016)
--,(10354017)
--,(10354024)
--,(10354034)
--,(10354042)
--,(10354044)
--,(10354052)
--,(10354055)
 --(10214011)
 --(10210006)
 --(10280004) -- AUBL PEDIATRICS
 --(10341002) -- CVPE UVA RHEU INF PNTP
 --(10228008) -- NRDG MAMMOGRAPHY
 --(10381003) -- UVEC RAD CT
 --(10354032) -- UVBB PHYSICAL THER FL4
 --(10242018) -- UVPC PULMONARY
 (10243003) -- UVHE DIGESTIVE HEALTH
 --(10239003) -- UVMS NEPHROLOGY
 --(10354015) -- UVBB PEDS ONCOLOGY CL
;

DECLARE @StaffResource TABLE (Resource_Type VARCHAR(8))

INSERT INTO @StaffResource
(
    Resource_Type
)
VALUES
-- ('Person')
--,('Resource)
 ('Person')
 --('Resource)
;

DECLARE @ProviderType TABLE (Provider_Type VARCHAR(40))

INSERT INTO @ProviderType
(
    Provider_Type
)
VALUES
--('Anesthesiologist') -- Person
--,('Audiologist') -- Person
--,('Case Manager') -- Person
--,('Clinical Social Worker') -- Person
--,('Community Provider') -- Person
--,('Counselor') -- Person
--,('Dentist') -- Person
--,('Doctor of Philosophy') -- Person
--,('Fellow') -- Person
--,('Financial Counselor') -- Person
--,('Genetic Counselor') -- Person
--,('Health Educator') -- Person
--,('Hygienist') -- Person
--,('Licensed Clinical Social Worker') -- Person
--,('Licensed Nurse') -- Person
--,('Medical Assistant') -- Person
--,('Medical Student') -- Person
--,('Nurse Practitioner') -- Person
--,('Occupational Therapist') -- Person
--,('Optometrist') -- Person
--,('P&O Practitioner') -- Person
--,('Pharmacist') -- Person
--,('Physical Therapist') -- Person
--,('Physical Therapy Assistant') -- Person
--,('Physician') -- Person
--,('Physician Assistant') -- Person
--,('Psychiatrist') -- Person
--,('Psychologist') -- Person
--,('RD Intern') -- Person
--,('Registered Dietitian') -- Person
--,('Registered Nurse') -- Person
--,('Resident') -- Person
--,('Scribe') -- Person
--,('Speech and Language Pathologist') -- Person
--,('Technician') -- Person
--,('Unknown') -- Person
--,('Nutritionist') -- Resource
--,('Pharmacist') -- Resource
--,('Registered Dietitian') -- Resource
--,('Registered Nurse') -- Resource
--,('Resident') -- Resource
--,('Resource') -- Resource
--,('Social Worker') -- Resource
--,('Unknown') -- Resource
--,('Financial Counselor') -- Unknown
--,('Nutritionist') -- Unknown
('Physician') -- Person
,('Physician Assistant') -- Person
,('Fellow') -- Person
,('Nurse Practitioner') -- Person
;

DECLARE @Provider TABLE (ProviderId VARCHAR(18))

INSERT INTO @Provider
(
    ProviderId
)
VALUES
 --('28813') -- FISHER, JOSEPH D
 --('1300563') -- ARTH INF
 --('41806') -- NORTHRIDGE DEXA
 --('1301100') -- CT6
 --('82262') -- CT APPOINTMENT ERC
 --('40758') -- PAYNE, PATRICIA
 --('73571') -- LEEDS, JOSEPH THOMAS
 --,('29303') -- KALANTARI, KAMBIZ
 --('73725') -- ROSS, BUERLEIN
 --('41013') -- MANN, JAMES A
 ('85744') -- CORBETT, SUSAN
;

DECLARE @SOMDepartment TABLE (SOMDepartmentId VARCHAR(100))

INSERT INTO @SOMDepartment
(
    SOMDepartmentId
)
VALUES
--('0'),--(All)
--('57'),--MD-INMD Internal Medicine
--('98'),--MD-NERS Neurological Surgery
--('139'),--MD-OBGY Ob & Gyn
--('163'),--MD-ORTP Orthopaedic Surgery
--('194'),--MD-OTLY Otolaryngology
--('29'),--MD-PBHS Public Health Sciences
--('214'),--MD-PEDT Pediatrics
--('261'),--MD-PSCH Psychiatric Medicine
--('267'),--MD-RADL Radiology
--('292'),--MD-SURG Surgery
--('305'),--MD-UROL Urology
('0') --(All)
--('57')--,--MD-INMD Internal Medicine
--('292')--,--MD-SURG Surgery
--('47')--,--MD-ANES Anesthesiology
;

DECLARE @SOMDivision TABLE (SOMDivisionId int)

INSERT INTO @SOMDivision
(
    SOMDivisionId
)
VALUES
(0)--,--(All)
--(14),--40445 MD-MICR Microbiology
--(22),--40450 MD-MPHY Mole Phys & Biophysics
--(30),--40415 MD-PBHS Public Health Sciences Admin
--(48),--40700 MD-ANES Anesthesiology
--(50),--40705 MD-DENT Dentistry
--(52),--40710 MD-DERM Dermatology
--(54),--40715 MD-EMED Emergency Medicine
--(56),--40720 MD-FMED Family Medicine
--(58),--40725 MD-INMD Int Med, Admin
--(60),--40730 MD-INMD Allergy
--(66),--40735 MD-INMD CV Medicine
--(68),--40745 MD-INMD Endocrinology
--(72),--40755 MD-INMD Gastroenterology
--(74),--40760 MD-INMD Gen, Geri, Pall, Hosp
--(76),--40761 MD-INMD Hospital Medicine
--(80),--40770 MD-INMD Hem/Onc
--(82),--40771 MD-INMD Community Oncology
--(84),--40775 MD-INMD Infectious Dis
--(86),--40780 MD-INMD Nephrology
--(88),--40785 MD-INMD Pulmonary
--(90),--40790 MD-INMD Rheumatology
--(98),--40746 MD-INMD Advanced Diabetes Mgt
--(101),--40800 MD-NERS Admin
--(111),--40820 MD-NERS CV Disease
--(113),--40830 MD-NERS Deg Spinal Dis
--(115),--40835 MD-NERS Gamma Knife
--(119),--40816 MD-NERS Minimally Invasive Spine
--(121),--40840 MD-NERS Multiple Neuralgia
--(123),--40825 MD-NERS Neuro-Onc
--(127),--40810 MD-NERS Pediatric
--(129),--40849 MD-NERS Pediatric Pituitary
--(131),--40806 MD-NERS Radiosurgery
--(138),--40850 MD-NEUR Neurology
--(142),--40860 MD-OBGY Ob & Gyn, Admin
--(144),--40865 MD-OBGY Gyn Oncology
--(146),--40870 MD-OBGY Maternal Fetal Med
--(148),--40875 MD-OBGY Reprod Endo/Infertility
--(150),--40880 MD-OBGY Midlife Health
--(152),--40885 MD-OBGY Northridge
--(154),--40890 MD-OBGY Primary Care Center
--(156),--40895 MD-OBGY Gyn Specialties
--(158),--40897 MD-OBGY Midwifery
--(163),--40900 MD-OPHT Ophthalmology
--(166),--40910 MD-ORTP Ortho Surg, Admin
--(168),--40915 MD-ORTP Adult Reconst
--(178),--40930 MD-ORTP Foot/Ankle
--(184),--40940 MD-ORTP Pediatric Ortho
--(188),--40950 MD-ORTP Spine
--(190),--40955 MD-ORTP Sports Med
--(192),--40960 MD-ORTP Hand Surgery
--(194),--40961 MD-ORTP Trauma
--(197),--40970 MD-OTLY Oto, Admin
--(201),--40980 MD-OTLY Audiology
--(208),--41005 MD-PATH Surgical Path
--(210),--41010 MD-PATH Clinical Pathology
--(212),--41015 MD-PATH Neuropathology
--(214),--41017 MD-PATH Research
--(219),--41025 MD-PEDT Pediatrics, Admin
--(223),--41035 MD-PEDT Cardiology
--(225),--41040 MD-PEDT Critical Care
--(227),--41045 MD-PEDT Developmental
--(229),--41050 MD-PEDT Endocrinology
--(233),--41056 MD-PEDT Bariatrics
--(237),--41058 MD-PEDT Adolescent Medicine
--(239),--41060 MD-PEDT Gastroenterology
--(241),--41065 MD-PEDT General Pediatrics
--(243),--41070 MD-PEDT Genetics
--(245),--41075 MD-PEDT Hematology
--(249),--41085 MD-PEDT Infectious Diseases
--(251),--41090 MD-PEDT Neonatology
--(253),--41095 MD-PEDT Nephrology
--(257),--41105 MD-PEDT Pulmonary
--(260),--41130 MD-PHMR Phys Med & Rehab
--(262),--41140 MD-PLSR Plastic Surgery
--(264),--41120 MD-PSCH Psychiatry and NB Sciences
--(270),--41160 MD-RADL Radiology, Admin
--(272),--41161 MD-RADL Community Division
--(274),--41165 MD-RADL Angio/Interv
--(276),--41166 MD-RADL Non-Invasive Cardio
--(278),--41170 MD-RADL Breast Imaging
--(280),--41175 MD-RADL Thoracoabdominal
--(282),--41180 MD-RADL Musculoskeletal
--(284),--41185 MD-RADL Neuroradiology
--(286),--41186 MD-RADL Interventional Neuroradiology (INR)
--(288),--41190 MD-RADL Nuclear Medicine
--(290),--41195 MD-RADL Pediatric Rad
--(295),--41150 MD-RONC Radiation Oncology
--(297),--41210 MD-SURG Surgery, Admin
--(310),--41250 MD-UROL Urology, Admin
--(314),--41255 MD-UROL Urology, General
--(327),--40480 MD-CDBT Ctr for Diabetes Tech
--(331),--40530 MD-CPHG Ctr for Public Health Genomics
--(373),--40204 MD-DMED School of Medicine Adm
--(435),--40230 MD-DMED Curriculum
--(435),--40250 MD-DMED Clin Performance Dev
--(435),--40265 MD-DMED Med Ed Chief of Staff
;

SELECT *
FROM DS_HSDM_App.TabRptg.Dash_BalancedScorecard_HCAHPS_Tiles
--WHERE
--  (event_date BETWEEN @locstartdate AND @locenddate
WHERE
  ((hs_area_id = 1)
  AND (event_count = 1)
  --AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT Staff_Resource FROM @StaffResource WHERE Staff_Resource = Staff_Resource)
  --AND EXISTS(SELECT Provider_Type FROM @ProviderType WHERE Provider_Type = Prov_Typ)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
   )		
--ORDER BY  event_count DESC
--        , event_date	
ORDER BY event_date

SELECT SUM(CASE WHEN event_category IN ('10-Best possible','9') THEN 1 ELSE 0 END) AS x_9_or_10_count
      ,SUM(event_count) AS x_event_count
FROM DS_HSDM_App.TabRptg.Dash_BalancedScorecard_HCAHPS_Tiles
--WHERE
--  (event_date BETWEEN @locstartdate AND @locenddate
WHERE
  ((hs_area_id = 1)
  AND (event_count = 1)
  --AND (appt_event_Canceled = 0 OR appt_event_Canceled_Late = 1 OR (appt_event_Provider_Canceled = 1 AND Cancel_Lead_Days <= 45)))
  AND event_date BETWEEN @locstartdate AND @locenddate
  --AND EXISTS(SELECT PodName FROM @Pod WHERE PodName = pod_name)
  AND EXISTS(SELECT ServiceLineName FROM @ServiceLine WHERE ServiceLineName = w_service_line_name)
  --AND EXISTS(SELECT DepartmentId FROM @Department WHERE DepartmentId = epic_department_id)
  --AND EXISTS(SELECT Staff_Resource FROM @StaffResource WHERE Staff_Resource = Staff_Resource)
  --AND EXISTS(SELECT Provider_Type FROM @ProviderType WHERE Provider_Type = Prov_Typ)
  --AND EXISTS(SELECT ProviderId FROM @Provider WHERE ProviderId = provider_id)
   )

GO


