USE CLARITY

DECLARE @StartDate SMALLDATETIME,
        @EndDate SMALLDATETIME,
        @in_NPI VARCHAR(MAX)

SET @StartDate = '7/1/2023 00:00 AM'
SET @EndDate = '8/1/2024 11:59 PM'

SET NOCOUNT ON

DECLARE @NPI TABLE (NPI VARCHAR(10))

INSERT INTO @NPI
(
    NPI
)
VALUES
--('1417946310'),
--('1700436839')
--('1417946310'),
--('1700436839'),
--('1164849089'),
--('1669542833')
('1417184946')
;

SELECT @in_NPI = COALESCE(@in_NPI+',' ,'') + CAST(NPI AS VARCHAR(MAX))
FROM @NPI

SELECT @in_NPI
/*
SELECT		npi.NPI
		,MAX(ser.PROV_NAME) 'Display name'
		,MAX(zcsp.NAME) 'Specialty'
		,MAX(dt.month_name) 'month'
		,dt.fmonth_num
		,dt.year_num 'Year'
		,dept.DEPARTMENT_NAME 'Primary clinic'
		,COUNT(enc.PAT_ENC_CSN_ID) 'Encounters'

FROM dbo.PAT_ENC enc
	INNER JOIN (
					SELECT idx.IDENTITY_ID 'NPI'
							,idx.PROV_ID
					FROM CLARITY.dbo.IDENTITY_SER_ID idx
					WHERE idx.IDENTITY_TYPE_ID='100001'
			   ) npi								ON enc.VISIT_PROV_ID=npi.PROV_ID
	INNER JOIN dbo.CLARITY_SER ser					ON ser.PROV_ID = npi.PROV_ID
	INNER JOIN (SELECT *
				FROM dbo.CLARITY_SER_SPEC
				WHERE LINE='1') sp					ON sp.PROV_ID = ser.PROV_ID
	INNER JOIN dbo.ZC_SPECIALTY zcsp					ON zcsp.SPECIALTY_C = sp.SPECIALTY_C
	INNER JOIN (SELECT *
				FROM dbo.CLARITY_SER_DEPT dep
				) dep				ON (dep.DEPARTMENT_ID = enc.DEPARTMENT_ID)
				AND (dep.PROV_ID = sp.PROV_ID)
	INNER JOIN dbo.CLARITY_DEP dept					ON dep.DEPARTMENT_ID=dept.DEPARTMENT_ID
	INNER JOIN CLARITY_App.dbo.Dim_Date dt				ON CAST(dt.day_date AS DATE)=CAST(enc.APPT_TIME AS DATE)


WHERE 1=1
AND  (@in_NPI IS NULL OR npi.NPI IN (SELECT * FROM CLARITY.ETL.fn_ParmParse(@in_NPI,',')))
AND enc.APPT_STATUS_C in ('2','6')--completed
AND dt.day_date >=@StartDate
AND dt.day_date <=@EndDate
AND npi.NPI IS NOT NULL 
GROUP BY npi.NPI,dt.year_num, dt.fmonth_num, dept.DEPARTMENT_NAME
ORDER BY npi.NPI,dt.year_num, dt.fmonth_num, dept.DEPARTMENT_NAME
*/
SELECT		npi.NPI
		,MAX(ser.PROV_NAME) 'Display name'
		--,ser.PROV_NAME 'Display name'
		,MAX(zcsp.NAME) 'Specialty'
		--,zcsp.NAME 'Specialty'
		,MAX(dt.month_name) 'month'
		--,dt.month_name 'month'
		,dt.fmonth_num
		,dt.year_num 'Year'
		,dept.DEPARTMENT_NAME 'Primary clinic'
		,COUNT(enc.PAT_ENC_CSN_ID) 'Encounters'
		--,enc.PAT_ENC_CSN_ID 'Encounters'

FROM dbo.PAT_ENC enc
	INNER JOIN (
					SELECT idx.IDENTITY_ID 'NPI'
							,idx.PROV_ID
					FROM CLARITY.dbo.IDENTITY_SER_ID idx
					WHERE idx.IDENTITY_TYPE_ID='100001'
			   ) npi								ON enc.VISIT_PROV_ID=npi.PROV_ID
	INNER JOIN dbo.CLARITY_SER ser					ON ser.PROV_ID = npi.PROV_ID
	LEFT OUTER JOIN
	(
	SELECT
		NPINumber,
		ProviderGroup
	FROM CLARITY_App.dbo.Dim_Physcn
	WHERE current_flag = 1
	) doc		ON doc.NPINumber = npi.NPI
	LEFT OUTER JOIN (SELECT *
				FROM dbo.CLARITY_SER_SPEC
				WHERE LINE='1') sp					ON sp.PROV_ID = ser.PROV_ID
	LEFT OUTER JOIN dbo.ZC_SPECIALTY zcsp					ON zcsp.SPECIALTY_C = sp.SPECIALTY_C
	LEFT OUTER JOIN (SELECT *
				FROM dbo.CLARITY_SER_DEPT dep
				) dep				ON (dep.DEPARTMENT_ID = enc.DEPARTMENT_ID)
				AND (dep.PROV_ID = COALESCE(sp.PROV_ID,ser.PROV_ID))
	INNER JOIN dbo.CLARITY_DEP dept					ON dep.DEPARTMENT_ID=dept.DEPARTMENT_ID
	INNER JOIN CLARITY_App.dbo.Dim_Date dt				ON CAST(dt.day_date AS DATE)=CAST(enc.APPT_TIME AS DATE)


WHERE 1=1
AND  (@in_NPI IS NULL OR npi.NPI IN (SELECT * FROM CLARITY.ETL.fn_ParmParse(@in_NPI,',')))
AND enc.APPT_STATUS_C in ('2','6')--completed
AND dt.day_date >=@StartDate
AND dt.day_date <=@EndDate
AND npi.NPI IS NOT NULL 
AND doc.ProviderGroup IN ('AHP', 'APP', 'Clin Staff')
GROUP BY npi.NPI,dt.year_num, dt.fmonth_num, dept.DEPARTMENT_NAME
--ORDER BY npi.NPI,dt.year_num, dt.fmonth_num, CAST(enc.APPT_TIME AS DATE), dept.DEPARTMENT_NAME
ORDER BY npi.NPI,dt.year_num, dt.fmonth_num, dept.DEPARTMENT_NAME