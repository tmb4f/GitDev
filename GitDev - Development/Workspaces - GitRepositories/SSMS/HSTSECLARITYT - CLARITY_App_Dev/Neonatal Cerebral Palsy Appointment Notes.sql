USE DS_HSDM_App_Dev

IF OBJECT_ID('tempdb..#neopt ') IS NOT NULL
DROP TABLE #neopt

IF OBJECT_ID('tempdb..#ntes ') IS NOT NULL
DROP TABLE #ntes

DECLARE @neodata TABLE (MedRecNum VARCHAR(8), MRN INTEGER, sk_Dim_Pt INTEGER, sk_Dim_Clrt_Pt INTEGER)

INSERT INTO @neodata
(
	MedRecNum,
    MRN,
    sk_Dim_Pt,
    sk_Dim_Clrt_Pt
)
SELECT DISTINCT
	  MedRecNum,
	  CAST([MedRecNum] AS INTEGER) AS MRN,
	  sk_Dim_Pt,
	  sk_Dim_Clrt_Pt
  FROM [DS_HSDM_App_Dev].[neodata].[cp_cases_controls]

SELECT
	   neodata.MedRecNum
	  ,neodata.MRN
	  ,neodata.sk_Dim_Pt
      ,enc.sk_Dim_Clrt_Pt
	  ,nte.[sk_Fact_Pt_Enc_Clrt]
      ,[Appt_Nte_LINE]
	  ,enc.sk_Cont_Dte
	  --,enc.Adm_Dtm
	  ,CONVERT(DATETIME, CAST(enc.sk_Cont_Dte AS VARCHAR(8)),112) AS Cont_Date
      --,nte.[PAT_ENC_CSN_ID]
      --,[PAT_ENC_DATE_REAL]
      ,[Appt_Note]
      --,[Load_Dtm]
  INTO #neopt
  FROM [DS_HSDW_Prod].[Rptg].[vwFact_Pt_Enc_Clrt_Appt_Note] nte
  LEFT OUTER JOIN DS_HSDW_Prod.Rptg.vwFact_Pt_Enc_Clrt enc
	ON enc.sk_Fact_Pt_Enc_Clrt = nte.sk_Fact_Pt_Enc_Clrt
  INNER JOIN @neodata neodata
	ON enc.sk_Dim_Clrt_Pt = neodata.sk_Dim_Clrt_Pt
  --WHERE Appt_Note LIKE 'cerebral palsy'

 -- SELECT
	--*
 -- FROM #neopt
 -- ORDER BY sk_Dim_Clrt_Pt, sk_Fact_Pt_Enc_Clrt, Appt_Nte_LINE

 --   SELECT
	--ntes.sk_Dim_Clrt_Pt,
 --   ntes.sk_Fact_Pt_Enc_Clrt,
 --   ntes.Appt_Nte_LINE,
 --   ntes.sk_Cont_Dte,
 --   ntes.Cont_Date,
 --   ntes.Appt_Note
	--, (SELECT COALESCE(MAX(nte.Appt_Note),'')  + ';' AS [text()]
	--   FROM #neopt nte
	--   WHERE nte.sk_Dim_Clrt_Pt = ntes.sk_Dim_Clrt_Pt
	--   AND nte.sk_Fact_Pt_Enc_Clrt = ntes.sk_Fact_Pt_Enc_Clrt
	--   GROUP BY
	--	nte.sk_Dim_Clrt_Pt,
	--	nte.sk_Fact_Pt_Enc_Clrt,
	--	nte.sk_Cont_Dte,
	--	nte.Cont_Date,
	--	nte.Appt_Nte_LINE
	--   FOR XML PATH ('')
	--  ) AS [Appointment Note]
 -- FROM #neopt ntes
  
    SELECT DISTINCT
	ntes.sk_Dim_Clrt_Pt,
    ntes.sk_Fact_Pt_Enc_Clrt,
    --ntes.Appt_Nte_LINE,
    --ntes.sk_Cont_Dte,
    --ntes.Cont_Date,
    --ntes.Appt_Note
	(SELECT COALESCE(MAX(nte.Appt_Note),'')  + ';' AS [text()]
	   FROM #neopt nte
	   WHERE nte.sk_Dim_Clrt_Pt = ntes.sk_Dim_Clrt_Pt
	   AND nte.sk_Fact_Pt_Enc_Clrt = ntes.sk_Fact_Pt_Enc_Clrt
	   GROUP BY
		nte.sk_Dim_Clrt_Pt,
		nte.sk_Fact_Pt_Enc_Clrt,
		nte.sk_Cont_Dte,
		nte.Cont_Date,
		nte.Appt_Nte_LINE
	   FOR XML PATH ('')
	  ) AS [Appointment Note]
  INTO #ntes
  FROM #neopt ntes

  SELECT DISTINCT
	pt.sk_Dim_Clrt_Pt,
	pt.sk_Dim_Pt,
	pt.MedRecNum,
    pt.sk_Fact_Pt_Enc_Clrt,
    pt.Cont_Date,
	SUBSTRING(ntes.[Appointment Note],1,LEN(ntes.[Appointment Note]) -1) AS Appt_Notes
  FROM #neopt pt
  INNER JOIN #ntes ntes
  ON pt.sk_Dim_Clrt_Pt = ntes.sk_Dim_Clrt_Pt
  AND pt.sk_Fact_Pt_Enc_Clrt = ntes.sk_Fact_Pt_Enc_Clrt
  ORDER BY pt.sk_Dim_Clrt_Pt,pt.Cont_Date, pt.sk_Fact_Pt_Enc_Clrt
