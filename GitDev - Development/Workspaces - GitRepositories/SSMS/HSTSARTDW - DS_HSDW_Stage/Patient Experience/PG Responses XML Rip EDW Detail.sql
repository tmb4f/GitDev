USE DS_HSDW_Stage

IF OBJECT_ID('tempdb..#surveys ') IS NOT NULL
DROP TABLE #surveys

IF OBJECT_ID('tempdb..#surveys2 ') IS NOT NULL
DROP TABLE #surveys2

IF OBJECT_ID('tempdb..#surveys3 ') IS NOT NULL
DROP TABLE #surveys3

-----------------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT
	surveys.RECDATE,
    surveys.SURVEY_ID,
	surveys.VARNAME,
    surveys.O4,
    surveys.Load_Dtm
INTO #surveys
FROM
(
SELECT-- DISTINCT
       [RECDATE],
       [SURVEY_ID],
	   [VARNAME],
	   VALUE AS O4,
	   Load_Dtm,
	   ROW_NUMBER() OVER(PARTITION BY RECDATE, SURVEY_ID ORDER BY Load_Dtm DESC) AS seq

  --INTO #surveys

  --FROM [DS_HSDW_Stage].[PressGaney].[PG_Responses_xmlrip]
  FROM [DS_HSDW_Stage].[PressGaney].[PG_Responses_xmlrip_OAS_CAHPS]
  --WHERE RECDATE >= '4/1/2023 00:00' AND RECDATE <= '4/30/2023 23:59'
  --WHERE RECDATE >= '3/20/2024 00:00' AND RECDATE <= '4/1/2024 23:59'
  --WHERE RECDATE >= '10/1/2023 00:00' AND RECDATE <= '5/31/2024 23:59'
  --WHERE RECDATE >= '7/1/2023 00:00' AND RECDATE <= '5/31/2024 23:59'
  WHERE RECDATE >= '5/1/2024 00:00' AND RECDATE <= '5/31/2024 23:59'
  --AND CLIENT_ID = 2561
  --AND SERVICE = 'PD'
  AND SERVICE = 'MD'
  AND VARNAME = 'O4'
  --AND VARNAME = 'ITSERVTY'
  AND VALUE IS NOT NULL
  --AND SUBSTRING(VALUE,1,6) = 'PD0101'
  ) surveys
  WHERE surveys.seq = 1

  SELECT *
  FROM #surveys
  ORDER BY RECDATE, SURVEY_ID

SELECT DISTINCT
       surveys.[RECDATE],
       surveys.[SURVEY_ID],
	   surveys.VARNAME,
	   xml.VALUE AS ITSERVTY,
	   surveys.O4,
	   surveys.Load_Dtm--,
	   --xml.VALUE AS ITPAT_CL

  INTO #surveys2

  --FROM [DS_HSDW_Stage].[PressGaney].[PG_Responses_xmlrip]
  FROM [DS_HSDW_Stage].[PressGaney].[PG_Responses_xmlrip_OAS_CAHPS] xml
  INNER JOIN #surveys surveys
  ON xml.SURVEY_ID = surveys.SURVEY_ID
  AND xml.Load_Dtm = surveys.Load_Dtm
  --WHERE xml.VARNAME = 'ITPAT_CL'
  WHERE xml.VARNAME = 'ITSERVTY'
  --AND xml.VALUE = '102'
  --WHERE xml.VARNAME = 'ITDEPT_I'

  --SELECT *
  --FROM #surveys2
  --ORDER BY RECDATE, SURVEY_ID

SELECT DISTINCT
       surveys.[RECDATE],
       surveys.[SURVEY_ID],
	   surveys.VARNAME,
	   --surveys.ITSERVTY,
	   surveys.O4,
	   surveys.Load_Dtm,
	   --surveys.ITPAT_CL,
	   surveys.ITSERVTY,
	   xml.VALUE AS ITDEPT_I

  INTO #surveys3

  --FROM [DS_HSDW_Stage].[PressGaney].[PG_Responses_xmlrip]
  FROM [DS_HSDW_Stage].[PressGaney].[PG_Responses_xmlrip_OAS_CAHPS] xml
  INNER JOIN #surveys2 surveys
  ON xml.SURVEY_ID = surveys.SURVEY_ID
  AND xml.Load_Dtm = surveys.Load_Dtm
  WHERE xml.VARNAME = 'ITDEPT_I'

  --SELECT *
  --FROM #surveys3
  --ORDER BY RECDATE, SURVEY_ID

SELECT
	resp.SURVEY_ID,
    resp.RECDATE,
	--xml.ITSERVTY,
	xml.O4,
	xml.Load_Dtm,
	--xml.ITPAT_CL,
	xml.ITSERVTY,
	xml.ITDEPT_I,
	resp.Svc_Cde,
	resp.sk_Dim_PG_Question,
	resp.QUESTION_TEXT,
    resp.sk_Fact_Pt_Acct,
    resp.PG_AcctNbr,
    resp.Pat_Enc_CSN_Id,
	acct.MRN_int,
	acct.PtAcctg_Typ,
	dep.Clrt_DEPt_Nme,
	dep2.DEPARTMENT_ID,
	dep2.Clrt_DEPt_Nme AS dep2_Clrt_DEPt_Nme
FROM
(
SELECT DISTINCT
	SURVEY_ID,
	RECDATE,
	resp.Svc_Cde,
	sk_Fact_Pt_Acct,
	PG_AcctNbr,
	Pat_Enc_CSN_Id,
	sk_Dim_Clrt_DEPt,
	resp.sk_Dim_PG_Question,
	qust.QUESTION_TEXT,
	qust.VARNAME
FROM DS_HSDW_Prod.dbo.Fact_PressGaney_Responses resp
LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_PG_Question qust
ON qust.sk_Dim_PG_Question = resp.sk_Dim_PG_Question
) resp
INNER JOIN #surveys3 xml
ON resp.SURVEY_ID = xml.SURVEY_ID
AND resp.VARNAME = xml.VARNAME
LEFT OUTER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Acct_Aggr acct
ON acct.sk_Fact_Pt_Acct = resp.sk_Fact_Pt_Acct
LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
ON dep.DEPARTMENT_ID = CAST(xml.ITDEPT_I AS NUMERIC(18,0))
LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep2
ON dep2.sk_Dim_Clrt_DEPt = resp.sk_Dim_Clrt_DEPt
--ORDER BY resp.RECDATE, resp.SURVEY_ID
ORDER BY resp.RECDATE, resp.SURVEY_ID, xml.Load_Dtm

SELECT DISTINCT
	resp.SURVEY_ID
FROM
(
SELECT DISTINCT
	SURVEY_ID,
	RECDATE,
	Svc_Cde,
	sk_Fact_Pt_Acct,
	PG_AcctNbr,
	Pat_Enc_CSN_Id,
	sk_Dim_Clrt_DEPt
FROM DS_HSDW_Prod.dbo.Fact_PressGaney_Responses
) resp
INNER JOIN #surveys3 xml
ON resp.SURVEY_ID = xml.SURVEY_ID
LEFT OUTER JOIN DS_HSDW_Prod.dbo.Fact_Pt_Acct_Aggr acct
ON acct.sk_Fact_Pt_Acct = resp.sk_Fact_Pt_Acct
LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep
ON dep.DEPARTMENT_ID = CAST(xml.ITDEPT_I AS NUMERIC(18,0))
LEFT OUTER JOIN DS_HSDW_Prod.dbo.Dim_Clrt_DEPt dep2
ON dep2.sk_Dim_Clrt_DEPt = resp.sk_Dim_Clrt_DEPt
--ORDER BY resp.RECDATE, resp.SURVEY_ID
ORDER BY resp.SURVEY_ID
