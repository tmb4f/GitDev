USE DS_HSDM_APP

SELECT DISTINCT
       [fyear_num]
      ,[metric]
      ,[Load_Dtm]
  FROM [DS_HSDM_APP].[TabRptg].[Dash_AmbOpt_ClinicRanking_Details]
  ORDER BY
	fyear_num,
	metric