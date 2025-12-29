USE DS_HSDW_Prod
-- 12/06/2024 163005
SELECT
       [PROV_ID] AS Atn_Dr_Prov_Id
      ,[Prov_Nme] AS Atn_Dr_Prov_Nme
      ,[Prov_Typ]
      --,[Identity_ID]
      --,[Raw_Identity_Id]
      ,ser.[sk_Dim_Physcn]
	  ,physcn.IDNumber AS Atn_Dr_SMS_Id
      --,[Usr_ID]
      ,[Staff_Resource]
      ,[Active_Status]
      ,[NPI]
  FROM [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc] ser
  LEFT OUTER JOIN
  (
  SELECT
	*
  FROM DS_HSDW_Prod.dbo.Dim_Physcn
  WHERE current_flag = 1
  ) physcn
  ON physcn.sk_Dim_Physcn = ser.sk_Dim_Physcn

  WHERE ser.Prov_Nme LIKE '%Reed%'

 -- ORDER BY
	--ser.PROV_ID
  ORDER BY
	ser.Prov_Nme