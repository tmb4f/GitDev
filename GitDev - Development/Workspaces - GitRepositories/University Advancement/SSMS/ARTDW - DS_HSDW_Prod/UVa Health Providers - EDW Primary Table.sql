USE DS_HSDW_Prod
-- 12/06/2024 163005
/*
SELECT
	COALESCE(GTNEG1.EDW_Atn_Dr_Prov_Nme, GTNEG1.Epic_Atn_Dr_Prov_Nme) AS Prov_Nme,
	GTNEG1.EDW_Atn_Dr_Prov_Nme,
    GTNEG1.EDW_Prov_Typ,
    GTNEG1.EDW_sk_Dim_Physcn,
    GTNEG1.EDW_Atn_Dr_SMS_Id,
    GTNEG1.EDW_Staff_Resource,
    GTNEG1.EDW_Active_Status,
    GTNEG1.EDW_NPI,
	GTNEG1.EDW_LastUpdate,
    GTNEG1.Epic_Atn_Dr_Prov_Id,
    GTNEG1.Epic_Atn_Dr_Prov_Nme,
    GTNEG1.Epic_Prov_Typ,
    GTNEG1.Epic_sk_Dim_Physcn,
    GTNEG1.Epic_Atn_Dr_SMS_Id,
    GTNEG1.Epic_Staff_Resource,
    GTNEG1.Epic_Active_Status,
    GTNEG1.Epic_NPI,
	GTNEG1.Epic_LastUpdate
FROM
(
SELECT
	physcn.EDW_Atn_Dr_Prov_Nme,
    physcn.EDW_Prov_Typ,
    physcn.sk_Dim_Physcn AS EDW_sk_Dim_Physcn,
    physcn.EDW_Atn_Dr_SMS_Id,
    physcn.EDW_Staff_Resource,
    physcn.EDW_Active_Status,
    physcn.EDW_NPI,
	physcn.EDW_LastUpdate,
    ser.Epic_Atn_Dr_Prov_Id,
    ser.Epic_Atn_Dr_Prov_Nme,
    ser.Epic_Prov_Typ,
    ser.sk_Dim_Physcn AS Epic_sk_Dim_Physcn,
    ser.Epic_Atn_Dr_SMS_Id,
    ser.Epic_Staff_Resource,
    ser.Epic_Active_Status,
    ser.Epic_NPI,
	ser.Epic_LastUpdate

FROM
(
SELECT
	DisplayName AS EDW_Atn_Dr_Prov_Nme
   ,ProviderType AS EDW_Prov_Typ
   ,sk_Dim_Physcn
   ,IDNumber AS EDW_Atn_Dr_SMS_Id
   ,ProviderGroup AS EDW_Staff_Resource
   ,Status AS EDW_Active_Status
   ,NPINumber AS EDW_NPI
   --,lastupdate AS EDW_LastUpdate
   ,load_dte AS EDW_LastUpdate
FROM dbo.Dim_Physcn
WHERE current_flag = 1
AND sk_Dim_Physcn > 0
) physcn
LEFT OUTER JOIN
(
SELECT
       [PROV_ID] AS Epic_Atn_Dr_Prov_Id
      ,[Prov_Nme] AS Epic_Atn_Dr_Prov_Nme
      ,[Prov_Typ] AS Epic_Prov_Typ
      --,[Identity_ID]
      --,[Raw_Identity_Id]
      ,[sk_Dim_Physcn]
	  , Identity_ID AS Epic_Atn_Dr_SMS_Id
      --,[Usr_ID]
      ,[Staff_Resource] AS Epic_Staff_Resource
      ,[Active_Status] AS Epic_Active_Status
      ,CAST([NPI] AS VARCHAR(12)) AS Epic_NPI
	  --,Updte_Dtm AS Epic_LastUpdate
	  ,Load_Dte AS Epic_LastUpdate
  FROM [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc]
  ) ser
  ON ser.sk_Dim_Physcn = physcn.sk_Dim_Physcn
UNION ALL
SELECT
	ser.EDW_Atn_Dr_Prov_Nme,
    ser.EDW_Prov_Typ,
    ser.EDW_sk_Dim_Physcn,
    ser.EDW_Atn_Dr_SMS_Id,
    ser.EDW_Staff_Resource,
    ser.EDW_Active_Status,
    ser.EDW_NPI,
	ser.EDW_LastUpdate,
    ser.Epic_Atn_Dr_Prov_Id,
    ser.Epic_Atn_Dr_Prov_Nme,
    ser.Epic_Prov_Typ,
    ser.Epic_sk_Dim_Physcn,
    ser.Epic_Atn_Dr_SMS_Id,
    ser.Epic_Staff_Resource,
    ser.Epic_Active_Status,
    ser.Epic_NPI,
	ser.Epic_LastUpdate
FROM
(
SELECT
		NULL AS EDW_Atn_Dr_Prov_Nme
	   ,NULL AS EDW_Prov_Typ
	   ,NULL AS EDW_sk_Dim_Physcn
	   ,NULL AS EDW_Atn_Dr_SMS_Id
	   ,NULL AS EDW_Staff_Resource
	   ,NULL AS EDW_Active_Status
	   ,NULL AS EDW_NPI
	   ,NULL AS EDW_LastUpdate
       ,[PROV_ID] AS Epic_Atn_Dr_Prov_Id
       ,[Prov_Nme] AS Epic_Atn_Dr_Prov_Nme
       ,[Prov_Typ] AS Epic_Prov_Typ
       ,[sk_Dim_Physcn] AS Epic_sk_Dim_Physcn
	   ,Identity_ID AS Epic_Atn_Dr_SMS_Id
       ,[Staff_Resource] AS Epic_Staff_Resource
       ,[Active_Status] AS Epic_Active_Status
       ,CAST([NPI] AS VARCHAR(12)) AS Epic_NPI
	   --,Updte_Dtm AS Epic_LastUpdate
	   ,Load_Dte AS Epic_LastUpdate
  FROM [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc]
  WHERE sk_Dim_Physcn IN (-6,-5,-4,-3,-1,0)
  ) ser
  ) GTNEG1

 -- ORDER BY
	--ser.PROV_ID
  ORDER BY
	COALESCE(GTNEG1.EDW_Atn_Dr_Prov_Nme, GTNEG1.Epic_Atn_Dr_Prov_Nme)
*/
SELECT
	GTNEG1.Epic_Atn_Dr_Prov_Id AS Prov_Id,
	--COALESCE(GTNEG1.Epic_Atn_Dr_Prov_Nme,GTNEG1.EDW_Atn_Dr_Prov_Nme) AS Prov_Name,
	GTNEG1.EDW_Atn_Dr_Prov_Nme AS EDW_Prov_Nme,
	GTNEG1.Epic_Atn_Dr_Prov_Nme AS Epic_Prov_Nme,
	COALESCE(GTNEG1.Epic_Prov_Typ,GTNEG1.EDW_Prov_Typ) AS Prov_Typ,
    COALESCE(GTNEG1.Epic_sk_Dim_Physcn,GTNEG1.EDW_sk_Dim_Physcn) AS sk_Dim_Physcn,
    COALESCE(GTNEG1.Epic_Atn_Dr_SMS_Id,GTNEG1.EDW_Atn_Dr_SMS_Id) AS SMS_Id,
    COALESCE(GTNEG1.EDW_Staff_Resource,GTNEG1.Epic_Staff_Resource) AS Staff_Resource,
    COALESCE(GTNEG1.Epic_Active_Status,GTNEG1.EDW_Active_Status) AS [Active_Status],
    COALESCE(GTNEG1.Epic_NPI,GTNEG1.EDW_NPI) AS NPI,
	COALESCE(GTNEG1.Epic_LastUpdate,GTNEG1.EDW_LastUpdate) AS Load_Date
FROM
(
SELECT
	physcn.EDW_Atn_Dr_Prov_Nme,
    physcn.EDW_Prov_Typ,
    physcn.sk_Dim_Physcn AS EDW_sk_Dim_Physcn,
    physcn.EDW_Atn_Dr_SMS_Id,
    physcn.EDW_Staff_Resource,
    physcn.EDW_Active_Status,
    physcn.EDW_NPI,
	physcn.EDW_LastUpdate,
    ser.Epic_Atn_Dr_Prov_Id,
    ser.Epic_Atn_Dr_Prov_Nme,
    ser.Epic_Prov_Typ,
    ser.sk_Dim_Physcn AS Epic_sk_Dim_Physcn,
    ser.Epic_Atn_Dr_SMS_Id,
    ser.Epic_Staff_Resource,
    ser.Epic_Active_Status,
    ser.Epic_NPI,
	ser.Epic_LastUpdate

FROM
(
SELECT
	DisplayName AS EDW_Atn_Dr_Prov_Nme
   ,ProviderType AS EDW_Prov_Typ
   ,sk_Dim_Physcn
   ,IDNumber AS EDW_Atn_Dr_SMS_Id
   ,ProviderGroup AS EDW_Staff_Resource
   ,Status AS EDW_Active_Status
   ,NPINumber AS EDW_NPI
   --,lastupdate AS EDW_LastUpdate
   ,load_dte AS EDW_LastUpdate
FROM dbo.Dim_Physcn
WHERE current_flag = 1
AND sk_Dim_Physcn > 0
) physcn
LEFT OUTER JOIN
(
SELECT
       [PROV_ID] AS Epic_Atn_Dr_Prov_Id
      ,[Prov_Nme] AS Epic_Atn_Dr_Prov_Nme
      ,[Prov_Typ] AS Epic_Prov_Typ
      --,[Identity_ID]
      --,[Raw_Identity_Id]
      ,[sk_Dim_Physcn]
	  , Identity_ID AS Epic_Atn_Dr_SMS_Id
      --,[Usr_ID]
      ,[Staff_Resource] AS Epic_Staff_Resource
      ,[Active_Status] AS Epic_Active_Status
      ,CAST([NPI] AS VARCHAR(12)) AS Epic_NPI
	  --,Updte_Dtm AS Epic_LastUpdate
	  ,Load_Dte AS Epic_LastUpdate
  FROM [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc]
  ) ser
  ON ser.sk_Dim_Physcn = physcn.sk_Dim_Physcn
UNION ALL
SELECT
	ser.EDW_Atn_Dr_Prov_Nme,
    ser.EDW_Prov_Typ,
    ser.EDW_sk_Dim_Physcn,
    ser.EDW_Atn_Dr_SMS_Id,
    ser.EDW_Staff_Resource,
    ser.EDW_Active_Status,
    ser.EDW_NPI,
	ser.EDW_LastUpdate,
    ser.Epic_Atn_Dr_Prov_Id,
    ser.Epic_Atn_Dr_Prov_Nme,
    ser.Epic_Prov_Typ,
    ser.Epic_sk_Dim_Physcn,
    ser.Epic_Atn_Dr_SMS_Id,
    ser.Epic_Staff_Resource,
    ser.Epic_Active_Status,
    ser.Epic_NPI,
	ser.Epic_LastUpdate
FROM
(
SELECT
		NULL AS EDW_Atn_Dr_Prov_Nme
	   ,NULL AS EDW_Prov_Typ
	   ,NULL AS EDW_sk_Dim_Physcn
	   ,NULL AS EDW_Atn_Dr_SMS_Id
	   ,NULL AS EDW_Staff_Resource
	   ,NULL AS EDW_Active_Status
	   ,NULL AS EDW_NPI
	   ,NULL AS EDW_LastUpdate
       ,[PROV_ID] AS Epic_Atn_Dr_Prov_Id
       ,[Prov_Nme] AS Epic_Atn_Dr_Prov_Nme
       ,[Prov_Typ] AS Epic_Prov_Typ
       ,[sk_Dim_Physcn] AS Epic_sk_Dim_Physcn
	   ,Identity_ID AS Epic_Atn_Dr_SMS_Id
       ,[Staff_Resource] AS Epic_Staff_Resource
       ,[Active_Status] AS Epic_Active_Status
       ,CAST([NPI] AS VARCHAR(12)) AS Epic_NPI
	   --,Updte_Dtm AS Epic_LastUpdate
	   ,Load_Dte AS Epic_LastUpdate
  FROM [DS_HSDW_Prod].[dbo].[Dim_Clrt_SERsrc]
  WHERE sk_Dim_Physcn IN (-6,-5,-4,-3,-1,0)
  ) ser
  ) GTNEG1

 -- ORDER BY
	--ser.PROV_ID
  ORDER BY
	COALESCE(GTNEG1.Epic_Atn_Dr_Prov_Nme,GTNEG1.EDW_Atn_Dr_Prov_Nme)