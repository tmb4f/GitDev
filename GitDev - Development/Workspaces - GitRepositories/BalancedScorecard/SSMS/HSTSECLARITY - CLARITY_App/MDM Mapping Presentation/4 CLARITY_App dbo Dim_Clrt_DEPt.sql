/******	Dim_Clrt_DEPt
1.	Single table contaiing commonly reported Epic-documented department attributes
2.	May be a collection of columns from all of the CLARITY_DEP{_#} tables
2.	Rarely used in CLARITY_App queries
3.	Layout per the EDW Dimensional Model structure, ex., rarely will you see a NULL name column
******/
SELECT [sk_Dim_Clrt_DEPt]
      ,[DEPARTMENT_ID]
      ,[Clrt_DEPt_Nme]
      ,[Clrt_DEPt_Abbrv]
      ,[Clrt_DEPt_Spclty]
      ,[LOC_ID]
      ,[HUB]	/*	Hub Name */
      ,[POD]	/* Pod Name */
      ,[Clrt_DEPt_Lic_Beds]
      ,[Clrt_DEPt_Ext_Nme]
      ,[Clrt_DEPt_Phn_Num]
      ,[Clrt_DEPt_Addr_Cty]
      ,[Clrt_DEPt_Addr_Zip]
      ,[Clrt_DEPt_Addr_St]
      ,[Clrt_DEPt_Loctn_Nme]
      ,[Clrt_DEPt_Loctn_POS_typ]
      ,[Clrt_DEPt_Loctn_Abbrv]
      ,[Clrt_DEPt_Svc_Area_Nme]
      ,[Clrt_DEPt_Svc_Area_Abbrv]
      ,[Clrt_DEPt_Svc_Area_Typ]
      ,[Clrt_DEPt_Billg_Nme]
      ,[Clrt_DEPt_Billg_Abbrv]
      ,[Rec_Sts]
      ,[Prov_Based_Clinic]
      ,[bchksum_DEP]
      ,[Updte_Dtm]
      ,[ETL_guid]
      ,[Load_Dte]
      ,[Reimb_340B_Eligible_Clinic]
      ,[Clrt_DEPt_Typ]
      ,[PBB_NonPBB]
      ,[MSPQ]
      ,[GL_Business_Unit]
      ,[GL_Operating_Unit]
      ,[GL_Cde]
      ,[DEPt_Rec_Sts]
      ,[ICU_Dept_YN]
      ,[SERV_AREA_ID]
      ,[BILLING_SYSTEM_C]
      ,[Service_Line]
      ,[PG_Survey_Designator]
  FROM [CLARITY_App].[dbo].[Dim_Clrt_DEPt]
  ORDER BY LOC_ID, DEPARTMENT_ID