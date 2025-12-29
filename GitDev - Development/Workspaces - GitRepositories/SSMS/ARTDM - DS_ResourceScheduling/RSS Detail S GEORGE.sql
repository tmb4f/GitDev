USE [DS_ResourceScheduling]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET NOCOUNT ON

IF OBJECT_ID('tempdb..#ScheduledAppointment ') IS NOT NULL
DROP TABLE #ScheduledAppointment

IF OBJECT_ID('tempdb..#ScheduledAppointmentDetail ') IS NOT NULL
DROP TABLE #ScheduledAppointmentDetail

SELECT 
       ra.[Med_Rec_No]
	  ,CAST(ra.[Med_Rec_No] AS INT) AS MRN_int
      ,ra.[Appt_Start_Date_Time]
	  ,ddte.date_key AS [Appt_Start_Date]
      ,CAST(SUBSTRING(CONVERT(VARCHAR(19),ra.[Appt_Start_Date_Time],20),12,2) +
            SUBSTRING(CONVERT(VARCHAR(19),ra.[Appt_Start_Date_Time],20),15,2) AS INTEGER) AS [Appt_Start_Time]
	  ,ddte.day_date AS [Appt_Date]
	  ,ddte.year_num AS [Appt_Date_Year]
	  ,ddte.month_num AS [Appt_Date_Month]
	  ,ddte.month_name AS [Appt_Date_Month_Name]
	  ,ddte.month_short_name AS [Appt_Date_Month_Short_Name]
	  ,ddte.day_of_week_num AS [Appt_Day_Of_Week]
      ,ra.[Appt_Stop_Date_Time]
      ,ra.[RSS_Group]
      ,ra.[RSS_Location]
	  ,dcu.[Care_Unit]
	  ,dcu.[Care_Unit_Descr]
	  ,dsi.[Site]
	  ,dsi.[Site_Descr]
	  ,COALESCE(CAD.POD, 'Unknown') AS POD
      ,ra.[Appt_Activity_Type]
      ,ra.[Appt_Activity_Desc]
      ,ra.[Appt_Status]
	  ,dst.Appt_Status_Descr
      ,ra.[Appt_Status_Date_Time]
      ,ra.[Appt_Process_Date_Time]
      ,ra.[Appt_Create_Date_Time]
      ,ra.[Appt_Create_UserId]
      ,ra.[Appt_Link_Code]
      ,ra.[Resource_Code]
	  --,udn.[Attending]
	  --,dphys.[DisplayName]
      ,ra.[Appt_Status_Reason]
	  ,ud.[ApptComment]
	  ,ud.Inmate
INTO #ScheduledAppointment
FROM [dbo].[Scheduled_Appointment_Current] ra
LEFT JOIN [dbo].[Scheduled_Appointment_UD_Current] ud
ON ((ra.[Appt_Link_Code] = ud.[Appt_Link_Code]) AND
    (ra.Appt_Create_Date_Time = ud.Appt_Create_Date_Time))
LEFT OUTER JOIN DS_EnterpriseDataIntegration.dbo.Dim_RSS_Group dgrp
ON ((ra.RSS_Group = dgrp.RSS_Group) AND
    (dgrp.Dim_Act_Ind = 'Y'))
LEFT OUTER JOIN DS_EnterpriseDataIntegration.dbo.Dim_RSS_Location dloc
ON ((ra.RSS_Location = dloc.RSS_Location) AND
    (dloc.Dim_Act_Ind = 'Y'))
LEFT OUTER JOIN DS_EnterpriseDataIntegration.dbo.Dim_Care_Unit dcu WITH(NOLOCK)
ON ((dcu.Dim_Care_Unit_sk = dgrp.Dim_Care_Unit_sk) AND
    (dcu.Dim_Act_Ind = 'Y'))
LEFT OUTER JOIN DS_EnterpriseDataIntegration.dbo.Dim_Site dsi WITH(NOLOCK)
ON ((dsi.Dim_Site_sk = dloc.Dim_Site_sk) AND
    (dsi.Dim_Act_Ind = 'Y'))
LEFT OUTER JOIN DS_EnterpriseDataIntegration.dbo.Dim_Appt_Status dst WITH(NOLOCK)
ON dst.Appt_Status = ra.Appt_Status
--LEFT OUTER JOIN DS_PatientScheduling.dbo.Patient_Appointment_Current pa
--ON ((pa.Appt_Link_Code = STUFF(ra.Appt_Link_Code, LEN(ra.Appt_Link_Code), 1, '1')) AND
--    (pa.Appt_Create_Date_Time = ra.Appt_Create_Date_Time))
--LEFT OUTER JOIN DS_PatientScheduling.dbo.[Patient_Appointment_UD_Current (NETLIST)] udn
--ON ((udn.Appt_Link_Code = pa.Appt_Link_Code) AND
--    (udn.Appt_Create_Date_Time = pa.Appt_Create_Date_Time))
--LEFT OUTER JOIN DS_HSODS_Prod.Rptg.vwDim_Physcn dphys
--ON ((dphys.IDNumber = CAST(udn.Attending AS INTEGER)) AND
--    (dphys.current_flag = 1))
LEFT OUTER JOIN (SELECT DISTINCT
					RSSGroup
				  , RSS_Location
				  , POD
                 FROM DS_PatientScheduling.Rptg.VwRef_Epic_POD_Dept_CadenceSrcd
				) CAD
ON ra.RSS_Group = CAD.RSSGroup and ra.RSS_Location = cad.RSS_Location
INNER JOIN DS_HSODS_Prod.Rptg.vwDim_Date ddte
ON ddte.day_date = CAST(CAST(ra.Appt_Start_Date_Time AS DATE) AS SMALLDATETIME)
--INNER JOIN DS_HSODS_Prod.ETL.fn_ParmParse(@CareUnit, ',') prmcu
--ON dcu.Care_Unit = prmcu.[Param]
--INNER JOIN DS_HSODS_Prod.ETL.fn_ParmParse(@Site, ',') prmsi
--ON dsi.Site = prmsi.[Param]
WHERE ISNUMERIC(ra.Med_Rec_No) = 1-- AND
--((dcu.Care_Unit = @CareUnit) AND (dsi.[Site] = @Site)) AND
--((ra.[Appt_Start_Date_Time] >= @strBegindt) AND
-- (ra.[Appt_Start_Date_Time] <= @strEnddt))
AND ra.Resource_Code = 'S GEORGE'

CREATE UNIQUE CLUSTERED INDEX IX_ScheduledAppointment ON #ScheduledAppointment ([MRN_int], [Appt_Status_Descr], [Appt_Link_Code], [Appt_Create_Date_Time], [POD])

SELECT ra.Med_Rec_No
      ,ra.MRN_int
      ,ra.Appt_Date
	  ,CONVERT(VARCHAR(2),ra.Appt_Start_Time/100) + ':' + RIGHT('00' + CONVERT(VARCHAR(2),ra.Appt_Start_Time % 100),2) AS Appt_Time
	  ,ra.Appt_Date_Year
	  ,ra.Appt_Date_Month
	  ,ra.Appt_Date_Month_Name
	  ,ra.Appt_Date_Month_Short_Name
	  ,ra.Appt_Start_Time AS Appt_Date_Time
	  ,ra.Appt_Status
	  ,ra.Appt_Status_Descr
	  ,dpt.PT_LNAME AS [Patient_Last_Name]
	  ,dpt.PT_FNAME_MI AS [Patient_First_Name]
	  ,dpt.PT_RACE AS [Patient_Ethnicity]
	  ,dpt.PT_SEX AS [Patient_Gender]
      ,DATEDIFF(YY, dpt.BIRTH_DT, ra.Appt_Date) - 
        CASE 
          WHEN((MONTH(dpt.BIRTH_DT)*100 + DAY(dpt.BIRTH_DT)) >
               (MONTH(ra.Appt_Date)*100 + DAY(ra.Appt_Date))) THEN 1
          ELSE 0
        END AS Age
	  ,ra.Appt_Activity_Type
	  ,ra.Appt_Activity_Desc
	  ,ra.RSS_Group
	  ,ra.RSS_Location
	  ,ra.Care_Unit_Descr
	  ,ra.[Site_Descr]
	  ,ra.[POD]
	  ,ra.Appt_Status_Reason
	  ,ra.ApptComment AS [Appt_Comment]
	  ,dpt.BIRTH_DT AS [Patient_Birth_Date]
	  ,dpt.CURR_PT_ADDR1 AS [Patient_Address_1]
	  ,dpt.CURR_PT_ADDR2 AS [Patient_Address_2]
	  ,dpt.CURR_PT_CITY AS [Patient_City]
	  ,dpt.CURR_PT_STATE AS [Patient_State]
	  ,dpt.CURR_PT_ZIP AS [Patient_Zipcode]
	  ,dpt.INS_1 AS [Insurance_Plan_Code_1]
	  ,dpt.INS_1_VERIF AS [Insurance_Name_1]
	  ,dpt.INS_2 AS [Insurance_Plan_Code_2]
	  ,dpt.INS_2_VERIF AS [Insurance_Name_2]
	  ,dpt.INS_3 AS [Insurance_Plan_Code_3]
	  ,dpt.INS_3_VERIF AS [Insurance_Name_3]
	  ,dpt.INS_4 AS [Insurance_Plan_Code_4]
	  ,dpt.INS_4_VERIF AS [Insurance_Name_4]
	  ,ra.Appt_Link_Code
	  ,ra.Resource_Code
	  --,ra.Attending
	  --,ra.DisplayName
	  ,dpt.PT_LANGUAGE AS [Patient_Language]
	  ,ra.Inmate
INTO #ScheduledAppointmentDetail
FROM #ScheduledAppointment ra
LEFT OUTER JOIN DS_HSODS_Prod.Rptg.vwDim_Pt dpt WITH(NOLOCK)
ON dpt.MED_REC = ra.MRN_int
--INNER JOIN DS_HSODS_Prod.ETL.fn_ParmParse(@Status, ',') st
--ON ra.Appt_Status_Descr = st.[Param]

if OBJECT_ID('tempdb..#RptgTemp ') is not null
DROP TABLE #RptgTemp

SELECT A.*
	,'Rptg.uspSrc_RSS_Resource_Scheduling' AS [ETL_guid]
	, GETDATE() AS Load_Dte
 INTO #RptgTemp FROM
 (
 SELECT [Med_Rec_No] AS [MRN]
       ,[Appt_Date] AS [Appt Date]
       ,[Appt_Time] AS [Appt Time]
	   ,[Appt_Date_Time] AS [Appt Date Time]
       ,[Appt_Status] AS [Appt Status]
       ,[Appt_Status_Descr] AS [Appt Status Descr]
	   ,[Appt_Status_Reason] AS [Appt Status Reason]
       ,[Patient_Last_Name] AS [Pt Last Name]
       ,[Patient_First_Name] AS [Pt First Name MI]
       ,[Patient_Ethnicity] AS [Pt Ethnicity]
       ,[Patient_Language] AS [Pt Language]
       ,[Patient_Gender] AS [Pt Gender]
	   --,[Inmate] AS [Inmate]
       ,[Age]
       ,[Appt_Activity_Type] AS [Activity Type]
       ,[Appt_Activity_Desc] AS [Activity Descr]
       ,[Appt_Comment] AS [Appt Descr]
       ,[RSS_Group] AS [RSS Group]
       ,[RSS_Location] AS [RSS Location]
       ,[Resource_Code] AS [RSS Resource]
	   ,[Care_Unit_Descr] AS [Care Unit]
	   ,[Site_Descr] AS [Site]
	   ,[POD] AS [Pod]
       ,[Patient_Birth_Date] AS [Pt Birth Date]
       ,[Patient_Address_1] AS [Pt Addr 1]
       ,[Patient_Address_2] AS [Pt Addr 2]
       ,[Patient_City] AS [Pt City]
       ,[Patient_State] AS [Pt State]
       ,[Patient_Zipcode] AS [Pt Zipcode]
       ,[Insurance_Name_1] AS [Ins 1]
       ,[Insurance_Name_2] AS [Ins 2]
       ,[Insurance_Name_3] AS [Ins 3]
       ,[Insurance_Name_4] AS [Ins 4]
       ,[Appt_Date_Year] AS [Appt Date Year]
       ,[Appt_Date_Month] AS [Appt Date Month]
       ,[Appt_Date_Month_Name] AS [Appt Date Month Name]
       ,[Appt_Date_Month_Short_Name] AS [Appt Date Month Short Name]
	   --,[Attending] AS [Attending]
	   --,[ra].[DisplayName] AS [Atn Name]
 FROM #ScheduledAppointmentDetail ra
) A

SELECT   *
FROM #RptgTemp
ORDER BY [Appt Date]
        ,[Appt Date Time]
        ,[MRN]
	   
--	--Drop temp tables

IF OBJECT_ID('tempdb..#ScheduledAppointment ') IS NOT NULL
DROP TABLE #ScheduledAppointment

IF OBJECT_ID('tempdb..#ScheduledAppointmentDetail ') IS NOT NULL
DROP TABLE #ScheduledAppointmentDetail

if OBJECT_ID('tempdb..#RptgTemp ') is not null
Drop table #RptgTemp

GO
