USE [CDW_App_Dev]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA='Rptg'
    AND TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME='TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider_20251118')
   DROP TABLE [Rptg].[TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider_20251118]
GO

--IF EXISTS(SELECT * FROM [Rptg].[TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider])
--	DROP TABLE [Rptg].[TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider]

CREATE TABLE [Rptg].[TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider_20251118](
	[Submitter_Name] [VARCHAR](150) NULL,
	[Submitter_Email] [VARCHAR](150) NULL,
	[Provider_Name] [VARCHAR](150) NULL,
	[Provider_Email] [VARCHAR](150) NULL,
	[Provider_Duplicate] [VARCHAR](150) NULL,
	[Rollout_Wave_Assignment] [VARCHAR](150) NULL,
	[Provider_Affiliation] [VARCHAR](150) NULL,
	[Provider_Total_FTE] [VARCHAR](150) NULL,
	[Provider_Clinic_FTE] [VARCHAR](150) NULL,
	[Provider_Specialty] [VARCHAR](150) NULL,
	[Provider_Location] [VARCHAR](150) NULL,
	[Provider_Weekdays_In_Clinic] [VARCHAR](MAX) NULL,
	[Notes] [VARCHAR](MAX) NULL,
	[Provider_Champion] [VARCHAR](150) NULL,
	[Provider_License_Status] [VARCHAR](150) NULL,
	[Provider_EmployeeEpicId] [VARCHAR](150) NULL
) ON [PRIMARY]
GO

GRANT DELETE, INSERT, SELECT, UPDATE ON [Rptg].[TEMP_tmb4f_TFS54501_Nuance_DAX_Study_Provider_20251118] TO [HSCDOM\Decision Support]
GO


