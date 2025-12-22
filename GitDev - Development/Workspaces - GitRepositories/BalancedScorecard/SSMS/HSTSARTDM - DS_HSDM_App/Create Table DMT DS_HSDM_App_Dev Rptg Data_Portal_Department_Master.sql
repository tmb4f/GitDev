USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Rptg].[Data_Portal_Department_Master](
	[key] [varchar](50) NULL,
	[level] [varchar](50) NULL,
	[hsArea_sid] [smallint] NULL,
	[sid] [VARCHAR](150) NULL,
	[display] [CHAR](5) NULL,
	[name_external] [VARCHAR](255) NULL,
	[name] [VARCHAR](255) NULL,
	[service_line_id] [INT] NULL,
	[service_line_type] [VARCHAR](150) NULL,
	[sub_service_line_id] [INT] NULL,
	[pod_id] [VARCHAR](66) NULL,
	[hub_id] [VARCHAR](66) NULL,
	[practice_group_id] [INT] NULL,
	[practice_id] [INT] NULL,
	[group_id] [INT] NULL,
	[department_id] [VARCHAR](150) NULL,
	[financial_division_id] [INT] NULL,
	[rev_location_id] [INT] NULL,
	[is_upg] [CHAR](5) NULL,
	[mc_som_id] [INT] NULL,
	[outpatient_flu] [CHAR](5) NULL,
	[ambulatory_scorecard] [CHAR](5) NULL
)

GRANT DELETE, INSERT, SELECT, UPDATE ON [Rptg].[Data_Portal_Department_Master] TO [HSCDOM\Decision Support]
GO