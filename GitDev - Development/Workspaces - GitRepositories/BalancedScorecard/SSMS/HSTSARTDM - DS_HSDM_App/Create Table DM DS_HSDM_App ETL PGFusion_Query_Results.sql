USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [ETL].[PGFusion_Query_Results](
	[Service] [varchar](50) NULL,
	[Received_Date] [varchar](30) NULL,
	[IT_DEPT_ID] [varchar](30) NULL,
	[Questions] [varchar](150) NULL,
	[Very_Poor_n] [varchar](30) NULL,
	[Poor_n] [varchar](30) NULL,
	[Fair_n] [varchar](30) NULL,
	[Good_n] [varchar](30) NULL,
	[Very_Good_n] [varchar](30) NULL
)

GRANT DELETE, INSERT, SELECT, UPDATE ON [ETL].[PGFusion_Query_Results] TO [HSCDOM\Decision Support]
GO