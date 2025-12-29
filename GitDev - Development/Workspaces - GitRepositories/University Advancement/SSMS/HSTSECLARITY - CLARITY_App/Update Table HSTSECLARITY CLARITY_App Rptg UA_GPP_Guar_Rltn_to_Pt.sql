USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

UPDATE [Rptg].[UA_GPP_Guar_Rltn_to_Pt] -- 20250205

	SET ASCEND_VALUE = 'Grateful Patient - Other'
	WHERE GUAR_REL_TO_PAT_C IN (10,12,21,23);
GO

UPDATE [Rptg].[UA_GPP_Guar_Rltn_to_Pt] -- 20250205

	SET ASCEND_VALUE = 'DNC - Delete the row for these guarantors; we are not importing these consitutents.'
	WHERE GUAR_REL_TO_PAT_C IN (15,20,22,100,101,102);
GO


