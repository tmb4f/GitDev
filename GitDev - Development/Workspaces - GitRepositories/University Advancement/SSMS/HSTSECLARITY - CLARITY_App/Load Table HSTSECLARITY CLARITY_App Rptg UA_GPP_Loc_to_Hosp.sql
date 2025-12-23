USE [CLARITY_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*******************************************************************************************
WHAT:	Grateful Patient Program data extract 
WHO :	University Advancement
WHEN:	Daily
WHY :	Load mapping table Rptg.UA_GPP_Loc_to_Hosp
AUTHOR:	Tom Burgan
SPEC:	
--------------------------------------------------------------------------------------------
INPUTS:   
	
OUTPUTS: 
		
MODS: 
		**		12/04/2024  -Tom B.  Create script

NOTES:

********************************************************************************************/

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--EXEC dbo.usp_TruncateTable @schema = 'Rptg', -- varchar(100)
	--                           @Table = 'UA_GPP_Loc_to_Hosp'   -- varchar(100)
	

	BEGIN

INSERT INTO Rptg.UA_GPP_Loc_to_Hosp
(
    LOC_ID,
    LOC_NAME,
    ASCEND_VALUE
)
VALUES
 (10295,'Culpeper Regional Hospital','Culpeper')
,(10369,'UVA Culpeper Medical Center','Culpeper')
,(10390,'UVA Health Primary Care Culpeper','Culpeper')
,(10274,'UVA Health Specialty and Same Day Care Culpeper','Culpeper')
,(10415,'UVA Outpatient Imaging Culpeper','Culpeper')
,(10272,'UVA Specialty Care at Culpeper','Culpeper')
,(10293,'UVA Specialty Care at Culpeper','Culpeper')
,(10743,'UVA Haymarket Medical Center','Haymarket')
,(10393,'UVA Prince William 8644 Sudley Road','Prince William')
,(10429,'UVA Prince William Cardiology Warrenton','Prince William')
,(10388,'UVA Prince William Hospital','Prince William')
,(10437,'UVA Prince William Surgical Associates Manassas','Prince William')

/************************************************************/
END

GO


