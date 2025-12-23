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
WHY :	Load mapping table Rptg.UA_GPP_Guar_Rltn_to_Pt
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
	--                           @Table = 'UA_GPP_Guar_Rltn_to_Pt'   -- varchar(100)
	

	BEGIN

INSERT INTO Rptg.UA_GPP_Guar_Rltn_to_Pt
(
    GUAR_REL_TO_PAT_C,
    NAME,
    ABBR,
    ASCEND_VALUE
)
VALUES
 (3,'Daughter','DAU','Child')
,(17,'Son','SON','Child')
,(7,'Grandfather','GFT','Grand-Parent')
,(8,'Grandmother','GMT','Grand-Parent')
,(10,'Legal Guardian','LGD','Other - PHI')
,(12,'Other','OTH','Other - PHI')
,(21,'Unverified Proxy','PROXY','Other - PHI')
,(23,'Visit Contact','VSTCNT','Other - PHI')
,(4,'Father','FAT','Parent')
,(11,'Mother','MOT','Parent')
,(2,'Brother','BRO','Sibling')
,(14,'Sister','SIS','Sibling')
,(18,'Spouse','SPO','Spouse')
,(13,'Step Father','SFT','Step-Parent')
,(16,'Step Mother','SMT','Step-Parent')
,(1,'Aunt','AUN','Uncle/Aunt')
,(19,'Uncle','UNC','Uncle/Aunt')
,(15,'Self','SLF','will be excluded')
,(20,'Employer','EMPLOYER','DNC')
,(22,'Transplant Recipient','TRANSPLANT R','DNC')
,(100,'Donor, Kidney','Kidney','DNC')
,(101,'Donor, Liver','Liver','DNC')
,(102,'Donor, Stem Cell','Stem Cell','DNC')

/************************************************************/
END

GO


