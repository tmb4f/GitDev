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
WHY :	Load mapping table Rptg.UA_GPP_Provider_Division_Mapping
AUTHOR:	Tom Burgan
SPEC:	
--------------------------------------------------------------------------------------------
INPUTS:   
	
OUTPUTS: 
		
MODS: 
		**		01/28/2025  -Tom B.  Create script

NOTES:

********************************************************************************************/

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	EXEC dbo.usp_TruncateTable @schema = 'Rptg', -- varchar(100)
	                           @Table = 'UA_GPP_Provider_Division_Mapping'   -- varchar(100)
	
	BEGIN

SELECT
	*
INTO #temp
FROM (
VALUES
('Anesthesiology [70000]','MD-ANES Anesthesiology')
,('Clinical Practice Group [77000]','UNKNOWN')
,('Dentistry [49000]','MD-DENT Dentistry')
,('Emergency Medicine [71000]','MD-EMED Emergency Medicine')
,('Medicine [57000]','MD-INMD General Medicine')
,('Neurosurgery [59000]','MD-NERS Neurosurgery')
,(NULL,'UNKNOWN')
,('OB/GYN [61000]','MD-OBGY Ob & Gyn')
,('Ophthalmology [54000]','UNKNOWN')
,('Orthopedics [62000]','UNKNOWN')
,('Pediatrics [63000]','MD-PEDT General Pediatrics')
,('Physical Medicine & Rehabilitation [62500]','MD-PHMR Phys Med & Rehab')
,('Plastic Surgery [66000]','MD-PLSR Plastic Surgery')
,('Psychiatry [67000]','MD-PSCH Psychiatry and NB Science')
,('Surgery [68000]','UNKNOWN')
,('Urology [55000]','MD-UROL Urology')
,('UVA Community Health [80000]','UNKNOWN')
,('UVA Community Health Hospital [81000]','UNKNOWN')
,('Radiology & Medical Imaging [73000]','MD-RADL Radiology')
,('Neurology [60000]','MD-NEUR Neurology')
,('Family Medicine [48000]','UNKNOWN')
,('Radiation Oncology [74000]','MD-RONC Radiation Oncology')
,('Otolaryngology [53000]','MD-OTLY Otolaryngology')
,('Dermatology [52000]','MD-DERM Dermatology')
) temp(Epic_Financial_Division,ASCEND_VALUE)

INSERT INTO Rptg.UA_GPP_Provider_Division_Mapping
(
    Epic_Financial_Division,
    ASCEND_VALUE
)
SELECT
	Epic_Financial_Division,
    ASCEND_VALUE
FROM #temp

/************************************************************/
END

GO


