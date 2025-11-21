USE [DS_HSDM_APP]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--EXEC [ETL].[uspSrc_PtPgr_ExternalXferStatus]

--CREATE PROCEDURE [ETL].[uspSrc_PtPgr_ExternalXferStatus]
--    (
--     @startdate SMALLDATETIME = NULL
--    ,@enddate SMALLDATETIME = NULL
	
--    )
--AS 

DECLARE @startdate DATETIME, @enddate DATETIME

--SET @startdate = '7/1/2023'
--SET @enddate = '2/21/2024'
SET @startdate = NULL
SET @enddate = NULL

--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_PtPgr_ExternalXferStatus
--WHO : Tom Burgan
--WHEN: 02/21/2024
--WHY : for Patient Progression Coalition metric
--			External Transfer Requests Status Frequencies (accepted, declined, consults, cancels)  
--			External Transfer Requests received by UVa Health - University Medical Center
--
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	
--              
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         02/21/2024 -TMB - create stored procedure

--************************************************************************************************************************

    SET NOCOUNT ON;
 
	----get default Balanced Scorecard date range
    IF @startdate IS NULL
        AND @enddate IS NULL
        EXEC ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;
 
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME
		
SET @locstartdate = @startdate
SET @locenddate   = @enddate

-------------------------------------------------------------------------------
SELECT
	CAST('Incoming Transfer Request' AS VARCHAR(50))	AS event_type,
	CAST('Intake Request Status' AS VARCHAR(150))	AS event_category,
	CASE WHEN summary.request > 0 THEN 1 ELSE 0 END AS event_count,
	dd.day_date AS event_date,
	summary.ReferringFacilityID,
    summary.Referring_Facility,
    summary.TierLevel,
    summary.TransferReasonID,
    summary.TransferReason,
    summary.TransferTypeHx,
    summary.EntryTime,
    summary.DestinationUnitID,
    summary.ProtocolNme,
    summary.request,
    summary.accept,
    summary.declin,
    summary.consult,
    summary.cancel,
    summary.pending,
	dd.fmonth_num,
	dd.FYear_num,
	dd.FYear_name,
	summary.DestinationUnitID AS epic_department_id,
	mdmhst.epic_department_name AS epic_department_name,
	mdmhst.epic_department_name_external AS epic_department_external,
	o.organization_name,
	s.service_name,
	c.clinical_area_name,
	mdmhst.hs_area_id,
	mdmhst.hs_area_name,
	mdmhst.LOC_ID AS rev_location_id,
	mdmhst.REV_LOC_NAME AS rev_location,
	CAST(LEFT(DATENAME(MM, dd.day_date), 3) + ' ' + CAST(DAY(dd.day_date) AS VARCHAR(2)) AS VARCHAR(10))			AS report_period,
	CAST(CAST(dd.day_date AS DATE) AS SMALLDATETIME)																AS report_date,
	CAST(NULL AS INT)  AS event_id,
	CAST(NULL AS SMALLINT) AS peds,
	CAST(NULL AS DATE) AS person_birth_date,
	CAST(NULL AS INT) AS person_id,
	CAST(NULL AS VARCHAR(200)) AS person_name,
	CAST(NULL AS VARCHAR(18)) AS provider_id,
	CAST(NULL AS VARCHAR(200)) AS provider_name,
	CAST(NULL AS INT) AS sk_Dim_Pt,
	CAST(NULL AS VARCHAR(255)) AS person_gender
FROM DS_HSDW_Prod.Rptg.vwDim_Date dd
LEFT OUTER JOIN
(
SELECT
	xtr.ReferringFacilityID,
    xtr.Referring_Facility,
    xtr.TierLevel,
    xtr.TransferReasonID,
    xtr.TransferReason,
    xtr.TransferTypeHx,
    xtr.EntryTime,
    xtr.DestinationUnitID,
    xtr.ProtocolNme,
	COUNT(*) AS request,
    SUM(xtr.accepted) AS accept,
    SUM(xtr.declined) AS declin,
    SUM(xtr.consult) AS consult,
    SUM(xtr.canceled) AS cancel,
    SUM(xtr.pending) AS pending
FROM
(
SELECT 
	  xt.[ReferringFacilityID]
    , xt.[Referring_Facility]
	, xt.[TierLevel]
	, xt.[TransferReasonID]
    , xt.[TransferReason]
	, xt.[TransferTypeHx]
	, CAST(xt.[EntryTime] AS DATE) AS [EntryTime]
	, xt.[DestinationUnitID]
	, xt.[ProtocolNme]
	, CASE WHEN xt.Disposition IN ('Completed','Accepted') THEN 1 ELSE 0 END AS accepted
	, CASE WHEN xt.DispositionReason IS NOT NULL THEN 1 ELSE 0 END AS declined
	, CASE WHEN xt.TransferTypeHx = 'Consult' THEN 1 ELSE 0 END AS consult
	, CASE WHEN xt.XTPlacementStatusName = 'Canceled' THEN 1 ELSE 0 END AS canceled
	, CASE WHEN xt.Disposition = 'Pending' THEN 1 ELSE 0 END AS pending

  FROM [DS_HSDM_Prod].[Rptg].[ADT_TransferCenter_ExternalTransfers] xt

  WHERE 1=1
 --   AND TransferTypeID = 1 -- REQUEST_TYPE_MAPPING_C = 1,	REQUEST_TYPE_MAPPING_NAME = Transfer, 	REQUEST_TYPE_C = 2026,	REQUEST_TYPE_NAME = Incoming Transfer
	--AND xt.TransferTypeHx = 'Incoming Transfer'

	AND CAST(EntryTime AS DATE) >= @locstartdate
	AND CAST(EntryTime AS DATE) <=  @locenddate
) xtr
GROUP BY
	  xtr.[ReferringFacilityID]
    , xtr.[Referring_Facility]
	, xtr.[TierLevel]
	, xtr.[TransferReasonID]
    , xtr.[TransferReason]
	, xtr.[TransferTypeHx]
	, CAST(xtr.[EntryTime] AS DATE)
	, xtr.[DestinationUnitID]
	, xtr.[ProtocolNme]
) summary
ON dd.day_date = summary.EntryTime
LEFT OUTER JOIN
	(
		SELECT
			history.MDM_BATCH_ID,
			history.EPIC_DEPARTMENT_ID,
			history.EPIC_DEPT_NAME AS epic_department_name,
			history.EPIC_EXT_NAME AS epic_department_name_external,
			--history.SERVICE_LINE_ID,
			--history.SERVICE_LINE,
			--history.SUB_SERVICE_LINE_ID,
			--history.SUB_SERVICE_LINE,
			history.LOC_ID,
			history.REV_LOC_NAME,
			history.HS_AREA_ID,
			history.HS_AREA_NAME--,
			--history.OPNL_SERVICE_ID,
			--history.OPNL_SERVICE_NAME,
			--history.PRESSGANEY_NAME,
			--history.FINANCE_COST_CODE,
			--history.CORP_SERVICE_LINE_ID,
			--history.CORP_SERVICE_LINE,
			--history.PRACTICE_GROUP_ID,
			--history.PRACTICE_GROUP_NAME,
			--history.POD_ID,
			--history.PFA_POD,
			--history.HUB_ID,
			--history.	HUB,
			--history.BUSINESS_UNIT
		FROM
		(
			SELECT
				MDM_BATCH_ID,
				EPIC_DEPARTMENT_ID,
				EPIC_DEPT_NAME,
				EPIC_EXT_NAME,
				--SERVICE_LINE_ID,
				--SERVICE_LINE,
				--SUB_SERVICE_LINE_ID,
				--SUB_SERVICE_LINE,
				LOC_ID,
				REV_LOC_NAME,
				HS_AREA_ID,
				HS_AREA_NAME,
				--OPNL_SERVICE_ID,
				--OPNL_SERVICE_NAME,
				--PRESSGANEY_NAME,
				--FINANCE_COST_CODE,
				--CORP_SERVICE_LINE_ID,
				--CORP_SERVICE_LINE,
				--PRACTICE_GROUP_ID,
				--PRACTICE_GROUP_NAME,
				--POD_ID,
				--PFA_POD,
				--HUB_ID,
				--HUB,
				--BUSINESS_UNIT,
				ROW_NUMBER() OVER(PARTITION BY EPIC_DEPARTMENT_ID ORDER BY MDM_BATCH_ID DESC) AS seq
			FROM DS_HSDW_Prod.Rptg.vwRef_MDM_Location_Master_History
		) history
		WHERE history.seq = 1
	) mdmhst
	ON mdmhst.EPIC_DEPARTMENT_ID = summary.DestinationUnitID
LEFT JOIN [DS_HSDM_App].[Mapping].[Epic_Dept_Groupers] g ON summary.DestinationUnitID = g.epic_department_id
LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Clinical_Area_Map c ON g.sk_Ref_Clinical_Area_Map = c.sk_Ref_Clinical_Area_Map
LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Service_Map s ON c.sk_Ref_Service_Map = s.sk_Ref_Service_Map
LEFT JOIN [DS_HSDM_App].[Mapping].Ref_Organization_Map o ON s.organization_id = o.organization_id

WHERE 1 =1
	AND dd.day_date >= @locstartdate
	AND dd.day_date <=  @locenddate

--ORDER BY
--	  summary.[ReferringFacilityID]
--    , summary.[Referring_Facility]
--	, summary.[TierLevel]
--	, summary.[TransferReasonID]
--    , summary.[TransferReason]
--	, summary.[TransferTypeHx]
--	, summary.[EntryTime]
--	, summary.[DestinationUnitID]
--	, summary.[ProtocolNme]
ORDER BY
	  dd.day_date
	, summary.[ReferringFacilityID]
    , summary.[Referring_Facility]
	, summary.[TierLevel]
	, summary.[TransferReasonID]
    , summary.[TransferReason]
	, summary.[TransferTypeHx]
	, summary.[DestinationUnitID]
	, summary.[ProtocolNme]
	
GO


