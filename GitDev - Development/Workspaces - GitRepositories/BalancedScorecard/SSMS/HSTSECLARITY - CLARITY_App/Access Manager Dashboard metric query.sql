USE [CLARITY_App]
GO

/****** Object:  StoredProcedure [ETL].[uspSrc_AmbOpt_Access_Message_Completion]    Script Date: 2/21/2025 14:46:58 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





ALTER PROCEDURE [ETL].[uspSrc_AmbOpt_Access_Message_Completion]
(
  @startdate SMALLDATETIME = NULL  
 ,@enddate SMALLDATETIME = NULL 
)
AS 
--/**********************************************************************************************************************
--WHAT: Create procedure ETL.uspSrc_AmbOpt_Access_Message_Completion
--WHO : Joe Mouton
--WHEN: 3/15/23 
--WHY : Report completed messages (based on recipent pool and status) and which user marked it as completed
--			
-------------------------------------------------------------------------------------------------------------------------
--INFO: 
--      INPUTS:	DS_HSDW_Prod.Rptg.vwDim_Date
--              DHSTSECLARITY.CLARITY.dbo.ib_messages 
--				HSTSECLARITY.CLARITY.dbo.ib_receiver 
--				HSTSECLARITY.CLARITY.dbo.CLARITY_HIP   
--				HSTSECLARITY.CLARITY.dbo.ZC_MSG_TYPE  
--				HSTSECLARITY.CLARITY.dbo.CLARITY_EMP 
--				HSTSECLARITY.CLARITY.dbo.CLARITY_SER 
--				HSTSECLARITY.CLARITY.dbo.ZC_MSG_PRIORITY  
--				HSTSECLARITY.CLARITY.dbo.zc_recipient_sts 
--				HSTSECLARITY.CLARITY.dbo.ZC_STATUS 
--				HSTSECLARITY.clarity.dbo.IB_RECIP_STAT_AUD
--				DS_HSDM_App.Mapping.REF_Access_EpicPool_Groupers
--
--                
--      OUTPUTS:  [ETL].[uspSrc_AmbOpt_Access_Message_Completion]
--
--------------------------------------------------------------------------------------------------------------------------
--MODS: 	
--         03/15/2023 - JBM - created Stored Procedure
--		   04/04/2023 - JBM - Added Completed_User_Supervisory_Organization_Description
--		   05/17/2023 - JBM - Added wd_Supervisory_Organization_id
--************************************************************************************************************************

    SET NOCOUNT ON;



	----get default Balanced Scorecard date range

--DECLARE @startdate DATE,
--@enddate DATE

IF @startdate IS NULL
   AND @enddate IS NULL BEGIN 
   EXEC CLARITY_App.ETL.usp_Get_Dash_Dates_BalancedScorecard @startdate OUTPUT, @enddate OUTPUT;



   SET @startdate = DATEADD(mm,-3,CONVERT(DATE ,GETDATE()))  -- 3 month lookback for this dataset

END


-------------------------------------------------------------------------------
DECLARE @locstartdate SMALLDATETIME,
        @locenddate SMALLDATETIME

SET @locstartdate = @startdate
SET @locenddate   = @enddate

;WITH CTE AS
(
SELECT m.MSG_ID AS Message_ID,
m.CREATE_TIME AS Message_Create_DateTime,
h.REGISTRY_ID AS Pool_ID,
h.REGISTRY_NAME AS Pool_Name,
m.REGARDING_TOPIC AS Message_Regarding_Topic,
mt.NAME AS Inbasket,
mp.NAME AS Message_Priority,
rs.NAME AS Pool_Recipient_Status,
ms.NAME AS Message_Status,
sender.NAME AS Sender,
rec.RECPNT_RECV_UTC_DTTM AS Pool_Recieved_Date_Time_UTC,
lg.CHANGE_TIME Pool_Completed_Time,
lg.UserChangeName AS Audit_Pool_Completed_User, 
lg.SYSTEM_LOGIN AS Audit_Pool_Completed_User_Computing_ID,
Wdemp.wd_Employee_ID AS Audit_Pool_Completed_User_Employee_ID,
wdemp.wd_Supervisory_Organization_id,
wdemp.wd_Supervisory_Organization_Description
FROM 
CLARITY.dbo.ib_messages m INNER JOIN 
CLARITY.dbo.ib_receiver rec ON m.MSG_ID = rec.MSG_ID INNER JOIN
CLARITY.dbo.CLARITY_HIP h ON h.REGISTRY_ID = rec.REGISTRY_ID AND rec.POOL_RECIPIENT_YN = 'Y' LEFT JOIN 
CLARITY.dbo.ZC_MSG_TYPE mt ON m.MSG_TYPE_C = mt.MSG_TYPE_C LEFT JOIN
CLARITY.dbo.CLARITY_EMP sender ON m.SENDER_USER_ID = sender.USER_ID LEFT JOIN
CLARITY.dbo.CLARITY_SER senders ON sender.PROV_ID = senders.PROV_ID LEFT JOIN
CLARITY.dbo.ZC_MSG_PRIORITY mp ON m.MSG_PRIORITY_C = mp.MSG_PRIORITY_C LEFT JOIN
CLARITY.dbo.zc_recipient_sts rs ON rec.RECIPIENT_STATUS_C = rs.RECIPIENT_STATUS_C LEFT JOIN
CLARITY.dbo.ZC_STATUS ms ON m.STATUS_C = ms.STATUS_C INNER JOIN
(
	SELECT a.MSG_ID, line, s.NAME AS StatusName,h.REGISTRY_ID, h.REGISTRY_NAME, a.CHANGE_TIME, e.NAME AS UserChangeName, e.SYSTEM_LOGIN, ROW_NUMBER() OVER (PARTITION BY a.MSG_ID ORDER BY a.CHANGE_TIME DESC) AS rownum 
	FROM CLARITY.dbo.IB_RECIP_STAT_AUD a INNER JOIN
	CLARITY.dbo.CLARITY_HIP h ON h.REGISTRY_ID = a.REGISTRY INNER JOIN 
	CLARITY.dbo.ZC_RECIPIENT_STS s ON a.RECIPIENT_STATUS_C = s.RECIPIENT_STATUS_C LEFT JOIN 
	CLARITY.dbo.CLARITY_EMP e ON a.CHG_BY_USER_ID = e.USER_ID INNER JOIN
	CLARITY_App.Mapping.REF_Access_EpicPool_Groupers pg ON pg.epic_pool_id = h.REGISTRY_ID
	WHERE 1=1
	AND  s.NAME = 'Done'
) lg ON lg.MSG_ID = m.MSG_ID AND lg.REGISTRY_ID = h.REGISTRY_ID  AND lg.rownum = 1 INNER JOIN
(			 
							SELECT UVA_Computing_ID, wd_Employee_ID, wd_Supervisory_Organization_id, wd_Supervisory_Organization_Description, ROW_NUMBER() OVER (PARTITION BY wd_Employee_ID ORDER BY wd_sk_Effective_Date DESC) AS rownum
							FROM CLARITY_App.rptg.vwCrosswalk_All_ActiveWorkers 
							WHERE wd_Is_Active = 1 
							AND wd_IS_Position_Active = 1
							AND wd_Is_Primary_Job = 1			
	) WDemp ON lg.SYSTEM_LOGIN = UPPER(WDemp.UVA_Computing_ID) AND WDemp.rownum = 1 INNER JOIN
CLARITY_App.Mapping.REF_Access_EpicPool_Groupers pg ON pg.epic_pool_id = h.REGISTRY_ID 
WHERE CONVERT(DATE,lg.CHANGE_TIME) BETWEEN @locstartdate AND @locenddate -- Using Recipient completed date for date range
)



SELECT CTE.Message_ID,
       CTE.Message_Create_DateTime,
       CTE.Pool_ID,
       CTE.Pool_Name,
       CTE.Message_Regarding_Topic,
       CTE.Inbasket,
       CTE.Message_Priority,
       CTE.Pool_Recipient_Status,
       CTE.Message_Status,
       CTE.Sender,
       CTE.Pool_Recieved_Date_Time_UTC,
       CTE.Pool_Completed_Time,
       CTE.Audit_Pool_Completed_User,
       CTE.Audit_Pool_Completed_User_Computing_ID,
       CTE.Audit_Pool_Completed_User_Employee_ID,
	   cte.wd_Supervisory_Organization_ID AS Completed_User_Supervisory_Organization_ID,
	   cte.wd_Supervisory_Organization_Description AS Completed_User_Supervisory_Organization_Description,
	   1 AS event_count,
	   CONVERT(DATE, cte.Pool_Completed_Time) AS event_date
/* Standard Fields */
	/* Date/times */
,	'Fmonth_num'					=	dd.Fmonth_num
,	'Fyear_num'						=	dd.Fyear_num
,	'Fyear_name'					=	dd.Fyear_name
/* Others */		
,	'event_type'					=	CAST('Access Completed Messages' AS VARCHAR(50)) 
,	'event_category'				=	CAST(NULL AS VARCHAR(150)) 
,	'sk_Dim_Pt'						=	CAST(NULL AS INT)
,	'peds' 							=	CAST(NULL AS SMALLINT) 
,	'transplant'					=	CAST(NULL AS SMALLINT) 
,	'oncology'						=	CAST(NULL AS SMALLINT) 
,	'sk_Fact_Pt_Acct'				=	CAST(NULL AS BIGINT) 
,	'sk_Fact_Pt_Enc_Clrt'			=	CAST(NULL AS INT) 
,	'sk_dim_physcn'					=	CAST(NULL AS INT) 

FROM 
CTE
LEFT JOIN CLARITY_App.Rptg.vwDim_Date	dd	ON	CONVERT(DATE, cte.Pool_Completed_Time) = dd.day_date

GO


