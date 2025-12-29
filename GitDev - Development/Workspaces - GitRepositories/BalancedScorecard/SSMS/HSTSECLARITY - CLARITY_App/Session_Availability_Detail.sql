USE CLARITY_App

DECLARE @p_SlotStart AS DATE
DECLARE @p_SlotEnd AS DATE

SET @p_SlotStart = '7/1/2021'
SET @p_SlotEnd = '2/28/2022'

SELECT 
    Slot_Begin_Month        =   UPPER(datedim.MONTH_NAME)
,   Slot_Begin_Week         =   datedim.WEEK_BEGIN_DT                                                                                             
,   Slot_Date               =   datedim.CALENDAR_DT
,   Slot_Date_DOW           =   LEFT(datedim.DAY_OF_WEEK, 3)
,	Slot_Date_DOW_Num		=	datedim.DAY_OF_WEEK_INDEX
,   Session_Flag            =   CASE    WHEN CAST(avail.SLOT_BEGIN_TIME AS TIME) >= '07:00:00' AND CAST(SLOT_BEGIN_TIME AS TIME) < '12:00:00' THEN 'AM' 
                                        WHEN CAST(avail.SLOT_BEGIN_TIME AS TIME) >= '12:00:00' AND CAST(SLOT_BEGIN_TIME AS TIME) < '17:00:00' THEN 'PM'   ELSE 'OTH' END      
,	Unavailable_Reason		=	avail.UNAVAILABLE_RSN_NAME
,   Location_Name_ID        =   avail.LOC_NAME + ' [' + CAST(avail.LOC_ID AS VARCHAR(80)) + ']'
,   Department_Name_ID      =   avail.DEPARTMENT_NAME + ' [' + CAST(avail.DEPARTMENT_ID AS VARCHAR(254)) + ']'
,   Prov_Fin_Div_ID         =   ser.RPT_GRP_SIX
,   Prov_Fin_Div_Nm         =   six.NAME
,   Prov_Fin_SubDiv_ID      =   ser.RPT_GRP_EIGHT                                                                                 
,   Prov_Fin_SubDiv_Nm      =   eight.NAME                                                                                                                   
,   Prov_Service_Line       =   ser.RPT_GRP_FIVE                                                                                                             
,   Prov_Name_ID            =   avail.PROV_NM_WID
,   ser.PROV_TYPE
,   Time_Type               =   CASE    WHEN avail.UNAVAILABLE_RSN_C IS NOT NULL										THEN 'Unavailable_Time'     
                                        WHEN avail.UNAVAILABLE_RSN_C IS NULL		AND avail.ORG_REG_OPENINGS  > 0		THEN 'Regular_Time'
                                        WHEN avail.UNAVAILABLE_RSN_C IS NULL		AND avail.ORG_REG_OPENINGS  = 0    
																					AND avail.ORG_OVBK_OPENINGS > 0		THEN 'Overbook_Only_Time'   
                                        WHEN avail.UNAVAILABLE_RSN_C IS NULL		AND avail.ORG_REG_OPENINGS  = 0    
																					AND avail.ORG_OVBK_OPENINGS = 0		THEN 'Slot_WO_Opening'
																														ELSE 'Other'                    END
,   Slot_Start				=   avail.SLOT_BEGIN_TIME
,   Slot_End				=   avail.SLOT_END_TIME
,   Slot_Length				=   avail.SLOT_LENGTH
,	Appointments			=	avail.NUM_APTS_SCHEDULED
,	Openings_Regular		=	avail.ORG_REG_OPENINGS
,	Openings_Overbook		=	avail.ORG_OVBK_OPENINGS
,	Held_Reason				=	avail.TIME_HELD_RSN_NAME
,	Outside_Template		=	avail.OUTSIDE_TEMPLATE_YN

FROM CLARITY.dbo.V_AVAILABILITY		avail
    LEFT JOIN CLARITY.dbo.CLARITY_SER       ser     ON avail.PROV_ID = ser.PROV_ID
    LEFT JOIN CLARITY.dbo.ZC_SER_RPT_GRP_6  six     ON ser.RPT_GRP_SIX = six.RPT_GRP_SIX
    LEFT JOIN CLARITY.dbo.ZC_SER_RPT_GRP_8  eight   ON ser.RPT_GRP_EIGHT = eight.RPT_GRP_EIGHT 
	LEFT JOIN CLARITY.dbo.DATE_DIMENSION	datedim	ON avail.SLOT_DATE = datedim.CALENDAR_DT

WHERE	1=1
	AND CAST(avail.SLOT_BEGIN_TIME AS DATE)   >=	@p_SlotStart 
	AND CAST(avail.SLOT_BEGIN_TIME AS DATE)   <=	@p_SlotEnd
	AND avail.SLOT_HOUR BETWEEN 7 AND 16
    AND datedim.WEEKEND_YN = 'N'
	AND avail.APPT_NUMBER = 0                         -- 0=Slots only
	--AND ser.PROVIDER_TYPE_C IN (@p_Type)
	--AND		(		 avail.PROV_ID                          IN (@p_Value)	AND @p_GroupingType IN (1,2,4)
	--	OR		CAST(avail.DEPARTMENT_ID AS VARCHAR(18))    IN (@p_Value) 	AND @p_GroupingType =	3)

	ORDER BY avail.PROV_ID, avail.SLOT_DATE, Slot_Start, Slot_End