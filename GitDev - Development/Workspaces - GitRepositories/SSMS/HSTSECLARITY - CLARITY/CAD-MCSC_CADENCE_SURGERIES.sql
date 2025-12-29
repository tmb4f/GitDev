USE CLARITY

--Added Location Parameter
------MDM_REV_LOC_ID is used for location filter to account for non UVA MC locations see @de_control parameter above for more details------

------FOR TESTING ONLY------
DECLARE  
	@p_DateStart DATETIME = '10/1/2024',
	@p_Dateend DATETIME = '12/31/2024',
	@de_control VARCHAR(255) = 'All';

;
WITH MDM_REV_LOC_ID AS 
(	
	SELECT 
		DISTINCT 	
		t1.REV_LOC_ID
		,t1.HOSPITAL_CODE
		,t1.DE_HOSPITAL_CODE
		,t1.HOSPITAL_GROUP
	FROM	
		[CLARITY_App].[Rptg].[vwRef_MDM_Location_Master_Hospital_Group] t1
	WHERE	
		(t1.REV_LOC_ID IS NOT NULL)
)


SELECT	zors.TITLE				'SERVICE'
	,	UPPER(cloc.LOC_NAME)	'LOC_NAME'
	,	idid.IDENTITY_ID		'MRN'
	,	orca.OR_CASE_ID
	,	orca.RECORD_CREATE_DATE
	,	orca.REQUESTED_DATE
	,	orca.TIME_SCHEDULED
	,	orca.SURGERY_DATE
	,	zoss.TITLE				'CASE_SCHED_STATUS_NM'
	,	empr.NAME				'CASE_CREATE_USER_NM'
	,	cser.PROV_NAME			'CASE_PRIM_PHYS_NM'
	,	orca.CANCEL_DATE
	,	zocr.TITLE				'CANCEL_REASON_NM'
	,	empc.NAME				'CANCEL_USER_NM'
	,	orca.CANCEL_COMMENTS	
	,	vsca.CANCEL_REASON_NAME	'APPT_CANCEL_REASON'
	,	orlo.LOG_ID
	,	orsl.TITLE 'LOG_STATUS_NM'
	
	,	vsca.PAT_ENC_CSN_ID
	,	vsca.APPT_MADE_DTTM
	,	vsca.APPT_DTTM
	,	UPPER(vsca.APPT_STATUS_NAME) 'APPT_STATUS_NAME'
	,	vsca.PRC_NAME			'VISIT_TYPE_NM'
	,	vsca.PROV_NAME_WID
	,	vsca.REFERRING_PROV_NAME_WID
	,	vsca.REFERRAL_ID

FROM	OR_CASE		orca
			INNER JOIN	IDENTITY_ID				idid	ON	orca.PAT_ID = idid.PAT_ID	AND idid.IDENTITY_TYPE_ID = 14
			INNER JOIN	VALID_PATIENT			vapa	ON	orca.PAT_ID = vapa.PAT_ID
			LEFT JOIN	OR_LOG					orlo	ON	orca.LOG_ID = orlo.LOG_ID
			LEFT JOIN	V_SCHED_APPT			vsca	ON	orca.PAT_ID = vsca.PAT_ID
														AND	orca.SURGERY_DATE = vsca.CONTACT_DATE
														--AND	vsca.PRC_ID = '11702965'
			
			LEFT JOIN	CLARITY_LOC				cloc	ON	orca.LOC_ID = cloc.LOC_ID			
			LEFT JOIN	CLARITY_EMP				empc	ON	orca.CANCEL_USER_ID = empc.USER_ID			--Cancel user
			LEFT JOIN	CLARITY_EMP				empr	ON	orca.REC_CREATE_USER_ID = empr.USER_ID		--Create user
			LEFT JOIN	CLARITY_SER				cser	ON	orca.PRIMARY_PHYSICIAN_ID = cser.PROV_ID			
			
			LEFT JOIN	ZC_OR_SERVICE			zors	ON	orca.SERVICE_C = zors.SERVICE_C
			LEFT JOIN	ZC_OR_SCHED_STATUS		zoss	ON	orca.SCHED_STATUS_C = zoss.SCHED_STATUS_C			
			LEFT JOIN	ZC_OR_CANCEL_RSN		zocr	ON	orca.CANCEL_REASON_C = zocr.CANCEL_REASON_C
			LEFT JOIN	ZC_OR_STATUS			orsl	ON	orlo.STATUS_C = orsl.STATUS_C
			LEFT JOIN   MDM_REV_LOC_ID AS mloc			ON mloc.REV_LOC_ID = cloc.LOC_ID		--Location Update	


WHERE	orca.RECORD_CREATE_DATE BETWEEN @p_DateStart AND @p_DateEnd
	--AND	orca.SERVICE_C = '230'			--230=Ophthalmology
	AND vapa.IS_VALID_PAT_YN = 'Y'
	AND 
  (
        (UPPER(@de_control)=coalesce(UPPER(mloc.de_hospital_code),'UVA-MC'))
        OR (UPPER(@de_control)=coalesce(UPPER(mloc.hospital_group),'UVA-MC'))
        OR (UPPER(@de_control)='ALL')
    )
	--AND	orca.LOC_ID IN (
	--					1071035400		--Battle
	--				,	1071036300		--Monticello
	--				)
	AND orca.CASE_BEGIN_INSTANT IS NOT NULL
	AND orca.CASE_END_INSTANT IS NOT NULL
ORDER BY orca.SURGERY_DATE, idid.IDENTITY_ID, orca.OR_CASE_ID