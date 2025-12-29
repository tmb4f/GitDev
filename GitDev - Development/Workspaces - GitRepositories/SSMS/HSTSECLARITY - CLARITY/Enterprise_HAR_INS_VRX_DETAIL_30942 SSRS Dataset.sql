USE CLARITY

/**************************************************************************************
[NAME]			: Verifications:  HAR and Insurance - 30942
[AUTHOR(s)]		: SUE GRONDIN SYG2D
[ORGANIZATION]	: UVA - OPERATIONAL BUSINESS INTELLIGENCE
[DESCRIPTION]	: For any encounter ( starting with PAT_ENC) FIND THE CORRESPONDING HAR RELATED TO THE PRIMARY CSN.  
				  FOR ANY ENCOUNTER, WAS THE FOLLOWING COMPLETED BEFORE THE SERVICE DATE ( APPT/ADM DATE)
				  WAS THE HAR VERIFIED YN. WAS THE PRIMARY COVERAGE VERIFIED YN, 
				 
				  DISPLAY DETAILS AND LET END USER SUMMARIZE AS DESIRED.
				  QUERY BY HAR Service DATE
				  Assumes HAR Servce Date = Appt/Admit Date 

[REVISION HISTORY]:
Date        Author			Version		Comment
----------	--------------	--------	-------------------------------------
2023.01.19 	SYG2D			1			CREATED
2023.03.30 syg2d - added dept specialty
2023.04.18 syg2d  added account class/payor/plan
2023.05.22 syg2d added Loc grouper as param
************************************************************************************/
--/*
DECLARE @StartDate DATETIME = '5/1/2023';
DECLARE @EndDate DATETIME = '5/7/2023';
DECLARE @Loc_group VARCHAR(250) = '3'
--*/

; WITH CTE_LOC_GRP AS (
	
	SELECT	DISTINCT
			[LOC_GROUPER_ID]   = ISNULL(LOC.RPT_GRP_NINE, 'NULL')
		  , [LOC_GROUPER_NAME] = ISNULL(ZEAF9.NAME, '*Not Indicated [NULL]')
		  , LOC.LOC_ID
		  , loc.LOC_NAME
	FROM	CLARITY..CLARITY_LOC				LOC
			LEFT JOIN CLARITY..ZC_LOC_RPT_GRP_9 ZEAF9 ON LOC.RPT_GRP_NINE = ZEAF9.RPT_GRP_NINE
	WHERE	LOC.RECORD_STATUS IS NULL AND	LOC.SERV_AREA_ID = 10 AND	LOC.LOC_NAME NOT LIKE 'EHS%'
			AND ISNULL(CAST(loc.RPT_GRP_NINE AS VARCHAR(MAX)), 'NULL') IN (@Loc_group)		
			--/*--For Stored Proc parcing
			--AND (ISNULL(CAST(LOC.RPT_GRP_NINE AS VARCHAR(MAX)), 'NULL')IN (SELECT Param FROM CLARITY.ETL.fn_ParmParse(@Loc_group,  ',') ))		
			--*/
)
SELECT
	hars.HAR_SERVICE_DATE [SERVICE_DATE]
  , hars.LOCATION_GROUPER
  , hars.LOC_NAME
  , hars.HAR_VERF_ATTRIB_DEPT
  , hars.DEPT_SPECIALTY
  , hars.HAR_DENOM
	,[HAR Verification]	=
				CAST(CASE WHEN hars.HAR_VERIF_DATE IS NOT NULL AND hars.HAR_VERIF_OFFSET_DAYS <=0 THEN
					1 ELSE 0
					END AS INT)		

	,[INS_DENOM]= CAST(CASE WHEN hars.COVERAGE_ID IS NOT NULL  THEN
					1 ELSE 0
					END AS INT)
	,[Ins Verification] =  
				CAST(CASE WHEN hars.INS_VER_DATE_HX_DTTM IS NOT NULL 
							AND hars.INS_VER_OFFSET_DAYS<=0 THEN
					1 ELSE 0
					END AS INT)																						 
	, [E-Verified] = 
				CAST(CASE WHEN hars.INS_VER_STATUS_HX_C = '6' --e-verified
							AND	hars.INS_VER_OFFSET_DAYS <= 0 THEN
					1 ELSE 0
					END AS INT)																					
	 ,[RTE Enabled] = 
				CAST(CASE WHEN hars.PLAN_ID IS NOT NULL
							AND (COALESCE(hars.USE_ELCT_VERIF_YN_epp, 'N') = 'Y'			 -- RTE enabled at benefit plan level
								OR  COALESCE(hars.USE_ELCT_VERIF_YN_epm, 'N') = 'Y' )THEN	 -- RTE enabled at payor level 	    
					1 ELSE 0
					END AS INT)																						
	  ,[RTE Override case Rate] = 
				CAST(CASE WHEN hars.PLAN_ID IS NOT NULL
					   AND ( COALESCE(hars.USE_ELCT_VERIF_YN_epp, 'N') = 'Y'
							   OR  COALESCE(hars.USE_ELCT_VERIF_YN_epm, 'N') = 'Y'  
						   )
					   AND HARS.INS_VER_STATUS_HX_C <> '6'  THEN--Not E-Verified
				  1 ELSE 0
					END AS INT)  

, hars.MRN
, hars.PAT_NAME
, hars.HSP_ACCOUNT_ID
, hars.PAT_ACCOUNT_CLASS
, hars.PB_VISIT_HAR_ID
, hars.PAT_ENC_CSN_ID
, hars.ENC_TYPE
-- har verif
, hars.HAR_VERIF_DATE
, hars.HAR_VERIF_OFFSET_DAYS
--==== ins verif
, hars.COVERAGE_ID
, hars.PAYOR_ID
, hars.PAYOR_NAME
, hars.PLAN_ID
, hars.BENEFIT_PLAN_NAME
, hars.USE_ELCT_VERIF_YN_epp
, hars.USE_ELCT_VERIF_YN_epm
, hars.MEM_VERIFICATION_ID
, hars.INS_VER_DATE_HX_DTTM
, hars.INS_VER_OFFSET_DAYS
, hars.INS_VER_STATUS_HX_C
, hars.INS_VER_NEXT_REV_DATE
--== appt status
, hars.APPT_STATUS
, hars.ADT_PAT_STS
, hars.ADMIT_STS

FROM (
SELECT  
		pe.EFFECTIVE_DATE_DT
		,iid.IDENTITY_ID	   [MRN]
		  , pat.PAT_NAME
		  , vm.HSP_ACCOUNT_ID
		  ,zpatclass.NAME[PAT_ACCOUNT_CLASS]
		  , pe4.PB_VISIT_HAR_ID
		  , vm.PAT_ENC_CSN_ID
		  , vm.ENC_TYPE_C
		  , zenc.NAME		   [ENC_TYPE]
		  --=== HAR VERIF
		  , vm.ENC_SERVICE_DATE
		  , vm.HAR_SERVICE_DATE
		  , vm.HAR_VERIF_DATE
		  , vm.HAR_VERIF_OFFSET_DAYS
		  , vm.HAR_ATTRIBUTION_DEPT_ID
		  , vm.HAR_ATTRIBUTION_DATE
		  , VM.HAR_DENOM
		  , vm.ENC_DENOM
		  , vm.USE_HSP_ACCT_YN
		  , [HAR_VERF_ATTRIB_DEPT] = CONCAT(dep.DEPT_ABBREVIATION, ' [', CAST(dep.DEPARTMENT_ID AS VARCHAR(250)), ']')
		  , dep.DEPT_ABBREVIATION
		  , DSPEC.NAME[DEPT_SPECIALTY]
		  , loc.LOC_ID
		  , loc.LOC_NAME
		  , loc.LOC_GROUPER_ID
		  , loc.LOC_GROUPER_NAME[LOCATION_GROUPER]
		  --== registration primary coverage
		  , pe.COVERAGE_ID
		  , epm.PAYOR_ID
		  , epm.PAYOR_NAME
		  , epp.BENEFIT_PLAN_ID		   [PLAN_ID]
		  , epp.BENEFIT_PLAN_NAME
			-- payor/plan ver
		  , epp2.USE_ELCT_VERIF_YN	   [USE_ELCT_VERIF_YN_epp]
		  , epm2.USE_ELCT_VERIF_YN	   [USE_ELCT_VERIF_YN_epm]
		  --=== appt status
		   , zapptsts.NAME				[APPT_STATUS]
		  , psts.NAME				   [ADT_PAT_STS]
		  , csts.NAME				   [ADMIT_STS]
		  --==== ins ver
		 , ins_ver.MEM_VERIFICATION_ID
		 , ins_ver.VERIF_DATE_HX_DTTM [INS_VER_DATE_HX_DTTM]
		 ,[INS_VER_OFFSET_DAYS] = DATEDIFF(DAY,PE.EFFECTIVE_DATE_DT,ins_ver.VERIF_DATE_HX_DTTM)
		 , ins_ver.VERIF_STATUS_HX_C [INS_VER_STATUS_HX_C]		
		 , ins_ver.NEXT_REV_DATE_HX_DT[INS_VER_NEXT_REV_DATE]
		  --  add appt status, add pre admit status (conf & status)
		  -- separate out auth, precert and referrals.  add on 
	
		FROM	CLARITY..PAT_ENC						pe
				INNER JOIN clarity..PAT_ENC_2			pe2 ON pe2.PAT_ENC_CSN_ID = pe.PAT_ENC_CSN_ID 
				INNER JOIN 	clarity..V_REG_VERIF_METRICS vm ON	      pe.PAT_ENC_CSN_ID = vm.PAT_ENC_CSN_ID AND vm.HAR_DENOM = 1	--AND pe.HSP_ACCOUNT_ID IN ( 13013702338)

				INNER JOIN clarity..DATE_DIMENSION dd ON CAST(vm.HAR_SERVICE_DATE  AS DATE) = CAST(dd.CALENDAR_DT AS DATE) AND	CAST(dd.CALENDAR_DT AS DATE) BETWEEN @StartDate AND @EndDate
				INNER JOIN CLARITY..PATIENT				pat ON pe.PAT_ID								   = pat.PAT_ID 
				INNER JOIN CLARITY..IDENTITY_ID			iid ON pe.PAT_ID								   = iid.PAT_ID AND	  iid.IDENTITY_TYPE_ID = 14
				LEFT JOIN CLARITY..VALID_PATIENT		valpat ON pe.PAT_ID								   = valpat.PAT_ID
				LEFT JOIN clarity..ZC_PAT_CLASS			zpatclass ON zpatclass.ADT_PAT_CLASS_C				= pe2.ADT_PAT_CLASS_C
 				LEFT JOIN CLARITY..PAT_ENC_4			pe4 ON pe.PAT_ENC_CSN_ID						   = pe4.PAT_ENC_CSN_ID 
				--LEFT JOIN CLARITY..PAT_ENC_5			pe5 ON pe.PAT_ENC_CSN_ID						   = pe5.PAT_ENC_CSN_ID
				LEFT JOIN CLARITY..PAT_ENC_HSP			peh ON pe.PAT_ENC_CSN_ID						   = peh.PAT_ENC_CSN_ID				
				
				
		    LEFT JOIN CLARITY..ZC_DISP_ENC_TYPE		zenc ON vm.ENC_TYPE_C							   = zenc.DISP_ENC_TYPE_C
				LEFT JOIN CLARITY..ZC_APPT_STATUS		zapptsts ON pe.APPT_STATUS_C					   = zapptsts.APPT_STATUS_C
				LEFT JOIN CLARITY..CLARITY_DEP			dep ON vm.HAR_ATTRIBUTION_DEPT_ID				= dep.DEPARTMENT_ID AND dep.SERV_AREA_ID = 10
				LEFT JOIN CLARITY..ZC_DEP_SPECIALTY     DSPEC  ON DEP.SPECIALTY_DEP_C					= DSPEC.DEP_SPECIALTY_C
			--	LEFT JOIN CLARITY..CLARITY_LOC			loc ON dep.REV_LOC_ID							   = loc.LOC_ID
				INNER JOIN CTE_LOC_GRP                    loc ON ISNULL(CAST(dep.REV_LOC_ID AS VARCHAR(MAX)),'NULL') = ISNULL(CAST(loc.LOC_ID AS VARCHAR(MAX)),'NULL')
			--	LEFT JOIN CLARITY..ZC_LOC_RPT_GRP_9		loc9 ON loc.RPT_GRP_NINE						   = loc9.RPT_GRP_NINE			
				LEFT JOIN CLARITY..ZC_PAT_STATUS		psts ON peh.ADT_PATIENT_STAT_C					   = psts.ADT_PATIENT_STAT_C
				LEFT JOIN CLARITY..ZC_CONF_STAT			csts ON peh.ADMIT_CONF_STAT_C					   = csts.ADMIT_CONF_STAT_C
				--=== CVG
				LEFT JOIN CLARITY..COVERAGE				cvg ON pe.COVERAGE_ID							   = cvg.COVERAGE_ID
				LEFT JOIN CLARITY..CLARITY_EPP			epp ON epp.BENEFIT_PLAN_ID						   = cvg.PLAN_ID
				LEFT JOIN CLARITY..CLARITY_EPM			epm ON epm.PAYOR_ID								   = cvg.PAYOR_ID
				LEFT JOIN CLARITY..CLARITY_EPP_2		epp2 ON epp.BENEFIT_PLAN_ID						   = epp2.BENEFIT_PLAN_ID
				LEFT JOIN CLARITY..CLARITY_EPM_2		epm2 ON epm.PAYOR_ID							   = epm2.PAYOR_ID
			   --==== ins verif
			  LEFT JOIN (
						SELECT
							pe.PAT_ID						
						  , vrx.RECORD_ID
						  , pe.PAT_ENC_CSN_ID
						  , pe.EFFECTIVE_DATE_DT
						  , pe.COVERAGE_ID
						  , cml.MEM_VERIFICATION_ID
						  , hx.VERIF_DATE_HX_DTTM
						  , hx.VERIF_STATUS_HX_C
						  , hx.LINE
						  , hx.NEXT_REV_DATE_HX_DT

						FROM	CLARITY..PAT_ENC						 pe
								INNER JOIN CLARITY..DATE_DIMENSION		 DD ON pe.EFFECTIVE_DATE_DT		 = DD.CALENDAR_DT
																			   AND DD.CALENDAR_DT BETWEEN @StartDate AND @EndDate
								INNER JOIN CLARITY..COVERAGE_MEMBER_LIST cml ON pe.COVERAGE_ID			 = cml.COVERAGE_ID 
														AND pe.PAT_ID = cml.PAT_ID
														AND ( CML.MEM_EFF_FROM_DATE <= PE.EFFECTIVE_DATE_DT AND (PE.EFFECTIVE_DATE_DT <= cml.MEM_EFF_TO_DATE OR CML.MEM_EFF_TO_DATE IS NULL))
								INNER JOIN CLARITY..VERIFICATION		 vrx ON cml.MEM_VERIFICATION_ID	 = vrx.RECORD_ID AND vrx.VERIFICATION_TYPE_C = 6
								INNER JOIN CLARITY..VERIF_STATUS_HX		 hx ON vrx.RECORD_ID			 = hx.RECORD_ID
																			   AND hx.VERIF_DATE_HX_DTTM <= pe.EFFECTIVE_DATE_DT
																			   AND	 hx.VERIF_STATUS_HX_C IN (	 '1'	--verified
																											   , '6'	--e-verified
																											   , '8'	--E-verified- Additional coverage
																											   , '12'	--verifed by phone
																											   , '13'	--verified by website
																											 )
						WHERE	1=1
						--AND pe.PAT_ID				= 'Z657898'
						--		AND	  pe.PAT_ENC_CSN_ID = 200071274887
								AND hx.LINE				=
									(
										SELECT	MAX(hx2.LINE) maxline
										FROM	CLARITY..PAT_ENC						 pe2
												INNER JOIN CLARITY..DATE_DIMENSION		 DD2 ON pe.EFFECTIVE_DATE_DT		   = DD2.CALENDAR_DT
																								AND DD.CALENDAR_DT BETWEEN @StartDate AND @EndDate
												INNER JOIN CLARITY..COVERAGE_MEMBER_LIST cml2 ON pe2.COVERAGE_ID			 = cml2.COVERAGE_ID 
																		AND pe2.PAT_ID = cml2.PAT_ID
																		AND ( CML2.MEM_EFF_FROM_DATE <= PE2.EFFECTIVE_DATE_DT AND (PE2.EFFECTIVE_DATE_DT <= cml2.MEM_EFF_TO_DATE OR CML2.MEM_EFF_TO_DATE IS NULL))
												INNER JOIN CLARITY..VERIFICATION		 vrx2 ON cml2.MEM_VERIFICATION_ID	   = vrx2.RECORD_ID
																								 AND  vrx2.VERIFICATION_TYPE_C = 6
												INNER JOIN CLARITY..VERIF_STATUS_HX		 hx2 ON vrx2.RECORD_ID				   = hx2.RECORD_ID
																								AND	  hx2.VERIF_DATE_HX_DTTM   <= pe2.EFFECTIVE_DATE_DT
																								AND hx2.VERIF_STATUS_HX_C IN (	 '1'	--verified
																															   , '6'	--e-verified
																															   , '8'	--E-verified- Additional coverage
																															   , '12'	--verifed by phone
																															   , '13'	--verified by website
																															 )
										WHERE  hx.RECORD_ID			 = hx2.RECORD_ID
											   AND pe.PAT_ENC_CSN_ID = pe2.PAT_ENC_CSN_ID
											   AND pe.COVERAGE_ID	 = pe2.COVERAGE_ID
											   AND cml.PAT_ID = cml2.PAT_ID
										GROUP BY hx2.RECORD_ID
											   , pe2.PAT_ENC_CSN_ID
											   , pe2.COVERAGE_ID
										)
			)ins_ver ON pe.PAT_ENC_CSN_ID = ins_ver.PAT_ENC_CSN_ID AND pe.COVERAGE_ID = ins_ver.COVERAGE_ID AND PE.PAT_ID = ins_ver.PAT_ID

WHERE	1										 = 1
				AND	  valpat.IS_VALID_PAT_YN	= 'Y'


				)hars
ORDER BY SERVICE_DATE,hars.LOCATION_GROUPER, hars.LOC_NAME,hars.HAR_VERF_ATTRIB_DEPT
OPTION(RECOMPILE);