USE CLARITY

SELECT CL_BEV_ALL.RECORD_ID NUMERICBASEID,
       CL_BEV_ALL.EPT_ID PATIENTID,
       CL_BEV_ALL.EPT_CSN ENCOUNTERCSN,
	   CL_BEV_ALL.DEP_ID,
	   dep.DEPARTMENT_NAME,
       COALESCE( CL_BEV_ALL.EVS_TYPE_C, 1 ) BEDCLEANTYPE,
       CASE WHEN COALESCE( CL_BEV_ALL.EVS_TYPE_C, 1 ) = 1 THEN CAST( CL_BEV_ALL.BED_ID AS varchar(50) )
            ELSE CAST( CL_BEV_ALL.EVS_NONBED_CLN_PLF_ID AS varchar(50) ) END STARTLOCATION,
       CASE WHEN CLARITY_POS.LOC_TYPE_C = 2 THEN CAST( CLARITY_POS.SERVICE_AREA_ID AS varchar(50) )
            ELSE CAST( CLARITY_POS.POS_ID AS varchar(50) ) END SERVICEAREAID,
       CASE WHEN CL_BEV_ALL.PRIORITY_C IS NULL THEN '*Unspecified'
            WHEN ZC_PRIORITY_2.PRIORITY_2_C IS NULL THEN '*Unknown'
            ELSE ZC_PRIORITY_2.NAME END CLEANPRIORITY,
       CASE WHEN CL_BEV_ALL.EVENT_SOURCE_C IS NULL THEN '*Unspecified'
            WHEN ZC_EVENT_SOURCE.EVENT_SOURCE_C IS NULL THEN '*Unknown'
            ELSE ZC_EVENT_SOURCE.NAME END PRIMARYLINKEDEVENTTYPE,
       CASE WHEN MultiStageInfoSub.BedCleanId IS NOT NULL THEN MultiStageInfoSub.MultiCleanStatus
            WHEN CL_BEV_ALL.ACTIVE_C IS NULL THEN '*Unspecified'
            WHEN ZC_ACTIVE_2.ACTIVE_2_C IS NULL THEN '*Unknown'
            ELSE ZC_ACTIVE_2.NAME END CLEANSTATUS,
       CASE WHEN MultiStageInfoSub.BedCleanId IS NOT NULL THEN MultiStageInfoSub.CancelReason
            WHEN CL_BEV_ALL.CANCEL_EVENT_TM IS NULL THEN NULL
            WHEN CL_BEV_ALL.CANCEL_REASON_C IS NULL THEN '*Unspecified'
            WHEN ZC_CANCEL_REASON_2.CANCEL_REASON_2_C IS NULL THEN '*Unknown'
            ELSE ZC_CANCEL_REASON_2.NAME END CANCELREASON,
       CASE WHEN COALESCE( CL_BEV_ALL.EVS_TYPE_C, 1 ) = 1 THEN BedCleanSubtype.NAME
            WHEN CL_BEV_ALL.EVS_NONBED_TMPLT_ID IS NULL THEN '*Unspecified'
            WHEN TASK_TEMPLATES.TASK_ID IS NULL THEN '*Unknown'
            ELSE TASK_TEMPLATES.TASK_NAME END TASK,
       CASE WHEN CL_BEV_ALL.EVENT_SOURCE_C = 4 THEN MaintenanceCleanSubtype.NAME
            ELSE COALESCE( BedCleanSubtype.NAME, OtherSubtype.NAME ) END TASKSUBTYPE,
       CASE WHEN RegionSub.RegionId IS NULL THEN '*Unspecified'
            WHEN CL_SEC.RECORD_ID IS NULL THEN '*Unknown'
            ELSE CL_SEC.RECORD_NAME END REGIONNAME,
       RegionSub.RegionId REGIONID,
       COALESCE( CL_BEV_ALL.CANCEL_USER_ID, MultiStageInfoSub.CancelUserId ) CANCELUSERID,
       CAST( CL_BEV_ALL.ADHOC_DEPT_ID AS varchar(50) ) CREATIONDEPARTMENTID,
       BedCleanEvents.DirtyDttm DIRTYINSTANT,
       BedCleanEvents.DirtyDttm FIRSTDIRTYINSTANT,
       CASE WHEN MultiStageInfoSub.BedCleanId IS NOT NULL AND MultiStageInfoSub.HasACanceledStage = 1 THEN NULL
            ELSE BedCleanEvents.CompletedDttm END COMPLETEDINSTANT,
       COALESCE( CL_BEV_ALL.CANCEL_EVENT_TM, MultiStageInfoSub.CanceledDttm ) CANCELEDINSTANT,
       COALESCE( MultiStageInfoSub.CanceledDttm, BedCleanEvents.CompletedDttm, CL_BEV_ALL.CANCEL_EVENT_TM ) RESOLUTIONINSTANT,
       CASE WHEN MultiStageInfoSub.HasACanceledStage = 1 THEN 0
            WHEN CL_BEV_ALL.ACTIVE_C = 0 THEN 1
            ELSE 0 END ISCOMPLETED,
       CASE WHEN MultiStageInfoSub.HasACanceledStage = 1 THEN 1
            WHEN CL_BEV_ALL.ACTIVE_C = 4 THEN 1
            ELSE 0 END ISCANCELED,
       CASE WHEN CL_BEV_ALL.PRIORITY_C = 0 THEN 1
            ELSE 0 END ISSTATPRIORITY
  FROM CL_BEV_ALL
    LEFT OUTER JOIN ( SELECT COALESCE( CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID ) BedCleanId,
                             MIN( CASE WHEN CL_BEV_EVENTS_ALL.STATUS_C = 1 THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END ) DirtyDttm,
                             MAX( CASE WHEN CL_BEV_EVENTS_ALL.STATUS_C = 5 THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END ) CompletedDttm
                        FROM CL_BEV_EVENTS_ALL
                          INNER JOIN CL_BEV_ALL
                            ON CL_BEV_EVENTS_ALL.RECORD_ID = CL_BEV_ALL.RECORD_ID
                        GROUP BY COALESCE( CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID ) ) BedCleanEvents
      ON CL_BEV_ALL.RECORD_ID = BedCleanEvents.BedCleanId
    LEFT OUTER JOIN TASK_TEMPLATES
      ON CL_BEV_ALL.EVS_NONBED_TMPLT_ID = TASK_TEMPLATES.TASK_ID
    LEFT OUTER JOIN ( SELECT CLARITY_DEP.DEPARTMENT_ID,
                             COALESCE( DepartmentRegionMap.RegionId, LocationRegionMap.RegionId, ServiceAreaRegionMap.RegionId ) RegionId
                        FROM CLARITY_DEP
                          LEFT OUTER JOIN ( SELECT MIN( HL_DEPTS.SEC_ID ) RegionId,
                                                   HL_DEPTS.DEPARTMENT_ID
                                              FROM HL_DEPTS
                                                INNER JOIN HL_SUBTYPES
                                                  ON HL_DEPTS.SEC_ID = HL_SUBTYPES.SEC_ID
                                                    AND HL_SUBTYPES.SUBTYPE_C = 2
                                              GROUP BY HL_DEPTS.DEPARTMENT_ID ) DepartmentRegionMap
                            ON CLARITY_DEP.DEPARTMENT_ID = DepartmentRegionMap.DEPARTMENT_ID
                          LEFT OUTER JOIN ( SELECT MIN( HL_HOSPITAL_LOCATIONS.SEC_ID ) RegionId,
                                                   HL_HOSPITAL_LOCATIONS.HOSPITAL_LOC_ID
                                              FROM HL_HOSPITAL_LOCATIONS
                                                INNER JOIN HL_SUBTYPES
                                                  ON HL_HOSPITAL_LOCATIONS.SEC_ID = HL_SUBTYPES.SEC_ID
                                                    AND HL_SUBTYPES.SUBTYPE_C = 2
                                              GROUP BY HL_HOSPITAL_LOCATIONS.HOSPITAL_LOC_ID ) LocationRegionMap
                            ON CLARITY_DEP.REV_LOC_ID = LocationRegionMap.HOSPITAL_LOC_ID
                          LEFT OUTER JOIN ( SELECT MIN( HL_SERVICE_AREAS.SEC_ID ) RegionId,
                                                   HL_SERVICE_AREAS.SERV_AREA_ID
                                              FROM HL_SERVICE_AREAS
                                                INNER JOIN HL_SUBTYPES
                                                  ON HL_SERVICE_AREAS.SEC_ID = HL_SUBTYPES.SEC_ID
                                                    AND HL_SUBTYPES.SUBTYPE_C = 2
                                              GROUP BY HL_SERVICE_AREAS.SERV_AREA_ID ) ServiceAreaRegionMap
                            ON CLARITY_DEP.SERV_AREA_ID = ServiceAreaRegionMap.SERV_AREA_ID ) RegionSub
      ON CL_BEV_ALL.DEP_ID = RegionSub.DEPARTMENT_ID
    LEFT OUTER JOIN ( SELECT CancelSub.BedCleanId,
                             CASE WHEN CancelSub.HasACanceledStage = 1 THEN COALESCE( ZcActiveCanceled.NAME, '*Unknown' )
                                  WHEN CancelSub.HasAnActiveStage = 0 THEN COALESCE( ZcActiveCompleted.NAME, '*Unknown' ) 
                                  ELSE '*Unknown' END MultiCleanStatus,
                             CancelSub.HasACanceledStage,
                             CancelSub.HasAnActiveStage,
                             CancelSub.CanceledDttm,
                             CASE WHEN CancelSub.CanceledDttm IS NULL THEN NULL
                                  WHEN CancelSub.CancelReason IS NULL THEN '*Unspecified'
                                  WHEN ZC_CANCEL_REASON_2.CANCEL_REASON_2_C IS NULL THEN '*Unknown'
                                  ELSE ZC_CANCEL_REASON_2.NAME END CancelReason,
                             CancelSub.CancelUserId
                        FROM ( SELECT CL_BEV_ALL.FIRST_STAGE_EVT_ID BedCleanId,
                                      MAX( CASE WHEN CL_BEV_ALL.ACTIVE_C = 4 THEN 1 ELSE 0 END ) HasACanceledStage,
                                      MAX( CASE WHEN CL_BEV_ALL.ACTIVE_C IN ( 1, 2, 3 ) THEN 1 ELSE 0 END ) HasAnActiveStage,
                                      MIN( CL_BEV_ALL.CANCEL_EVENT_TM ) CanceledDttm,
                                      MAX( CL_BEV_ALL.CANCEL_REASON_C ) CancelReason,
                                      MAX( CL_BEV_ALL.CANCEL_USER_ID ) CancelUserId
                                 FROM CL_BEV_ALL
                                 WHERE CL_BEV_ALL.STAGE_NUMBER IS NOT NULL
                                 GROUP BY CL_BEV_ALL.FIRST_STAGE_EVT_ID ) CancelSub
                          LEFT OUTER JOIN ZC_ACTIVE_2 ZcActiveCompleted
                            ON ZcActiveCompleted.ACTIVE_2_C = 0
                          LEFT OUTER JOIN ZC_ACTIVE_2 ZcActiveCanceled
                            ON ZcActiveCanceled.ACTIVE_2_C = 4
                          LEFT OUTER JOIN ZC_CANCEL_REASON_2
                            ON CancelSub.CancelReason = ZC_CANCEL_REASON_2.CANCEL_REASON_2_C ) MultiStageInfoSub
      ON CL_BEV_ALL.RECORD_ID = MultiStageInfoSub.BedCleanId
    LEFT OUTER JOIN CL_SEC
      ON RegionSub.RegionId = CL_SEC.RECORD_ID
    LEFT OUTER JOIN CLARITY_POS
      ON CL_BEV_ALL.EAF_ID = CLARITY_POS.POS_ID
    LEFT OUTER JOIN ZC_PRIORITY_2
      ON CL_BEV_ALL.PRIORITY_C = ZC_PRIORITY_2.PRIORITY_2_C
    LEFT OUTER JOIN ZC_ACTIVE_2
      ON CL_BEV_ALL.ACTIVE_C = ZC_ACTIVE_2.ACTIVE_2_C
    LEFT OUTER JOIN ZC_CANCEL_REASON_2
      ON CL_BEV_ALL.CANCEL_REASON_C = ZC_CANCEL_REASON_2.CANCEL_REASON_2_C
    LEFT OUTER JOIN ZC_EVENT_SOURCE
      ON CL_BEV_ALL.EVENT_SOURCE_C = ZC_EVENT_SOURCE.EVENT_SOURCE_C
    LEFT OUTER JOIN ZC_HL_TASK_SUBTYPE BedCleanSubtype
      ON COALESCE( CL_BEV_ALL.EVS_TYPE_C, 1 ) = 1
        AND BedCleanSubtype.HL_TASK_SUBTYPE_C = 2
    LEFT OUTER JOIN ZC_HL_TASK_SUBTYPE OtherSubtype
      ON COALESCE( CL_BEV_ALL.EVS_TYPE_C, 1 ) = 2
        AND OtherSubtype.HL_TASK_SUBTYPE_C = 99
    LEFT OUTER JOIN ZC_HL_TASK_SUBTYPE MaintenanceCleanSubtype
      ON CL_BEV_ALL.EVENT_SOURCE_C = 4
        AND MaintenanceCleanSubtype.HL_TASK_SUBTYPE_C = 3
	LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	  ON CL_BEV_ALL.DEP_ID = dep.DEPARTMENT_ID
  WHERE CL_BEV_ALL.EVENT_TYPE_C = 0
    AND CL_BEV_ALL.ACTIVE_C IN ( 0, 4 )
    AND ( CL_BEV_ALL.STAGE_NUMBER IS NULL OR CL_BEV_ALL.STAGE_NUMBER = 1 )
    AND ( MultiStageInfoSub.HasAnActiveStage IS NULL OR MultiStageInfoSub.HasAnActiveStage = 0 )
    --AND CL_BEV_ALL.RECORD_ID > <<LowerBound>>
    --AND CL_BEV_ALL.RECORD_ID <= <<UpperBound>>
	--AND BedCleanEvents.DirtyDttm BETWEEN '3/9/2024 00:00:00' AND '3/9/2024 23:59:59'
	AND BedCleanEvents.DirtyDttm BETWEEN '2/1/2024 00:00:00' AND '3/9/2024 23:59:59'
	ORDER BY dep.DEPARTMENT_NAME, BedCleanEvents.DirtyDttm