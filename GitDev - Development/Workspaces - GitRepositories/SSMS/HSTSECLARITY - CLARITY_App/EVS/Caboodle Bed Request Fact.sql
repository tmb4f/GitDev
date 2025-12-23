USE CLARITY

IF OBJECT_ID('tempdb..##PTOB ') IS NOT NULL
DROP TABLE ##PTOB

IF OBJECT_ID('tempdb..##PTPBT ') IS NOT NULL
DROP TABLE ##PTPBT

CREATE TABLE ##PTOB
(
  PendId varchar(18) NOT NULL,
  CompRecoveryDttm datetime,
  OutOrDttm datetime
);

INSERT INTO ##PTOB WITH ( TABLOCK )
(
  PendId,
  CompRecoveryDttm,
  OutOrDttm
)
SELECT PEND_ACTION.PEND_ID PendId,
       MAX( OrTimingEvents.CompRecoveryDttm ) CompRecoveryDttm,
       MAX( OrTimingEvents.OutOrDttm ) OutOrDttm
  FROM PEND_ACTION
    INNER JOIN PAT_ENC_HSP_2
      ON PEND_ACTION.PAT_ENC_CSN_ID = PAT_ENC_HSP_2.PAT_ENC_CSN_ID
        AND ( PAT_ENC_HSP_2.LEGACY_ADT_ENC_YN IS NULL OR PAT_ENC_HSP_2.LEGACY_ADT_ENC_YN = 'N' )
    INNER JOIN CLARITY_DEP DestinationDepartment
      ON PEND_ACTION.UNIT_ID = DestinationDepartment.DEPARTMENT_ID
        AND ( DestinationDepartment.ADT_UNIT_TYPE_C IS NULL OR DestinationDepartment.ADT_UNIT_TYPE_C <> 1 )
        AND DestinationDepartment.OR_UNIT_TYPE_C IS NULL 
        AND ( DestinationDepartment.IS_PERIOP_DEP_YN IS NULL OR DestinationDepartment.IS_PERIOP_DEP_YN = 'N' )
    INNER JOIN CLARITY_ADT
      ON PEND_ACTION.LINKED_EVENT_ID = CLARITY_ADT.XFER_IN_EVENT_ID
        AND CLARITY_ADT.EVENT_SUBTYPE_C <> 2
    INNER JOIN CLARITY_DEP SourceDepartment
      ON CLARITY_ADT.DEPARTMENT_ID = SourceDepartment.DEPARTMENT_ID
        AND ( SourceDepartment.ADT_UNIT_TYPE_C IS NULL OR SourceDepartment.ADT_UNIT_TYPE_C <> 1 )
        AND ( SourceDepartment.OR_UNIT_TYPE_C IS NOT NULL OR SourceDepartment.IS_PERIOP_DEP_YN = 'Y' )
    INNER JOIN PAT_OR_ADM_LINK 
      ON PEND_ACTION.PAT_ENC_CSN_ID = PAT_OR_ADM_LINK.OR_LINK_CSN
    INNER JOIN ( SELECT OR_LOG_TIMING_EVENTS.LOG_ID,
                        MAX( CASE WHEN OR_LOG_TIMING_EVENTS.TIMING_EVENT_C = 1700 THEN OR_LOG_TIMING_EVENTS.TIMING_EVENT_DTTM ELSE NULL END ) CompRecoveryDttm,
                        MAX( CASE WHEN OR_LOG_TIMING_EVENTS.TIMING_EVENT_C = 1200 THEN OR_LOG_TIMING_EVENTS.TIMING_EVENT_DTTM ELSE NULL END ) OutOrDttm
                   FROM OR_LOG_TIMING_EVENTS
                   GROUP BY OR_LOG_TIMING_EVENTS.LOG_ID ) OrTimingEvents
      ON PAT_OR_ADM_LINK.LOG_ID = OrTimingEvents.LOG_ID
  WHERE PEND_ACTION.PEND_EVENT_TYPE_C IN ( 1, 3 )
    AND PEND_ACTION.PEND_REQ_STATUS_C IS NOT NULL
    AND PEND_ACTION.DELETE_TIME IS NOT NULL
    AND OrTimingEvents.OutOrDttm <= PEND_ACTION.DELETE_TIME
    AND PEND_ACTION.COMPLETED_YN = 'Y'
    --AND PEND_ACTION.PEND_ID > '<<LowerBound>>'
    --AND PEND_ACTION.PEND_ID <= '<<UpperBound>>'
  GROUP BY PEND_ACTION.PEND_ID; 


CREATE TABLE ##PTPBT
(
  InEventId numeric(18,0) NOT NULL,
  OutEventType numeric(18,0),
  OutPatEncCsnId numeric(18,0),
  OutgoingEntryDttm datetime,
  OrderReleaseDttm datetime,
  OutgoingTnpPendingDttm datetime,
  OutgoingTnpAssignedDttm datetime,
  OutgoingTnpInProgressDttm datetime,
  BedVacantDttm datetime,
  BedDirtyDttm datetime,
  CleanAssignedDttm datetime,
  CleanInProgressDttm datetime,
  CleanCompDttm datetime,
  CurrentTnpPendingDttm datetime,
  CurrentTnpAssignedDttm datetime,
  CurrentTnpInProgressDttm datetime
);

INSERT INTO ##PTPBT WITH ( TABLOCK )
(
  InEventId,
  OutEventType,
  OutPatEncCsnId,
  OutgoingEntryDttm,
  OrderReleaseDttm,
  OutgoingTnpPendingDttm,
  OutgoingTnpAssignedDttm,
  OutgoingTnpInProgressDttm,
  BedVacantDttm,
  BedDirtyDttm,
  CleanAssignedDttm,
  CleanInProgressDttm,
  CleanCompDttm,
  CurrentTnpPendingDttm,
  CurrentTnpAssignedDttm,
  CurrentTnpInProgressDttm
)
SELECT MainSub.IN_EVENT_ID InEventId,
       MAX( MainSub.EVENT_TYPE_C ) OutEventType,
       MAX( MainSub.OutPatEncCsnId ) OutPatEncCsnId,
       MAX( MainSub.OutgoingEntryDttm ) OutgoingEntryDttm,
       MAX( MainSub.OrderReleaseDttm ) OrderReleaseDttm,
       MAX( MainSub.OutgoingTnpPendingDttm ) OutgoingTnpPendingDttm,
       MAX( MainSub.OutgoingTnpAssignedDttm ) OutgoingTnpAssignedDttm,
       MAX( MainSub.OutgoingTnpInProgressDttm ) OutgoingTnpInProgressDttm,
       MAX( MainSub.BedVacantDttm ) BedVacantDttm,
       MAX( MainSub.BedDirtyDttm ) BedDirtyDttm,
       MAX( MainSub.CleanAssignedDttm ) CleanAssignedDttm,
       MAX( MainSub.CleanInProgressDttm ) CleanInProgressDttm,
       MAX( MainSub.CleanCompDttm ) CleanCompDttm,
       MAX( MainSub.CurrentTnpPendingDttm ) CurrentTnpPendingDttm,
       MAX( MainSub.CurrentTnpAssignedDttm ) CurrentTnpAssignedDttm,
       MAX( MainSub.CurrentTnpInProgressDttm ) CurrentTnpInProgressDttm
  FROM ( SELECT BedTurnoverSub.IN_EVENT_ID,
                BedTurnoverSub.EVENT_TYPE_C,
                BedTurnoverSub.OUT_PAT_ENC_CSN_ID OutPatEncCsnId,
                BedTurnoverSub.OUT_ENTRY_DTTM OutgoingEntryDttm,
                BedTurnoverSub.OrderReleaseDttm,
                COALESCE( PriorHlrTransportStatusTimes.ActivationDttm, PriorTransportStatusTimes.PendingDttm ) OutgoingTnpPendingDttm,
                COALESCE( PriorHlrTransportStatusTimes.AssignedDttm, PriorTransportStatusTimes.AssignedDttm ) OutgoingTnpAssignedDttm,
                COALESCE( PriorHlrTransportStatusTimes.InProgressDttm, PriorTransportStatusTimes.InProgressDttm ) OutgoingTnpInProgressDttm,
                COALESCE( PriorHlrTransportStatusTimes.InProgressDttm, PriorTransportStatusTimes.InProgressDttm, BedTurnoverSub.OUT_EFFECTIVE_DTTM ) BedVacantDttm,
                COALESCE( BedTurnoverSub.CLEAN_START_DTTM, PriorHlrTransportStatusTimes.InProgressDttm, PriorTransportStatusTimes.InProgressDttm, BedTurnoverSub.OUT_EFFECTIVE_DTTM ) BedDirtyDttm,
                BedTurnoverSub.CLEAN_ASGN_DTTM CleanAssignedDttm,
                BedTurnoverSub.CLEAN_INP_DTTM CleanInProgressDttm,
                BedTurnoverSub.CLEAN_COMP_DTTM CleanCompDttm,
                COALESCE( CurrentHlrTransportStatusTimes.ActivationDttm, CurrentTransportStatusTimes.PendingDttm ) CurrentTnpPendingDttm,
                COALESCE( CurrentHlrTransportStatusTimes.AssignedDttm, CurrentTransportStatusTimes.AssignedDttm ) CurrentTnpAssignedDttm,
                COALESCE( CurrentHlrTransportStatusTimes.InProgressDttm, CurrentTransportStatusTimes.InProgressDttm ) CurrentTnpInProgressDttm
           FROM ( SELECT F_ADT_BED_TURNOVER.IN_EVENT_ID,
                         ClarityAdtOut.EVENT_TYPE_C,
                         F_ADT_BED_TURNOVER.CLEAN_START_DTTM,
                         F_ADT_BED_TURNOVER.CLEAN_ASGN_DTTM,
                         F_ADT_BED_TURNOVER.CLEAN_INP_DTTM,
                         F_ADT_BED_TURNOVER.CLEAN_COMP_DTTM,
                         F_ADT_BED_TURNOVER.OUT_PAT_ENC_CSN_ID,
                         F_ADT_BED_TURNOVER.OUT_ENTRY_DTTM,
                         F_ADT_BED_TURNOVER.OUT_EFFECTIVE_DTTM,
                         OrderProcSub.OrderReleaseDttm,
                         F_ADT_BED_TURNOVER.OUT_TNP_ID,
                         F_ADT_BED_TURNOVER.IN_TNP_ID,
                         F_ADT_BED_TURNOVER.IN_TXPORT_HLR_ID,
                         F_ADT_BED_TURNOVER.OUT_TXPORT_HLR_ID
                    FROM F_ADT_BED_TURNOVER
                      LEFT OUTER JOIN ( SELECT CLARITY_ADT.EVENT_ID,
                                               CLARITY_ADT.ORDER_ID,
                                               CLARITY_ADT.EVENT_TYPE_C
                                          FROM CLARITY_ADT ) ClarityAdtOut
                        ON F_ADT_BED_TURNOVER.OUT_EVENT_ID = ClarityAdtOut.EVENT_ID
                      LEFT OUTER JOIN ( SELECT PEND_ACTION.LINKED_EVENT_ID,
                                               PEND_ACTION.ADT_ORDER_ID
                                          FROM PEND_ACTION ) PendActionOut
                        ON ClarityAdtOut.EVENT_ID = PendActionOut.LINKED_EVENT_ID
                      LEFT OUTER JOIN ( SELECT ORDER_PROC.ORDER_PROC_ID,
                                               ORDER_PROC.ORDER_INST OrderReleaseDttm
                                          FROM ORDER_PROC ) OrderProcSub
                        ON COALESCE( ClarityAdtOut.ORDER_ID, PendActionOut.ADT_ORDER_ID ) = OrderProcSub.ORDER_PROC_ID
                    WHERE F_ADT_BED_TURNOVER.IN_EVENT_ID IS NOT NULL ) BedTurnoverSub
             LEFT OUTER JOIN ( SELECT TXPORT_EVENTS.TXPORT_ID,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 1 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) PendingDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 2 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 3 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) InProgressDttm
                                 FROM TXPORT_EVENTS
                                 GROUP BY TXPORT_EVENTS.TXPORT_ID ) PriorTransportStatusTimes
               ON BedTurnoverSub.OUT_TNP_ID = PriorTransportStatusTimes.TXPORT_ID
             LEFT OUTER JOIN ( SELECT TXPORT_EVENTS.TXPORT_ID,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 1 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) PendingDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 2 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 3 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) InProgressDttm
                                 FROM TXPORT_EVENTS
                                 GROUP BY TXPORT_EVENTS.TXPORT_ID ) CurrentTransportStatusTimes
               ON BedTurnoverSub.IN_TNP_ID = CurrentTransportStatusTimes.TXPORT_ID
             LEFT OUTER JOIN ( SELECT HL_REQ_STATUS_AUDIT.HLR_ID,
                                      MAX( HL_REQ_INFO.REQ_ACTIVATION_LOCAL_DTTM ) ActivationDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 10 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 25 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) InProgressDttm
                                 FROM HL_REQ_STATUS_AUDIT
                                   INNER JOIN HL_REQ_INFO
                                     ON HL_REQ_STATUS_AUDIT.HLR_ID = HL_REQ_INFO.HLR_ID
                                 GROUP BY HL_REQ_STATUS_AUDIT.HLR_ID ) PriorHlrTransportStatusTimes
               ON BedTurnoverSub.OUT_TXPORT_HLR_ID = PriorHlrTransportStatusTimes.HLR_ID
             LEFT OUTER JOIN ( SELECT HL_REQ_STATUS_AUDIT.HLR_ID,
                                      MAX( HL_REQ_INFO.REQ_ACTIVATION_LOCAL_DTTM ) ActivationDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 10 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 25 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) InProgressDttm
                                 FROM HL_REQ_STATUS_AUDIT
                                   INNER JOIN HL_REQ_INFO
                                     ON HL_REQ_STATUS_AUDIT.HLR_ID = HL_REQ_INFO.HLR_ID
                                 GROUP BY HL_REQ_STATUS_AUDIT.HLR_ID ) CurrentHlrTransportStatusTimes
               ON BedTurnoverSub.IN_TXPORT_HLR_ID = CurrentHlrTransportStatusTimes.HLR_ID
         
         UNION
         
         SELECT BedTurnoverSub.IN_EVENT_ID,
                BedTurnoverSub.EVENT_TYPE_C,
                BedTurnoverSub.OUT_PAT_ENC_CSN_ID OutPatEncCsnId,
                BedTurnoverSub.OUT_ENTRY_DTTM OutgoingEntryDttm,
                BedTurnoverSub.OrderReleaseDttm,
                COALESCE( PriorHlrTransportStatusTimes.ActivationDttm, PriorTransportStatusTimes.PendingDttm ) OutgoingTnpPendingDttm,
                COALESCE( PriorHlrTransportStatusTimes.AssignedDttm, PriorTransportStatusTimes.AssignedDttm ) OutgoingTnpAssignedDttm,
                COALESCE( PriorHlrTransportStatusTimes.InProgressDttm, PriorTransportStatusTimes.InProgressDttm ) OutgoingTnpInProgressDttm,
                COALESCE( PriorHlrTransportStatusTimes.InProgressDttm, PriorTransportStatusTimes.InProgressDttm, BedTurnoverSub.OUT_EFFECTIVE_DTTM ) BedVacantDttm,
                COALESCE( BedTurnoverSub.CLEAN_START_DTTM, PriorHlrTransportStatusTimes.InProgressDttm, PriorTransportStatusTimes.InProgressDttm, BedTurnoverSub.OUT_EFFECTIVE_DTTM ) BedDirtyDttm,
                BedTurnoverSub.CLEAN_ASSIGNED_DTTM CleanAssignedDttm,
                BedTurnoverSub.CLEAN_IN_PROGRESS_DTTM CleanInProgressDttm,
                BedTurnoverSub.CLEAN_COMPLETED_DTTM CleanCompDttm,
                COALESCE( CurrentHlrTransportStatusTimes.ActivationDttm, CurrentTransportStatusTimes.PendingDttm ) CurrentTnpPendingDttm,
                COALESCE( CurrentHlrTransportStatusTimes.AssignedDttm, CurrentTransportStatusTimes.AssignedDttm ) CurrentTnpAssignedDttm,
                COALESCE( CurrentHlrTransportStatusTimes.InProgressDttm, CurrentTransportStatusTimes.InProgressDttm ) CurrentTnpInProgressDttm
           FROM ( SELECT F_ADT_HL_BED_TURNOVER.IN_EVENT_ID,
                         ClarityAdtOut.EVENT_TYPE_C,
                         F_ADT_HL_BED_TURNOVER.CLEAN_START_DTTM,
                         F_ADT_HL_BED_TURNOVER.CLEAN_ASSIGNED_DTTM,
                         F_ADT_HL_BED_TURNOVER.CLEAN_IN_PROGRESS_DTTM,
                         F_ADT_HL_BED_TURNOVER.CLEAN_COMPLETED_DTTM,
                         F_ADT_HL_BED_TURNOVER.OUT_PAT_ENC_CSN_ID,
                         F_ADT_HL_BED_TURNOVER.OUT_ENTRY_DTTM,
                         F_ADT_HL_BED_TURNOVER.OUT_EFFECTIVE_DTTM,
                         OrderProcSub.OrderReleaseDttm,
                         F_ADT_HL_BED_TURNOVER.OUT_TRANSPORT_ID,
                         F_ADT_HL_BED_TURNOVER.IN_TRANSPORT_ID,
                         F_ADT_HL_BED_TURNOVER.OUT_TXPORT_HLR_ID,
                         F_ADT_HL_BED_TURNOVER.IN_TXPORT_HLR_ID
                    FROM F_ADT_HL_BED_TURNOVER
                      LEFT OUTER JOIN ( SELECT CLARITY_ADT.EVENT_ID,
                                               CLARITY_ADT.ORDER_ID,
                                               CLARITY_ADT.EVENT_TYPE_C
                                          FROM CLARITY_ADT ) ClarityAdtOut
                        ON F_ADT_HL_BED_TURNOVER.OUT_EVENT_ID = ClarityAdtOut.EVENT_ID
                      LEFT OUTER JOIN ( SELECT PEND_ACTION.LINKED_EVENT_ID,
                                               PEND_ACTION.ADT_ORDER_ID
                                          FROM PEND_ACTION ) PendActionOut
                        ON ClarityAdtOut.EVENT_ID = PendActionOut.LINKED_EVENT_ID
                      LEFT OUTER JOIN ( SELECT ORDER_PROC.ORDER_PROC_ID,
                                               ORDER_PROC.ORDER_INST OrderReleaseDttm
                                          FROM ORDER_PROC ) OrderProcSub
                        ON COALESCE( ClarityAdtOut.ORDER_ID, PendActionOut.ADT_ORDER_ID ) = OrderProcSub.ORDER_PROC_ID
                    WHERE F_ADT_HL_BED_TURNOVER.IN_EVENT_ID IS NOT NULL ) BedTurnoverSub
             LEFT OUTER JOIN ( SELECT TXPORT_EVENTS.TXPORT_ID,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 1 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) PendingDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 2 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 3 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) InProgressDttm
                                 FROM TXPORT_EVENTS
                                 GROUP BY TXPORT_EVENTS.TXPORT_ID ) PriorTransportStatusTimes
               ON BedTurnoverSub.OUT_TRANSPORT_ID = PriorTransportStatusTimes.TXPORT_ID
             LEFT OUTER JOIN ( SELECT TXPORT_EVENTS.TXPORT_ID,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 1 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) PendingDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 2 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 3 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM END ) InProgressDttm
                                 FROM TXPORT_EVENTS
                                 GROUP BY TXPORT_EVENTS.TXPORT_ID ) CurrentTransportStatusTimes
               ON BedTurnoverSub.IN_TRANSPORT_ID = CurrentTransportStatusTimes.TXPORT_ID
             LEFT OUTER JOIN ( SELECT HL_REQ_STATUS_AUDIT.HLR_ID,
                                      MAX( HL_REQ_INFO.REQ_ACTIVATION_LOCAL_DTTM ) ActivationDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 10 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 25 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) InProgressDttm
                                 FROM HL_REQ_STATUS_AUDIT
                                   INNER JOIN HL_REQ_INFO
                                     ON HL_REQ_STATUS_AUDIT.HLR_ID = HL_REQ_INFO.HLR_ID
                                 GROUP BY HL_REQ_STATUS_AUDIT.HLR_ID ) PriorHlrTransportStatusTimes
               ON BedTurnoverSub.OUT_TXPORT_HLR_ID = PriorHlrTransportStatusTimes.HLR_ID
             LEFT OUTER JOIN ( SELECT HL_REQ_STATUS_AUDIT.HLR_ID,
                                      MAX( HL_REQ_INFO.REQ_ACTIVATION_LOCAL_DTTM ) ActivationDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 10 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) AssignedDttm,
                                      MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 25 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM END ) InProgressDttm
                                 FROM HL_REQ_STATUS_AUDIT
                                   INNER JOIN HL_REQ_INFO
                                     ON HL_REQ_STATUS_AUDIT.HLR_ID = HL_REQ_INFO.HLR_ID
                                 GROUP BY HL_REQ_STATUS_AUDIT.HLR_ID ) CurrentHlrTransportStatusTimes
               ON BedTurnoverSub.IN_TXPORT_HLR_ID = CurrentHlrTransportStatusTimes.HLR_ID ) MainSub
  GROUP BY MainSub.IN_EVENT_ID;
  
  SELECT PEND_ACTION.PEND_ID BEDREQUESTID,
       PEND_ACTION.PAT_ID PATIENTID,
       CAST( CLARITY_POS.SERVICE_AREA_ID AS varchar(50) ) SERVICEAREAID,
       AdtEventBeforeLinkedAdtEffective.BED_ID SOURCEBEDID,
       TxIn.BED_ID DESTINATIONBEDID,
       CASE WHEN AdtEventBeforeLinkedAdtEffective.PAT_CLASS_C IS NULL THEN '*Unspecified'
            WHEN TxOutPatClass.ADT_PAT_CLASS_C IS NULL THEN '*Unknown'
            ELSE TxOutPatClass.NAME END SOURCEPATIENTCLASS,
       CASE WHEN TxIn.PAT_CLASS_C IS NULL THEN '*Unspecified'
            WHEN TxInPatClass.ADT_PAT_CLASS_C IS NULL THEN '*Unknown'
            ELSE TxInPatClass.NAME END DESTINATIONPATIENTCLASS,
       CASE WHEN AdtEventBeforeLinkedAdtEffective.TO_BASE_CLASS_C IS NULL THEN '*Unspecified'
            WHEN TxOutBasePatClass.INT_REP_BASE_CLS_C IS NULL THEN '*Unknown'
            ELSE TxOutBasePatClass.NAME END SOURCEBASEPATIENTCLASS,
       CASE WHEN TxIn.TO_BASE_CLASS_C IS NULL THEN '*Unspecified'
            WHEN TxInBasePatClass.INT_REP_BASE_CLS_C IS NULL THEN '*Unknown'
            ELSE TxInBasePatClass.NAME END DESTINATIONBASEPATIENTCLASS,
       CASE WHEN AdtEventBeforeLinkedAdtEffective.PAT_SERVICE_C IS NULL THEN '*Unspecified'
            WHEN TxOutService.HOSP_SERV_C IS NULL THEN '*Unknown'
            ELSE TxOutService.NAME END SOURCEPATIENTSERVICE,
	   InDep.DEPARTMENT_NAME AS SOURCEDEPARTMENTNAME,
       CASE WHEN PEND_ACTION.PEND_EVENT_TYPE_C = 5 THEN 'LOA'
            WHEN PEND_ACTION.PEND_EVENT_TYPE_C = 1 AND IsTransferCenter.ATCHMENT_PT_CSN_ID IS NOT NULL THEN 'Transfer Center'
            WHEN PEND_ACTION.PEND_EVENT_TYPE_C = 1 THEN 'Direct Admission'
            WHEN PEND_ACTION.PEND_EVENT_TYPE_C = 3 AND OutDep.ADT_UNIT_TYPE_C  = 1 THEN 'ED' 
            WHEN PEND_ACTION.PEND_EVENT_TYPE_C = 3 AND OutDep.IS_PERIOP_DEP_YN = 'Y' THEN 'OR'
            WHEN PEND_ACTION.PEND_EVENT_TYPE_C = 3  THEN 'IP'
            ELSE 'Other' END REQUESTORIGIN,
       CASE WHEN TxIn.PAT_SERVICE_C IS NULL THEN '*Unspecified'
            WHEN TxInService.HOSP_SERV_C IS NULL THEN '*Unknown'
            ELSE TxInService.NAME END DESTINATIONPATIENTSERVICE,
	   OutDep.DEPARTMENT_NAME AS DESTINATIONDEPARTMENTNAME,
       CASE WHEN AdtEventBeforeLinkedAdtEffective.PAT_LVL_OF_CARE_C IS NULL THEN '*Unspecified'
            WHEN TxOutLevelOfCare.LEVEL_OF_CARE_C IS NULL THEN '*Unknown'
            ELSE TxOutLevelOfCare.NAME END SOURCEPATIENTLEVELOFCARE,
       CASE WHEN TxIn.PAT_LVL_OF_CARE_C IS NULL THEN '*Unspecified'
            WHEN TxInLevelOfCare.LEVEL_OF_CARE_C IS NULL THEN '*Unknown'
            ELSE TxInLevelOfCare.NAME END DESTINATIONPATIENTLEVELOFCARE,
       CASE WHEN AdtEventBeforeLinkedAdtEffective.ACCOMMODATION_C IS NULL THEN '*Unspecified'
            WHEN TxOutAccomodation.ACCOMMODATION_C IS NULL THEN '*Unknown'
            ELSE TxOutAccomodation.NAME END SOURCEACCOMMODATION,
       CASE WHEN TxIn.ACCOMMODATION_C IS NULL THEN '*Unspecified'
            WHEN TxInAccomodation.ACCOMMODATION_C IS NULL THEN '*Unknown'
            ELSE TxInAccomodation.NAME END DESTINATIONACCOMMODATION,
       CASE WHEN PEND_ACTION.PEND_EVENT_TYPE_C IS NULL THEN '*Unspecified'
            WHEN ZC_PEND_EVENT_TYPE.PEND_EVENT_TYPE_C IS NULL THEN '*Unknown'
            ELSE ZC_PEND_EVENT_TYPE.NAME END EVENTTYPE,
       PEND_ACTION.PAT_ENC_CSN_ID ENCOUNTEREPICCSN,
       BedRequestTimes.CreationDttm CREATIONINSTANT,
       PEND_ACTION.REQUEST_TIME REQUESTEDINSTANT,
       BedRequestTimes.FirstReadyToPlanDttm FIRSTREADYTOPLANINSTANT,
       BedRequestTimes.LastReadyToPlanDttm LASTREADYTOPLANINSTANT,
       BedRequestTimes.FirstPreassignedDttm FIRSTPREASSIGNEDINSTANT,
       BedRequestTimes.LastPreassignedDttm LASTPREASSIGNEDINSTANT,
       BedRequestTimes.FirstAssignedDttm FIRSTASSIGNEDINSTANT,
       BedRequestTimes.LastAssignedDttm LASTASSIGNEDINSTANT,
       BedRequestTimes.FirstApprovedDttm FIRSTAPPROVEDINSTANT,
       BedRequestTimes.LastApprovedDttm LASTAPPROVEDINSTANT,
       BedRequestTimes.FirstRejectedDttm FIRSTREJECTEDINSTANT,
       BedRequestTimes.LastRejectedDttm LASTREJECTEDINSTANT,
       BedRequestTimes.FirstBedReadyDttm FIRSTBEDREADYINSTANT,
       BedRequestTimes.LastBedReadyDttm LASTBEDREADYINSTANT,
       BedRequestTimes.FirstReadyToMoveDttm FIRSTREADYTOMOVEINSTANT,
       BedRequestTimes.LastReadyToMoveDttm LASTREADYTOMOVEINSTANT,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            ELSE PEND_ACTION.DELETE_TIME END COMPLETIONINSTANT,
       CASE WHEN PriorBedTurnover.OutEventType = 2 THEN COALESCE( PriorBedTurnover.OrderReleaseDttm, DischargeOrders.OrderReleaseDttm ) 
            ELSE NULL END DISCHARGEORDERRELEASEINSTANT,
       PriorBedTurnover.BedVacantDttm BEDVACANTINSTANT,
       PriorBedTurnover.OutgoingTnpPendingDttm OUTTRANSPORTREQUESTEDINSTANT,
       PriorBedTurnover.OutgoingTnpAssignedDttm OUTTRANSPORTASSIGNEDINSTANT,
       PriorBedTurnover.OutgoingTnpInProgressDttm OUTTRANSPORTINPROGRESSINSTANT,
       PriorBedTurnover.BedDirtyDttm BEDDIRTYINSTANT,
       PriorBedTurnover.CleanAssignedDttm CLEANASSIGNEDINSTANT,
       PriorBedTurnover.CleanInProgressDttm CLEANINPROGRESSINSTANT,
       PriorBedTurnover.CleanCompDttm CLEANCOMPINSTANT,
       PriorBedTurnover.CurrentTnpPendingDttm INTRANSPORTREQUESTEDINSTANT,
       PriorBedTurnover.CurrentTnpAssignedDttm INTRANSPORTASSIGNEDINSTANT,
       PriorBedTurnover.CurrentTnpInProgressDttm INTRANSPORTINPROGRESSINSTANT,
       BedRequestTimes.WaitStartDttm WAITSTARTINSTANT,
       CASE WHEN EdAdmissionCsnSub.PAT_ENC_CSN_ID IS NULL THEN NULL
            WHEN FirstIpObsEffectiveTimeSub.FirstIpObsEffectiveTime IS NULL THEN NULL
            WHEN InDep.OR_UNIT_TYPE_C IS NOT NULL THEN NULL
            WHEN COALESCE( InDep.IS_PERIOP_DEP_YN, 'N' ) <> 'N' THEN NULL
            WHEN COALESCE( InDep.ADT_UNIT_TYPE_C, 0 ) = 1 THEN NULL
            WHEN CASE WHEN TxIn.EVENT_TYPE_C = 3 AND TxIn.EVENT_SUBTYPE_C <> 2 AND OutDep.ADT_UNIT_TYPE_C = 1 AND PEND_ACTION.COMPLETED_YN = 'Y' THEN 1
                      ELSE 0 END = 0 AND
                 CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) = 'N' AND PEND_ACTION.PEND_EVENT_TYPE_C = 3 AND DischargeDepartment.ADT_UNIT_TYPE_C = 1 THEN 1
                      ELSE 0 END = 0 THEN NULL
            WHEN PEND_ACTION.AUTOCREATE_SOURCE_C = 6 THEN COALESCE ( BedRequestTimes.OrderDttm, FirstIpObsEffectiveTimeSub.FirstIpObsEffectiveTime )
            WHEN BedRequestTimes.RequestedDttm IS NULL THEN FirstIpObsEffectiveTimeSub.FirstIpObsEffectiveTime
            WHEN FirstIpObsEffectiveTimeSub.FirstIpObsEffectiveTime < BedRequestTimes.RequestedDttm THEN FirstIpObsEffectiveTimeSub.FirstIpObsEffectiveTime
            ELSE BedRequestTimes.RequestedDttm END EDBOARDSTARTINSTANT,
       COALESCE( OrBoarding.CompRecoveryDttm, OrBoarding.OutOrDttm ) ORBOARDSTARTINSTANT,
       CASE WHEN PEND_ACTION.COMPLETED_YN = 'Y' THEN COALESCE( PriorBedTurnover.CurrentTnpInProgressDttm, PEND_ACTION.DELETE_TIME )
            ELSE PAT_ENC_HSP.HOSP_DISCH_TIME END WAITENDINSTANT,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.BedDirtyDttm IS NULL THEN NULL
            ELSE CAST( PEND_ACTION.DELETE_TIME - PriorBedTurnover.BedDirtyDttm AS float ) * 1440 END DIRTYTOOCCUPIEDTIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.BedDirtyDttm IS NULL THEN NULL
            WHEN BedRequestTimes.WaitStartDttm > PEND_ACTION.DELETE_TIME THEN 0
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.BedDirtyDttm THEN CAST( PEND_ACTION.DELETE_TIME - BedRequestTimes.WaitStartDttm AS float ) * 1440
            ELSE CAST( PEND_ACTION.DELETE_TIME - PriorBedTurnover.BedDirtyDttm AS float ) * 1440 END WAITINGDIRTYTOOCCUPIEDTIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.CleanCompDttm IS NULL THEN NULL
            ELSE CAST( PEND_ACTION.DELETE_TIME - PriorBedTurnover.CleanCompDttm AS float ) * 1440 END BEDCLEANTOOCCUPIEDTIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.CleanCompDttm IS NULL THEN NULL
            WHEN BedRequestTimes.WaitStartDttm > PEND_ACTION.DELETE_TIME THEN 0
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.CleanCompDttm THEN CAST( PEND_ACTION.DELETE_TIME - BedRequestTimes.WaitStartDttm AS float ) * 1440
            ELSE CAST( PEND_ACTION.DELETE_TIME - PriorBedTurnover.CleanCompDttm AS float ) * 1440 END WAITINGBEDCLEANTOOCCUPIEDTIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.BedVacantDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanInProgressDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanCompDttm IS NULL THEN NULL
            ELSE ( CAST( PEND_ACTION.DELETE_TIME - PriorBedTurnover.BedVacantDttm AS float ) * 1440 ) - ( CAST( PriorBedTurnover.CleanCompDttm - PriorBedTurnover.CleanInProgressDttm AS float ) * 1440 ) END IDLEBEDTIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.BedVacantDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanInProgressDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanCompDttm IS NULL THEN NULL
            WHEN BedRequestTimes.WaitStartDttm > PEND_ACTION.DELETE_TIME THEN 0
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.CleanCompDttm THEN CAST( PEND_ACTION.DELETE_TIME - BedRequestTimes.WaitStartDttm AS float ) * 1440
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.CleanInProgressDttm THEN CAST( PEND_ACTION.DELETE_TIME - PriorBedTurnover.CleanCompDttm AS float ) * 1440
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.BedVacantDttm THEN ( CAST( PEND_ACTION.DELETE_TIME - BedRequestTimes.WaitStartDttm AS float ) * 1440 ) - ( CAST( PriorBedTurnover.CleanCompDttm - PriorBedTurnover.CleanInProgressDttm AS float ) * 1440 )
            ELSE ( CAST( PEND_ACTION.DELETE_TIME - PriorBedTurnover.BedVacantDttm AS float ) * 1440 ) - ( CAST( PriorBedTurnover.CleanCompDttm - PriorBedTurnover.CleanInProgressDttm AS float ) * 1440 ) END WAITINGIDLEBEDTIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.BedVacantDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanInProgressDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanCompDttm IS NULL THEN NULL
            ELSE ( CAST( COALESCE( PriorBedTurnover.CurrentTnpInProgressDttm, PEND_ACTION.DELETE_TIME ) - PriorBedTurnover.BedVacantDttm AS float ) * 1440 ) - ( CAST( PriorBedTurnover.CleanCompDttm - PriorBedTurnover.CleanInProgressDttm AS float ) * 1440 ) END AVOIDABLETIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.BedVacantDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanInProgressDttm IS NULL THEN NULL
            WHEN PriorBedTurnover.CleanCompDttm IS NULL THEN NULL
            WHEN BedRequestTimes.WaitStartDttm > COALESCE( PriorBedTurnover.CurrentTnpInProgressDttm, PEND_ACTION.DELETE_TIME ) THEN 0
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.CleanCompDttm THEN CAST( COALESCE( PriorBedTurnover.CurrentTnpInProgressDttm, PEND_ACTION.DELETE_TIME ) - BedRequestTimes.WaitStartDttm AS float ) * 1440
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.CleanInProgressDttm THEN CAST( COALESCE( PriorBedTurnover.CurrentTnpInProgressDttm, PEND_ACTION.DELETE_TIME ) - PriorBedTurnover.CleanCompDttm AS float ) * 1440
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.BedVacantDttm THEN ( CAST( COALESCE( PriorBedTurnover.CurrentTnpInProgressDttm, PEND_ACTION.DELETE_TIME ) - BedRequestTimes.WaitStartDttm AS float ) * 1440 ) - ( CAST( PriorBedTurnover.CleanCompDttm - PriorBedTurnover.CleanInProgressDttm AS float ) * 1440 )
            ELSE ( CAST( COALESCE( PriorBedTurnover.CurrentTnpInProgressDttm, PEND_ACTION.DELETE_TIME ) - PriorBedTurnover.BedVacantDttm AS float ) * 1440 ) - ( CAST( PriorBedTurnover.CleanCompDttm - PriorBedTurnover.CleanInProgressDttm AS float ) * 1440 ) END WAITINGAVOIDABLETIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN BedRequestTimes.WaitStartDttm > PEND_ACTION.DELETE_TIME THEN 0
            ELSE CAST( PEND_ACTION.DELETE_TIME - BedRequestTimes.WaitStartDttm AS float ) * 1440 END WAITSTARTTOOCCUPIEDTIME,
       CASE WHEN COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN NULL
            WHEN PriorBedTurnover.CurrentTnpInProgressDttm IS NULL THEN NULL
            WHEN BedRequestTimes.WaitStartDttm > PriorBedTurnover.CurrentTnpInProgressDttm THEN 0
            ELSE CAST( PriorBedTurnover.CurrentTnpInProgressDttm - BedRequestTimes.WaitStartDttm AS float ) * 1440 END WAITSTARTTOTNPINPROGRESS,
       BedRequestTimes.NumberOfRejections NUMBEROFREJECTIONS,
       CASE WHEN PEND_ACTION.ASSIGNED_TIME < PriorBedTurnover.CleanCompDttm THEN 1
            ELSE 0 END WASASSIGNEDDIRTYBED,
       CASE WHEN PEND_ACTION.IS_DELETED_YN = 'Y' AND COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) <> 'Y' THEN 1
            ELSE 0 END CANCELED,
       CASE WHEN CL_DEP_LEVEL_OF_CARE.DEPARTMENT_ID IS NULL THEN 1
            ELSE 0 END OFFLEVELOFCARE,
       CASE WHEN DepartmentService.DEPARTMENT_ID IS NULL THEN 1
            ELSE 0 END OFFSERVICE
  FROM PEND_ACTION
    INNER JOIN PAT_ENC_HSP_2
      ON PEND_ACTION.PAT_ENC_CSN_ID = PAT_ENC_HSP_2.PAT_ENC_CSN_ID
        AND ( PAT_ENC_HSP_2.LEGACY_ADT_ENC_YN IS NULL OR PAT_ENC_HSP_2.LEGACY_ADT_ENC_YN = 'N' )
    LEFT OUTER JOIN CLARITY_ADT TxIn
      ON PEND_ACTION.LINKED_EVENT_ID = TxIn.EVENT_ID
    LEFT OUTER JOIN CLARITY_DEP InDep
      ON PEND_ACTION.UNIT_ID = InDep.DEPARTMENT_ID
    LEFT OUTER JOIN CLARITY_ADT TxOut
      ON TxIn.XFER_EVENT_ID = TxOut.EVENT_ID
    LEFT OUTER JOIN CLARITY_DEP OutDep
      ON TxOut.DEPARTMENT_ID = OutDep.DEPARTMENT_ID
    LEFT OUTER JOIN ( SELECT PEND_ACTION.PEND_ID,
                             MAX( AdtOnCsn.SEQ_NUM_IN_ENC ) SequenceNumBeforeLinkedAdt
                        FROM PEND_ACTION
                          LEFT OUTER JOIN CLARITY_ADT AdtOnCsn
                            ON PEND_ACTION.PAT_ENC_CSN_ID = AdtOnCsn.PAT_ENC_CSN_ID
                          LEFT OUTER JOIN CLARITY_ADT LinkedAdt
                            ON PEND_ACTION.LINKED_EVENT_ID = LinkedAdt.EVENT_ID
                          LEFT OUTER JOIN ( SELECT BED_PLAN_HX.PEND_ID,
                                                   MAX( BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ) LastHxDttm
                                              FROM BED_PLAN_HX
                                              --WHERE BED_PLAN_HX.PEND_ID > '<<LowerBound>>'
                                              --  AND BED_PLAN_HX.PEND_ID <= '<<UpperBound>>'
                                              GROUP BY BED_PLAN_HX.PEND_ID ) PndHx
                            ON PEND_ACTION.PEND_ID = PndHx.PEND_ID
                        WHERE AdtOnCsn.EFFECTIVE_TIME < ( CASE WHEN LinkedAdt.EFFECTIVE_TIME IS NULL THEN PndHx.LastHxDttm ELSE LinkedAdt.EFFECTIVE_TIME END )
                          --AND PEND_ACTION.PEND_ID > '<<LowerBound>>'
                          --AND PEND_ACTION.PEND_ID <= '<<UpperBound>>'
                        GROUP BY PEND_ACTION.PEND_ID ) AdtSequenceNumBeforeLinkedAdt
      ON PEND_ACTION.PEND_ID = AdtSequenceNumBeforeLinkedAdt.PEND_ID
    LEFT OUTER JOIN CLARITY_ADT AdtEventBeforeLinkedAdtEffective
      ON PEND_ACTION.PAT_ENC_CSN_ID = AdtEventBeforeLinkedAdtEffective.PAT_ENC_CSN_ID
        AND AdtEventBeforeLinkedAdtEffective.SEQ_NUM_IN_ENC = AdtSequenceNumBeforeLinkedAdt.SequenceNumBeforeLinkedAdt
    LEFT OUTER JOIN ( SELECT CLARITY_ADT.PAT_ENC_CSN_ID,
                             COALESCE( MIN( CASE WHEN CLARITY_ADT.TO_BASE_CLASS_C = 1 THEN CLARITY_ADT.EFFECTIVE_TIME ELSE NULL END ),
                               MIN( CASE WHEN CLARITY_ADT.TO_BASE_CLASS_C = 4 THEN CLARITY_ADT.EFFECTIVE_TIME ELSE NULL END ) ) FirstIpObsEffectiveTime
                        FROM CLARITY_ADT
                        WHERE CLARITY_ADT.EVENT_TYPE_C IN ( 3, 5 )
                          AND CLARITY_ADT.EVENT_SUBTYPE_C <> 2
                          AND CLARITY_ADT.TO_BASE_CLASS_C IN ( 1, 4 )
                        GROUP BY CLARITY_ADT.PAT_ENC_CSN_ID ) FirstIpObsEffectiveTimeSub
      ON PEND_ACTION.PAT_ENC_CSN_ID = FirstIpObsEffectiveTimeSub.PAT_ENC_CSN_ID
    LEFT OUTER JOIN ( SELECT CLARITY_ADT.PAT_ENC_CSN_ID
                        FROM CLARITY_ADT
                          INNER JOIN CLARITY_DEP
                            ON CLARITY_ADT.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
                              AND CLARITY_DEP.ADT_UNIT_TYPE_C = 1
                        WHERE CLARITY_ADT.EVENT_TYPE_C = 1
                          AND CLARITY_ADT.EVENT_SUBTYPE_C <> 2
                          AND CLARITY_ADT.TO_BASE_CLASS_C = 3 ) EdAdmissionCsnSub
      ON PEND_ACTION.PAT_ENC_CSN_ID = EdAdmissionCsnSub.PAT_ENC_CSN_ID
    LEFT OUTER JOIN PAT_ENC_HSP
      ON PEND_ACTION.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
    LEFT OUTER JOIN CLARITY_DEP DischargeDepartment
      ON PAT_ENC_HSP.DEPARTMENT_ID = DischargeDepartment.DEPARTMENT_ID
    LEFT OUTER JOIN ( SELECT CUST_SERV_ATCHMENT.ATCHMENT_PT_CSN_ID
                        FROM CUST_SERV_ATCHMENT
                        WHERE CUST_SERV_ATCHMENT.ATCHMENT_TYPE_C = 18
                        GROUP BY CUST_SERV_ATCHMENT.ATCHMENT_PT_CSN_ID ) IsTransferCenter
      ON PAT_ENC_HSP.PAT_ENC_CSN_ID = IsTransferCenter.ATCHMENT_PT_CSN_ID
    LEFT OUTER JOIN ##PTPBT PriorBedTurnover
      ON PEND_ACTION.LINKED_EVENT_ID = PriorBedTurnover.InEventId
    LEFT OUTER JOIN ( SELECT BED_PLAN_HX.PEND_ID,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 1 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) CreationDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 2 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) RequestedDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 3 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) FirstAssignedDttm,
                             MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 3 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) LastAssignedDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 4 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) FirstRejectedDttm,
                             MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 4 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) LastRejectedDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 5 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) FirstApprovedDttm,
                             MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 5 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) LastApprovedDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 10 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) FirstReadyToPlanDttm,
                             MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 10 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) LastReadyToPlanDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 11 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) FirstPreassignedDttm,
                             MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 11 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) LastPreassignedDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 12 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) FirstBedReadyDttm,
                             MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 12 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) LastBedReadyDttm,
                             MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 13 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) FirstReadyToMoveDttm,
                             MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 13 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) LastReadyToMoveDttm,
                             COALESCE( MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 13 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ),
                                        MAX( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 10 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ),
                                        MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 1 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) ) WaitStartDttm,
                             COUNT( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 4 THEN 1 ELSE NULL END ) NumberOfRejections,
                             MIN( CASE WHEN BED_PLAN_HX.ORD_ID IS NOT NULL THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) OrderDttm
                        FROM BED_PLAN_HX
                        --WHERE BED_PLAN_HX.PEND_ID > '<<LowerBound>>'
                        --  AND BED_PLAN_HX.PEND_ID <= '<<UpperBound>>'
                        GROUP BY BED_PLAN_HX.PEND_ID ) BedRequestTimes
      ON PEND_ACTION.PEND_ID = BedRequestTimes.PEND_ID
    LEFT OUTER JOIN ( SELECT ORDER_PROC.PAT_ENC_CSN_ID,
                             MAX( ORDER_PROC.ORDER_INST ) OrderReleaseDttm
                        FROM ORDER_PROC
                          INNER JOIN ORDER_PROC_5
                            ON ORDER_PROC.ORDER_PROC_ID = ORDER_PROC_5.ORDER_ID
                              AND ORDER_PROC_5.ADT_ORDER_TYPE_C = 3
                        GROUP BY ORDER_PROC.PAT_ENC_CSN_ID ) DischargeOrders
      ON PriorBedTurnover.OutPatEncCsnId = DischargeOrders.PAT_ENC_CSN_ID
    LEFT OUTER JOIN ##PTOB OrBoarding
      ON PEND_ACTION.PEND_ID = OrBoarding.PendId
    LEFT OUTER JOIN CL_DEP_LEVEL_OF_CARE
      ON PEND_ACTION.UNIT_ID = CL_DEP_LEVEL_OF_CARE.DEPARTMENT_ID
        AND PEND_ACTION.LVL_OF_CARE_C = CL_DEP_LEVEL_OF_CARE.ADT_ALLOWED_LOC_C
    LEFT OUTER JOIN ( SELECT DISTINCT CL_DEP_SERVICE.DEPARTMENT_ID, 
                                      CL_DEP_SERVICE.ADT_SERVICE_C 
                        FROM CL_DEP_SERVICE ) DepartmentService
      ON PEND_ACTION.UNIT_ID = DepartmentService.DEPARTMENT_ID
        AND PEND_ACTION.PAT_SERVICE_C = DepartmentService.ADT_SERVICE_C
    LEFT OUTER JOIN CLARITY_POS
      ON PEND_ACTION.HOSPITAL_AREA_ID = CLARITY_POS.POS_ID
    LEFT OUTER JOIN ZC_PAT_CLASS TxInPatClass
      ON TxIn.PAT_CLASS_C = TxInPatClass.ADT_PAT_CLASS_C
    LEFT OUTER JOIN ZC_REP_BASE_CLASS TxInBasePatClass
      ON TxIn.TO_BASE_CLASS_C = TxInBasePatClass.INT_REP_BASE_CLS_C
    LEFT OUTER JOIN ZC_PAT_CLASS TxOutPatClass
      ON AdtEventBeforeLinkedAdtEffective.PAT_CLASS_C = TxOutPatClass.ADT_PAT_CLASS_C
    LEFT OUTER JOIN ZC_REP_BASE_CLASS TxOutBasePatClass
      ON AdtEventBeforeLinkedAdtEffective.TO_BASE_CLASS_C = TxOutBasePatClass.INT_REP_BASE_CLS_C
    LEFT OUTER JOIN ZC_PAT_SERVICE TxInService
      ON TxIn.PAT_SERVICE_C = TxInService.HOSP_SERV_C
    LEFT OUTER JOIN ZC_PAT_SERVICE TxOutService
      ON AdtEventBeforeLinkedAdtEffective.PAT_SERVICE_C = TxOutService.HOSP_SERV_C
    LEFT OUTER JOIN ZC_LVL_OF_CARE TxInLevelOfCare
      ON TxIn.PAT_LVL_OF_CARE_C = TxInLevelOfCare.LEVEL_OF_CARE_C
    LEFT OUTER JOIN ZC_LVL_OF_CARE TxOutLevelOfCare
      ON AdtEventBeforeLinkedAdtEffective.PAT_LVL_OF_CARE_C = TxOutLevelOfCare.LEVEL_OF_CARE_C
    LEFT OUTER JOIN ZC_ACCOMMODATION TxInAccomodation
      ON TxIn.ACCOMMODATION_C = TxInAccomodation.ACCOMMODATION_C
    LEFT OUTER JOIN ZC_ACCOMMODATION TxOutAccomodation
      ON AdtEventBeforeLinkedAdtEffective.ACCOMMODATION_C = TxOutAccomodation.ACCOMMODATION_C
    LEFT OUTER JOIN ZC_PEND_EVENT_TYPE
      ON PEND_ACTION.PEND_EVENT_TYPE_C = ZC_PEND_EVENT_TYPE.PEND_EVENT_TYPE_C
  WHERE PEND_ACTION.PEND_EVENT_TYPE_C IN ( 1, 3 ) -- Admission, Transfer
    AND PEND_ACTION.PEND_REQ_STATUS_C IS NOT NULL
    AND PEND_ACTION.DELETE_TIME IS NOT NULL
	--AND BedRequestTimes.CreationDttm >= '1/1/2024 00:00:00'
	--AND BedRequestTimes.CreationDttm >= '2/9/2024 00:00:00' AND BedRequestTimes.CreationDttm <= '2/9/2024 23:59:59'
	AND PriorBedTurnover.BedDirtyDttm >= '2/9/2024 00:00:00' AND PriorBedTurnover.BedDirtyDttm <= '2/9/2024 23:59:59'
	
    --AND PEND_ACTION.PEND_ID > '<<LowerBound>>'
    --AND PEND_ACTION.PEND_ID <= '<<UpperBound>>'
	--ORDER BY CAST(BedRequestTimes.CreationDttm AS DATE), InDep.DEPARTMENT_NAME
	ORDER BY CAST(PriorBedTurnover.BedDirtyDttm AS DATE), InDep.DEPARTMENT_NAME