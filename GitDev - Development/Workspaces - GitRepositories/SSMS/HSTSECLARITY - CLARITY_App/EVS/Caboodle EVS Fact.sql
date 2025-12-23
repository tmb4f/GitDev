USE CLARITY

SELECT CAST(CL_BEV_ALL.RECORD_ID AS NUMERIC(18,0)) NUMERICBASEID,
       CAST(CL_BEV_ALL.BED_ID AS VARCHAR(50)) BEDID,
	   CL_BEV_ALL.DEP_ID,
	   dep.DEPARTMENT_NAME,
       CAST(CL_BEV_ALL.EPT_CSN AS varchar(50)) ENCOUNTERID,
       CAST(CASE WHEN CL_BEV_ALL.EVENT_SOURCE_C = 3 THEN 'EpicUserId'
                 ELSE 'NotApplicable' END AS varchar(50)) REQUESTERIDTYPE,
       CAST(CL_BEV_ALL.ADHOC_USER_ID AS varchar(50)) REQUESTERID,
       UnpivotedEvents.STAGEONECREATEDINSTANT STAGEONECREATEDINSTANT,
       UnpivotedEvents.STAGETWOCREATEDINSTANT STAGETWOCREATEDINSTANT,
       UnpivotedEvents.STAGETHREECREATEDINSTANT STAGETHREECREATEDINSTANT,
       UnpivotedEvents.STAGEFOURCREATEDINSTANT STAGEFOURCREATEDINSTANT,
       UnpivotedEvents.STAGEONEFIRSTASSIGNINSTANT STAGEONEFIRSTASSIGNINSTANT,
       UnpivotedEvents.STAGETWOFIRSTASSIGNINSTANT STAGETWOFIRSTASSIGNINSTANT,
       UnpivotedEvents.STAGETHREEFIRSTASSIGNINSTANT STAGETHREEFIRSTASSIGNINSTANT,
       UnpivotedEvents.STAGEFOURFIRSTASSIGNINSTANT STAGEFOURFIRSTASSIGNINSTANT,
       UnpivotedEvents.STAGEONELASTASSIGNINSTANT STAGEONELASTASSIGNINSTANT,
       UnpivotedEvents.STAGETWOLASTASSIGNINSTANT STAGETWOLASTASSIGNINSTANT,
       UnpivotedEvents.STAGETHREELASTASSIGNINSTANT STAGETHREELASTASSIGNINSTANT,
       UnpivotedEvents.STAGEFOURLASTASSIGNINSTANT STAGEFOURLASTASSIGNINSTANT,
       UnpivotedEvents.STAGEONEFIRSTINPROGRESS STAGEONEFIRSTINPROGRESS,
       UnpivotedEvents.STAGETWOFIRSTINPROGRESS STAGETWOFIRSTINPROGRESS,
       UnpivotedEvents.STAGETHREEFIRSTINPROGRESS STAGETHREEFIRSTINPROGRESS,
       UnpivotedEvents.STAGEFOURFIRSTINPROGRESS STAGEFOURFIRSTINPROGRESS,
       UnpivotedEvents.STAGEONELASTINPROGRESS STAGEONELASTINPROGRESS,
       UnpivotedEvents.STAGETWOLASTINPROGRESS STAGETWOLASTINPROGRESS,
       UnpivotedEvents.STAGETHREELASTINPROGRESS STAGETHREELASTINPROGRESS,
       UnpivotedEvents.STAGEFOURLASTINPROGRESS STAGEFOURLASTINPROGRESS,
       UnpivotedEvents.STAGEONECOMPLETEDINSTANT STAGEONECOMPLETEDINSTANT,
       UnpivotedEvents.STAGETWOCOMPLETEDINSTANT STAGETWOCOMPLETEDINSTANT,
       UnpivotedEvents.STAGETHREECOMPLETEDINSTANT STAGETHREECOMPLETEDINSTANT,
       UnpivotedEvents.STAGEFOURCOMPLETEDINSTANT STAGEFOURCOMPLETEDINSTANT,
       UnpivotedEvents.CLEANCOMPLETEDINSTANT CLEANCOMPLETEDINSTANT,
       CAST(UnpivotedEvents.STAGEONEHOUSEKEEPERID AS varchar(50)) STAGEONEHOUSEKEEPERID,
       CAST(UnpivotedEvents.STAGETWOHOUSEKEEPERID AS varchar(50)) STAGETWOHOUSEKEEPERID,
       CAST(UnpivotedEvents.STAGETHREEHOUSEKEEPERID AS varchar(50)) STAGETHREEHOUSEKEEPERID,
       CAST(UnpivotedEvents.STAGEFOURHOUSEKEEPERID AS varchar(50)) STAGEFOURHOUSEKEEPERID,
       CAST(UnpivotedEvents.STAGEONEHOUSEKEEPERIDTYPE AS varchar(50)) STAGEONEHOUSEKEEPERIDTYPE,
       CAST(UnpivotedEvents.STAGETWOHOUSEKEEPERIDTYPE AS varchar(50)) STAGETWOHOUSEKEEPERIDTYPE,
       CAST(UnpivotedEvents.STAGETHREEHOUSEKEEPERIDTYPE AS varchar(50)) STAGETHREEHOUSEKEEPERIDTYPE,
       CAST(UnpivotedEvents.STAGEFOURHOUSEKEEPERIDTYPE AS varchar(50)) STAGEFOURHOUSEKEEPERIDTYPE,
       CAST(COALESCE(PivotedOut.StageOneMinutesDelayed, 0) AS integer) STAGEONEMINUTESDELAYED,
       CAST(COALESCE(PivotedOut.StageTwoMinutesDelayed, 0) AS integer) STAGETWOMINUTESDELAYED,
       CAST(COALESCE(PivotedOut.StageThreeMinutesDelayed, 0) AS integer) STAGETHREEMINUTESDELAYED,
       CAST(COALESCE(PivotedOut.StageFourMinutesDelayed, 0) AS integer) STAGEFOURMINUTESDELAYED,
       CAST(COALESCE(PivotedOut.TotalMinutesDelayed, 0) AS integer) TOTALMINUTESDELAYED,
       CAST(COALESCE(PivotedOut.StageOneMinutesHeld, 0) AS integer) STAGEONEMINUTESONHOLD,
       CAST(COALESCE(PivotedOut.StageTwoMinutesHeld, 0) AS integer) STAGETWOMINUTESONHOLD,
       CAST(COALESCE(PivotedOut.StageThreeMinutesHeld, 0) AS integer) STAGETHREEMINUTESONHOLD,
       CAST(COALESCE(PivotedOut.StageFourMinutesHeld, 0) AS integer) STAGEFOURMINUTESONHOLD,
       CAST(COALESCE(PivotedOut.TotalMinutesHeld, 0) AS integer) TOTALMINUTESONHOLD,
       CAST(COALESCE(UnpivotedEvents.NUMBEROFSTAGES, 1) AS integer) NUMBEROFSTAGES,
       CAST(CASE WHEN NULLIF(PivotedOut.TotalMinutesHeld, 0) IS NOT NULL THEN 1 ELSE 0 END AS integer) ONHOLD,
       CAST(CASE WHEN NULLIF(PivotedOut.TotalMinutesDelayed, 0) IS NOT NULL THEN 1 ELSE 0 END AS integer) DELAYED,
       CAST(CASE WHEN Escalations.RECORD_ID IS NOT NULL THEN 1 ELSE 0 END AS integer) ESCALATED, 
       CAST(COALESCE(Protocols.FirstProtocol, '*Not Applicable') AS varchar(300)) FIRSTPROTOCOL,
       CAST(COALESCE(Protocols.SecondProtocol, '*Not Applicable') AS varchar(300)) SECONDPROTOCOL,
       CAST(COALESCE(Protocols.ThirdProtocol, '*Not Applicable') AS varchar(300)) THIRDPROTOCOL,
       CAST(UnpivotedEvents.STAGEONEFINALPRIORITY AS varchar(300)) STAGEONEFINALPRIORITY,
       CAST(COALESCE(UnpivotedEvents.STAGETWOFINALPRIORITY, '*Not Applicable') AS varchar(300)) STAGETWOFINALPRIORITY,
       CAST(COALESCE(UnpivotedEvents.STAGETHREEFINALPRIORITY, '*Not Applicable') AS varchar(300)) STAGETHREEFINALPRIORITY,
       CAST(COALESCE(UnpivotedEvents.STAGEFOURFINALPRIORITY, '*Not Applicable') AS varchar(300)) STAGEFOURFINALPRIORITY,
       CAST(FinalPriority.CLEANFINALPRIORITY AS varchar(300)) CLEANFINALPRIORITY,
       CAST(UnpivotedEvents.STAGEONENAME AS varchar(300)) STAGEONENAME,
       CAST(COALESCE(UnpivotedEvents.STAGETWONAME, '*Not Applicable') AS varchar(300)) STAGETWONAME,
       CAST(COALESCE(UnpivotedEvents.STAGETHREENAME, '*Not Applicable') AS varchar(300)) STAGETHREENAME,
       CAST(COALESCE(UnpivotedEvents.STAGEFOURNAME, '*Not Applicable') AS varchar(300)) STAGEFOURNAME,
       CAST(COALESCE(PivotedOut.StageOneFirstDelayReason, '*Not Applicable') AS varchar(300)) STAGEONEFIRSTDELAYREASON,
       CAST(COALESCE(PivotedOut.StageTwoFirstDelayReason, '*Not Applicable') AS varchar(300)) STAGETWOFIRSTDELAYREASON,
       CAST(COALESCE(PivotedOut.StageThreeFirstDelayReason, '*Not Applicable') AS varchar(300)) STAGETHREEFIRSTDELAYREASON,
       CAST(COALESCE(PivotedOut.StageFourFirstDelayReason, '*Not Applicable') AS varchar(300)) STAGEFOURFIRSTDELAYREASON,
       CAST(COALESCE(PivotedOut.StageOneSecondDelayReason, '*Not Applicable') AS varchar(300)) STAGEONESECONDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageTwoSecondDelayReason, '*Not Applicable') AS varchar(300)) STAGETWOSECONDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageThreeSecondDelayReason, '*Not Applicable') AS varchar(300)) STAGETHREESECONDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageFourSecondDelayReason, '*Not Applicable') AS varchar(300)) STAGEFOURSECONDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageOneThirdDelayReason, '*Not Applicable') AS varchar(300)) STAGEONETHIRDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageTwoThirdDelayReason, '*Not Applicable') AS varchar(300)) STAGETWOTHIRDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageThreeThirdDelayReason, '*Not Applicable') AS varchar(300)) STAGETHREETHIRDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageFourThirdDelayReason, '*Not Applicable') AS varchar(300)) STAGEFOURTHIRDDELAYREASON,
       CAST(COALESCE(PivotedOut.StageOneFirstHoldReason, '*Not Applicable') AS varchar(300)) STAGEONEFIRSTONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageTwoFirstHoldReason, '*Not Applicable') AS varchar(300)) STAGETWOFIRSTONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageThreeFirstHoldReason, '*Not Applicable') AS varchar(300)) STAGETHREEFIRSTONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageFourFirstHoldReason, '*Not Applicable') AS varchar(300)) STAGEFOURFIRSTONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageOneSecondHoldReason, '*Not Applicable') AS varchar(300)) STAGEONESECONDONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageTwoSecondHoldReason, '*Not Applicable') AS varchar(300)) STAGETWOSECONDONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageThreeSecondHoldReason, '*Not Applicable') AS varchar(300)) STAGETHREESECONDONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageFourSecondHoldReason, '*Not Applicable') AS varchar(300)) STAGEFOURSECONDONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageOneThirdHoldReason, '*Not Applicable') AS varchar(300)) STAGEONETHIRDONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageTwoThirdHoldReason, '*Not Applicable') AS varchar(300)) STAGETWOTHIRDONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageThreeThirdHoldReason, '*Not Applicable') AS varchar(300)) STAGETHREETHIRDONHOLDREASON,
       CAST(COALESCE(PivotedOut.StageFourThirdHoldReason, '*Not Applicable') AS varchar(300)) STAGEFOURTHIRDONHOLDREASON,
       CAST(CASE WHEN CL_BEV_ALL.EVENT_SOURCE_C IS NULL THEN '*Unspecified'
                 WHEN ZC_EVENT_SOURCE.EVENT_SOURCE_C IS NULL THEN '*Unknown'
                 ELSE ZC_EVENT_SOURCE.NAME END AS varchar(300)) REQUESTSOURCE
  FROM CL_BEV_ALL
    LEFT OUTER JOIN (SELECT COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) RECORD_ID,
                            MAX(CASE WHEN CL_BEV_ISOLATIONS.LINE = 1 THEN 
                                       CASE WHEN NULLIF(CL_BEV_ISOLATIONS.ISOLATION_C, '') IS NULL THEN '*Unspecified'
                                            WHEN NULLIF(ZC_CLEANING_PROTCL.CLEANING_PROTCL_C, '') IS NULL THEN '*Unknown'
                                            ELSE ZC_CLEANING_PROTCL.NAME END END) FirstProtocol,
                            MAX(CASE WHEN CL_BEV_ISOLATIONS.LINE = 2 THEN 
                                       CASE WHEN NULLIF(CL_BEV_ISOLATIONS.ISOLATION_C, '') IS NULL THEN '*Unspecified'
                                            WHEN NULLIF(ZC_CLEANING_PROTCL.CLEANING_PROTCL_C, '') IS NULL THEN '*Unknown'
                                            ELSE ZC_CLEANING_PROTCL.NAME END END) SecondProtocol,
                            MAX(CASE WHEN CL_BEV_ISOLATIONS.LINE = 3 THEN 
                                       CASE WHEN NULLIF(CL_BEV_ISOLATIONS.ISOLATION_C, '') IS NULL THEN '*Unspecified'
                                            WHEN NULLIF(ZC_CLEANING_PROTCL.CLEANING_PROTCL_C, '') IS NULL THEN '*Unknown'
                                            ELSE ZC_CLEANING_PROTCL.NAME END END) ThirdProtocol
                       FROM CL_BEV_ALL
                         LEFT OUTER JOIN CL_BEV_ISOLATIONS
                           ON CL_BEV_ALL.RECORD_ID = CL_BEV_ISOLATIONS.RECORD_ID
                         LEFT OUTER JOIN ZC_CLEANING_PROTCL
                           ON CL_BEV_ISOLATIONS.ISOLATION_C = ZC_CLEANING_PROTCL.CLEANING_PROTCL_C
                       WHERE CL_BEV_ALL.EVENT_TYPE_C = 0
                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) > <<LowerBound>>
                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) <= <<UpperBound>>
                       GROUP BY COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID)) Protocols
      ON CL_BEV_ALL.RECORD_ID = Protocols.RECORD_ID
    LEFT OUTER JOIN (SELECT COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) RECORD_ID
                       FROM CL_BEV_ALL
                         INNER JOIN CL_BEV_ESC_AUDIT
                           ON CL_BEV_ALL.RECORD_ID = CL_BEV_ESC_AUDIT.RECORD_ID
                       WHERE CL_BEV_ALL.EVENT_TYPE_C = 0
                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) > <<LowerBound>>
                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) <= <<UpperBound>>
                       GROUP BY COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID)) Escalations
      ON CL_BEV_ALL.RECORD_ID = Escalations.RECORD_ID
    LEFT OUTER JOIN (SELECT COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) RECORD_ID,
                            MIN(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEONECREATEDINSTANT,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETWOCREATEDINSTANT,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETHREECREATEDINSTANT,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEFOURCREATEDINSTANT,
                            MIN(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL AND CL_BEV_EVENTS_ALL.STATUS_C = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEONEFIRSTASSIGNINSTANT,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 AND CL_BEV_EVENTS_ALL.STATUS_C = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETWOFIRSTASSIGNINSTANT,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 AND CL_BEV_EVENTS_ALL.STATUS_C = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETHREEFIRSTASSIGNINSTANT,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 AND CL_BEV_EVENTS_ALL.STATUS_C = 2
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEFOURFIRSTASSIGNINSTANT,
                            MAX(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL AND CL_BEV_EVENTS_ALL.STATUS_C = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEONELASTASSIGNINSTANT,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 AND CL_BEV_EVENTS_ALL.STATUS_C = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETWOLASTASSIGNINSTANT,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 AND CL_BEV_EVENTS_ALL.STATUS_C = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETHREELASTASSIGNINSTANT,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 AND CL_BEV_EVENTS_ALL.STATUS_C = 2 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEFOURLASTASSIGNINSTANT,
                            MIN(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEONEFIRSTINPROGRESS,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETWOFIRSTINPROGRESS,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETHREEFIRSTINPROGRESS,
                            MIN(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEFOURFIRSTINPROGRESS,
                            MAX(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEONELASTINPROGRESS,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETWOLASTINPROGRESS,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETHREELASTINPROGRESS,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 AND CL_BEV_EVENTS_ALL.STATUS_C = 3 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEFOURLASTINPROGRESS,
                            MAX(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEONECOMPLETEDINSTANT,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETWOCOMPLETEDINSTANT,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGETHREECOMPLETEDINSTANT,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) STAGEFOURCOMPLETEDINSTANT,
                            MAX(CASE WHEN CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN CL_BEV_EVENTS_ALL.INSTANT_TM ELSE NULL END) CLEANCOMPLETEDINSTANT,
                            MAX(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL AND CL_BEV_EVENTS_ALL.STATUS_C = 5  
                                       THEN CASE WHEN CL_HKR.EMP_ID IS NOT NULL THEN 'EpicUserId' ELSE 'EpicHousekeeperId' END ELSE NULL END) STAGEONEHOUSEKEEPERIDTYPE,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 AND CL_BEV_EVENTS_ALL.STATUS_C = 5  
                                       THEN CASE WHEN CL_HKR.EMP_ID IS NOT NULL THEN 'EpicUserId' ELSE 'EpicHousekeeperId' END ELSE NULL END) STAGETWOHOUSEKEEPERIDTYPE,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 AND CL_BEV_EVENTS_ALL.STATUS_C = 5  
                                       THEN CASE WHEN CL_HKR.EMP_ID IS NOT NULL THEN 'EpicUserId' ELSE 'EpicHousekeeperId' END ELSE NULL END) STAGETHREEHOUSEKEEPERIDTYPE,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 AND CL_BEV_EVENTS_ALL.STATUS_C = 5  
                                       THEN CASE WHEN CL_HKR.EMP_ID IS NOT NULL THEN 'EpicUserId' ELSE 'EpicHousekeeperId' END ELSE NULL END) STAGEFOURHOUSEKEEPERIDTYPE,
                            MAX(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN COALESCE(CL_HKR.EMP_ID, CAST(CL_BEV_EVENTS_ALL.HKR_ID AS varchar(20))) ELSE NULL END) STAGEONEHOUSEKEEPERID,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN COALESCE(CL_HKR.EMP_ID, CAST(CL_BEV_EVENTS_ALL.HKR_ID AS varchar(20))) ELSE NULL END) STAGETWOHOUSEKEEPERID,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN COALESCE(CL_HKR.EMP_ID, CAST(CL_BEV_EVENTS_ALL.HKR_ID AS varchar(20))) ELSE NULL END) STAGETHREEHOUSEKEEPERID,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 AND CL_BEV_EVENTS_ALL.STATUS_C = 5 
                                       THEN COALESCE(CL_HKR.EMP_ID, CAST(CL_BEV_EVENTS_ALL.HKR_ID AS varchar(20))) ELSE NULL END) STAGEFOURHOUSEKEEPERID,
                            MAX(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL THEN 
                                       CASE WHEN CL_BEV_ALL.PRIORITY_C IS NULL THEN '*Unspecified'
                                            WHEN ZC_PRIORITY_2.PRIORITY_2_C IS NULL THEN '*Unknown'
                                            ELSE ZC_PRIORITY_2.NAME END
                                     ELSE NULL END) STAGEONEFINALPRIORITY,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 THEN 
                                       CASE WHEN CL_BEV_ALL.PRIORITY_C IS NULL THEN '*Unspecified'
                                            WHEN ZC_PRIORITY_2.PRIORITY_2_C IS NULL THEN '*Unknown'
                                            ELSE ZC_PRIORITY_2.NAME END
                                     ELSE NULL END) STAGETWOFINALPRIORITY,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 THEN 
                                       CASE WHEN CL_BEV_ALL.PRIORITY_C IS NULL THEN '*Unspecified'
                                            WHEN ZC_PRIORITY_2.PRIORITY_2_C IS NULL THEN '*Unknown'
                                            ELSE ZC_PRIORITY_2.NAME END
                                     ELSE NULL END) STAGETHREEFINALPRIORITY,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 THEN 
                                       CASE WHEN CL_BEV_ALL.PRIORITY_C IS NULL THEN '*Unspecified'
                                            WHEN ZC_PRIORITY_2.PRIORITY_2_C IS NULL THEN '*Unknown'
                                            ELSE ZC_PRIORITY_2.NAME END
                                     ELSE NULL END) STAGEFOURFINALPRIORITY,
                            MAX(CASE WHEN NULLIF(CL_BEV_ALL.STAGE_NUMBER, 1) IS NULL THEN 
                                       CASE WHEN CL_BEV_ALL.CUR_STAGE_ID IS NULL THEN '*Unspecified'
                                            WHEN TASK_TEMPLATES.TASK_ID IS NULL THEN '*Unknown'
                                            ELSE TASK_TEMPLATES.TASK_NAME END
                                     ELSE NULL END) STAGEONENAME,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 2 THEN 
                                       CASE WHEN CL_BEV_ALL.CUR_STAGE_ID IS NULL THEN '*Unspecified'
                                            WHEN TASK_TEMPLATES.TASK_ID IS NULL THEN '*Unknown'
                                            ELSE TASK_TEMPLATES.TASK_NAME END
                                     ELSE NULL END) STAGETWONAME,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 3 THEN 
                                       CASE WHEN CL_BEV_ALL.CUR_STAGE_ID IS NULL THEN '*Unspecified'
                                            WHEN TASK_TEMPLATES.TASK_ID IS NULL THEN '*Unknown'
                                            ELSE TASK_TEMPLATES.TASK_NAME END
                                     ELSE NULL END) STAGETHREENAME,
                            MAX(CASE WHEN CL_BEV_ALL.STAGE_NUMBER = 4 THEN 
                                       CASE WHEN CL_BEV_ALL.CUR_STAGE_ID IS NULL THEN '*Unspecified'
                                            WHEN TASK_TEMPLATES.TASK_ID IS NULL THEN '*Unknown'
                                            ELSE TASK_TEMPLATES.TASK_NAME END
                                     ELSE NULL END) STAGEFOURNAME,
                            MAX(CL_BEV_ALL.STAGE_NUMBER) NUMBEROFSTAGES
                       FROM CL_BEV_ALL
                         LEFT OUTER JOIN CL_BEV_EVENTS_ALL
                           ON CL_BEV_ALL.RECORD_ID = CL_BEV_EVENTS_ALL.RECORD_ID
                         LEFT OUTER JOIN TASK_TEMPLATES
                           ON CL_BEV_ALL.CUR_STAGE_ID = TASK_TEMPLATES.TASK_ID
                         LEFT OUTER JOIN CL_HKR
                           ON CL_BEV_EVENTS_ALL.HKR_ID = CL_HKR.RECORD_ID
                         LEFT OUTER JOIN ZC_PRIORITY_2
                           ON CL_BEV_ALL.PRIORITY_C = ZC_PRIORITY_2.PRIORITY_2_C
                       WHERE CL_BEV_ALL.EVENT_TYPE_C = 0
                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) > <<LowerBound>>
                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) <= <<UpperBound>>
                       GROUP BY COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID)) UnpivotedEvents
      ON CL_BEV_ALL.RECORD_ID = UnpivotedEvents.RECORD_ID
    LEFT OUTER JOIN (SELECT COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) RECORD_ID,
                            CASE WHEN CL_BEV_ALL.PRIORITY_C IS NULL THEN '*Unspecified'
                                 WHEN ZC_PRIORITY_2.PRIORITY_2_C IS NULL THEN '*Unknown'
                                 ELSE ZC_PRIORITY_2.NAME END CLEANFINALPRIORITY,
                            ROW_NUMBER() OVER(PARTITION BY COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) 
                                                ORDER BY COALESCE(CL_BEV_ALL.STAGE_NUMBER, 1) DESC) Ranking
                       FROM CL_BEV_ALL
                         LEFT OUTER JOIN ZC_PRIORITY_2
                           ON CL_BEV_ALL.PRIORITY_C = ZC_PRIORITY_2.PRIORITY_2_C
                       --WHERE COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) > <<LowerBound>>
                       --  AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) <= <<UpperBound>>) FinalPriority
                       ) FinalPriority
      ON CL_BEV_ALL.RECORD_ID = FinalPriority.RECORD_ID
        AND FinalPriority.Ranking = 1
    LEFT OUTER JOIN (SELECT TopReasons.RecordId,
                            SUM(CASE WHEN TopReasons.STATUS_C = 4 
                                       THEN TopReasons.EventTime ELSE 0 END) TotalMinutesHeld,
                            SUM(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.StageNumber = 1 
                                       THEN TopReasons.EventTime ELSE 0 END) StageOneMinutesHeld,
                            SUM(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.StageNumber = 2 
                                       THEN TopReasons.EventTime ELSE 0 END) StageTwoMinutesHeld,
                            SUM(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.StageNumber = 3 
                                       THEN TopReasons.EventTime ELSE 0 END) StageThreeMinutesHeld,
                            SUM(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.StageNumber = 4 
                                       THEN TopReasons.EventTime ELSE 0 END) StageFourMinutesHeld,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 1
                                       THEN TopReasons.Reason ELSE NULL END) StageOneFirstHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 1
                                       THEN TopReasons.Reason ELSE NULL END) StageOneSecondHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 1
                                       THEN TopReasons.Reason ELSE NULL END) StageOneThirdHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 2
                                       THEN TopReasons.Reason ELSE NULL END) StageTwoFirstHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 2
                                       THEN TopReasons.Reason ELSE NULL END) StageTwoSecondHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 2
                                       THEN TopReasons.Reason ELSE NULL END) StageTwoThirdHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 3
                                       THEN TopReasons.Reason ELSE NULL END) StageThreeFirstHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 3
                                       THEN TopReasons.Reason ELSE NULL END) StageThreeSecondHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 3
                                       THEN TopReasons.Reason ELSE NULL END) StageThreeThirdHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 4
                                       THEN TopReasons.Reason ELSE NULL END) StageFourFirstHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 4
                                       THEN TopReasons.Reason ELSE NULL END) StageFourSecondHoldReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 4 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 4
                                       THEN TopReasons.Reason ELSE NULL END) StageFourThirdHoldReason,
                            SUM(CASE WHEN TopReasons.STATUS_C = 6 
                                       THEN TopReasons.EventTime ELSE 0 END) TotalMinutesDelayed,
                            SUM(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.StageNumber = 1
                                       THEN TopReasons.EventTime ELSE 0 END) StageOneMinutesDelayed,
                            SUM(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.StageNumber = 2 
                                       THEN TopReasons.EventTime ELSE 0 END) StageTwoMinutesDelayed,
                            SUM(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.StageNumber = 3 
                                       THEN TopReasons.EventTime ELSE 0 END) StageThreeMinutesDelayed,
                            SUM(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.StageNumber = 4 
                                       THEN TopReasons.EventTime ELSE 0 END) StageFourMinutesDelayed,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 1 
                                       THEN TopReasons.Reason ELSE NULL END) StageOneFirstDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 1 
                                       THEN TopReasons.Reason ELSE NULL END) StageOneSecondDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 1 
                                       THEN TopReasons.Reason ELSE NULL END) StageOneThirdDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 2 
                                       THEN TopReasons.Reason ELSE NULL END) StageTwoFirstDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 2 
                                       THEN TopReasons.Reason ELSE NULL END) StageTwoSecondDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 2 
                                       THEN TopReasons.Reason ELSE NULL END) StageTwoThirdDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 3 
                                       THEN TopReasons.Reason ELSE NULL END) StageThreeFirstDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 3 
                                       THEN TopReasons.Reason ELSE NULL END) StageThreeSecondDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 3 
                                       THEN TopReasons.Reason ELSE NULL END) StageThreeThirdDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 1 AND TopReasons.StageNumber = 4 
                                       THEN TopReasons.Reason ELSE NULL END) StageFourFirstDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 2 AND TopReasons.StageNumber = 4 
                                       THEN TopReasons.Reason ELSE NULL END) StageFourSecondDelayReason,
                            MAX(CASE WHEN TopReasons.STATUS_C = 6 AND TopReasons.Ranking = 3 AND TopReasons.StageNumber = 4 
                                       THEN TopReasons.Reason ELSE NULL END) StageFourThirdDelayReason
                       FROM (SELECT EventRanges.RecordId,
                                    EventRanges.STATUS_C,
                                    EventRanges.Reason,
                                    EventRanges.StageNumber,
                                    SUM(COALESCE( (CAST(EventRanges.EventEndTime - EventRanges.INSTANT_TM AS float)) * 1440, 0)) EventTime,
                                    ROW_NUMBER() OVER(PARTITION BY EventRanges.RecordId, EventRanges.STATUS_C, EventRanges.StageNumber
                                                        ORDER BY SUM(COALESCE( (
                                                          CAST(EventRanges.EventEndTime - EventRanges.INSTANT_TM AS float)) * 1440, 0)) DESC, MAX(EventRanges.MINLINE)) Ranking
                               FROM (SELECT COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) RecordId,
                                            Base.INSTANT_TM,
                                            Base.STATUS_C,
                                            COALESCE(CL_BEV_ALL.STAGE_NUMBER, 1) StageNumber,
                                            MAX(CASE WHEN Base.STATUS_C = 4 THEN
                                                     CASE WHEN Base.HOLD_REASON_C IS NULL THEN '*Unspecified'
                                                          WHEN ZC_HOLD_REASON_2.HOLD_REASON_2_C IS NULL THEN '*Unknown'
                                                          ELSE ZC_HOLD_REASON_2.NAME END
                                                     WHEN Base.STATUS_C = 6 THEN
                                                     CASE WHEN Base.DELAY_REASON_C IS NULL THEN '*Unspecified'
                                                          WHEN ZC_DELAY_REASON.DELAY_REASON_C IS NULL THEN '*Unknown'
                                                          ELSE ZC_DELAY_REASON.NAME END END) Reason,
                                            MIN(NextEvent.INSTANT_TM) EventEndTime,
                                            MIN( Base.LINE ) MINLINE
                                       FROM CL_BEV_ALL
                                         LEFT OUTER JOIN CL_BEV_EVENTS_ALL Base
                                           ON CL_BEV_ALL.RECORD_ID = Base.RECORD_ID
                                         LEFT OUTER JOIN CL_BEV_EVENTS_ALL NextEvent
                                           ON Base.RECORD_ID = NextEvent.RECORD_ID
                                             AND Base.LINE < NextEvent.LINE
                                             AND Base.INSTANT_TM <= NextEvent.INSTANT_TM
                                         LEFT OUTER JOIN ZC_HOLD_REASON_2
                                           ON Base.HOLD_REASON_C = ZC_HOLD_REASON_2.HOLD_REASON_2_C
                                         LEFT OUTER JOIN ZC_DELAY_REASON
                                           ON Base.DELAY_REASON_C = ZC_DELAY_REASON.DELAY_REASON_C
                                       WHERE Base.STATUS_C IN (4, 6)
                                         AND CL_BEV_ALL.EVENT_TYPE_C = 0
                                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) > <<LowerBound>>
                                         --AND COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID) <= <<UpperBound>>
                                       GROUP BY COALESCE(CL_BEV_ALL.FIRST_STAGE_EVT_ID, CL_BEV_ALL.RECORD_ID), Base.INSTANT_TM, 
                                                Base.STATUS_C, COALESCE(CL_BEV_ALL.STAGE_NUMBER, 1))  EventRanges
                               GROUP BY EventRanges.RecordId, EventRanges.STATUS_C, EventRanges.Reason, EventRanges.StageNumber) TopReasons
                       GROUP BY TopReasons.RecordId) PivotedOut
      ON CL_BEV_ALL.RECORD_ID = PivotedOut.RecordId
    LEFT OUTER JOIN ZC_EVENT_SOURCE
      ON CL_BEV_ALL.EVENT_SOURCE_C = ZC_EVENT_SOURCE.EVENT_SOURCE_C
	LEFT OUTER JOIN CLARITY.dbo.CLARITY_DEP dep
	  ON CL_BEV_ALL.DEP_ID = dep.DEPARTMENT_ID
  WHERE CL_BEV_ALL.EVENT_TYPE_C = 0
    AND (CL_BEV_ALL.FIRST_STAGE_EVT_ID IS NULL
           OR CL_BEV_ALL.RECORD_ID = CL_BEV_ALL.FIRST_STAGE_EVT_ID)
    --AND CL_BEV_ALL.RECORD_ID > <<LowerBound>>
    --AND CL_BEV_ALL.RECORD_ID <= <<UpperBound>>
	--AND UnpivotedEvents.STAGEONECREATEDINSTANT BETWEEN '2/9/2024' AND '2/10/2024'
	AND UnpivotedEvents.STAGEONECREATEDINSTANT BETWEEN '3/9/2024' AND '3/10/2024'
	--AND CL_BEV_ALL.DEP_ID = 10243110
	ORDER BY UnpivotedEvents.STAGEONECREATEDINSTANT