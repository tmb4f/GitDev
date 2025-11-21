USE DS_HSDM_APP

select 
       Unit,
       Bed_Name,
       Bed_ID,
       Source_Load_dtm,
	   sk_PtProg_Dash_Data_Archive,
       MRN_int,
       Patient_Name,
       Bed_Available,
       Bed_Blocked,
       Block_Reason,
       Bed_Type,
       Bed_Status,
       Unit_Name,
       Unit_ID,
       Unit_Group,
       Patient_Group,
       Admitted_Patient_Group,
       Newborn_Bed,
       Hs_Area_ID,
       Hs_Area_Name,
       COVID_Beds,
       Active_Flag,
       Load_dtm,
       Event_Type,
       Attending_Physician_Name,
       Attending_Physician_ext_ID,
       Attending_Physician_CID,
       Attending_Physician_int_ID,
       Confirmed_Discharge_Order,
       Pending_Discharge_Order,
       PAT_ENC_CSN_ID from DS_HSDM_APP.Stage.PtProg_Dash_Data_Archive WHERE 1 =1
--AND Active_Flag = 1
AND LEN(Unit) > 0
--AND Bed_Name = '8125B'
AND Event_Type = 'BED'
AND Source_Load_dtm >= '4/15/2024 00:00:00'
ORDER BY
    Unit,
	Bed_Name,
	Source_Load_dtm