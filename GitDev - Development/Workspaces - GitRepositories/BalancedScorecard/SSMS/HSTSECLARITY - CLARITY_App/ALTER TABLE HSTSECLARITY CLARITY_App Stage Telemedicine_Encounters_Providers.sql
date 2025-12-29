USE CLARITY_App

ALTER TABLE Stage.Telemedicine_Encounters_Providers
	ADD Pt_Home_Resource_Pool VARCHAR(200) NULL,
			Prov_1_Pool VARCHAR(200) NULL,
			Prov_2_Pool VARCHAR(200) NULL,
			Prov_3_Pool VARCHAR(200) NULL,
			Prov_4_Pool VARCHAR(200) NULL,
			Prov_5_Pool VARCHAR(200) NULL
;