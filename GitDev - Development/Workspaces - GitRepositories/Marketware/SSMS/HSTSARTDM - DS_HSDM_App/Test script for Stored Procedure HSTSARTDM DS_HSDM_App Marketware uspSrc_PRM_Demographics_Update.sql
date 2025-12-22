USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [Marketware].[uspSrc_PRM_Demographics_Update]

AS
/*******************************************************************************************
WHAT :  create procedure [Marketware].[uspSrc_PRM_Demographics_Update]
WHO  :  Bill Reed
WHEN :  8/22/2022
WHY  :  Generate data extract for PRM_Demographics_Update]
--------------------------------------------------------------------------------------------
INFO:   Create PRM_Demographics_Update

    INPUTS:
        DS_HSDM_App.ETL.Cactus_CurrentActive_Providers
        DS_HSDM_VisualCactus_shadow.VISUALCACTUS.PROVIDERS
        DS_HSDM_VisualCactus_shadow.VISUALCACTUS.PROVIDERADDRESSES
        DS_HSDM_VisualCactus_shadow.VISUALCACTUS.PROVIDERLICENSES
        DS_HSDM_VisualCactus_shadow.VISUALCACTUS.ADDRESSES
        DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFTABLE
        DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFERENCEMASTER

    OUTPUTS:
        [Marketware].[uspSrc_PRM_Demographics_Update]
--------------------------------------------------------------------------------------------
MODS:
    08/22/2022 - New Build
--------------------------------------------------------------------------------------------
RUNS:    na
********************************************************************************************/
SET NOCOUNT ON


  SELECT
     prov.NPI                           AS NPI
    ,prov.ID                            AS UniqueID
    ,prov.FIRSTNAME                     AS FirstName
    ,prov.MIDDLENAME                    AS MiddleName
    ,prov.LASTNAME                      AS LastName
    ,CONVERT(VARCHAR(50),prov.DISPLAYDEGREES)  AS Degree
    ,ISNULL(rt_tax.SHORTDESCRIPTION,'') AS Taxonomy
    ,''                                 AS Specialty                     -- Only if Taxonomy code is unavailable
    ,''                                 AS SubSpecialty                  -- Only if Taxonomy code is unavailable
    ,SUBSTRING(cp.Provider_Category,1,50)  AS Type
    ,''                                 AS EmploymentStatus              -- Employed by us, Employed elsewhere
  --,prov.InNetworkStatus               AS EmploymentStatus              -- Employed by us, Employed elsewhere
    ,SUBSTRING(pa.SALUTATION,1,20)      AS Title
    ,'In Network'                       AS NetworkStatus
    ,''                                 AS NetworkGroup
    ,ISNULL(rt_lic.Description,'')      AS Credential1Type               -- state license
    ,lic.LICENSENUMBER                  AS Credential1Value              -- Non-NPI practitioner value
    ,''                                 AS Credential2Type
    ,''                                 AS Credential2Value
    ,CASE
       WHEN TRIM(pa.PROVIDERSPECIFIC_PHONE) = '' THEN CAST('' AS VARCHAR(12))
       ELSE SUBSTRING(pa.PROVIDERSPECIFIC_PHONE,1,3) + '-' +
            SUBSTRING(pa.PROVIDERSPECIFIC_PHONE,4,3) + '-' +
             SUBSTRING(pa.PROVIDERSPECIFIC_PHONE,7,4)
      END                               AS PractitionerPhone
    ,CASE
       WHEN TRIM(prov.CONTACTCELLPHONE) = '' THEN CAST('' AS VARCHAR(12))
       ELSE SUBSTRING(prov.CONTACTCELLPHONE,1,3) + '-' +
            SUBSTRING(prov.CONTACTCELLPHONE,4,3) + '-' +
            SUBSTRING(prov.CONTACTCELLPHONE,7,4)
     END                                AS PractitionerCell
    ,CASE
       WHEN TRIM(pa.PROVIDERSPECIFIC_FAX) = '' THEN CAST('' AS VARCHAR(12))
       ELSE SUBSTRING(pa.PROVIDERSPECIFIC_FAX,1,3) + '-' +
            SUBSTRING(pa.PROVIDERSPECIFIC_FAX,4,3) + '-' +
            SUBSTRING(pa.PROVIDERSPECIFIC_FAX,7,4)
     END                                AS PractitionerFax
    ,pa.EMAILADDRESS                    AS Email
    ,prov.SEX                           AS Gender
    ,prov.DATEOFBIRTH                   AS Birthdate
    ,CASE pa.ACCEPTNEWPATIENTS
        WHEN '0'             THEN 'No '
        WHEN '1'             THEN 'Yes'
     ELSE 'Unknown'
     END                                AS TakingNewPatients             -- Yes, No, Limited
    ,''                                 AS Website
    ,''                                 AS Comments
    ,0                                  AS AccessLevel                   -- values are 1-5
    ,''                                 AS Salutation                    -- Miss, Mr., Mrs., Ms.
    ,''                                 AS SatisfactionLevel             -- High, Medium, At Risk
    ,''                                 AS AlignmentStatus               -- Alignment complete, Evaluating opportunity, In progress, No possibility, Not a prospect
    ,''                                 AS LoyaltyLevel                  -- Very loyal, loyal, splitter, Not loyal, Not applicable
    ,SUBSTRING(rt_mar.Description,1,50) AS MaritalStatus
    ,ISNULL(rt_eth.Description,'')      AS Ethnicity
    ,''                                 AS NotificationPreference        -- email, fax, letter, phone, text
    ,''                                 AS CommunicationPreference       -- email, fax, letter, phone, text
    ,''                                 AS CarePhilosophy
    ,''                                 AS OrganizationName
    ,''                                 AS OrganizationUniqueId
    ,''                                 AS OrganizationType              -- Group practice, Hospital, General company, PAC organization
    ,''                                 AS OrganizationFax
    ,''                                 AS OrganizationPhone
    ,''                                 AS OrganizationWebsite
    ,''                                 AS OrganizationIsMain            -- Y, N
    ,ad.ADDRESSLINE1                    AS AddressLine1
    ,ad.ADDRESSLINE2                    AS AddressLine2
    ,SUBSTRING(ad.CITY,1,20)            AS City
    ,ad.STATE                           AS State
    ,SUBSTRING(ad.ZIPCODE,1,10)         AS Zip
    ,ad.COUNTY                          AS County
    ,''                                 AS ServiceArea                   -- Primary, Secondary, North, South
    ,ISNULL(rt_adt.Description,'')      AS AddressType
    ,CASE WHEN rt_adt.Description = 'Primary Practice Location'
            THEN 'Y'
            ELSE 'N'
     END                                AS PracAddrIsMain
    ,''                                 AS PracAddrPhone
    ,''                                 AS PracAddrFax
    ,GETDATE()                          AS Load_Dtm
  FROM DS_HSDM_VisualCactus_shadow.VISUALCACTUS.PROVIDERS               AS prov
  INNER JOIN DS_HSDM_App.ETL.Cactus_CurrentActive_Providers             AS cp     ON cp.PROVIDER_K      = prov.PROVIDER_K
  INNER JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.PROVIDERADDRESSES AS pa     ON pa.PROVIDER_K      = prov.PROVIDER_K
  INNER JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.ADDRESSES         AS ad     ON ad.ADDRESS_K       = pa.ADDRESS_K
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.PROVIDERLICENSES   AS lic    ON lic.PROVIDER_K     = prov.PROVIDER_K
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFTABLE           AS rt_tax ON prov.HIPPATAXONOMY_RTK    = rt_tax.REFTABLE_K AND rt_tax.REFERENCEMASTER_K = 'PSTAXONOMY'
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFERENCEMASTER    AS rm_mar ON rm_mar.ALIASNAME   = 'MARITALSTATUS_RTK'
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFTABLE           AS rt_mar ON rt_mar.REFTABLE_K  = prov.MARITALSTATUS_RTK AND rt_mar.REFERENCEMASTER_K = rm_mar.REFERENCEMASTER_K
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFERENCEMASTER    AS rm_eth ON rm_eth.ALIASNAME   = 'ETHNICITY_RTK'
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFTABLE           AS rt_eth ON rt_eth.REFTABLE_K  = prov.ETHNICITY_RTK     AND rt_eth.REFERENCEMASTER_K = rm_eth.REFERENCEMASTER_K
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFERENCEMASTER    AS rm_adt ON rm_adt.ALIASNAME   = 'ADDRESSTYPE_RTK'
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFTABLE           AS rt_adt ON rt_adt.REFTABLE_K  = pa.ADDRESSTYPE_RTK     AND rt_adt.REFERENCEMASTER_K = rm_adt.REFERENCEMASTER_K
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFERENCEMASTER    AS rm_lic ON rm_lic.ALIASNAME   = 'LICENSE_RTK'
  LEFT JOIN DS_HSDM_VisualCactus_shadow.VISUALCACTUS.REFTABLE           AS rt_lic ON rt_lic.REFTABLE_K  = lic.LICENSE_RTK        AND rt_lic.REFERENCEMASTER_K = rm_lic.REFERENCEMASTER_K
  WHERE COALESCE(prov.NPI,'') <> ''
    AND cp.EntityStatus LIKE 'Active%'
    AND lic.EXPIRATIONDATE > GETDATE()
    AND RTRIM(rt_lic.Description) = 'State License'


GO


