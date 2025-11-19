WITH provider_list AS (
    SELECT DISTINCT computing_id
    FROM your_provider_excel_import -- Replace with real import table or CTE
),

provider_dim AS (
    SELECT ProviderKey, ProviderEpicId, Name AS computing_id
    FROM dbo.ProviderDim
    WHERE IsCurrent = 1
),

note_agg AS (
    SELECT
        p.Name AS computing_id,
        CAST(cn.CreationInstant AS DATE) AS activity_date,
        COUNT(*) AS notes_written,
        SUM(CASE WHEN DATEPART(HOUR, cn.LastEditedInstant) >= 18 THEN TotalEditTime ELSE 0 END) / 60.0 AS pajama_minutes,
        SUM(TotalEditTime) / 60.0 AS total_note_minutes,
        SUM(ISNULL(ManualCharCount, 0) + ISNULL(TemplateSourceCharCount, 0) + ISNULL(NoSourceCharCount, 0)) AS total_note_characters
    FROM dbo.ClinicalNoteFact cn
    JOIN provider_dim p ON cn.AuthoringProviderKey = p.ProviderKey
    JOIN provider_list pl ON p.computing_id = pl.computing_id
    WHERE cn.Status = 'Signed'
      AND cn.CreationInstant >= '2024-06-01'
    GROUP BY p.Name, CAST(cn.CreationInstant AS DATE)
),

appt_agg AS (
    SELECT
        p.Name AS computing_id,
        CAST(e.DateKey AS DATE) AS activity_date,
        COUNT(DISTINCT e.EncounterKey) AS appointments_seen
    FROM dbo.EncounterFact e
    JOIN provider_dim p ON e.ProviderKey = p.ProviderKey
    JOIN provider_list pl ON p.computing_id = pl.computing_id
    WHERE e.DateKey >= '2024-06-01'
    GROUP BY p.Name, CAST(e.DateKey AS DATE)
)

-- Final output
SELECT
    cal.computing_id,
    cal.activity_date,
    COALESCE(na.total_note_minutes, 0) AS note_minutes,
    COALESCE(na.pajama_minutes, 0) AS pajama_minutes,
    COALESCE(na.total_note_characters, 0) AS total_note_characters,
    COALESCE(na.notes_written, 0) AS notes_written,
    COALESCE(aa.appointments_seen, 0) AS appointments_seen
FROM (
    SELECT DISTINCT
        p.computing_id,
        d.date AS activity_date
    FROM provider_list p
    CROSS JOIN dbo.DateDim d
    WHERE d.date BETWEEN '2024-06-01' AND GETDATE()
) cal
LEFT JOIN note_agg na ON cal.computing_id = na.computing_id AND cal.activity_date = na.activity_date
LEFT JOIN appt_agg aa ON cal.computing_id = aa.computing_id AND cal.activity_date = aa.activity_date
ORDER BY cal.computing_id, cal.activity_date;
