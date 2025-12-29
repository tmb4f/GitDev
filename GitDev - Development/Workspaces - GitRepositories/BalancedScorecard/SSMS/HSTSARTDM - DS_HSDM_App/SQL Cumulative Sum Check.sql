USE [DS_HSDM_App]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

    SET NOCOUNT ON;

DECLARE @PartitionTest TABLE (ID INTEGER, Date DATE, Amount INTEGER, [Count] INTEGER)

INSERT INTO @PartitionTest
(
    ID,
    Date,
    Amount,
    [Count]
)
VALUES
(   1, -- ID - integer
    '1/1/2022', -- Date - date
    10, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/2/2022', -- Date - date
    20, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/3/2022', -- Date - date
    30, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/4/2022', -- Date - date
    40, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/5/2022', -- Date - date
    50, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/6/2022', -- Date - date
    60, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/7/2022', -- Date - date
    70, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/8/2022', -- Date - date
    80, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/9/2022', -- Date - date
    90, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/10/2022', -- Date - date
    100, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/11/2022', -- Date - date
    110, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/12/2022', -- Date - date
    120, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/13/2022', -- Date - date
    130, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/14/2022', -- Date - date
    140, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/15/2022', -- Date - date
    150, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/16/2022', -- Date - date
    160, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/17/2022', -- Date - date
    170, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/18/2022', -- Date - date
    180, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/19/2022', -- Date - date
    190, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/20/2022', -- Date - date
    200, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/21/2022', -- Date - date
    210, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/22/2022', -- Date - date
    220, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/23/2022', -- Date - date
    230, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/24/2022', -- Date - date
    240, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/25/2022', -- Date - date
    250, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/26/2022', -- Date - date
    260, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/27/2022', -- Date - date
    270, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/28/2022', -- Date - date
    280, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/29/2022', -- Date - date
    290, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/30/2022', -- Date - date
    300, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '1/31/2022', -- Date - date
    310, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/1/2022', -- Date - date
    10, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/2/2022', -- Date - date
    20, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/3/2022', -- Date - date
    30, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/4/2022', -- Date - date
    40, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/5/2022', -- Date - date
    50, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/6/2022', -- Date - date
    60, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/7/2022', -- Date - date
    70, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/8/2022', -- Date - date
    80, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/9/2022', -- Date - date
    90, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/10/2022', -- Date - date
    100, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/11/2022', -- Date - date
    110, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/12/2022', -- Date - date
    120, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/13/2022', -- Date - date
    130, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/14/2022', -- Date - date
    140, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/15/2022', -- Date - date
    150, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/16/2022', -- Date - date
    160, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/17/2022', -- Date - date
    170, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/18/2022', -- Date - date
    180, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/19/2022', -- Date - date
    190, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/20/2022', -- Date - date
    200, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/21/2022', -- Date - date
    210, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/22/2022', -- Date - date
    220, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/23/2022', -- Date - date
    230, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/24/2022', -- Date - date
    240, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/25/2022', -- Date - date
    250, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/26/2022', -- Date - date
    260, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/27/2022', -- Date - date
    270, -- Amount - integer
    2  -- Count - integer
    ),
(   1, -- ID - integer
    '2/28/2022', -- Date - date
    280, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/1/2022', -- Date - date
    10, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/2/2022', -- Date - date
    20, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/3/2022', -- Date - date
    30, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/4/2022', -- Date - date
    40, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/5/2022', -- Date - date
    50, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/6/2022', -- Date - date
    60, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/7/2022', -- Date - date
    70, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/8/2022', -- Date - date
    80, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/9/2022', -- Date - date
    90, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/10/2022', -- Date - date
    100, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/11/2022', -- Date - date
    110, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/12/2022', -- Date - date
    120, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/13/2022', -- Date - date
    130, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/14/2022', -- Date - date
    140, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/15/2022', -- Date - date
    150, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/16/2022', -- Date - date
    160, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/17/2022', -- Date - date
    170, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/18/2022', -- Date - date
    180, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/19/2022', -- Date - date
    190, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/20/2022', -- Date - date
    200, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/21/2022', -- Date - date
    210, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/22/2022', -- Date - date
    220, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/23/2022', -- Date - date
    230, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/24/2022', -- Date - date
    240, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/25/2022', -- Date - date
    250, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/26/2022', -- Date - date
    260, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/27/2022', -- Date - date
    270, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/28/2022', -- Date - date
    280, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/29/2022', -- Date - date
    290, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/30/2022', -- Date - date
    300, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '1/31/2022', -- Date - date
    310, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/1/2022', -- Date - date
    10, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/2/2022', -- Date - date
    20, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/3/2022', -- Date - date
    30, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/4/2022', -- Date - date
    40, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/5/2022', -- Date - date
    50, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/6/2022', -- Date - date
    60, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/7/2022', -- Date - date
    70, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/8/2022', -- Date - date
    80, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/9/2022', -- Date - date
    90, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/10/2022', -- Date - date
    100, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/11/2022', -- Date - date
    110, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/12/2022', -- Date - date
    120, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/13/2022', -- Date - date
    130, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/14/2022', -- Date - date
    140, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/15/2022', -- Date - date
    150, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/16/2022', -- Date - date
    160, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/17/2022', -- Date - date
    170, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/18/2022', -- Date - date
    180, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/19/2022', -- Date - date
    190, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/20/2022', -- Date - date
    200, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/21/2022', -- Date - date
    210, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/22/2022', -- Date - date
    220, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/23/2022', -- Date - date
    230, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/24/2022', -- Date - date
    240, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/25/2022', -- Date - date
    250, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/26/2022', -- Date - date
    260, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/27/2022', -- Date - date
    270, -- Amount - integer
    2  -- Count - integer
    ),
(   2, -- ID - integer
    '2/28/2022', -- Date - date
    280, -- Amount - integer
    2  -- Count - integer
    )

SELECT *
FROM @PartitionTest
ORDER BY ID, [Date]

SELECT ID,
       [Date],
       Amount,
       [Count],
	   SUM(Amount) OVER (PARTITION BY ID
                  ORDER BY [Date]
                  ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
                 )  as Cumulative_Amount_Prev,
	   SUM([Count]) OVER (PARTITION BY ID
                  ORDER BY [Date]
                  ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
                 )  as Cumulative_Count_Prev,
	   SUM(Amount) OVER (PARTITION BY ID
                  ORDER BY [Date]
                  ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                 )  as Cumulative_Amount_Incl,
	   SUM([Count]) OVER (PARTITION BY ID
                  ORDER BY [Date]
                  ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                 )  as Cumulative_Count_Incl,
	   CAST(CAST(SUM(Amount) OVER (PARTITION BY ID
                  ORDER BY [Date]
                  ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
                 ) AS DECIMAL(8,2)) /
				CAST(SUM([Count]) OVER (PARTITION BY ID
				ORDER BY [Date]
				ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
				)  AS DECIMAL(8,2)) AS DECIMAL(8,2)) as Cumulative_Amount_Average_Prev,
	   CAST(CAST(SUM(Amount) OVER (PARTITION BY ID
                  ORDER BY [Date]
                  ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                 ) AS DECIMAL(8,2)) /
				CAST(SUM([Count]) OVER (PARTITION BY ID
				ORDER BY [Date]
				ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
				)  AS DECIMAL(8,2)) AS DECIMAL(8,2)) as Cumulative_Amount_Average_Incl

FROM @PartitionTest

ORDER BY ID, [Date]

GO


