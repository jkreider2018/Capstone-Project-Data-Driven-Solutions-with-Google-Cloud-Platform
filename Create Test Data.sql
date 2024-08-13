-- Create a new table called spydataTrain by keeping the last 20% of table spydataCLEAN using the sorted Date in ascending order.
CREATE OR REPLACE TABLE `cobalt-howl-428506-h2.capstone.spydataTest` AS
WITH RankedData AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY Date ASC) AS RowNum
    FROM
        `cobalt-howl-428506-h2.capstone.spydataCLEAN`
)
SELECT
  * EXCEPT(RowNum)
FROM
  `RankedData`
WHERE RowNum > (SELECT CAST(COUNT(*) * 0.8 AS INT64) FROM `cobalt-howl-428506-h2.capstone.spydataCLEAN`);