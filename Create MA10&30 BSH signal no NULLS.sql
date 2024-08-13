-- Create 10 & 30 day moving averages for Closing, create BSH signal, add lagged values, and delete rows with NULLs
CREATE OR REPLACE TABLE `cobalt-howl-428506-h2.capstone.spydataCLEAN` AS
WITH MovingAverages AS (
    SELECT
        *,
        AVG(Close) OVER (ORDER BY Date ASC ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS MA10,
        AVG(Close) OVER (ORDER BY Date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA30
    FROM
        `cobalt-howl-428506-h2.capstone.spydataRAW`
),
BSH_Signal AS (
    SELECT
        *,
        CASE
            WHEN Close < MA10 AND MA10 < MA30 THEN -1
            WHEN Close > MA10 AND MA10 > MA30 THEN 1
            ELSE 0
        END AS Trade_Action
    FROM
        MovingAverages
),
LaggedData AS (
    SELECT
        *,
        LAG(Open, 1) OVER (ORDER BY Date ASC) AS Open_Lag1,
        LAG(Open, 2) OVER (ORDER BY Date ASC) AS Open_Lag2,
        LAG(Open, 3) OVER (ORDER BY Date ASC) AS Open_Lag3,
        LAG(Open, 4) OVER (ORDER BY Date ASC) AS Open_Lag4,
        LAG(Open, 5) OVER (ORDER BY Date ASC) AS Open_Lag5,
        LAG(Open, 6) OVER (ORDER BY Date ASC) AS Open_Lag6,
        LAG(Open, 7) OVER (ORDER BY Date ASC) AS Open_Lag7,
        LAG(High, 1) OVER (ORDER BY Date ASC) AS High_Lag1,
        LAG(High, 2) OVER (ORDER BY Date ASC) AS High_Lag2,
        LAG(High, 3) OVER (ORDER BY Date ASC) AS High_Lag3,
        LAG(High, 4) OVER (ORDER BY Date ASC) AS High_Lag4,
        LAG(High, 5) OVER (ORDER BY Date ASC) AS High_Lag5,
        LAG(High, 6) OVER (ORDER BY Date ASC) AS High_Lag6,
        LAG(High, 7) OVER (ORDER BY Date ASC) AS High_Lag7,
        LAG(Low, 1) OVER (ORDER BY Date ASC) AS Low_Lag1,
        LAG(Low, 2) OVER (ORDER BY Date ASC) AS Low_Lag2,
        LAG(Low, 3) OVER (ORDER BY Date ASC) AS Low_Lag3,
        LAG(Low, 4) OVER (ORDER BY Date ASC) AS Low_Lag4,
        LAG(Low, 5) OVER (ORDER BY Date ASC) AS Low_Lag5,
        LAG(Low, 6) OVER (ORDER BY Date ASC) AS Low_Lag6,
        LAG(Low, 7) OVER (ORDER BY Date ASC) AS Low_Lag7,
        LAG(Close, 1) OVER (ORDER BY Date ASC) AS Close_Lag1,
        LAG(Close, 2) OVER (ORDER BY Date ASC) AS Close_Lag2,
        LAG(Close, 3) OVER (ORDER BY Date ASC) AS Close_Lag3,
        LAG(Close, 4) OVER (ORDER BY Date ASC) AS Close_Lag4,
        LAG(Close, 5) OVER (ORDER BY Date ASC) AS Close_Lag5,
        LAG(Close, 6) OVER (ORDER BY Date ASC) AS Close_Lag6,
        LAG(Close, 7) OVER (ORDER BY Date ASC) AS Close_Lag7,
        LAG(Volume, 1) OVER (ORDER BY Date ASC) AS Volume_Lag1,
        LAG(Volume, 2) OVER (ORDER BY Date ASC) AS Volume_Lag2,
        LAG(Volume, 3) OVER (ORDER BY Date ASC) AS Volume_Lag3,
        LAG(Volume, 4) OVER (ORDER BY Date ASC) AS Volume_Lag4,
        LAG(Volume, 5) OVER (ORDER BY Date ASC) AS Volume_Lag5,
        LAG(Volume, 6) OVER (ORDER BY Date ASC) AS Volume_Lag6,
        LAG(Volume, 7) OVER (ORDER BY Date ASC) AS Volume_Lag7,
        LAG(Trade_Action, 1) OVER (ORDER BY Date ASC) AS Trade_Action_Lag1,
        LAG(Trade_Action, 2) OVER (ORDER BY Date ASC) AS Trade_Action_Lag2,
        LAG(Trade_Action, 3) OVER (ORDER BY Date ASC) AS Trade_Action_Lag3,
        LAG(Trade_Action, 4) OVER (ORDER BY Date ASC) AS Trade_Action_Lag4,
        LAG(Trade_Action, 5) OVER (ORDER BY Date ASC) AS Trade_Action_Lag5,
        LAG(Trade_Action, 6) OVER (ORDER BY Date ASC) AS Trade_Action_Lag6,
        LAG(Trade_Action, 7) OVER (ORDER BY Date ASC) AS Trade_Action_Lag7
    FROM
        BSH_Signal
)
SELECT
    *
FROM
    LaggedData
WHERE
    MA10 IS NOT NULL
    AND MA30 IS NOT NULL
    AND Open_Lag7 IS NOT NULL
    AND High_Lag7 IS NOT NULL
    AND Low_Lag7 IS NOT NULL
    AND Close_Lag7 IS NOT NULL
    AND Volume_Lag7 IS NOT NULL
    AND Trade_Action_Lag7 IS NOT NULL
ORDER BY
    Date ASC;
