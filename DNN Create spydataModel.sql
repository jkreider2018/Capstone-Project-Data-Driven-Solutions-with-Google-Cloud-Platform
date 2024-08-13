CREATE OR REPLACE MODEL `cobalt-howl-428506-h2.capstone.spydataModel`
OPTIONS (
    MODEL_TYPE='DNN_CLASSIFIER',
    HIDDEN_UNITS=[128, 64, 32],  -- Adjust hidden units as needed
    ACTIVATION_FN='RELU',
    LEARN_RATE=0.001,
    MAX_ITERATIONS=500,
    EARLY_STOP=TRUE  -- Enable early stopping
)
AS
WITH labeled_data AS (
    SELECT
        Date,
        Open,
        High,
        Low,
        Close,
        Volume,
        Trade_Action,
        Open_Lag1,
        High_Lag1,
        Low_Lag1,
        Close_Lag1,
        Volume_Lag1,
        Trade_Action_Lag1,
        Open_Lag2,
        High_Lag2,
        Low_Lag2,
        Close_Lag2,
        Volume_Lag2,
        Trade_Action_Lag2,
        Open_Lag3,
        High_Lag3,
        Low_Lag3,
        Close_Lag3,
        Volume_Lag3,
        Trade_Action_Lag3,
        Open_Lag4,
        High_Lag4,
        Low_Lag4,
        Close_Lag4,
        Volume_Lag4,
        Trade_Action_Lag4,
        Open_Lag5,
        High_Lag5,
        Low_Lag5,
        Close_Lag5,
        Volume_Lag5,
        Trade_Action_Lag5,
        Open_Lag6,
        High_Lag6,
        Low_Lag6,
        Close_Lag6,
        Volume_Lag6,
        Trade_Action_Lag6,
        Open_Lag7,
        High_Lag7,
        Low_Lag7,
        Close_Lag7,
        Volume_Lag7,
        Trade_Action_Lag7,
        LEAD(Trade_Action, 1) OVER (ORDER BY Date) AS label  -- Label column
    FROM
        `cobalt-howl-428506-h2.capstone.spydataTrain`
)
SELECT
    *
FROM
    labeled_data
WHERE label IS NOT NULL
ORDER BY
    Date;

