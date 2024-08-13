-- 1. apply the existing model "spydataModel" to the table "spydataRealTime" and lag by 1 day predicted_label and convert the lagged predicted_label to an integer nxdy_Trade_Action.  Then output the variables Date, Open, High, Low, Close, Volume, and nxdy_Trade_Action, to the table "spydataRealTimePred".
-- Correct the error "Column predicted_Close in SELECT * EXCEPT list does not exist at [3:14]"
-- 2. correct Invalid table-valued function ML.PREDICT Column Close_Lag1 is not found in the input data to the PREDICT function. at [11:5]
-- Output to table "spydataRealTimePred", Date, Open, High, Low, Close, Volume, Trade_Action, Pred_Trade_Action
CREATE OR REPLACE TABLE cobalt-howl-428506-h2.capstone.spydataRealTimePred AS
SELECT
    Date,
    Open,
    High,
    Low,
    Close,
    Volume,
    Trade_Action,
    CAST(LAG(predicted_label, 1) OVER (ORDER BY Date ASC) AS INT64) AS Pred_Trade_Action
  FROM
    (
      SELECT
          * EXCEPT(predicted_label),
          CAST(predicted_label AS BIGNUMERIC) AS predicted_label
        FROM
          ML.PREDICT(MODEL `cobalt-howl-428506-h2.capstone.spydataModel`,
            (
              SELECT
                  *
                FROM
                  `cobalt-howl-428506-h2.capstone.spydataRealTime`
            )
          )
    )
ORDER BY
  Date ASC;