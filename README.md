# Leveraging GCP to Analyze and Predict the S&P 500

## Capstone Project: Data Driven Solutions with Google Cloud Platform 
**Group 6 - John Kreider, Jacob Michaels, Daelano Pacheco, John Robertson, Ronald Ishii**

**Objective**:
Successfully integrate at least three GCP technologies into Data Studio where both archived and real-time data sources are analyzed to provide insights and actionable solutions.

**Problem Domain:** 
The target domain is the financial sector, however the model generated could be applied to other sectors which similarly are looking for a signal on what actions to take given historical context.

**Data Source:**
Data comes from Yahoo Finance's archive of the SPY ETF. SPY is the ticker symbol for the SPDR® S&P 500® ETF Trust which looks to mirror the S&P 500®. This ticker symbol effectively acts as a proxy to the overall market and will allow for a robust analysis.

**Analysis Goals:**
The goal of our analysis is to construct a predictive model that leverages historical SPY stock data to generate timely trading signals (buy, hold, sell) that capitalize on market movements. 

This model will utilize Google Cloud Platform (GCP) tools, including BigQuery, Cloud Scheduler, Pub/Sub, and Looker Studio, to analyze and execute trades based on the Standard & Poor's 500 Index (SPY) data.

**Metrics and Model:**


**Results:**


**Key Findings:**

**Software & Process Description:**

  >One time process to create the model...

    Cloud Shell: downloadspy.sh
      Collects historical Yahoo Finance SPY data, SPY.csv, and sends it to spy_1994-2024 bucket
      BigQuery table spydataRAW manually created from the SPY.csv
  
    BigQuery: "Create MA10&30 BSH signal no NULLS.sql"
      Creates...
          Closing MA10 and MA30
          Trade_Action signal (-1=buy, 0=hold, 1=sell)
          7 lags for Open, Low, High, Close, Volume, Trade_Action
          Removes any records that contain NULL values
          Sorts by Date ascending
          Stores data in table spydataCLEAN
    
    BigQuery: "Create Training Data.sql" "Create Test Data.sql"
      Creates tables spydataTrain (80% of spydataCLEAN) & spydataTest (remaining 20% of spydataCLEAN)  respectively
  
    BigQuery: "DNN Create spydataModel.sql"
      Trains a deep learning neural network classifier (224 hidden nodes in three layers) to predict the next day Trade_Action using current Date, Open, High, Low, Close, Volume, and all 7 days of lagged data for those variables.
      Uses the spydataTrain table for training.
      Model name is spydataModel
  
    BigQuery: "DNN spydataModel evaluation
      Uses the spydataTest table for statistical performance evaluation.
  
    BigQuery: "DNN spydataModel confusion"
      Produces confusion matrix using spydataTest table.
  
  >Recurring process to predict next day...

    Cloud Function: download_alphaventure_data once per day at 11pm PST via Cloud Scheduler.
      Runs python code to fetch Alpha Venture SPY API historical data (spyrtdata.csv)
      Creates...
        Closing MA10 and MA30
        Trade_Action signal (-1=buy, 0=hold, 1=sell)
        7 lags for Open, Low, High, Close, Volume, Trade_Action
        Removes any records that contain NULL values
        Sorts by Date ascending
        Stores data in table spydataCLEAN
      Stores data in bucket spydata_realtime

    BigQuery: "update_spydataRealTime.sql"
      Takes SPY historical data (spyrtdata.csv) and stores it in bucket spydata_realtime and updates spydataRealTime table

    BigQuery: "predict_trade_action.sql"
      Applies the spydataModel to the spydataRealTime to predict the next day trade action (Pred_Trade_Action).
      Updates Date, Open, High, Low, Close, Volume, Trade_Action, Pred_Trade_Action in table spydataRealTimePred.
