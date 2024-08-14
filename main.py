import requests
import pandas as pd
from google.cloud import storage, bigquery
import io
from flask import jsonify

def download_alphaventure_data(request):
    """
    Fetches data from the Alpha Vantage API, processes it, and stores it in a
    Google Cloud Storage bucket as a CSV file. Then triggers BigQuery queries.
    """
    try:
        # Define names
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=SPY&apikey=CO6Y2YGAI1537F8M'
        bucket_name = 'spydata_realtime'
        csv_filename = 'spyrtdata.csv'   

        # Fetch data from Alpha Vantage API
        r = requests.get(url)

        # Check if the request was successful
        if r.status_code == 200:
            data = r.json()

            # Extract and load the time series data into a DataFrame
            time_series = data.get('Time Series (Daily)', {})
            df = pd.DataFrame.from_dict(time_series, orient='index')
            df = df.astype(float)  # Convert all columns to floats
            df.index = pd.to_datetime(df.index)  # Convert the index to datetime
            df.sort_index(ascending=True, inplace=True)  # Sort by date ascending

            # Rename columns for easier access
            df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

            # Add the Date as a column
            df['Date'] = df.index

            # Create 10-day and 30-day moving averages for Close prices
            df['MA10'] = df['Close'].rolling(window=10).mean()
            df['MA30'] = df['Close'].rolling(window=30).mean()

            # Create Buy/Sell/Hold signal
            df['Trade_Action'] = 0  # Initialize column with 0
            df.loc[(df['Close'] < df['MA10']) & (df['MA10'] < df['MA30']), 'Trade_Action'] = -1  # Sell signal
            df.loc[(df['Close'] > df['MA10']) & (df['MA10'] > df['MA30']), 'Trade_Action'] = 1  # Buy signal

            # Convert 'Trade_Action' and 'Volume' to INT64
            df['Trade_Action'] = df['Trade_Action'].astype('Int64')
            df['Volume'] = df['Volume'].astype('Int64')

            # Create lagged features
            for lag in range(1, 8):
                df[f'Open_Lag{lag}'] = df['Open'].shift(lag)
                df[f'High_Lag{lag}'] = df['High'].shift(lag)
                df[f'Low_Lag{lag}'] = df['Low'].shift(lag)
                df[f'Close_Lag{lag}'] = df['Close'].shift(lag)
                df[f'Volume_Lag{lag}'] = df['Volume'].shift(lag).astype('Int64')  # Ensure Volume_Lag is INT64
                df[f'Trade_Action_Lag{lag}'] = df['Trade_Action'].shift(lag).astype('Int64')  # Ensure Trade_Action_Lag is INT64

            # Drop rows with any NULL values in the columns of interest
            df_cleaned = df.dropna(subset=[
                'MA10', 'MA30', 
                'Open_Lag7', 'High_Lag7', 'Low_Lag7', 'Close_Lag7', 'Volume_Lag7', 'Trade_Action_Lag7'
            ])

            # Reorder columns so that 'Date' is the first column
            cols = ['Date'] + [col for col in df_cleaned.columns if col != 'Date']
            df_cleaned = df_cleaned[cols]

            # Convert the DataFrame to a CSV format in-memory
            csv_buffer = io.StringIO()
            df_cleaned.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            # Upload the CSV to Google Cloud Storage
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(csv_filename)

            # Upload the CSV data from memory
            blob.upload_from_string(csv_data, content_type='text/csv')

            # Trigger the BigQuery update_spydataRealTime to update table spydataRealTime
            client = bigquery.Client()
            update_query = """
            # Create a temporary external table linked to your GCS CSV file
            CREATE OR REPLACE EXTERNAL TABLE capstone.temp_table
            OPTIONS (
              format = 'CSV',
              uris = ['gs://spydata_realtime/spyrtdata.csv'],
              skip_leading_rows = 1
            );

            # Replace data in your BigQuery table with the data from the external table
            CREATE OR REPLACE TABLE cobalt-howl-428506-h2.capstone.spydataRealTime AS
            SELECT * FROM capstone.temp_table;

            # Drop the temporary external table after use
            DROP TABLE capstone.temp_table;
            """
            update_job = client.query(update_query)  # Run the BigQuery query to update spydataRealTime
            update_job.result()  # Wait for the query to finish

            # Trigger the BigQuery predict_trade_action query after update_spydataRealTime is complete
            predict_query = """
            CREATE OR REPLACE TABLE `cobalt-howl-428506-h2.capstone.spydataRealTimePred` AS
            SELECT
                Date,
                Open,
                High,
                Low,
                Close,
                Volume,
                Trade_Action,
                CAST(predicted_label AS INT64) AS Pred_Trade_Action
            FROM
                ML.PREDICT(
                    MODEL `cobalt-howl-428506-h2.capstone.spydataModel`,
                    (
                        SELECT
                            Date,
                            Open,
                            High,
                            Low,
                            Close,
                            Volume,
                            Trade_Action,
                            -- Include only the columns required by the model, ensuring no missing columns
                            Close_Lag1, High_Lag1, Low_Lag1, Open_Lag1, Volume_Lag1, Trade_Action_Lag1,
                            Close_Lag2, High_Lag2, Low_Lag2, Open_Lag2, Volume_Lag2, Trade_Action_Lag2,
                            Close_Lag3, High_Lag3, Low_Lag3, Open_Lag3, Volume_Lag3, Trade_Action_Lag3,
                            Close_Lag4, High_Lag4, Low_Lag4, Open_Lag4, Volume_Lag4, Trade_Action_Lag4,
                            Close_Lag5, High_Lag5, Low_Lag5, Open_Lag5, Volume_Lag5, Trade_Action_Lag5,
                            Close_Lag6, High_Lag6, Low_Lag6, Open_Lag6, Volume_Lag6, Trade_Action_Lag6,
                            Close_Lag7, High_Lag7, Low_Lag7, Open_Lag7, Volume_Lag7, Trade_Action_Lag7
                        FROM
                            `cobalt-howl-428506-h2.capstone.spydataRealTime`
                    )
                )
            ORDER BY
                Date ASC;
            """
            predict_job = client.query(predict_query)  # Run the BigQuery query to generate predictions
            predict_job.result()  # Wait for the query to finish

            # Return success response
            return jsonify({'message': f'All data records have been written to the bucket {bucket_name} with filename {csv_filename}, BigQuery tables updated, and predictions generated.'}), 200

        else:
            # Handle the error if the request was not successful
            return jsonify({'error': f'Failed to fetch data from the API. Status code: {r.status_code}'}), r.status_code

    except Exception as e:
        # Return a JSON response with the exception message
        return jsonify({'error': str(e)}), 500
