import requests
import pandas as pd
from google.cloud import storage
import io
from flask import jsonify

def download_alphaventure_data(request):
    """
    Fetches data from the Alpha Vantage API, processes it, and stores it in a
    Google Cloud Storage bucket as a CSV file.
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

            # Upload the CSV to Google Cloud Storage
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(csv_filename)

            # Upload the CSV data from memory
            blob.upload_from_string(csv_data, content_type='text/csv')

            # Return success response
            return jsonify({'message': f'All Date records have been written to the bucket {bucket_name} with filename {csv_filename}'}), 200

        else:
            # Handle the error if the request was not successful
            return jsonify({'error': f'Failed to fetch data from the API. Status code: {r.status_code}'}), r.status_code

    except Exception as e:
        # Return a JSON response with the exception message
        return jsonify({'error': str(e)}), 500
