#!/bin/bash

# Set the URL to download the CSV file
url="https://query1.finance.yahoo.com/v7/finance/download/SPY?period1=757382400&period2=1723017918&interval=1d&events=history&includeAdjustedClose=true"

# Set the filename for the downloaded file
filename="SPY.csv"

# Download the file using curl
curl -o "$filename" "$url"

# Check if the download was successful
if [[ $? -eq 0 ]]; then
  echo "File downloaded successfully!"

  # Set the Google Cloud project ID
  project_id="Big Data Analytics - Cloud"

  # Set the Cloud Storage bucket name
  bucket_name="spydata_1994_2024"

  # Upload the file to the bucket using gsutil
  gsutil cp "$filename" gs://$bucket_name

  echo "File uploaded to Cloud Storage bucket: gs://$bucket_name"
else
  echo "Error downloading file."
fi