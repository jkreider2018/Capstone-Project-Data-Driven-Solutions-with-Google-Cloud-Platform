# Create a temporary external table linked to your GCS CSV file
CREATE OR REPLACE EXTERNAL TABLE `capstone.temp_table`
OPTIONS (
  format = 'CSV',
  uris = ['gs://spydata_realtime/spyrtdata.csv'],
  skip_leading_rows = 1
);

# Replace data in your BigQuery table with the data from the external table
CREATE OR REPLACE TABLE `cobalt-howl-428506-h2.capstone.spydataRealTime` AS
SELECT * FROM `capstone.temp_table`;

# Drop the temporary external table after use
DROP TABLE `capstone.temp_table`;