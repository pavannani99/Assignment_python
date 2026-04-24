Retail Data Ingestion API

I built this Retail Data Ingestion API using FastAPI to handle uploading csv files like stores, users and mapping data.

The main idea of the Retail Data Ingestion API was to make sure that even if the data is messy the Retail Data Ingestion API system should still work properly and not break.

What the Retail Data Ingestion API does

* It takes files checks each row properly and only inserts valid data into the database.

* If there are errors it does not stop everything. It just skips those rows. Tells which rows failed and why.

I used SQLAlchemy with SQLite for the Retail Data Ingestion API for now. It can be changed to Postgres easily.

For files instead of processing everything at once I used chunking and bulk insert so the Retail Data Ingestion API does not become slow.

How to run the Retail Data Ingestion API

1. Run pip install -r requirements.txt

2. Run app.main:app --reload

I did a performance test of the Retail Data Ingestion API using a 500k file.

Here are the results of the performance test of the Retail Data Ingestion API:

* Total rows: 500000

* Inserted: 491449

* Failed: 8551

* Chunk size: 5000

* Time taken: around 4.5 minutes

* Speed: around 1800 rows per

Some errors were, like duplicate store_id, missing fields, invalid values etc.

Finally the Retail Data Ingestion API works fine for large data handles errors properly and does not crash.

The performance of the Retail Data Ingestion API can still be. The main focus was to make sure the data is correct and errors are clearly shown by the Retail Data Ingestion API.
