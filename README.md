## Streaming Incremental Aggregates
### Explanation
- Less data written: .outputMode("update") and the upsert logic process and write only changed rows.
- Reduced processing time: Targeted updates and inserts avoid reprocessing the entire table.
- Optimized storage use: The incremental processing and selective updates reduce the state store size and checkpoint overhead.
- Scalable for large streams: This approach handles high row counts more effectively, making it suitable for production environments where the dataset can grow significantly.