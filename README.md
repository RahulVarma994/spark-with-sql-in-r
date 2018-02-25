# spark-with-sql-in-r
# Write SQL query
query <- "SELECT * FROM track_metadata WHERE year < 1935 AND duration > 300"

# Run the query
(results <- dbGetQuery(spark_conn, query))


# Left join artist terms to track metadata by artist_id
joined <- left_join(track_metadata_tbl, artist_terms_tbl, by = c("artist_id"))

# How many rows and columns are in the joined table?
dim(joined)

# Anti join artist terms to track metadata by artist_id
joined <- anti_join(track_metadata_tbl,
artist_terms_tbl,
by = c("artist_id"))- ___

# How many rows and columns are in the joined table?
dim(joined)
