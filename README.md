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
by = c("artist_id"))

# How many rows and columns are in the joined table?
dim(joined)

# Anti join artist terms to track metadata by artist_id
joined <- anti_join(track_metadata_tbl,
artist_terms_tbl,
by = c("artist_id"))

hotttnesss <- track_metadata_tbl %>%
 # Select artist_hotttnesss
 select(artist_hotttnesss) %>%
 # Binarize to is_hottt_or_nottt
 ft_binarizer("artist_hotttnesss",
 "is_hottt_or_nottt",
 threshold = 0.5) %>%
 # Collect the result
 collect() %>%
 # Convert is_hottt_or_nottt to logical
 mutate(is_hottt_or_nottt = as.logical(is_hottt_or_nottt))
# Draw a barplot of is_hottt_or_nottt
ggplot(hotttnesss,
aes(is_hottt_or_nottt)) +
 geom_bar()
