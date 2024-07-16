# album_stream

CREATE STREAM album_stream (
    id STRING,
    artist_id STRING,
    name STRING,
    release_date STRING
) WITH (
    KAFKA_TOPIC='album-topic',
    VALUE_FORMAT='JSON'
);



# track_stream

CREATE STREAM track_stream (
    id STRING,
    album_id STRING,
    name STRING,
    popularity INT,
    danceability DOUBLE,
    energy DOUBLE,
    key INT,
    loudness DOUBLE,
    mode INT,
    speechiness DOUBLE,
    acousticness DOUBLE,
    instrumentalness DOUBLE,
    liveness DOUBLE,
    valence DOUBLE,
    tempo DOUBLE
) WITH (
    KAFKA_TOPIC='track-topic',
    VALUE_FORMAT='JSON'
);




# popular_track
CREATE STREAM popular_track AS
SELECT
    name,
    popularity
FROM track_stream
WHERE popularity > 60;




# total_album
CREATE TABLE total_album AS
  SELECT
    artist_id,
    COUNT(*) AS album_count
  FROM album_stream
  GROUP BY artist_id;




# total_track
CREATE TABLE total_track AS
SELECT 
    'id' AS id, 
    COUNT(*) AS track_count
FROM track_stream
GROUP BY 'id';