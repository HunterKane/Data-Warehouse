import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")


# DROP TABLES:

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES:

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (

    aritst         VARCHAR,
    auth           VARCHAR,
    firstName      VARCHAR,
    gender         CHAR(1),
    itemInSession  INTEGER,
    lastName       VARCHAR,
    Length         FLOAT,
    location       VARCHAR,
    method         VARCHAR,
    page           VARCHAR,
    registration   FLOAT,
    session_id     INTEGER NOT NULL SORTKEY DISTKEY,
    song           VARCHAR,
    status         INTEGER,
    ts             TIMESTAMP NOT NULL,
    user_agent     VARCHAR,
    user_id        INTEGER
    );
""")

staging_songs_table_create = (""" 
    CREATE TABLE IF NOT EXISTS staging_songs (
    
    num_songs           INTEGER,
    artist_id           VARCHAR NOT NULL SORTKEY,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR NOT NULL,
    title               VARCHAR,
    duration            FLOAT,
    year                INTEGER    
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_songplay (
    
    songplay_id INTEGER IDENTITY (0, 1) PRIMARY KEY, 
    start_time  TIMESTAMP NOT NULL SORTKEY DISTKEY,
    user_id     INTEGER NOT NULL,
    level       VARCHAR,
    song_id     VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    session_id  INTEGER,
    location    VARCHAR,
    user_agent  VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_user (
    
    user_id     INTEGER NOT NULL SORTKEY PRIMARY KEY,
    first_name  TEXT NOT NULL,
    last_name   TEXT NOT NULL,
    gender      CHAR(1),
    level       TEXT
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_song (
    
    song_id      VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    title        VARCHAR,
    artist_id    VARCHAR distkey,
    year         INTEGER,
    duration     NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artist (
    
    artist_id  VARCHAR NOT NULL SORTKEY PRIMARY KEY,
    name       VARCHAR NOT NULL,
    location   VARCHAR,
    latitude   FLOAT,
    longtitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time (
    
    start_time     TIMESTAMP NOT NULL DISTKEY SORTKEY PRIMARY KEY,
    hour           INTEGER,
    day            INTEGER,
    week           INTEGER,
    month          INTEGER,
    year           INTEGER,
    weekDay        INTEGER 
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time, 
            e.userId        AS user_id, 
            e.level         AS level, 
            s.song_id       AS song_id, 
            s.artist_id     AS artist_id, 
            e.sessionId     AS session_id, 
            e.location      AS location, 
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  ==  'NextSong';
""")

song_table_insert = ("""
   INSERT INTO dim_song(song_id, title, artist_id, year, duration)
   SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
  FROM staging_songs
  WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
  INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
  SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
  FROM staging_songs
  where artist_id IS NOT NULL;
""")

time_table_insert = ("""
  INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
  SELECT distinct ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
  FROM staging_events
  WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]