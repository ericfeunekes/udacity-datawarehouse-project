import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN=config.get('IAM_ROLE','ARN')
LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stg_events (
    artist TEXT           ,
    auth TEXT             NOT NULL,
    firstname TEXT        ,
    gender TEXT           ,
    itemInSession INT     NOT NULL,
    lastName TEXT         ,
    length FLOAT          ,
    level TEXT            NOT NULL,
    location TEXT         ,
    method TEXT           NOT NULL,
    page TEXT             NOT NULL,
    registration FLOAT    ,
    sessionID INT         NOT NULL,
    song TEXT             ,
    status INT            NOT NULL,
    ts BIGINT             NOT NULL SORTKEY DISTKEY PRIMARY KEY,
    userAgent TEXT        ,
    userID INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE stg_songs (
    "num_songs" INT             NOT NULL,
    "artist_id" TEXT            NOT NULL,
    "artist_latitude" FLOAT,
    "artist_longitude" FLOAT,
    "artist_location" TEXT,
    "artist_name" TEXT          NOT NULL,
    "song_id" TEXT              NOT NULL SORTKEY PRIMARY KEY,
    "title" TEXT                NOT NULL DISTKEY,
    "duration" FLOAT            NOT NULL,
    "year" INT                  NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id INT IDENTITY(0,1)   NOT NULL DISTKEY PRIMARY KEY,
    start_time  TIMESTAMP       NOT NULL SORTKEY,
    user_id     INT             NOT NULL,
    level       TEXT            NOT NULL,
    song_id     TEXT            NOT NULL,
    artist_id   TEXT            NOT NULL,
    session_id  INT             NOT NULL,
    location    TEXT            NOT NULL,
    user_agent  TEXT            NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id     INT         NOT NULL SORTKEY PRIMARY KEY,
    first_name  TEXT        NOT NULL,
    last_name   TEXT        NOT NULL,
    gender      TEXT        NOT NULL,
    level       TEXT        NOT NULL
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id     TEXT    NOT NULL SORTKEY PRIMARY KEY,
    title       TEXT    NOT NULL,
    artist_id   TEXT    NOT NULL,
    year        INT     NOT NULL,
    duration    FLOAT   NOT NULL
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id   TEXT    NOT NULL SORTKEY PRIMARY KEY,
    name        TEXT    NOT NULL,
    location    TEXT,
    lattitude   FLOAT,
    longitude   FLOAT
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE time (
    start_time  TIMESTAMP   NOT NULL SORTKEY DISTKEY PRIMARY KEY,
    hour        INT         NOT NULL,
    day         INT         NOT NULL,
    week        INT         NOT NULL,
    month       INT         NOT NULL,
    year        INT         NOT NULL,
    weekday     TEXT        NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY stg_events FROM {LOG_DATA}
iam_role {ARN}
FORMAT AS JSON {LOG_JSONPATH}
""")

staging_songs_copy = (f"""
COPY stg_songs FROM {SONG_DATA}
iam_role {ARN}
FORMAT AS JSON 'auto'
ACCEPTINVCHARS AS '^'
STATUPDATE ON
region 'us-west-2'
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id   ,
    level     ,
    song_id   ,
    artist_id ,
    session_id,
    location  ,
    user_agent
)
SELECT
    TIMESTAMP 'epoch' + (events.ts / 1000) * INTERVAL '1 second',
    events.userid,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.sessionid,
    events.location,
    events.useragent
FROM stg_events as events
INNER JOIN stg_songs as songs ON events.song = songs.title
WHERE events.song IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userid,
    firstname,
    lastname,
    gender,
    level
FROM stg_events
WHERE userid IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
	song_id,
    title,
    artist_id,
    year,
    duration
FROM stg_songs
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    lattitude,
    longitude
)
SELECT DISTINCT
	artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM public.stg_songs
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEKS FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    to_char(start_time, 'Day') AS weekday
    FROM stg_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
