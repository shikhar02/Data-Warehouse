# Importing an important library.
import configparser


# Access and read credentials from 'dwh.file'.
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Drop tables.

staging_events_table_drop = "DROP TABLE IF EXISTS events"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# Creating staging events table.

staging_events_table_create= ("""
                              CREATE TABLE IF NOT EXISTS stage_events
                              (artist varchar(500),
                              auth varchar(50),
                              firstName varchar(50),
                              gender varchar(6),
                              itemInSession int,
                              lastName varchar(50),
                              length real,
                              level varchar(50),
                              location varchar(500),
                              method varchar,
                              page varchar,
                              registration real,
                              session_id smallint,
                              song varchar,
                              status smallint,
                              ts bigint,
                              userAgent varchar(500),
                              userId smallint);  
                              """)

# Creating staging songs table.

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS stage_songs
                              (num_songs smallint,
                              artist_id varchar(50),
                              artist_latitude float,
                              artist_longitude float,
                              artist_location varchar,
                              artist_name varchar(500),
                              song_id varchar(50),
                              title varchar,
                              duration real,
                              year int);
                              """)

# Creating songplays (fact) table.

songplay_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songplays
                        (songplay_id int IDENTITY(0,1) NOT NULL PRIMARY KEY SORTKEY,
                        start_time timestamp NOT NULL REFERENCES time(start_time), 
                        user_id smallint NOT NULL REFERENCES users(user_id) DISTKEY, 
                        level varchar(500), 
                        song_id varchar(500) NOT NULL REFERENCES songs(song_id),
                        artist_id varchar(500) NOT NULL REFERENCES artists(artist_id), 
                        session_id smallint, 
                        location varchar(500), 
                        user_agent varchar(500));
                        """)

# Creating users (dimension) table.

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users
                     (user_id smallint NOT NULL PRIMARY KEY SORTKEY DISTKEY, 
                     first_name varchar(50), 
                     last_name varchar(50), 
                     gender varchar(6), 
                     level varchar(50));
                     """)

# Creating songs (dimension) table.

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs
                     (song_id varchar(50) NOT NULL PRIMARY KEY SORTKEY,
                     title varchar, 
                     artist_id varchar(50), 
                     year int, 
                     duration real)
                     diststyle all;
                     """)

# Creating artists (dimension) table.

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists
                       (artist_id varchar(500) NOT NULL PRIMARY KEY sortkey, 
                       name varchar(500), 
                       location varchar(500), 
                       latitude float, 
                       longitude float)
                       diststyle all;
                       """)

# Creating time (dimension) table.

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time
                     (start_time timestamp NOT NULL PRIMARY KEY, 
                     hour int, 
                     day int, 
                     week int, 
                     month int, 
                     year int, 
                     weekday int)
                     diststyle all;
                     """)

# Copy data from S3 to redshift.

staging_events_copy = ("""
                       COPY stage_events FROM {}
                       iam_role {}
                       FORMAT json {} 
                       region 'us-west-2'
                       timeformat as 'epochmillisecs';
                       """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
                      COPY stage_songs FROM {}
                      iam_role {}
                      FORMAT json 'auto'
                      region 'us-west-2';
                      """).format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# Queries to insert data into fact table songplays.

songplay_table_insert = ("""
                         INSERT INTO songplays (start_time, user_id, level, song_id,
                         artist_id, session_id, location, user_agent)                          
                         SELECT
                         to_timestamp(stage_events.ts::text, 'YYYYMMDDHH24MISS') AS start_time,
                         stage_events.userId AS user_id,
                         stage_events.level,
                         stage_songs.song_id,
                         stage_songs.artist_id,
                         stage_events.session_id,
                         stage_events.location,
                         stage_events.userAgent AS user_agent
                         FROM stage_events
                         JOIN stage_songs ON (stage_events.artist = stage_songs.artist_name 
                         AND stage_events.song = stage_songs.title)
                         WHERE stage_events.page = 'NextSong'
                         AND stage_events.userId NOT IN (SELECT DISTINCT user_id FROM songplays);
                         """)

# Queries to insert data into dimension table users.


user_table_insert = ("""
                     INSERT INTO users(user_id, first_name, last_name, gender, level)
                     SELECT DISTINCT userId AS user_id,
                     firstName AS first_name,
                     lastName AS last_name,
                     gender,
                     level
                     FROM stage_events
                     WHERE userId IS NOT NULL;
                     """)

# Queries to insert data into dimension table songs.


song_table_insert = ("""
                     INSERT INTO songs(song_id, title, artist_id, year, duration)
                     SELECT DISTINCT song_id,
                     title,
                     artist_id,
                     year,
                     duration
                     FROM stage_songs
                     WHERE song_id IS NOT NULL;
                     """)

# Queries to insert data into dimension table artists.


artist_table_insert = ("""
                       INSERT INTO artists(artist_id, name, location, latitude, longitude)
                       SELECT DISTINCT artist_id,
                       artist_name AS name,
                       artist_location AS location,
                       artist_latitude AS latitude,
                       artist_longitude AS longitude
                       FROM stage_songs
                       WHERE artist_id IS NOT NULL;
                       """)

# Queries to insert data into dimension table time.


time_table_insert = ("""
                     INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                     SELECT DISTINCT s.start_time,
                     EXTRACT(HOUR FROM s.start_time) AS hour,
                     EXTRACT(DAY FROM s.start_time) AS day,
                     EXTRACT(WEEK FROM s.start_time) AS week,
                     EXTRACT(MONTH FROM s.start_time) AS month,
                     EXTRACT(YEAR FROM s.start_time) AS year,
                     EXTRACT(DOW FROM s.start_time) AS weekday
                     FROM songplays s
                     WHERE s.start_time IS NOT NULL;
                     """)

# Creating variables with lists of various queries to be imported in etl.py and create_tables.py

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
