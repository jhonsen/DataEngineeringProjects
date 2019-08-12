import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table" 

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                    (
                                    event_key           INTEGER          IDENTITY(0,1),
                                    artist              VARCHAR(MAX),
                                    auth                VARCHAR(MAX),
                                    first_name          VARCHAR(MAX),
                                    gender              VARCHAR(MAX),
                                    item_in_session     INTEGER,
                                    last_name           VARCHAR(MAX),
                                    length              NUMERIC,
                                    level               VARCHAR(MAX),
                                    location            VARCHAR(MAX),
                                    method              VARCHAR(MAX),
                                    page                VARCHAR(MAX),
                                    registration        DECIMAL(15,1),
                                    session_id          INTEGER,
                                    song_title          VARCHAR(MAX),
                                    status              INTEGER,
                                    ts                  BIGINT,
                                    user_agent          VARCHAR(MAX),
                                    user_id             VARCHAR(22)
                                    ) 
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                    (
                                    num_songs        INTEGER,
                                    artist_id        VARCHAR(MAX),
                                    artist_latitude  NUMERIC,
                                    artist_longitude NUMERIC,
                                    artist_location  VARCHAR(MAX),
                                    artist_name      VARCHAR(MAX),
                                    song_id          VARCHAR(MAX),
                                    title            VARCHAR(MAX),
                                    duration         NUMERIC,
                                    year             INTEGER
                                    )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table 
                            (
                            songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY NOT NULL , 
                            start_time  TIMESTAMP WITHOUT TIME ZONE ,
                            user_id     VARCHAR ,
                            level       VARCHAR , 
                            song_id     VARCHAR ,
                            artist_id   VARCHAR ,
                            session_id  INTEGER ,
                            location    VARCHAR ,
                            user_agent  VARCHAR 
                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
                        (user_id VARCHAR PRIMARY KEY NOT NULL, 
                        first_name      VARCHAR,
                        last_name       VARCHAR,
                        gender          VARCHAR,
                        level           VARCHAR
                        )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table 
                        (song_id      VARCHAR PRIMARY KEY NOT NULL,
                        title         VARCHAR, 
                        artist_id     VARCHAR,
                        year          INTEGER,
                        duration      DECIMAL
                        )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table
                            (artist_id  VARCHAR PRIMARY KEY NOT NULL,
                            name        VARCHAR,
                            location    VARCHAR,
                            latitude    DECIMAL,
                            longitude   DECIMAL
                            )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
                        (start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY NOT NULL,
                         hour       INTEGER,
                         day        VARCHAR,
                         week       VARCHAR,
                         month      INTEGER,
                         year       INTEGER,
                         weekday    VARCHAR
                        )
""")

# STAGING TABLES

staging_events_copy = ("""
                        COPY     staging_events
                        FROM     {}
                        IAM_ROLE {}
                        JSON     {}
                        """).format(config['S3']['LOG_DATA'],
                                    config['IAM_ROLE']['ARN'],
                                    config['S3']['LOG_JSONPATH']    
                       )


staging_songs_copy = ("""
                        COPY staging_songs
                        FROM     {}
                        IAM_ROLE {}
                        FORMAT AS JSON  'auto'
                        """).format(config['S3']['SONG_DATA'],
                                    config['IAM_ROLE']['ARN']
                       )

                                    
                                    
                                    
# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table 
                            (start_time, user_id, level,
                            song_id, artist_id, session_id,
                            location, user_agent) 
                            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
                                se.user_id, 
                                se.level, 
                                ss.song_id, 
                                ss.artist_id, 
                                se.session_id, 
                                se.location, 
                                se.user_agent 
                            FROM staging_events AS se,
                                 staging_songs AS ss 
                            WHERE se.song_title = ss.title
                              AND se.page = 'NextSong'
                              AND se.artist = ss.artist_name
                              AND se.length = ss.duration
;
                        """)


                      
user_table_insert = ("""INSERT INTO user_table 
                        (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT se.user_id, 
                                se.first_name,
                                se.last_name,
                                se.gender,
                                se.level 
                        FROM staging_events AS se         
                        WHERE user_id IS NOT NULL
                    """)

song_table_insert = ("""INSERT INTO song_table 
                        (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT ss.song_id, 
                                ss.title,
                                ss.artist_id,
                                ss.year,
                                ss.duration
                        FROM staging_songs AS ss
                        
                        """)

artist_table_insert = ("""INSERT INTO artist_table 
                            (artist_id, name, location, latitude, longitude) 
                            SELECT DISTINCT ss.artist_id,
                                    ss.artist_name,
                                    ss.artist_location,
                                    ss.artist_latitude,
                                    ss.artist_longitude
                            FROM staging_songs AS ss
         
                        """)

time_table_insert = ("""INSERT INTO time_table 
                        (start_time, hour, day, week, month, year, weekday)
                        SELECT start_time, 
                            EXTRACT(hr FROM start_time),
                            EXTRACT(d from start_time),
                            EXTRACT(w from start_time),
                            EXTRACT(mon from start_time),
                            EXTRACT(yr from start_time),
                            EXTRACT(weekday from start_time)
                        FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
                                FROM staging_events)
                                
                    """)

                      
                     
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


# Queries
select_count_staging_events = ("""SELECT COUNT(*) FROM staging_events""")
select_count_staging_songs = ("""SELECT COUNT(*) FROM staging_songs""")
select_count_songplays = ("""SELECT COUNT(*) FROM songplay_table""")
select_count_users = ("""SELECT COUNT(*) FROM user_table""")
select_count_songs = ("""SELECT COUNT(*) FROM song_table""")
select_count_artists = ("""SELECT COUNT(*) FROM artist_table""")
select_count_times = ("""SELECT COUNT(*) FROM time_table""")


# COUNTS IN TABLES
select_count_queries = [select_count_staging_events, select_count_staging_songs, select_count_songplays,
                       select_count_users, select_count_songs, select_count_artists, select_count_times]