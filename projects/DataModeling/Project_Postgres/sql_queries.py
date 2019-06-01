# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

# Necessary columns = songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table (songplay_id SERIAL PRIMARY KEY, 
                                                               start_time timestamp without time zone NOT NULL REFERENCES time_table(start_time), 
                                                               user_id int NOT NULL REFERENCES user_table(user_id), 
                                                               level varchar, 
                                                               song_id varchar REFERENCES song_table(song_id), 
                                                               artist_id varchar REFERENCES artist_table(artist_id), 
                                                               session_id int, 
                                                               location varchar, 
                                                               user_agent varchar);""")

# Necessary columns = user_id, first_name, last_name, gender, level
user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table (user_id int PRIMARY KEY NOT NULL, 
                                                             first_name varchar, 
                                                             last_name varchar, 
                                                             gender varchar, 
                                                             level varchar);""")

# Necessary columns = song_id, title, artist_id, year, duration
song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table (song_id varchar PRIMARY KEY NOT NULL, 
                                                            title varchar, 
                                                            artist_id varchar, 
                                                            year int, 
                                                            duration float);""")

# Necessary columns = artist_id, name, location, lattitude, longitude
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table (artist_id varchar PRIMARY KEY NOT NULL, 
                                                                 name varchar, 
                                                                 location varchar, 
                                                                 lattitude float, 
                                                                 longitude float);""")

# Necessary columns = start_time, hour, day, week, month, year, weekday
time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table (start_time timestamp without time zone PRIMARY KEY NOT NULL, 
                                                             hour int, 
                                                             day varchar, 
                                                             week varchar, 
                                                             month int, 
                                                             year int, 
                                                             weekday varchar);""")
# INSERT RECORDS
songplay_table_insert = ("""INSERT INTO songplay_table (songplay_id, start_time, user_id, 
                                                    level, song_id, artist_id, session_id, 
                                                    location, user_agent) 
                                                    VALUES (DEFAULT, %s, %s, %s, %s, 
                                                            %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
                                            VALUES (%s, %s, %s, %s, %s) 
                                            ON CONFLICT (user_id) 
                                            DO UPDATE SET first_name=EXCLUDED.first_name, 
                                                        last_name=EXCLUDED.last_name, 
                                                        level=EXCLUDED.level""")
                                                    

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration) 
                                            VALUES (%s, %s, %s, %s, %s)
                                            ON CONFLICT (song_id)
                                            DO NOTHING""")


artist_table_insert = ("""INSERT INTO artist_table (artist_id, name, location, lattitude, longitude) 
                                            VALUES (%s, %s, %s, %s, %s)
                                            ON CONFLICT (artist_id)
                                            DO NOTHING""")


time_table_insert = (""" INSERT INTO time_table (start_time, hour, day, week, month, year, weekday) 
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (start_time)
                                            DO NOTHING""")


# FIND SONGS

song_select = ("""SELECT s.song_id, a.artist_id, s.duration
                        FROM song_table as s
                        JOIN artist_table as a
                        ON s.artist_id = a.artist_id
                        WHERE s.title = (%s) AND a.name = (%s) AND s.duration = (%s)
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]