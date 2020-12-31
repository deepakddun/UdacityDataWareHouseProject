import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS DWH_STAGE_EVENT_TBL;"
staging_songs_table_drop = "DROP TABLE IF EXISTS DWH_STAGE_SONGS_TBL;"
songplay_table_drop = "DROP TABLE IF EXISTS DWH_SONGPLAY_TBL;"
user_table_drop = "DROP TABLE IF EXISTS  DWH_USERS_TBL;"
song_table_drop = "DROP TABLE IF EXISTS DWH_SONG_TBL;"
artist_table_drop = "DROP TABLE IF EXISTS DWH_ARTIST_TBL;"
time_table_drop = "DROP TABLE IF EXISTS  DWH_TIME_TBL;"

# CREATE TABLES

staging_events_table_create = ("""

CREATE TABLE IF NOT EXISTS DWH_STAGE_EVENT_TBL
(
    event_pk bigint IDENTITY(1,1) PRIMARY KEY,
    artist text,
    auth text,
    firstName text,
    gender char(1),
    iteminSession smallint,
    lastName text,
    length numeric,
    level text,
    location text,
    method text,
    page text,
    registration numeric,
    sessionId smallint,
    song text,
    status smallint,
    ts numeric,
    userAgent text,
    userId integer
);

""")

staging_songs_table_create = ("""

CREATE TABLE IF NOT EXISTS DWH_STAGE_SONGS_TBL
(
    song_stage_pk bigint IDENTITY(1,1) PRIMARY KEY,
    num_songs smallint,
    artist_id text,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar(max),
    artist_name text,
    song_id text,
    title text,
    duration numeric,
    year smallint
);

""")

songplay_table_create = ("""

CREATE TABLE IF NOT EXISTS dwh_songplay_tbl
(
    songplay_pk bigint IDENTITY(1,1) PRIMARY KEY,
    start_time timestamp,
    user_id integer,
    level text,
    song_id text,
    artist_id text,
    session_id text,
    location varchar(max),
    user_agent text
);

""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS dwh_users_tbl
(
    user_pk bigint IDENTITY(1,1) PRIMARY KEY,
    user_id integer,
    first_name text,
    last_name text,
    gender char(1),
    level text
);

""")

song_table_create = ("""

CREATE TABLE IF NOT EXISTS dwh_songs_tbl
(
    songs_pk bigint IDENTITY(1,1) PRIMARY KEY,
    song_id text,
    title text,
    artist_id text,
    year smallint,
    duration numeric
);

""")

artist_table_create = ("""

CREATE TABLE IF NOT EXISTS dwh_artists_tbl
(
    artist_pk bigint IDENTITY(1,1) PRIMARY KEY,
    artist_id text,
    name text,
    location varchar(max),
    lattitude numeric,
    longitude numeric
);

""")

time_table_create = ("""

CREATE TABLE IF NOT EXISTS dwh_time_tbl
(
    time_pk bigint IDENTITY(1,1) PRIMARY KEY,
    start_time timestamp ,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday boolean
);

""")

# IAM role has been removed
role_arn = config.get('IAM_ROLE', 'ARN')

# STAGING TABLES
s3_song_url = config.get('S3', 'SONG_DATA')
staging_events_copy = f"copy dwh_stage_songs_tbl from '{s3_song_url}' credentials 'aws_iam_role={role_arn}' json 'auto'"

s3_log_path = config.get('S3', 'LOG_DATA')
s3_log_json_path = config.get('S3', 'LOG_JSONPATH')
staging_songs_copy = f"copy DWH_STAGE_EVENT_TBL from '{s3_log_path}' credentials 'aws_iam_role={role_arn}' json '{s3_log_json_path}'"

# FINAL TABLES

songplay_table_insert = ("""

    INSERT INTO dwh_songplay_tbl(start_time , user_id ,level , song_id, artist_id, session_id, location, user_agent)	
    SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL  '1 second' as Start_time , dset.userid  ,dset.level,
    dst.song_id , dat.artist_id , dset.sessionid , dset.location , dset.useragent 
    FROM dwh_stage_event_tbl dset , dwh_songs_tbl dst , dwh_artists_tbl dat 
    WHERE dst.artist_id = dat.artist_id and dset.artist LIKE '%'||dat.name||'%' and dset.song LIKE '%'||dst.title||'%'
    AND page = 'NextSong';
    
""")

user_table_insert = ("""

          INSERT INTO dwh_users_tbl(user_id,first_name,last_name , gender , level)
        SELECT ds.userid , ds.firstname , ds.lastname , ds.gender , ds.level FROM
        (
	        SELECT userid uid, max(ts) max_time FROM DWH_STAGE_EVENT_TBL
	        where page = 'NextSong'
	        GROUP BY userid order by userid
        ) tt , DWH_STAGE_EVENT_TBL  ds
        WHERE tt.uid = ds.userid  and tt.max_time = ds.ts  order by tt.uid;
    
""")

song_table_insert = ("""

            INSERT INTO dwh_songs_tbl(song_id, title, artist_id,year,duration)
            select DISTINCT song_id , title , artist_id , year , duration from dwh_stage_songs_tbl
            order by song_id , artist_id ;

""")

artist_table_insert = ("""
    
          INSERT INTO dwh_artists_tbl (artist_id,name , location , lattitude , longitude)
          SELECT artist_id , artist_name , artist_location  , artist_latitude  , artist_longitude FROM 
          (
                SELECT artist_id , artist_name , artist_location  , artist_latitude , artist_longitude , ROW_NUMBER()
                OVER (PARTITION BY artist_id) as num
                FROM dwh_stage_songs_tbl
          ) 
          temp_table WHERE num = 1;  

""")

time_table_insert = ("""

            INSERT INTO dwh_time_tbl (start_time , hour , day , week , month , year , weekday)
            SELECT converted_time , extract(hour from converted_time) as hour ,  extract(day from converted_time) as day,
            extract(week from converted_time)as week , extract(month from converted_time) as month , extract(year from converted_time) as year,
            case extract(dow from converted_time)
            WHEN 0 Then FALSE
            WHEN 6 Then FALSE
            ELSE TRUE
            END as  isdayofweek
            from (
            SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL  '1 second'  as converted_time FROM (
            select DISTINCT ts  from dwh_stage_event_tbl
        ));

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert,songplay_table_insert]
