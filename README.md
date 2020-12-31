# Sparkify DataWareHouse


### Purpose

The purpose of this task is to develop  the datawarehouse   for the 
music streaming app Spartify in the cloud using AWS RedShift


### Task

The Task involves moving data available in AWS S3 buckets as json file into AWS Redshift
tables using python and boto3.Below are the tables created for the datawarehouse


- Staging Tables
 1. dwh_stage_event_tbl  -  Hold data related events on the app
 2. dwh_stage_songs_tbl  - Hold data related to songs

- Fact Table
 1. dwh_songplay_tbl - contains every songs and associated artists 

- Dimension tables

1. dwh_artists_tbl - contains unique artirts.
2. dwh_users_tbl - contains unique users.
3. dwh_songs_tbl - contains unique songs.
4. dwh_time_tbl - contains start time broken down into more details.

Reason for hosting datawarehouse in AWS is that  we were more focussed on the analytical side of the data without 
investing any effort in setting up the cluster for the datawarehouse.






