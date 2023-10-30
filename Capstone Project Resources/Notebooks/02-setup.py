# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class SetupHelper():   
    def __init__(self, env):
        Conf = Config()
        self.landing_zone = Conf.base_dir_data + "/raw"
        self.checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints"        
        self.catalog = env
        self.db_name = Conf.db_name
        self.initialized = False
        
    def create_db(self):
        spark.catalog.clearCache()
        print(f"Creating the database {self.catalog}.{self.db_name}...", end='')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        self.initialized = True
        print("Done")
        
    def create_registered_users_bz(self):
        if(self.initialized):
            print(f"Creating registered_users_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.registered_users_bz(
                    user_id long,
                    device_id long, 
                    mac_address string, 
                    registration_timestamp double,
                    load_time timestamp,
                    source_file string                    
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    
    def create_gym_logins_bz(self):
        if(self.initialized):
            print(f"Creating gym_logins_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logins_bz(
                    mac_address string,
                    gym bigint,
                    login double,                      
                    logout double,                    
                    load_time timestamp,
                    source_file string
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_kafka_multiplex_bz(self):
        if(self.initialized):
            print(f"Creating kafka_multiplex_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_multiplex_bz(
                  key string, 
                  value string, 
                  topic string, 
                  partition bigint, 
                  offset bigint, 
                  timestamp bigint,                  
                  date date, 
                  week_part string,                  
                  load_time timestamp,
                  source_file string)
                  PARTITIONED BY (topic, week_part)
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")       
    
            
    def create_users(self):
        if(self.initialized):
            print(f"Creating users table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                    user_id bigint, 
                    device_id bigint, 
                    mac_address string,
                    registration_timestamp timestamp
                    )
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")            
    
    def create_gym_logs(self):
        if(self.initialized):
            print(f"Creating gym_logs table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                    mac_address string,
                    gym bigint,
                    login timestamp,                      
                    logout timestamp
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_user_profile(self):
        if(self.initialized):
            print(f"Creating user_profile table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                    user_id bigint, 
                    dob DATE, 
                    sex STRING, 
                    gender STRING, 
                    first_name STRING, 
                    last_name STRING, 
                    street_address STRING, 
                    city STRING, 
                    state STRING, 
                    zip INT, 
                    updated TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_heart_rate(self):
        if(self.initialized):
            print(f"Creating heart_rate table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                    device_id LONG, 
                    time TIMESTAMP, 
                    heartrate DOUBLE, 
                    valid BOOLEAN)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

            
    def create_user_bins(self):
        if(self.initialized):
            print(f"Creating user_bins table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workouts(self):
        if(self.initialized):
            print(f"Creating workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                    user_id INT, 
                    workout_id INT, 
                    time TIMESTAMP, 
                    action STRING, 
                    session_id INT)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_completed_workouts(self):
        if(self.initialized):
            print(f"Creating completed_workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT, 
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workout_bpm(self):
        if(self.initialized):
            print(f"Creating workout_bpm table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT,
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP,
                    time TIMESTAMP, 
                    heartrate DOUBLE)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_date_lookup(self):
        if(self.initialized):
            print(f"Creating date_lookup table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.date_lookup(
                    date date, 
                    week int, 
                    year int, 
                    month int, 
                    dayofweek int, 
                    dayofmonth int, 
                    dayofyear int, 
                    week_part string)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_workout_bpm_summary(self):
        if(self.initialized):
            print(f"Creating workout_bpm_summary table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                    workout_id INT, 
                    session_id INT, 
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING, 
                    min_bpm DOUBLE, 
                    avg_bpm DOUBLE, 
                    max_bpm DOUBLE, 
                    num_recordings BIGINT)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_gym_summary(self):
        if(self.initialized):
            print(f"Creating gym_summar gold view...", end='')
            spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
                            SELECT to_date(login::timestamp) date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id
                        """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_db()       
        self.create_registered_users_bz()
        self.create_gym_logins_bz() 
        self.create_kafka_multiplex_bz()        
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_date_lookup()
        self.create_workout_bpm_summary()  
        self.create_gym_summary()
        print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES IN {self.catalog}.{self.db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{self.db_name}: Success")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        self.assert_table("registered_users_bz")   
        self.assert_table("gym_logins_bz")        
        self.assert_table("kafka_multiplex_bz")
        self.assert_table("users")
        self.assert_table("gym_logs")
        self.assert_table("user_profile")
        self.assert_table("heart_rate")
        self.assert_table("workouts")
        self.assert_table("completed_workouts")
        self.assert_table("workout_bpm")
        self.assert_table("user_bins")
        self.assert_table("date_lookup")
        self.assert_table("workout_bpm_summary") 
        self.assert_table("gym_summary") 
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self): 
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")
        print(f"Deleting {self.landing_zone}...", end='')
        dbutils.fs.rm(self.landing_zone, True)
        print("Done")
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)
        print("Done")    
