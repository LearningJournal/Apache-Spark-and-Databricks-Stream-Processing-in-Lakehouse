# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Producer():
    def __init__(self):
        self.Conf = Config()
        self.landing_zone = self.Conf.base_dir_data + "/raw"      
        self.test_data_dir = self.Conf.base_dir_data + "/test_data"
               
    def user_registration(self, set_num):
        source = f"{self.test_data_dir}/1-registered_users_{set_num}.csv"
        target = f"{self.landing_zone}/registered_users_bz/1-registered_users_{set_num}.csv" 
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def profile_cdc(self, set_num):
        source = f"{self.test_data_dir}/2-user_info_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/2-user_info_{set_num}.json"
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")        
        
    def workout(self, set_num):
        source = f"{self.test_data_dir}/4-workout_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/4-workout_{set_num}.json"
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def bpm(self, set_num):
        source = f"{self.test_data_dir}/3-bpm_{set_num}.json"
        target = f"{self.landing_zone}/kafka_multiplex_bz/3-bpm_{set_num}.json"
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def gym_logins(self, set_num):
        source = f"{self.test_data_dir}/5-gym_logins_{set_num}.csv"
        target = f"{self.landing_zone}/gym_logins_bz/5-gym_logins_{set_num}.csv" 
        print(f"Producing {source}...", end='')
        dbutils.fs.cp(source, target)
        print("Done")
        
    def produce(self, set_num):
        import time
        start = int(time.time())
        print(f"\nProducing test data set {set_num} ...")
        if set_num <=2:
            self.user_registration(set_num)
            self.profile_cdc(set_num)        
            self.workout(set_num)
            self.gym_logins(set_num)
        if set_num <=10:
            self.bpm(set_num)
        print(f"Test data set {set_num} produced in {int(time.time()) - start} seconds")
    
    def _validate_count(self, format, location, expected_count):
        print(f"Validating {location}...", end='')
        target = f"{self.landing_zone}/{location}_*.{format}"
        actual_count = (spark.read
                             .format(format)
                             .option("header","true")
                             .load(target).count())
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {location}"
        print(f"Found {actual_count:,} / Expected {expected_count:,} records: Success")
          
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating test data {sets} sets...")       
        self._validate_count("csv", "registered_users_bz/1-registered_users", 5 if sets == 1 else 10)
        self._validate_count("json","kafka_multiplex_bz/2-user_info", 7 if sets == 1 else 13)
        self._validate_count("json","kafka_multiplex_bz/3-bpm", sets * 253801)
        self._validate_count("json","kafka_multiplex_bz/4-workout", 16 if sets == 1 else 32)  
        self._validate_count("csv", "gym_logins_bz/5-gym_logins", 8 if sets == 1 else 16)
        #print(f"Test data validation completed in {int(time.time()) - start} seconds")
