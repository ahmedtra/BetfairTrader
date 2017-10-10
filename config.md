[files]
root = C:\BetfairArb\
certif = certificate
histdata = HistData
plot = plot
histdataEvent = histdataEvent 

[auth]
username = ahmed.trablsi@gmail.com  
pass = trappatoni123
appkey = 3o3etcx6sszwkBhG
certif = betfair.pem

[logging] 
log_format = %%(asctime)s - %%(name)-40s:%%(lineno)-3d (%%(thread)d) - %%(levelname)-7s: %%(message)s 
log_datefmt = %%Y-%%m-%%d %%H:%%M:%%S 
log_lvl = DEBUG

[cassandra]
hostname=127.0.0.1 
username=cassandra 
password=cassandra 
keyspace=betfair

[mysql]
hostname = localhost
port = 3306
username = root
password = root
db = betfairdb

[mysql-betfair]
hostname = localhost
port = 3306
username = root
password = root
db = betfairdb


[json_files]
root_path = C:\Betfair\decompressed_data
completed = C:\Betfair\completed_data