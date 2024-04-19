# Introduction.
This applications use selenium to extrac data from DCS platform by NOAA, download excel file base on filter made from netlist, extrac data by each estation an seve it into database
# requirements
You need to save the __config.ini__ file in the root directory of the application, the content of this file is like the following example, this format needs the section name in square brackets and key=value pair, as many as needed

```
[dcsweb]
main_url=https://dcs1.noaa.gov/Messages/List
username=dcstool-user-rname
password=password

# The new filters that must be configured in the DCS platform are written here
# add the filter name registered in DCS platform comma separates,
# add prefix to the name file for each filter, and add a list stations in nl format for each filter
[dsc_filters]
filter_name = [NETLIST] HIDROALERTA [1HOUR], [NETLIST] METEO [1HOUR]
pre_file = hidro, meteo
list_stations = HidroAlertas.nl, METEOS.nl

# test to load config
[test] 
key_1 = key1
key_2 = key2

# postgres Database configuration to extrac metadata
[postgresql]
host = host
database = database
user = user
password = password

##### this section save data form hourly data collection in this section save data
[mongodb]
host = host
database = database
user = user
password = password
collection = mongocollection # mongo collection to save data data1hidro for test


[days_chpass]
# day when password was change
date = 06-06-2023
# days passed from the las change
count_day = 0
# max day to change the password
max_day_chpass = 25

# path were data in excel format is save
[path_down] 
path = /home/darwin/Documentos/DCS_files/


```

The .nl files has a list of station use to make filters

- **NESDIS** adress from dcs platform
- **ID_INAMHI_DB** id from the station in INAMHI database
- **NOMBRE** Name of station (human-readable)
- **GRUPO** group from var_group file where params order are listen
- **HORA_UTC** the number of utc hour from Ecuator -5
- **TYPE_MSG** type of massage existing actually there is 0 from clear text message separate by colon, 1 for pseudo binary codificatinng message, 2 for two message in one the actual hour an de last hour
- **QC_D** if var_group file has "Calidad del dato" 1 has field, 0 if not