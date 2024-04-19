

import utils.manage_conf as conf

cre = conf.get_cred(fileconf='../config.ini',section='dsc_filters')

filters = cre['filter_name'].split(',')
for index, f in enumerate(filters):
    prefs = cre['pre_file'].split(',')
    ls_st = cre['list_stations'].split(',')
    print("index", index, 'valor', f, "... ", prefs[index], "... ", ls_st[index])

print(cre)