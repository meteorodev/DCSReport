"""
version: 1.0, date: 14/11/24
Class ProcesHistFile.
this class use decoder class to proces excel file downloaded from DCS platform, and get all data from this file,
the excel file should by contain just data from one nesdis addresses
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2024 <www.inamhi.gob.ec>. all rights reserved.
"""
import os
from  utils.decoder_met import Msg_Met_Decoder
import utils.manage_conf as conf
import pandas as pd
import datetime

class ProcesHistFile():

    def __init__(self, pathhistfiles):
        self.pathhistfiles = pathhistfiles

    def get_config_sta(self, id_group):
        """ This function load from disk var_groupX.csv
            Args:
                id_group (int): group identify read from .nl file
            Raises:
                : No Raises implement
            Returns:
                data_frame or None: A Datafreme whit file information or None if file not exist
        """
        group = "../var_grupo" + str(id_group) + ".csv"
        if os.path.exists(group):
            var_order = pd.read_csv(group, sep=";")
            # print(var_order)
            return var_order
        else:
            return None

    def readExcel(self,nesdis, group_file, id_estacion, estacion):
        #path_from_historics
        file_his=conf.get_cred(fileconf="../config.ini",section="path_from_historics")
        print(file_his)
        file_his = file_his['pathhistfiles']+nesdis+".xlsx"
        print(file_his)
        cd = Msg_Met_Decoder()

        if group_file is not None:
            messages = pd.read_excel(file_his, engine="openpyxl")
            messages = messages[['ADDRESS','CAR TIME','MSG-DATA-EXPORT']]
            for idx, msg in enumerate(messages['MSG-DATA-EXPORT']):
                print(msg)
                dtxt = messages['CAR TIME'][idx]
                ftd = datetime.datetime.strptime(dtxt[:-4], "%m/%d/%Y %H:%M:%S")
                insert = cd.bynary2text(msg, group_file, "3937659C", id_estacion, estacion, ftd)
                print(insert)
            print(messages.head(3))
        else:
            print("archivo del grupo no encontrado")


if __name__ == '__main__':
    process_hist = ProcesHistFile("historico")
    group_file = process_hist.get_config_sta(9)
    process_hist.readExcel("393257C6", group_file,65011,'M5190')























