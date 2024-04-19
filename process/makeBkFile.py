# this class meake file that will be use like a backup from data getting from
import datetime
import os

"""
version: 1.0, date: 2023-05-20
Class MakeBackupFile
This class make file that will be use like a backup from data getting from
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""
class MakeBackupFile(object):
    '''
    This classs make backup file to store like a bk
    '''

    def add_line(self, path, cod_inamhi, nesdis, fecha):
        """ revisa que exista el archivo de respaldo para la estacion y el día de datos, de no ser asi crea una archivo para cada dia
        Args:
            path (str): ruta de creacion del archivo
            cod_inamhi (str): codigo de estacion formato INAMHI
            nesdis (str): codigo nesdis
            fecha (date): fecha de toma del dato

        Returns:
            :
        """
        dts = datetime.datetime.strftime(fecha,"%Y%m%d")
        abs_path = path + "" + str(cod_inamhi)+"_"+nesdis+"_F10_STGO_"+dts+".csv"
        print(abs_path)
        if not os.path.exists(abs_path):
            print("no existe el archivo hay que crearlo y agregarle la cabecera ")


    def add_line(self, path):
        pass

    def send_file(self):
        pass