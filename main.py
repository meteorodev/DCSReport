"""
version: 1.0, date: 2023-04-24
Class main
main class
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""

import datetime

from fromChrome.dcsChrome import DcsChromeReport
from fromChrome.change_DCS_pass import ManageDCS
from process.processfile import ProcessDownloadFile
import logging
import utils.manage_conf as conf

if __name__ == '__main__':
    timelog = datetime.datetime.now().strftime("%Y_%m_%d %H:%M:%S")
    ### seccion de consulta de datos al dcs
    print("Ejecucion ******* ",timelog," *********")
    ### this section check from password in the platform and change it

    #dcs = DcsFirefoxReport()
    dcs = DcsChromeReport()
    #dcs.path_down = "/home/darwin/Documentos/DCS_files/" ### ruta de descarga para los archivos
    cre = conf.get_cred()
    dcs_url = cre['main_url']

    mdcs = ManageDCS()
    mdcs.check_day(dcs_url)

    file2process = dcs.scraping(dcs_url)

    #file2process = [['/home/darwin/Documentos/DCS_files/meteo2023_11_01_00_18.xlsx', 'METEOS.nl']]
    #file2process = [['/home/darwin/Documentos/DCS_files/hidro2023_11_02_21_29.xlsx', 'HidroAlertas.nl']] #,  #,
    #                ['/home/darwin/Documentos/DCS_files/meteo2023_06_15_13_10.xlsx', 'METEOS.nl'],
    #                ['/home/darwin/Documentos/DCS_files/hidro2023_07_21_14_10.xlsx', 'HidroAlertas.nl']]
    #
    # quitar comentario a las lineas que siguen

    if file2process is None:
        print("no se pudo descargar el archivo ")
    else:
        for i in file2process:
            if i == 500:
                print("no se pudo descargar le archivo para el filtro", i[1])
            else:
                print("fileprocesss : for :",i,"  \n",i[0],"\n"),
                ### seecion de procesamiento de datos descargados en excel
                proc = ProcessDownloadFile()
                #proc.test_mongo_connection()
                print(i[0]," - ", i[1]," - ", dcs.path_down)
                #TODO se debe tambien pasar el archivo NL de configuracion y la ruta
                proc.read_excel_data(i[0], i[1], dcs.path_down)
