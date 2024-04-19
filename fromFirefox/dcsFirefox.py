"""
version: 1.0, date: 2023-05-20
Class DcsFirefoxReport
This script make a webscraping to get DCS meteorological data from Ecuador
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""
import datetime
# from bs4 import BeatifulSoup
## apagar los logs del webdrivermanager
import logging
import os
import time
#leer un archivo de configuracion
from configparser import ConfigParser
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
# parametrizar el web driver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as exp
# controlar el tiempo de aparicion de lso elementos
from selenium.webdriver.support.ui import WebDriverWait
# para instalar automaticamente el webDriver
from webdriver_manager.firefox import GeckoDriverManager


class DcsFirefoxReport():

    def __int__(self, path_down):
        self.path_down = path_down

    def getcred(selc, fileconf='./config.ini', section='dcsweb'):
        config = ConfigParser()
        config.read(fileconf)
        conf = {}
        if config.has_section(section):
            params = config.items(section)
            for param in params:
                conf[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, fileconf))
        return conf

    ### verifica si el web darvier es compatible con el navegador instalado
    def checkDriver(self):
        #ruta = GeckoDriverManager(path='../firefoxdiver').install()
        ruta = GeckoDriverManager().install()
        #os.environ['WDM_LOG'] = str(logging.NOTSET)
         # log_level = 0 # no muestra salidas en el termina
        #print(ruta)
        return ruta

    def getDia(self):
        # por ejemplo, si quieres el día de hoy
        dt = datetime.datetime.now()
        # simplemente con el método `strftime`
        return dt.strftime("%y%j")

    def optsDriver(self):
        optionsWD = Options() ## instacnia de la clase Options del Webdriver
        user_aget = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"


        return optionsWD

    def initFirefoxDriver(self):
        #instancia el servicio del webdriver
        service = Service(self.checkDriver())
        #os.environ['WDM_LOCAL'] = '1'
        #os.environ['WDM_SSL_VERIFY'] = '0'
        #os.environ['WDM_LOG'] = str(logging.NOTSET)
        driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()))
        return driver

    def login(self, driver):
        cred = self.getcred()
        try:
            element = driver.find_element(By.ID,"UserName")
            #print("username",element.text)
            element.send_keys(cred['username'])
            element = driver.find_element(By.ID,"Password")
            #print("username", element.text)
            element.send_keys(cred['password'])
            # <button class="btn btn-primary btn-login" type="submit" tabindex="3">SIGN IN</button>
            element = driver.find_element(By.CLASS_NAME, "btn-login")
            #print("voy a entrar")
            element.click()
            return 200
        except TimeoutException:
            print("Error to loggin")
            return 500

    def setExportSize(self,driver,size_val=200):
        try:
            print("set export size from export")
            # Cambia el tamaño de registros a exportar a 200
            exp_zise = driver.find_element(By.ID, "export-size")
            action = ActionChains(driver)
            action.double_click(on_element=exp_zise).perform()
            exp_zise.send_keys(size_val)
            exp_zise.send_keys(Keys.ENTER)
            return 200
        except TimeoutException:
            print("Error to set export size")
            return 500

    # wait to data files download is complited
    # limit = limit time to wait for download is completed by default is 5 seconds
    def verifiDownload(self, limit, pref_file):
        name_of_file = self.path_down + "MessagesExport.xlsx"
        for i in range(0, limit):
            if os.path.exists(name_of_file):
                # set name to downloading file by data + date on  data_yyyy_mm_dd_hh_mm.xlsx
                act_d = datetime.datetime.now()
                new_name = self.path_down + pref_file + act_d.strftime("%Y_%m_%d_%H_%M")+".xlsx"
                os.rename(name_of_file,new_name)
                print("descarga Completa")
                res = new_name
                break
            else:
                #print("algo paso y no se descargo el archivo ",i)
                res = 500
                time.sleep(1)
        return res

    def selectFilter(self, driver, wait, filterName,pref_file):
        #print("Filter name ",filterName)
        try:
            list_filt = driver.find_element(By.ID, "view-list").find_elements(By.TAG_NAME, 'option')
            for el in list_filt:
                # print(el.text, "comapara con ", filterName)
                if (el.text.find(filterName) > -1):
                    # print("filtro selecionado ",el.text)
                    el.click()
            print("En el try")
            time.sleep(5)
            #exp_btn = wait.until(exp.element_to_be_clickable((By.ID, "export-data")))
            exp_btn = driver.find_element(By.ID,"export-data")
            exp_btn.click()
            return self.verifiDownload(40,pref_file)

        except TimeoutException:
            print("Error en el filtro")
            return 500
        #time.sleep(60)


    def scraping(self, dcs_url):

        driver = self.initFirefoxDriver()
        wait = WebDriverWait(driver, 10) #espera hasta que el elemento este disponible
        url = dcs_url
        driver.get(url)
        try:
            wait.until(exp.visibility_of_element_located((By.ID, "UserName")))
            if (self.login(driver) == 500):
                #@TODO implements send mail function to notifify error login
                print("error de login see credentials")
            else:
                welcome = wait.until(exp.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/button')))
                if (welcome.text.find("WELCOME") > -1):
                    self.setExportSize(driver, 300)
                    # Descarga los datos para el filtro de estaciones hidrologicas
                    res = self.selectFilter(driver,wait, "[NETLIST] HIDROALERTA [1HOUR]","hidro")
                    #@TODO aqui se debe agregar los otros filtras a descargar por ejemplo para las meteo
                    #res = self.selectFilter(driver, wait, "[NETLIST]  METEONORMAL [1HOUR]")
            return res

        except TimeoutException:
            # @TODO implements send mail function to notifify page no found
            print("Page not found")
            return 500
        ##### this secction try to get data base on netlist previewsly configuring

        ### guardamos las cokies

        driver.quit()

#
# if __name__ == '__main__':
#     dcs = DcsChromeReport()
#     dcs.path_down = "/home/darwin/Documentos/" ### ruta de descarga para los archivos
#     print(dcs.getDia())
#     file2process = dcs.scraping("https://dcs1.noaa.gov/Messages/List")
