"""
version: 1.0, date: 2023-05-20
Class DcsChromeReport
Class for get data from DCS base on selenium.
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""
import datetime
## apagar los logs del webdrivermanager
import logging
import os
import time

# from turtle import mode

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
# parametrizar el web driver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as exp
# controlar el tiempo de aparicion de lso elementos
from selenium.webdriver.support.ui import WebDriverWait
# para instalar automaticamente el webDriver
#from webdriver_manager.chrome import ChromeDriverManager
import utils.manage_conf as conf
from utils.send_notification import Sender


class DcsChromeReport:

    path_down = None
    def __init__(self):
        cre = conf.get_cred(section="path_down")
        if self.path_down is None:
            self.path_down = cre['path']

    @staticmethod
    def check_driver():
        """ This functions check if de chrome driver exist and if it's the actual version
        Args:
            no args ():
        Raises:
            RuntimeError: no errors
        Returns:
            dpath: the path where chrome driver is downloaded
        """
        #dpath = ChromeDriverManager(path='./chromediver').install()
        os.environ['WDM_LOG'] = str(logging.NOTSET)

        #return dpath

    @staticmethod
    def get_dia():
        """ This functions return de count day from the actual date
        Args:
            no args ():
        Raises:
            RuntimeError: no errors
        Returns:
            string: day count format
        """
        dt = datetime.datetime.now()
        return dt.strftime("%y%j")

    @staticmethod
    def opts_driver(path_down):
        """ This function set chrome drivers parameters, to get data from dcs without detect like a boot
        Args: 
            mo args (): 
        Raises:
            RuntimeError: no errors
        Returns:
            optionsWD: object with the chrome driver object
        """
        options_wd = Options()  ## instacnia de la clase Options del Webdriver
        user_aget = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"
        options_wd.add_argument(f"user-agent={user_aget}")  # f"" formato texto
        options_wd.add_argument("--windows-size=800,600")
        options_wd.add_argument("start-maximized") # tamaño del la ventana del navegador
        options_wd.add_argument("--disable-extensions")
        options_wd.add_argument("--disable-web-security")  # desabilita CRS coors origin Same origin police
        options_wd.add_argument("--disable-notifications")  #
        options_wd.add_argument("--ignore-certificate-errors")  # desabilita el mensaje de su conexion no es privada
        options_wd.add_argument("--no-sandbox")
        #options_wd.add_argument("--log-level=3") #no muestra mensaejas en la consola
        options_wd.add_argument("--allow-running-insecure-content")  # desactiva el aviso de contenido no seguro
        options_wd.add_argument("--no-defaul-browser-check")  # evita comprobar si es el navegador por defecto
        options_wd.add_argument("--no-first-run")  # evita ejecutar tareas de cron la primera ves
        options_wd.add_argument("--no-proxy-server")
        options_wd.add_argument("--disable-blink-features=AutomationControlled")  # evita que selenium sea detectado
        ###### parametros de prueba
        #options_wd.add_argument("--headless")  # silence mode from chrome
        ## parametros a omitir al inicio del webdriver
        exp_opt = [
            'enable-automation',
            # no muestra el mensaje de una software automatizado esta controlando chrome... en el navegador
            'ignore-certificate-errors',  # ignora mensajes de certificados ssl
            # 'enable-logging'# no muestra en el terminal el mensaje devTools listening on..
        ]

        options_wd.add_experimental_option("excludeSwitches", exp_opt)
        # crea el directorio de descarga de los archivo si no existe
        os.makedirs(path_down, exist_ok=True)
        # paramtros de preferencias de chromedriver
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            # notificaciones: 0 preguntar | 1 permitir | 2 no permitir
            "intl.accept_languages": ["es-ES", "es"],  # Idioma del navegador
            "credentials_enable_service": False,  # evitar qeu chroem pregunte si desea guardas credenciales del login
            "download.default_directory": path_down,  # cambia la ruta de descargo por defecto
        }
        options_wd.add_experimental_option("prefs", prefs)
        # print(optionsWD)
        return options_wd

    def init_chrome_driver(self):
        """ This function inicailize chrome driver objects
        Args:
            no params :
        Raises:
            RuntimeError: no returns errors
        Returns:
            driver: chrome driver objects tto use selenium functions
        """

        #service = Service(self.check_driver())
        #this line make path to chromedriver web
        chromedrive_path = conf.get_cred(section="chromedriver")
        #print(chromedrive_path['pth'])
        service = Service(executable_path=chromedrive_path['pth'])
        # instancia el webDriver
        #print(self.path_down)
        driver = webdriver.Chrome(service=service, options=self.opts_driver(self.path_down))
        #driver = webdriver.Chrome(executable_path='/home/darwin/Documentos/PythonProyects/DCSReport/chromediver/.wdm/drivers/chromedriver/linux64/119.0.6045.105/chromediver/')
        return driver

    def login(self, driver):
        """ This functions logon into DCS platform to get geos data
        Args: 
            driver (chrome driver): chrome driver inicialize in check Driver
        Raises:
            RuntimeError: TimeoutException
        Returns:
            200 : if all it's OK
            500 : if sometimes was wrong
        """
        cred = conf.get_cred()
        try:
            element = driver.find_element(By.ID, "UserName")
            element.send_keys(cred['username'])
            element = driver.find_element(By.ID, "Password")
            element.send_keys(cred['password'])
            element = driver.find_element(By.CLASS_NAME, "btn-login")
            # print("voy a entrar")
            element.click()
            wait = WebDriverWait(driver, 10)
            welcome = wait.until(
                exp.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/button')))
            if welcome.text.find("WELCOME") > -1:
                return 200 # login is correct!
            else:
                notify = Sender()
                mail_cre = conf.get_cred(section='email_noti_conf')
                message2send = conf.get_cred(section='email_noti_log_mess')
                notify.mail_notifier(mail_cre['smtp_server'], mail_cre['sender'], mail_cre['password'],
                                     mail_cre['recipies'], message2send['subject'], message2send['message'])
                return 400 ## error in the page
        except TimeoutException:
            notify = Sender()
            mail_cre = conf.get_cred(section='email_noti_conf')
            message2send = conf.get_cred(section='email_noti_log_mess')
            notify.mail_notifier(mail_cre['smtp_server'], mail_cre['sender'], mail_cre['password'],
                                 mail_cre['recipies'], message2send['subject'], message2send['message'])
            print("Error to loggin return 500")

            return 500

    @staticmethod
    def set_export_size(driver, size_val=300):
        """ This function set de export size in dcs platform to download file
        Args: 
            driver (chrome driver): chrome driver to manage webroweser to download file from dcs platform
            size_val (int): value to describe number of rows to be download
        Raises:
            RuntimeError : TimeoutException when dcs no response
        Returns:
            200 : if all it's OK
            500 : if sometimes was wrong 
        """
        try:
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
    def verifi_download(self, limit, pref_file):
        """ Verify if the file was download correctly and return name of this file
        Args: 
             limit (int): time in seconds that wait download
             pref_file (str): prefs of the name of file,
        Returns:
            new_name (str) : name of file with a full path
        """
        name_of_file = self.path_down + "MessagesExport.xlsx"
        res = None
        for i in range(0, limit):
            if os.path.exists(name_of_file):
                # set name to downloading file by data + date on  data_yyyy_mm_dd_hh_mm.xlsx
                act_d = datetime.datetime.now()
                new_name = self.path_down + pref_file + act_d.strftime("%Y_%m_%d_%H_%M") + ".xlsx"
                os.rename(name_of_file, new_name)
                print("Descarga Completa ", new_name)
                res = new_name
                break
            else:
                # @TODO notify when the file was not download
                # print("algo paso y no se descargo el archivo ",i)
                res = 500
                time.sleep(1)
        return res

    def select_filter(self, driver, twait, filter_name, pref_file):
        """ On dcs website select a filter configured in the platform with files.nl and click over export button
        Args: 
            driver (selenium driver): Driver for managed browser
            twait (int): time to wait for verify if the file was download
            filter_name (str) : the nome of filter to find into to dcs platform
            pref_file (str) : prefs form de file's name
        Raises:
            RuntimeError: timeout exception
        Returns:
           name of file or none
        """
        try:
            list_filt = driver.find_element(By.ID, "view-list").find_elements(By.TAG_NAME, 'option')
            filter_exist = 0
            for el in list_filt:
                #print(el.text, "comapara con ", filter_name)
                if el.text.find(filter_name) > -1:
                    #print("selected filter ",el.text)
                    el.click()
                    filter_exist = 1
            #print("En el try")
            time.sleep(5)
            # exp_btn = wait.until(exp.element_to_be_clickable((By.ID, "export-data")))
            exp_btn = driver.find_element(By.ID, "export-data")
            exp_btn.click()
            return self.verifi_download(twait, pref_file)

        except TimeoutException:
            print("Error no se aplico el filtro")
            return None
        # time.sleep(60)

    def scraping(self, dcs_url, twait=40):
        """ Main function to download file from dcs website used selenium from Google Chrome browser
        Args: 
            dcs_url (str): url from dcs website
            twait (int) : time to wait to verify if file was download by default 40
        Raises:
            RuntimeError: Timeout exception
        Returns:
           res (list) : List with the names of files downloads
        """

        driver = self.init_chrome_driver()
        wait = WebDriverWait(driver, 10)  # espera hasdsc_filtersta que el elemento este disponible
        url = dcs_url
        driver.get(url)
        res = []
        try:
            wait.until(exp.visibility_of_element_located((By.ID, "UserName")))
            log_ok = self.login(driver)
            cre = conf.get_cred(section="dsc_filters")
            welcome = wait.until(
                exp.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/button')))
            if welcome.text.find("WELCOME") > -1:
                self.set_export_size(driver, 300)
                    # The new filters that must be configured in the DCS platform are written in config.ini file
                filters = cre['filter_name'].split(',')
                for index, f in enumerate(filters):
                    prefs = cre['pre_file'].split(',')
                    ls_st = cre['list_stations'].split(',')
                    res.append([self.select_filter(driver, twait, f, prefs[index]),ls_st[index]])
                    print("index", index, 'valor', f, "... ", prefs[index], "... ", ls_st[index])
            return res

        except TimeoutException:
            notify = Sender()
            mail_cre = conf.get_cred(section='email_noti_conf')
            message2send = conf.get_cred(section='email_noti_pg_not_found')
            notify.mail_notifier(mail_cre['smtp_server'], mail_cre['sender'], mail_cre['password'],
                                 mail_cre['recipies'], message2send['subject'], message2send['message'])
            print("Page not found")
            return None
        # driver.quit()

#
# if __name__ == '__main__':
#     dcs = DcsChromeReport()
#     dcs.path_down = "/home/darwin/Documentos/" ### ruta de descarga para los archivos
#     print(dcs.getDia())
#     file2process = dcs.scraping("https://dcs1.noaa.gov/Messages/List")
