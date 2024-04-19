"""
version: 1.0, date: 2023-05-20
Class ManageDCS
This class implements functions that allow to manage DCS platform
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""
import datetime
import os.path
import time

from fromChrome.dcsChrome import DcsChromeReport, conf, WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as exp
from selenium.webdriver.common.by import By

from utils.make_sec_pass import PassGen
class ManageDCS:

    def check_day(self, url):
        """
            Args:
                 ():
            Raises:
                :
            Returns:
                :
        """
        #con = DcsChromeReport()
        print(os.path.curdir)
        change_dcs_pass = conf.get_cred(section="chpass_dcs")
        days2change =  int(change_dcs_pass['max_day_chpass'])
        date_ch = datetime.datetime.strptime(change_dcs_pass['date'],"%d-%m-%Y")
        dif_time = datetime.datetime.now() - date_ch

        if dif_time.days > days2change:
            print("dif changes start to scraping")
            self.scrapping_change_pass(url)


        return " se ha cambiado la contrseña"

    def scrapping_change_pass(self,url):
        """ This function changes password in the dcs platforms using scraping over the web
            Args:
                 ():
            Raises:
                :
            Returns:
                :
        """
        dcs_scrap = DcsChromeReport()
        drr = dcs_scrap.init_chrome_driver()
        drr.get(url)
        log_ok = dcs_scrap.login(drr)
        wait = WebDriverWait(drr, 10)
        try:
            #wait.until(exp.visibility_of_element_located((By.ID, "UserName")))

            welcome = wait.until(
                    exp.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/button')))
            if welcome.text.find("WELCOME") > -1:
                welcome.click()
                macc = drr.find_element(By.XPATH, "/html/body/div[2]/div[2]/div[1]/ul/li[1]/a")
                macc.click()
                sec_section = wait.until(
                        exp.visibility_of_element_located((By.XPATH,
                        "/html/body/div[3]/div[2]/div/div[2]/form/div/ul/li[2]/a"))
                    )
                sec_section.click()
                    ## here gen de new password
                ps = PassGen()
                n_pass = ps.make_ran_pass()
                lb_pass = drr.find_element(
                        By.XPATH, "/html/body/div[3]/div[2]/div/div[2]/form/div/div/div[3]/div/div/div[2]/div/input")
                lb_pass.click()
                lb_pass.send_keys(n_pass)
                lb_pass_rep = drr.find_element(
                        By.XPATH, "/html/body/div[3]/div[2]/div/div[2]/form/div/div/div[3]/div/div/div[3]/div/input")
                lb_pass_rep.click()
                lb_pass_rep.send_keys(n_pass)
                    ## save button
                save_btn = drr.find_element(By.XPATH,"/html/body/div[3]/div[2]/div/div[1]/button[1]")
                    #save_btn = drr.find_element(By.XPATH,"/html/body/div[3]/div[2]/div/div[1]/button[2]")
                save_btn.click()
                    #### here save new pass in config file
                conf.set_cred(section="dcsweb", key="password", value=n_pass)
                cu_da = datetime.datetime.now()
                conf.set_cred(section="chpass_dcs", key="date", value=cu_da.strftime('%d-%m-%Y'))
                    # here logout from dcs
                time.sleep(10)
                welcome = wait.until(
                        exp.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[1]/button')))
                if welcome.text.find("WELCOME") > -1:
                    welcome.click()
                    logout_btn = wait.until(exp.visibility_of_element_located((By.XPATH,
                                                                            "/html/body/div[2]/div[2]/div[1]/ul/li[3]/a")))

                    if(logout_btn.text.find("LOGOUT") > -1):
                        logout_btn.click()


            return 200

        except TimeoutException:
            print("Error no se cambio la contraseña")
            return None



    def test_password(self, url):
        dcs_scrap = DcsChromeReport()
        drr = dcs_scrap.init_chrome_driver()
        drr.get(url)
        return dcs_scrap.login(drr)


if __name__ == '__main__':

    md=ManageDCS()
    md.check_day()