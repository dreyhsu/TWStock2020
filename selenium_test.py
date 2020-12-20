from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import random

# Sets up web driver using Google chrome
driver = webdriver.Chrome()
driver.get('    ')
# 疫情專區
btnDis = driver.find_element_by_id("btnDis")
btnDis.click()
# 健康聲明書
decla = driver.find_element_by_id("ImageButton2")
decla.click()
time.sleep(2)
# 無警示簡訊
radio_none1 = driver.find_element_by_id("mes2")
radio_none1.click()
time.sleep(1)
# 無旅遊史
radio_none2 = driver.find_element_by_id("TYPE4")
radio_none2.click()
time.sleep(1)
# save
Btn_Save = driver.find_element_by_id("Btn_Save")
Btn_Save.click()
time.sleep(2)
# 回前頁
driver.back()
# 溫度監控
temp = driver.find_element_by_id("ImageButton1")
temp.click()
# 填兩次體溫
for i in [1,2]:
    inputElement = driver.find_element_by_id("Txt_Temp")
    time.sleep(1)
    inputElement.send_keys(str(round(random.uniform(35.7, 36.8), 1)))
    time.sleep(1)
    inputElement.send_keys(Keys.ENTER)
    time.sleep(1)
    driver.switch_to_alert().accept()
    time.sleep(1)
    driver.switch_to_alert().accept() 
    time.sleep(1)
# 關閉driver
driver.close()



