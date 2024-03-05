import time
from datetime import datetime
import pickle

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

import pandas as pd
import duckdb

from email.message import EmailMessage
from email.utils import make_msgid
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib
from secrets import EMAIL_ADDRESS, EMAIL_PASSWORD, recipients

def browser_config():
	service=Service(ChromeDriverManager().install())
 
	options = webdriver.ChromeOptions()
	#options.add_argument('--headless')
	#options.add_argument('--no-sandbox')
	options.add_argument("--disable-gpu")
	options.add_argument("--enable-javascript")
	options.add_argument('--disable-blink-features=AutomationControlled')
	options.add_experimental_option('excludeSwitches', ['enable-logging'])
	options.add_experimental_option("detach", True)
	#options.add_argument("--incognito")
	ua = UserAgent()
	userAgent = ua.random

	driver = webdriver.Chrome(service=service, options=options)
	driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
	driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": userAgent})
 
	return driver

def get_all_pages():
    htmls = []
    for nb in range(1,5):
        nro_pag = '' if nb == 1 else '-pagina-'+str(nb)	
        url = f"https://www.zonaprop.com.ar/casas-departamentos-ph-alquiler-capital-federal-vicente-lopez-san-isidro-2-ambientes-publicado-hace-menos-de-2-dias-menos-300000-pesos-orden-precio-ascendente{nro_pag}.html"
        #url = f"https://www.zonaprop.com.ar/casas-departamentos-ph-alquiler-capital-federal-vicente-lopez-san-isidro-2-ambientes-publicado-hace-menos-de-1-semana-menos-300000-pesos-orden-precio-ascendente{nro_pag}.html"
        browser = browser_config()
        browser.get(url)
        html_source = browser.page_source
        htmls.append(html_source)
        browser.quit()
        time.sleep(5)
    return htmls

def parse_html(html_data):
	dicts = []
	soup = BeautifulSoup(html_data, 'html.parser')
	main_cajas = soup.find('div', class_='postings-container')
	try:
		cajas_depto = [caja for caja in main_cajas.find_all('div', recursive=False)]

		for d in cajas_depto:
			data_dict = {}
			data_dict['fecha_ejecucion'] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
			data_dict['url'] = "https://www.zonaprop.com.ar"+ d.find('div').get("data-to-posting")
			data_dict['property_id'] = d.find('div').get("data-id")
			left_box = d.find('div', {"data-qa" :"POSTING_CARD_GALLERY"})
			right_box = left_box.next_sibling()
			right_box_up = right_box[0].find('div')
			data_dict['price'] = right_box_up.find('div', {"data-qa" :"POSTING_CARD_PRICE"}).text
			data_dict['direccion'] = right_box_up.find('div', {"data-qa" :"POSTING_CARD_LOCATION"}).previous_sibling.text
			data_dict['barrio'] = right_box_up.find('div', {"data-qa" :"POSTING_CARD_LOCATION"}).text
			features = right_box_up.find('div', {"data-qa" :"POSTING_CARD_FEATURES"}).find_all('span',recursive=False)
			data_dict['features_str'] = "{"+", ".join([f.text.strip() for f in features])+"}"
			data_dict['desc_corta'] = right_box_up.find('h2').text.strip()
			dicts.append(data_dict)

		return dicts
	except:
		return None

def ddbb_duckdb(df):
    conn = duckdb.connect('data.db')
    conn.sql("INSERT INTO raw_data SELECT * FROM df")
    conn.close()
   
def extraccion():
	htmls_list = get_all_pages()
	file = open('htmls_backup', 'wb')
	pickle.dump(htmls_list, file)
	
	data = []
 
	for idx,ht in enumerate(htmls_list):
		nro_pag = "" if idx == 0 else '-pagina-'+str(idx+1)
		url = f"https://www.zonaprop.com.ar/casas-departamentos-ph-alquiler-capital-federal-vicente-lopez-san-isidro-2-ambientes-publicado-hace-menos-de-1-semana-menos-300000-pesos-orden-precio-ascendente{nro_pag}.html"
		print(url)
		if parse_html(ht)== None:
			pass
		else:
			data += parse_html(ht)
	
	df = pd.DataFrame(data)
	ddbb_duckdb(df)
	
def data_duckdb():
	conn = duckdb.connect('data.db')
	date_today = datetime.now().strftime("%Y-%m-%d")
	df = conn.sql(f"SELECT DISTINCT * FROM raw_data WHERE fecha_ejecucion = '{date_today}' AND url NOT IN (SELECT url FROM sended);").df()
	conn.sql("INSERT INTO sended SELECT url FROM df")
	return df

def email_deptos():
	df = data_duckdb()

	subject = "Búsqueda de Deptos"
	emaillist = [elem.strip().split(',') for elem in recipients]

	msg = MIMEMultipart()
	msg['Subject'] = subject
	msg['From'] = EMAIL_ADDRESS

	html = """\
	<html>
	<head></head>
	<body>
	<h2><a href="https://docs.google.com/spreadsheets/d/16y667JdvUStZqIIeAMMpANlLyExua8YNPe9c7qFjmwQ/edit#gid=0" target="_blank">Google Drive Deptos</a></h2>
		{0}
	</body>
	</html>
	""".format(df.to_html())

	part1 = MIMEText(html, 'html')
	msg.attach(part1)

	with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
		smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
		smtp.sendmail(msg['From'],emaillist,msg.as_string())

def main():
	extraccion()
	email_deptos()
 
if __name__ == "__main__":
	main()
    