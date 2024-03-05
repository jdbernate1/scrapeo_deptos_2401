import pandas as pd
import duckdb
from datetime import datetime
from email.message import EmailMessage
from email.utils import make_msgid
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib
from secrets import EMAIL_ADDRESS, EMAIL_PASSWORD, recipients

def data_duckdb():
	conn = duckdb.connect('data.db')
	date_today = datetime.now().strftime("%Y-%m-%d")
	df = conn.sql(f"SELECT DISTINCT * FROM raw_data WHERE fecha_ejecucion = '{date_today}' AND url NOT IN (SELECT url FROM sended);").df()
	conn.sql("INSERT INTO sended SELECT url FROM df")
	return df

def email_deptos():
	df = data_duckdb()

	subject = "Mensaje "
	emaillist = [elem.strip().split(',') for elem in recipients]

	msg = MIMEMultipart()
	msg['Subject'] = subject
	msg['From'] = EMAIL_ADDRESS

	html = """\
	<html>
	<head></head>
	<body>
		{0}
	</body>
	</html>
	""".format(df.to_html())

	part1 = MIMEText(html, 'html')
	msg.attach(part1)

	with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
		smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
		smtp.sendmail(msg['From'],emaillist,msg.as_string())


