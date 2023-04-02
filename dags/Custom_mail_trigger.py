import datetime as dt
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email_operator import EmailOperator
import pendulum
from tempfile import NamedTemporaryFile
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago



# local_tz = pendulum.timezone("US/Eastern")


# DAG structure
default_args = {
    'owner': 'nischay',
    'depends_on_past': False,
    'start_date': datetime(2023,3,26),
    'end_date': datetime(2023,3,31),
    # 'start_date': dt.datetime.today(),
    'email': ['nischaygowda105@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    dag_id='Automated_Email_Test_1',
    default_args=default_args,
    description='Weather Data ETL process 1-min',
    # schedule_interval='@daily',
    # schedule_interval='@hourly',
    # At every 30th minute
    # schedule_interval='*/30 * * * *',
    # At minute 0 past every hour.
    # 3am, 11am, and 7pm daily
    # by default it takes PST time.
    schedule_interval= '0 3,11,19 * * *',
    catchup=False,
    params={"description": ""},
    tags = ["email"]
    # schedule_interval=dt.timedelta(minutes=50),
)


def send_email():
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    body = ''' Hi Team, \n Please find the link attached below.
                \n
             https://app.powerbi.com/groups/me/reports/b8c605a3-fd4b-4f06-8266-2b939f7f3bed?ctid=41f88ecb-ca63-404d-97dd-ab0a169fd138&pbi_source=linkShare 
             
             \n
             \n
    Regards and Thank you, \n
    Nischay Gowda
    ''' 
    msg = MIMEMultipart()

    msg['Subject'] = 'Airflow Alert - Dashboard Link'
    msg['From'] = "nischaygowda105@gmail.com"
    recipients = 'nischaygowda777@gmail.com'
    msg['To'] = (', ').join(recipients.split(','))

    msg.attach(MIMEText(body,'plain'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login("nischaygowda105@gmail.com", 'guzyeprtvmimboid')
    server.send_message(msg)
    server.quit()

with dag:
   
    # Task 1 - sending mail.
    send_email_task = PythonOperator(
        task_id = 'Send_Email',
        python_callable = send_email,
        dag = dag,
    )
    
    # DAG order.
    send_email_task
    
