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


# DAG structure
default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023,3,26),
    'end_date': datetime(2023,3,31),
    # 'start_date': dt.datetime.today(),
    'email': ['sender@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    dag_id='Your_DAG_name',
    default_args=default_args,
    description='DAG description',
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
                 Body Text
                \n
    Regards and Thank you, \n
    Your Name
    ''' 
    msg = MIMEMultipart()

    msg['Subject'] = 'Mail Subject'
    msg['From'] = "sender@gmail.com"
    # add mutpltile emails in this way - "email1, email2"
    recipients = "reciever1@gmail.com, reciever2@gmail.com"
    msg['To'] = (', ').join(recipients.split(','))
    msg.attach(MIMEText(body,'plain'))
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login("sender@gmail.com", 'Application_password')
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
    
