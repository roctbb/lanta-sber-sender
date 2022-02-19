import psycopg2
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
import pytz
from config import *
import pandas as pd
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


def send_mail(send_from, send_to, subject, text, files=None,
              server="localhost", port=587, username='', password='',
              use_tls=True):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

def send_report(cursor):
    user_ids = get_users(cursor)

    columns = ["ФИО", "Дата рождения", "Дата начала мониторинга", "Время заполнения", "Тревога", "Температура", "Сатурация", "Пульс", "ЧДД", "Сухой кашель", "Одышка", "Боль в грудной клетке", "Кровь в мокроте", "Слабость, боль в мышцах", "Неукротимая рвота", "Нарастание периферических отеков", "Неконтролируемая температура", "Невозможность коррекции уровня глюкозы", "Прочие жалобы"]
    df = pd.DataFrame(columns=columns)

    for user in user_ids:
        build_report(cursor, user, df)

    if len(df):
        filename = save(df)
        send_mail(EMAIL, RECEIVERS, "Ланта: отчет дистанционного мониторинга COVID-19", "Отчет во вложении.", [filename], username=EMAIL, password=EMAIL_PASSWORD, server=EMAIL_SERVER)

        print("report sent")


def save(df):
    filename = f"./reports/report-{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}.xlsx"
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')

    # Write excel to file using pandas to_excel
    df.to_excel(writer, startrow=1, sheet_name='Sheet1', index=False)

    # Indicate workbook and worksheet for formatting
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # Iterate through each column and set the width == the max length in that column. A padding length of 2 is also added.
    for i, col in enumerate(df.columns):
        # find length of column i
        column_len = df[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) + 2
        # set the column length
        worksheet.set_column(i, i, column_len)
    writer.save()

    return filename


def build_report(cursor, user, df):
    user_id, birthday, name, start = user
    now = (datetime.now() + timedelta(days=-1)).strftime('%d-%m-%Y 21:00:00')
    q = f"SELECT \"group\" FROM medical_records WHERE user_id = {user_id} and created_at > '{now}' order by created_at"
    cursor.execute(q)
    result_groups = []

    for g in map(lambda x:x[0], cursor.fetchall()):
        if g not in result_groups:
            result_groups.append(g)

    for group_id in result_groups:
        q = f"SELECT * FROM medical_records WHERE \"group\" = '{group_id}'"
        cursor.execute(q)

        alert = False

        row = { column: "" for column in df.columns }
        row["ФИО"] = name
        row["Дата рождения"] = birthday.strftime('%d.%m.%Y')
        row["Дата начала мониторинга"] = start.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("Asia/Vladivostok")).strftime('%d.%m.%Y')

        for record in cursor.fetchall():
            print(record)
            category = record[2]
            value = record[1]

            if record[-2]:
                alert = True

                print("-- ", record[-2])

            if category == 25: #temp
                row["Температура"] = float(value)
            elif category == 1: # pulse
                row["Пульс"] = int(value)
            elif category == 44: # breath
                row["ЧДД"] = int(value)
            elif category == 31: # action
                row["Время заполнения"] = record[5].replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("Asia/Vladivostok")).strftime('%d.%m.%Y %H:%M:%S')
            elif category == 22: # spo
                row["Сатурация"] = int(value)
            elif category == 30: # symptom
                if "жалобы пациента -" not in value:
                    fname = value.replace("COVID-19: ", "").capitalize()
                    row[fname] = "да"
                else:
                    row["Прочие жалобы"] = value.replace("COVID-19, жалобы пациента - ", "")

        if alert:
            row["Тревога"] = "!!!"

        print(list(row.values()))
        df.loc[len(df)] = list(row.values())

def get_users(cursor):
    now = datetime.now().strftime('%d-%m-%Y')
    q = f"SELECT u.id, u.birthday, u.name, \"startDate\" FROM contracts JOIN patient_clinics pc ON contracts.patient = pc.id JOIN users u ON u.id = pc.user WHERE \"endDate\" > '{now}' and scenario_id = 49"
    cursor.execute(q)

    results = cursor.fetchall()

    return results


def db_execute(F, port=5432):
    params = {
        'database': DATABASE,
        'user': USER,
        'password': PASS,
        'host': 'localhost',
        'port': port
    }

    medsenger_conn = psycopg2.connect(**params)
    cursor = medsenger_conn.cursor()

    F(cursor)

    medsenger_conn.commit()


def ssh_excecute(F):
    with SSHTunnelForwarder(
            (SSH_HOST, 22),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASS,
            remote_bind_address=('localhost', 5432)) as server:
        server.start()
        print("server connected")

        db_execute(F, server.local_bind_port)


if __name__ == "__main__":
    if USE_SSH:
        ssh_excecute(send_report)
    else:
        db_execute(send_report)
