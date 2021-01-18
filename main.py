from sqlalchemy import create_engine
from pandas import read_sql_query
import json
import smtplib

servers_errors = []


def test_connect(database: dict):
    try:
        print(str(database))
        engine = create_engine(database['databasePyUrl'])
        query = get_query_by_type(database['sgbd_name'])
        result = read_sql_query(query, engine)

    except Exception as e:
        servers_errors.append(database['host'])
        print(str(e))
        pass


def get_query_by_type(sgbd_name: str):
    if sgbd_name == 'Oracle':
        return 'select * from dual'
    elif sgbd_name == 'Postgres':
        return 'select count(*) from pg_stat_activity'
    elif sgbd_name == 'SQLServer':
        return 'SELECT COUNT(dbid) FROM sys.sysprocesses WHERE dbid > 0'
    elif sgbd_name == 'MySQL':
        return 'select count(*) from information_schema.processlist'


def send_mail(smtp: dict, servers: str, email: dict):
    server = smtplib.SMTP(smtp['host'], smtp['port'])
    server.starttls()
    server.login(smtp['user'], smtp['password'])
    server.set_debuglevel(1)
    fromaddr = email['from']
    toaddres = email['to']

    msg = """Subject: {subject}
    \n\r
    {body} 
    {hostnames}
    """.format(
        subject=email['subject'],
        body=email['body'],
        hostnames=servers
    )

    server.sendmail(fromaddr, toaddres, msg)
    server.quit()


def run():
    with open('databases.json') as data:
        if data == None:
            print('List of database is empty.')
            return

        databases = json.load(data)

    # get databases
    for i in range(len(databases)):
        try:
            test_connect(databases[i])

        except Exception as e:
            print(str(e))
            pass

    # get smtp info
    with open('smtp.json') as data:
        smtp = json.load(data)

        if smtp is None:
            print('SMTP config is empty.')
            return

    # get email info
    with open('email.json') as data:
        email = json.load(data)

        if email is None:
            print('Email info is empty.')
            return

    if servers_errors is not None:
        send_mail(smtp, servers_errors, email)


if __name__ == '__main__':
    run()
