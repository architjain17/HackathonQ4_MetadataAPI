#!/usr/bin/python
from flask import Flask
import psycopg2
import os
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from collections import namedtuple
from sqlalchemy.orm import sessionmaker, scoped_session
import csv
import json
# temp3888620
app = Flask(__name__)


class VersionControl:
    
    def init(self):
        self.header=None
        self.query_result=[]

    # @app.route('/')
    def db_conn(self):

        engine = create_engine('postgresql+psycopg2://{username}:{password}@pg-dev-1:5432/dw', echo=True)
        Session = scoped_session(sessionmaker(bind=engine))
        s=Session()
        result=s.execute("Select * from temphitesh_dbo.temp3888620")

        col_headers=result._cursor_description()
        print [header.name for header in col_headers]

        with open('test.csv', 'wb+') as csvfile:
            save=csv.writer(csvfile, delimiter=',')
            save.writerow([header.name for header in col_headers])
            for row in result:
                save.writerow(row)
            csvfile.close()
        s.close()
        return str(self.query_result)

    # def table_to_csv():

# VersionControl()
if __name__ == "__main__":
    ver=VersionControl()
    print ver.db_conn()









