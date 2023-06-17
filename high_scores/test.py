#!/usr/bin/env python3

import sqlite3
import time
import random
from datetime import datetime

 
from sqlite3 import Error
 
def sql_connection():
    try:
        con = sqlite3.connect('high_scores.db')
        return con
    except Error:
        print(Error)
 
def create_sql_table(con):
    cursorObj = con.cursor()
    cursorObj.execute("create table if not exists high_score_table(time_stamp integer PRIMARY KEY, name text, score integer)")
    con.commit()


def insert_sql_table(con, time_stamp, name, score):
    cursorObj = con.cursor()
    cursorObj.execute('INSERT INTO high_score_table(time_stamp, name, score) VALUES(?, ?, ?)', (time_stamp, name, score))
    con.commit()

def print_sql_table(con):
    cursorObj = con.cursor()
    cursorObj.execute('SELECT * FROM high_score_table')
    rows = cursorObj.fetchall()
    for row in rows:
        print(row)

def drop_sql_table(con):
    cursorObj = con.cursor()
    cursorObj.execute('drop table if exists high_score_table')


con = sql_connection()
create_sql_table(con)
print_sql_table(con)
drop_sql_table(con)

con.close()

""" How to get the highest values:
select * 
from your_table
where product_price = (SELECT max(product_price) FROM your_table)
order by id desc
limit 1
"""
