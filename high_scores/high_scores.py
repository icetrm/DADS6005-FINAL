#!/usr/bin/env python3

import sqlite3
import time
import random
from datetime import datetime
from sqlite3 import Error
 

def high_scores_connect_to_db(db_file_name = 'high_scores.db'):
    try:
        db_connection = sqlite3.connect(db_file_name)
        return db_connection
    except Error:
        print(Error)

def high_scores_create_table(db_connection):
    cursor_object = db_connection.cursor()
    cursor_object.execute("create table if not exists high_score_table(time_stamp integer PRIMARY KEY, name text, score integer, date)")
    db_connection.commit()


def high_scores_update_db(db_connection, name, score):
    '''updates high_scores db table with a new entry
    '''
    now = datetime.now()
    time_stamp = int(datetime.timestamp(now))
    cursor_object = db_connection.cursor()
    cursor_object.execute('INSERT INTO high_score_table(time_stamp, name, score, date) VALUES(?, ?, ?, ?)', (time_stamp, name, score, now.date()))
    db_connection.commit()



def high_scores_top_list(db_connection, length=5, name='*'):
    ''' returns a sorted list of tops scores from db of lenght=length
    '''
    def take_score(elem):
        ''' score is the third element in the row '''     
        return elem[2]

    cursor_object = db_connection.cursor()
    cursor_object.execute('SELECT * FROM high_score_table')
    rows = [row for row in cursor_object.fetchall()]
    rows.sort(key=take_score, reverse=True)
    if len(rows) < length:
        return rows
    else:
        return(rows[:length])

def high_scores_db_delete(db_connection):
    cursor_object = db_connection.cursor()
    cursor_object.execute('drop table if exists high_score_table')
    db_connection.commit()


def test():
    print('This is printout from high_scores test function! => import successful!')

#-----------------------------------------------------------------
# Only used for testing teh
if __name__ == '__main__':

   pass

