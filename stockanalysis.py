import bs4
import requests
from bs4 import BeautifulSoup
import time
import sqlite3
from sqlite3 import Error
from datetime import datetime
import pandas as pd
def sql_connection():
    try:
        con=sqlite3.connect('mydatabase0025.db') # it is used to connect to  a data base i.e mydatabase.db file
        # this file is created on disk . if we want to create the db on ram then we have to use connect(:memory)
        print("connection is established...")
        return con
    except Error:
        print(" Sql Connection Error!")

# it is used to create a table
def sql_create_table(con,tname):
    try:
        mcur=con.cursor() # it gives the cursor object that is used to execute the sqlite statemsnts    
        mcur.execute(f"CREATE TABLE {tname}(dtime text PRIMARY KEY,companyname text,perchg text)")
        con.commit()
        # in order to drop the table just write DROP TABLE tablename inside execute.
    except Error:
        print(" Table Creation Error!")

# it used to insert data into the table
def sql_insert(con,entities,tname):
    try:
        mcur=con.cursor() # it gives the cursor object that is used to execute the sqlite statemsnts    
        mcur.execute(f"INSERT INTO {tname}(dtime,companyname,perchg)VALUES(?,?,?)",entities)
        con.commit()
    except Error:
        print(" Insert Error!")

# it used to extract data from the table and display it also checks for exact word
def sql_select(con,tname):
    try:
        df=pd.read_sql_query(f"select * from {tname};",con)
        print(df)
    except Error:
        print(" Select Error!")

def getValue(con,stock):
    
    count=0
    pvalue=0
    cond=0
    
    try:
        mcur=con.cursor() # it gives the cursor object that is used to execute the sqlite statemsnts    
        for i in stock:
            try:
                comname=i.find("td").find("b").text
                pvalue=i.find_all('td')[4].text
            except Exception as e:
                cond=1

            if cond==0:
                mcur.execute(f"SELECT perchg from stock{count}")
                rows=mcur.fetchall()
                index=len(rows)-1
                tvalue=rows[index]
                if float(tvalue[0])-float(pvalue)>=2:
                    print(f"{comname} Stock Changed By 2 Percent")
                else:
                    print(f"{comname} Stock Remains Same")
                count=count+1
            cond=0            
    except Error:
        print(" get Value Error!")

rcon=sql_connection()

count=0

r=requests.get('https://www.moneycontrol.com/stocks/marketstats/indexcomp.php?optex=NSE&opttopic=indexcomp&index=9')
soup=bs4.BeautifulSoup(r.text,"lxml")

def getprice(soup,rcon,count):

    check=0
    p=0
    min=0
    stock=soup.find('table',{'class':"tbldata14 bdrtpg"}).find_all('tr')#.find_all('td')[4]
    
    while True:    
        if min<=4:
            for i in stock:
                try:
                    entities=(str(datetime.now()),i.find("td").find("b").text,i.find_all('td')[4].text)
                    if check==0:
                        sql_create_table(rcon,f"stock{count}")
                        p=p+1
                    sql_insert(rcon,entities,f"stock{count}")
                    count=count+1;
                except Exception as e:
                    pass
            count=0
            time.sleep(30)
            min=min+0.5
            if min%2==0: 
                getValue(rcon,stock)
        else:
            break    
        check=check+1
    
    for j in range(p):
        sql_select(rcon,f"stock{j}")


getprice(soup,rcon,count)
