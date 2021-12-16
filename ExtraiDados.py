# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 11:27:32 2021

@author: thiago
"""

print('#########################################Programa iniciado###################################')
      
      
   

import pandas as pd
from datetime import datetime
import time
import pyodbc

server = 'DESKTOP-JFM8RSO' 
database = 'Raizen' 
username = 'usuario' 
password = 'password' 
now = datetime.now()
s = now.strftime("%Y-%m-%d %H:%M:%S")
currentDate = s
#dt = datetime.fromtimestamp(currentDate)

#import datetime
#currentDate = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())



salesFuel   = pd.read_excel("dataset/vendas-combustiveis-m3.xls", sheet_name="salesFuel") 

salesDiesel = pd.read_excel("dataset/vendas-combustiveis-m3_diesel.xls", sheet_name="salesDiesel") 


def LoadData(df):

    tab = df[['COMBUSTÍVEL', 'ANO', 'REGIÃO', 'ESTADO', 'UNIDADE', 'Jan', 'Fev',
       'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']]

    total = df['TOTAL']

    tab_unpvt = pd.melt(tab, 
                    id_vars=['COMBUSTÍVEL', 'ANO', 'REGIÃO', 'ESTADO', 'UNIDADE'], 
                    var_name='MES', 
                    value_name='Volume')

    tab_unpvt['MES'] = tab_unpvt['MES'].map({'Jan' : '01', 'Fev' : '02', 'Mar' : '03', 'Abr' : '04',
                                         'Mai' : '05', 'Jun' : '06', 'Jul' : '07', 'Ago' : '08',
                                         'Set' : '09', 'Out' : '10', 'Nov' : '11', 'Dez' : '12'}) 


    tab_unpvt['ANO_MES'] = tab_unpvt['ANO'].astype(str) + tab_unpvt['MES'] + '01'


    tab_unpvt['ANO_MES'] = pd.to_datetime(tab_unpvt['ANO_MES'], format='%Y%m%d')

    #tab_unpvt['ANO_MES'] = tab_unpvt['ANO_MES'].map(lambda x: 100*x.year + x.month)

    tab_unpvt['ANO_MES'] = tab_unpvt['ANO_MES'].map(lambda x: x.strftime('%Y-%m-%d'))

    #tab_unpvt['ANO_MES'] = tab_unpvt['ANO_MES'].apply(lambda x:x.strftime('%Y%m'))

    tab_unpvt.drop(columns=['ANO', 'MES', 'REGIÃO'], inplace = True)

    #tab_unpvt.columns

    tab_unpvt = tab_unpvt[['ANO_MES', 'ESTADO', 'COMBUSTÍVEL', 'UNIDADE', 'Volume']]

    tab_unpvt.columns = ['year_month', 'uf', 'product', 'unit', 'volume']

    tab_unpvt['volume'].fillna(0, inplace=True)

    #tab_unpvt.info()
    
    return tab_unpvt

ds_salesFuel = LoadData(salesFuel)
ds_salesFuel['created_at'] = currentDate

ds_salesDiesel = LoadData(salesDiesel)
ds_salesDiesel['created_at'] = currentDate

#Conection DB

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#print(cnxn) 

def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True
    else:
        dbcur.close()
        return False


    
if checkTableExists(cnxn, 'sales_fuel_uf_product') == False:
    cursor.execute("""
        create table sales_fuel_uf_product(
           year_month date,
	       uf varchar(50), 
           product varchar(50), 
           unit varchar(2),
           volume float(30),
           created_at timestamp 
           
           )""")
    cnxn.commit()
    cursor.close()
    
    

    
if checkTableExists(cnxn, 'sales_diesel_uf_type') == False:
    cursor.execute("""
        create table sales_diesel_uf_type(
           year_month date,
	       uf varchar(50), 
           product varchar(50), 
           unit varchar(2),
           volume float(30),
           created_at timestamp 
           
           )""")
    cnxn.commit()
    cursor.close()
    
    



# Insert Dataframe into SQL Server:
def InsertTable(cnxn, df, table):
    for index,row in df.iterrows(): 
        cursor.execute("insert into [Raizen].[dbo].["+table+"] (year_month, uf, product, unit, volume) values (?, ?, ?, ?, ?)", row['year_month'], row['uf'], row['product'], row['unit'], row['volume'])
    cnxn.commit()
    cursor.close()

    
    
InsertTable(cnxn, ds_salesFuel,'sales_fuel_uf_product')

InsertTable(cnxn, ds_salesDiesel,'sales_diesel_uf_type')


# Valida dados
query = '''
    select year_month, sum(volume) volume 
    from sales_fuel_uf_product
    group by year_month 
    order by year_month'''
vl_salesFuel = pd.read_sql(query, cnxn)
   
vl_salesFuel.set_index('year_month', inplace=True)

vl_salesFuel_2 = ds_salesFuel.groupby('year_month').sum()


query = '''
    select year_month, sum(volume) volume 
    from sales_diesel_uf_type
    group by year_month 
    order by year_month'''
vl_ds_salesDiesel = pd.read_sql(query, cnxn)
   
vl_ds_salesDiesel.set_index('year_month', inplace=True)

ds_salesDiesel_2 = ds_salesDiesel.groupby('year_month').sum()








