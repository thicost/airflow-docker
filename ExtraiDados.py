# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 11:27:32 2021

@author: thiago
"""

import pandas as pd
import datetime
import pyodbc

server = 'DESKTOP-JFM8RSO' 
database = 'Raizen' 
username = 'usuario' 
password = 'password' 
tablename = 'vendas_combustivel'


df = pd.read_excel("dataset/vendas-combustiveis-m3.xls", sheet_name="Dados") 

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

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

print(cnxn) 


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

    dbcur.close()
    return False

    
if checkTableExists(cnxn, tablename) == False:
    cursor.execute("""
        create table vendas_combustivel(
           year_month date,
	       uf varchar(50), 
           product varchar(50), 
           unit varchar(2),
           volume float(30)
           
           )""")
    cnxn.commit()
    cursor.close()
    cnxn.close()
    

# Valida dados
query = '''
    select year_month, sum(volume) volume 
    from vendas_combustivel
    group by year_month 
    order by year_month'''
dados = pd.read_sql(query, cnxn)
   
   
dados.head(3)

dados.set_index('year_month', inplace=True)

dados_2 = tab_unpvt.groupby('year_month').sum()

dados_2.head(3)

 


# Insert Dataframe into SQL Server:
for index,row in tab_unpvt.iterrows(): 
    cursor.execute("insert into [Raizen].[dbo].[vendas_combustivel] (year_month, uf, product, unit, volume) values (?, ?, ?, ?, ?)", row['year_month'], row['uf'], row['product'], row['unit'], row['volume'])
cnxn.commit()
cursor.close()




