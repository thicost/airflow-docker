# Step 1: Import all necessary packages.

# For scheduling
import datetime as dt

# For ETL
import pandas as pd

# For DB Connection
import pyodbc,socket
#server = f'{socket.gethostname()}.local'
#print(server)

import os

cwd = os.getcwd()
#print(cwd)

# For Apache Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator


def getData():

    dataset = cwd + '/dataset/vendas-combustiveis-m3.xls'
    
    df = pd.read_excel(dataset, sheet_name="Dados") 

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

    tab_unpvt['ANO_MES'] = tab_unpvt['ANO_MES'].map(lambda x: x.strftime('%Y-%m-%d'))

    tab_unpvt.drop(columns=['ANO', 'MES', 'REGIÃO'], inplace = True)

    tab_unpvt = tab_unpvt[['ANO_MES', 'ESTADO', 'COMBUSTÍVEL', 'UNIDADE', 'Volume']]

    tab_unpvt.columns = ['year_month', 'uf', 'product', 'unit', 'volume']

    tab_unpvt['volume'].fillna(0, inplace=True)

    return print(tab_unpvt.head(3))


def InsertToDW():

    server    = f'{socket.gethostname()}.local'
    database  = 'Raizen' 
    username  = 'usuario' 
    password  = 'password' 
    tablename = 'vendas_combustivel'

    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

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
   
   
    # Insert Dataframe into SQL Server:
    for index,row in tab_unpvt.iterrows(): 
        cursor.execute("insert into [Raizen].[dbo].[vendas_combustivel] (year_month, uf, product, unit, volume) values (?, ?, ?, ?, ?)", row['year_month'], row['uf'], row['product'], row['unit'], row['volume'])
    cnxn.commit()
    cursor.close()


# Step 3: Define the DAG, i.e. the workflow

# DAG's arguments
default_args = {
        'owner': 'thicost',
        'start_date':dt.datetime(2020, 4, 16, 11, 00, 00),
        'concurrency': 1,
        'retries': 0
        }

# DAG's operators, or bones of the workflow
with DAG('ExtraiDados',
        catchup=False, # To skip any intervals we didn't run
        default_args=default_args,
        schedule_interval='* 1 * * * *', # 's m h d mo y'; set to run every minute.
        ) as dag:

    opr_get_Data = PythonOperator(
            task_id='get_Data',
            python_callable=getData
            )

    opr_Insert_To_DW = PythonOperator(
            task_id='Insert_To_DW',
            python_callable=InsertToDW
            )

# The actual workflow
opr_get_Data >> opr_Insert_To_DW

    



