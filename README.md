# airflow-docker

A pasta dataset esta armazenado dois arquivos 
* vendas-combustiveis-m3.xls 
* vendas-combustiveis-m3_diesel.xls

Os arquivos contem uma macro que atualiza as sheets SalesFuel e SalesDiesel com o detalhamento da pivot table desejada prlo clique

O arquivo ExtraiDadosAirflow.py é o arquivo que esta as configurações das DAGs do Airflow 

O arquivo ExtraiDados.py é um script python que faz a extração dos dados do excel para um banco de dados SQL Server

