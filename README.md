# airflow-docker

A pasta dataset esta armazenado dois arquivos 
* vendas-combustiveis-m3.xls 
* vendas-combustiveis-m3_diesel.xls

Os arquivos contem uma macro que atualiza as sheets SalesFuel e SalesDiesel com o detalhamento da pivot table desejada prlo clique

O arquivo ExtraiDadosAirflow.py contem a extração dos dados do excelpara o SQL Server e as configurações das DAGs do Airflow 

O arquivo ExtraiDados.py é um script python que extrai os dados do excel para o SQL Server

O arquivo docker-compose contem as configurações para criação do container airflow

