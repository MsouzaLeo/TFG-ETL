import pandas as pd
import sqlalchemy
from tkinter import filedialog

arquivo = filedialog.askopenfilenames(title='Selecione a planilha')

df = pd.read_csv(arquivo[0], sep=';', encoding='cp1252', low_memory=False)


engine = sqlalchemy.create_engine('postgresql://postgres:0812@localhost/TFG')
conexao = engine.connect()
table_name = 'unidade_requisitante'

try:
    frame = df.to_sql(table_name, conexao, if_exists='append', index=False)
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print(f"A tabela {table_name} foi carregada com sucesso.")
finally:
    conexao.close()