import pandas as pd
import sqlalchemy
from tkinter import filedialog

arquivo = filedialog.askopenfilenames(title='Selecione a planilha')

df = pd.read_csv(arquivo[0], sep=';', encoding='cp1252', low_memory=False)

df = df[['masp', 'nome', 'nmefet']]
df.columns = ['masp', 'nome_perito_responsavel', 'nmefet']
df_filtado = df[df.nmefet.eq('PERITO CRIMINAL')]
df_final = df_filtado[['masp', 'nome_perito_responsavel']]
df_final['masp'] = df_final['masp'].map(lambda x: str(x)[:-1])
print(df_final['masp'])


engine = sqlalchemy.create_engine('postgresql://postgres:0812@localhost/TFG')
conexao = engine.connect()
table_name = 'perito_responsavel'

try:
    frame = df_final.to_sql(table_name, conexao, if_exists='append', index=False)
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print(f"A tabela {table_name} foi criada com sucesso.")
finally:
    conexao.close()
