import pandas as pd
import sqlalchemy
from tkinter import filedialog

pd.options.mode.chained_assignment = None

#Selecionando o arquivo com a planilhas dos Laudos
arquivo = filedialog.askopenfilenames(title='Selecione a planilha')

#print(arquivo[0])
#Lendo o arquivo selecionado e transformando-o em um DataFrame
IC_mes = pd.read_csv(arquivo[0], sep=';', encoding='cp1252')


#Separando os dados dos Laudos para o Banco de Dados
df = IC_mes[['Número da Requisição', 'Código da natureza do exame', 'Código da espécie do exame', 'Código da unidade requisitante',
        'Código da unidade do exame', 'MASP/matrícula do perito responsável',
       'Tipo da Requisição', 'Número do Procedimento', 'Código modelo laudo',
       'Data da requisição de perícia', 'Data de distribuição da requisição',
        'Data da redistribuição', 'Data da devolução da requisição',
       'Data do aceite da requisição', 'Data da expedição do laudo']]


#Alterando o nome das colunas para o mesmo do banco de dados
df.columns = ['nmr_requisicao', 'cod_natureza_exame', 'cod_especie_exame', 'cod_unidade_requisitante',
              'cod_unidade_exame', 'masp_perito', 'tipo_requisicao', 'nmr_procedimento', 'cod_modelo_laudo',
              'data_requisicao_pericia', 'data_distribuicao_requisicao', 'data_redistribuicao',
              'data_devolucao_requisicao', 'data_aceite_requisicao', 'data_expedicao_laudo']


#Alterando o DataType do 'masp_perito' e removendo a letra 'm' do começo
df['masp_perito'] = df.masp_perito.astype('string').str.replace('m', '')


#Alterando o DataType de todas as datas do DF
df['data_requisicao_pericia'] = pd.to_datetime(df['data_requisicao_pericia'].astype('datetime64').dt.strftime("%d/%m/%y %H:%M"))
df['data_distribuicao_requisicao'] = pd.to_datetime(df['data_distribuicao_requisicao'].astype('datetime64').dt.strftime("%d/%m/%y %H:%M"))
df['data_redistribuicao'] = pd.to_datetime(df['data_redistribuicao'].astype('datetime64').dt.strftime("%d/%m/%y %H:%M"))
df['data_devolucao_requisicao'] = pd.to_datetime(df['data_devolucao_requisicao'].astype('datetime64').dt.strftime("%d/%m/%y %H:%M"))
df['data_aceite_requisicao'] = pd.to_datetime(df['data_aceite_requisicao'].astype('datetime64').dt.strftime("%d/%m/%y %H:%M"))
df['data_expedicao_laudo'] = pd.to_datetime(df['data_expedicao_laudo'].astype('datetime64').dt.strftime("%d/%m/%y %H:%M"))


#Adicionando o atributo calculado "tempo_confeccao_laudo" ao DataFrame
df['tempo_confeccao_laudo'] = df['data_expedicao_laudo'] - df['data_distribuicao_requisicao']
df['tempo_confeccao_laudo'].fillna('-1 day', inplace=True)


#Tratando os valores nulos em masp
df['masp_perito'].fillna('devolvida', inplace=True)


#Tratando os valores nulos em nmr_procedimento
df['nmr_procedimento'].fillna('devolvida', inplace=True)


#Criando a conexao ao Banco
engine = sqlalchemy.create_engine('postgresql://postgres:0812@localhost/TFG')
conexao = engine.connect()


#Tabela onde vão ser inseridos os dados tratados
table_name = 'laudo'


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
