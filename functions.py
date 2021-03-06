import pandas as pd
import pyodbc
import os


# Função para carregar os dados
def dados(name):
    #diretório local onde estão as pastas 2017, 2018, 2019
    DIR = 'dados\\'
    list_path = os.listdir(DIR)

    #Carregando os dados e concatenando em um único dataframe
    list_df = []
    for i in list_path:
        DIR2 = DIR+i+'\\'
        list_dados = os.listdir(DIR2)
        file= [x for x in list_dados if x.startswith(i+name+'.txt')]
        local_file= os.path.join(DIR2, file[0])
        df = pd.read_csv(local_file,sep=';', low_memory=False, decimal=',')
        list_df.append(df)
    df = pd.concat(list_df)
    return df

#Função para conecção com o SQL Server a partir da biblioteca pyodbc
def connection():
    cnxn = pyodbc.connect(Trusted_Connection='Yes',
                          Driver='{ODBC Driver 17 for SQL Server}',
                          Server='RAFABO\SQLEXPRESS',
                          Database='Observatorio_IndustriaCE')
    return cnxn


#Função para a contrução da tabela atracacao_fato
def atracacao_fato (df_atracacao,df_TemposAtracacao):
    df = pd.merge(df_atracacao, df_TemposAtracacao,on='IDAtracacao', how="outer" )
    df.columns = [c.replace(' ', '_') for c in df.columns]

#A partir das informações das variáveis 'TEsperaAtracacao' , 'TEsperaInicioOp' , 'TOperacao'
# 'TEsperaDesatracacao' , 'TAtracado'  e 'TEstadia', foi criado o seguinte código (está inativo pelas ''')
# Para o cálculo automatizado das mesmas
    '''
    f = '%d/%m/%Y %H:%M:%S'
    df['TEsperaAtracacao'] = (pd.to_datetime(df['Data Atracação'], format=f) - pd.to_datetime(
                            df['Data Chegada'],format=f)).dt.total_seconds()/(3600.0)

    df['TEsperaInicioOp'] = (pd.to_datetime(df['Data Início Operação'],format=f) - pd.to_datetime(
                            df['Data Atracação'], format=f)).dt.total_seconds() / (3600.0)

    df['TOperacao'] = (pd.to_datetime(df['Data Término Operação'],format=f) - pd.to_datetime(
                            df['Data Início Operação'], format=f)).dt.total_seconds() / (3600.0)

    df['TEsperaDesatracacao'] = (pd.to_datetime(df['Data Desatracação'],format=f) - pd.to_datetime(
                            df['Data Término Operação'], format=f)).dt.total_seconds() / (3600.0)

    df['TAtracado'] = df.loc[:,['TEsperaInicioOp','TOperacao',
                                'TEsperaDesatracacao']].sum(axis=1,numeric_only=True)

    df['TEstadia'] = df.loc[:,['TEsperaAtracacao','TEsperaInicioOp',
                               'TOperacao','TEsperaDesatracacao']].sum(axis=1,numeric_only=True)
    '''
    return df

#Função para a contrução da tabela carga_fato
def carga_fato(df_carga,df_atracacao,df_Carga_Cont):
    data_atrac = df_atracacao.loc[:,['IDAtracacao','Ano','Mes','Porto Atracação','SGUF']]
    df_Carga_Cont = df_Carga_Cont.groupby('IDCarga')['VLPesoCargaConteinerizada'].sum()
    df = pd.merge(df_carga, data_atrac, on='IDAtracacao', how="left" )
    df = pd.merge(df, df_Carga_Cont, on='IDCarga', how="left" )
    df.columns = [c.replace(' ', '_') for c in df.columns]
    return df


# Driver Function(teste das funções)
if __name__ == "__main__":
    # Careegando os dados em um data frame
    df_atracacao = dados('Atracacao')
    df_carga = dados('Carga')
    table_Atracacao = atracacao_fato(df_atracacao)
    table_Carga = carga_fato(df_carga,df_atracacao)
