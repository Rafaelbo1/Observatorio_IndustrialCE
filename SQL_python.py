import pandas as pd
import functions as f

cnxn = f.connection()
cursor = cnxn.cursor()
df = pd.read_sql(" select * from atracacao_fato ", cnxn)
cnxn.close()

df_CE = df.loc[df['SGUF'] == 'CE']
df_natrac=df_CE.groupby(['Mês','Ano','SGUF'])['IDAtracacao'].count()
df_natrac=  df_natrac.to_frame().reset_index()

df_TEspera = df_CE.groupby(['Mês','Ano','SGUF'])['TEsperaAtracacao'].mean()
df_TEspera=  df_TEspera.to_frame().reset_index()

df_TAtracado = df_CE.groupby(['Mês','Ano','SGUF'])['TAtracado'].mean()
df_TAtracado =  df_TAtracado.to_frame().reset_index()

df_natrac = df_natrac.rename(columns={'IDAtracacao': 'Número de Atracações'})
df_TEspera = df_TEspera.rename(columns={'TEsperaAtracacao': 'Tempo de espera médio'})
df_TAtracado = df_TAtracado.rename(columns={'TAtracado': 'Tempo atracado médio'})
tabela_ceara = pd.merge(pd.merge(df_natrac,df_TEspera),df_TAtracado)
tabela_ceara = tabela_ceara.rename(columns={'SGUF': 'Localidade'})
tabela_ceara = tabela_ceara.reindex(columns=['Localidade','Número de Atracações',
                                             'Tempo de espera médio','Tempo atracado médio',
                                             'Mês','Ano'])

writer = pd.ExcelWriter('tabela_ceara.xlsx')
tabela_ceara.to_excel(writer)
writer.save()

df_ND = df.loc[df['Região Geográfica'] == 'Nordeste']
df_natrac=df_ND.groupby(['Mês','Ano','Região Geográfica'])['IDAtracacao'].count()
df_natrac=  df_natrac.to_frame().reset_index()

df_TEspera = df_ND.groupby(['Mês','Ano','Região Geográfica'])['TEsperaAtracacao'].mean()
df_TEspera=  df_TEspera.to_frame().reset_index()

df_TAtracado = df_ND.groupby(['Mês','Ano','Região Geográfica'])['TAtracado'].mean()
df_TAtracado =  df_TAtracado.to_frame().reset_index()

df_natrac = df_natrac.rename(columns={'IDAtracacao': 'Número de Atracações'})
df_TEspera = df_TEspera.rename(columns={'TEsperaAtracacao': 'Tempo de espera médio'})
df_TAtracado = df_TAtracado.rename(columns={'TAtracado': 'Tempo atracado médio'})
tabela_Ndeste = pd.merge(pd.merge(df_natrac,df_TEspera),df_TAtracado)
tabela_Ndeste = tabela_Ndeste.rename(columns={'Região Geográfica': 'Localidade'})
tabela_Ndeste = tabela_Ndeste.reindex(columns=['Localidade','Número de Atracações',
                                             'Tempo de espera médio','Tempo atracado médio',
                                             'Mês','Ano'])

writer = pd.ExcelWriter('tabela_Ndeste.xlsx')
tabela_Ndeste.to_excel(writer)
writer.save()


df_natrac=df.groupby(['Mês','Ano'])['IDAtracacao'].count()
df_natrac=  df_natrac.to_frame().reset_index()

df_TEspera = df.groupby(['Mês','Ano'])['TEsperaAtracacao'].mean()
df_TEspera=  df_TEspera.to_frame().reset_index()

df_TAtracado = df.groupby(['Mês','Ano'])['TAtracado'].mean()
df_TAtracado =  df_TAtracado.to_frame().reset_index()

df_natrac = df_natrac.rename(columns={'IDAtracacao': 'Número de Atracações'})
df_TEspera = df_TEspera.rename(columns={'TEsperaAtracacao': 'Tempo de espera médio'})
df_TAtracado = df_TAtracado.rename(columns={'TAtracado': 'Tempo atracado médio'})
tabela_BR = pd.merge(pd.merge(df_natrac,df_TEspera),df_TAtracado)
tabela_BR['Localidade'] = 'BR'
tabela_BR = tabela_BR.reindex(columns=['Localidade','Número de Atracações',
                                             'Tempo de espera médio','Tempo atracado médio',
                                             'Mês','Ano'])

writer = pd.ExcelWriter('tabela_BR.xlsx')
tabela_BR.to_excel(writer)
writer.save()
