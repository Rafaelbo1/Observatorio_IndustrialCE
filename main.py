import functions as f
import pandas as pd


df_atracacao = f.dados('Atracacao')
df_TemposAtracacao = f.dados('TemposAtracacao')
table_Atracacao = f.atracacao_fato(df_atracacao,df_TemposAtracacao)
table_Atracacao.columns = [c.replace(' ', '_') for c in table_Atracacao.columns]
table_Atracacao.columns = [c.replace('º', 'o') for c in table_Atracacao.columns]
table_Atracacao = table_Atracacao.where(pd.notnull(table_Atracacao), None)

cnxn = f.connection()
cursor = cnxn.cursor()

# Inserir Dataframe no SQL Server:
for index, row in table_Atracacao.iterrows():
    try:
        cursor.execute("""INSERT INTO atracacao_fato(IDAtracacao,CDTUP,IDBerco,Berço,[Porto Atracação],
                    [Apelido Instalação Portuária],[Complexo Portuário],[Tipo da Autoridade Portuária],
                    [Data Atracação],[Data Chegada],[Data Desatracação],[Data Início Operação],
                    [Data Término Operação],Ano,Mês,[Tipo de Operação],[Tipo de Navegação da Atracação],
                    [Nacionalidade do Armador],FlagMCOperacaoAtracacao,Terminal,Município,UF,SGUF,
                    [Região Geográfica],[Nº da Capitania],[Nº do IMO],TEsperaAtracacao,
                    TEsperaInicioOp,TOperacao,TEsperaDesatracacao,TAtracado,TEstadia)
                    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                   row.IDAtracacao,row.CDTUP,row.IDBerco,row.Berço,
                   row.Porto_Atracação,row.Apelido_Instalação_Portuária,row.Complexo_Portuário,
                   row.Tipo_da_Autoridade_Portuária,row.Data_Atracação,row.Data_Chegada,row.Data_Desatracação,
                   row.Data_Início_Operação,row.Data_Término_Operação,row.Ano,row.Mes,row.Tipo_de_Operação,
                   row.Tipo_de_Navegação_da_Atracação,row.Nacionalidade_do_Armador,row.FlagMCOperacaoAtracacao,
                   row.Terminal,row.Município,row.UF,row.SGUF,row.Região_Geográfica,row.No_da_Capitania,
                   row.No_do_IMO,row.TEsperaAtracacao,row.TEsperaInicioOp,row.TOperacao,row.TEsperaDesatracacao,row.TAtracado,
                   row.TEstadia)
        #print(f"Registro inserido com sucesso: {index}")
    except Exception as e:
        print(f"Não foi possível inserir o registro {index}. O erro encontrado foi: {e}")
        break
cnxn.commit()
cursor.close()