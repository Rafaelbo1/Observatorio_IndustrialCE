import functions as f
import pandas as pd

df_Carga_Conteinerizada = f.dados('Carga_Conteinerizada')
df_carga = f.dados('Carga')
df_atracacao = f.dados('Atracacao')

table_Carga = f.carga_fato(df_carga,df_atracacao,df_Carga_Conteinerizada)
table_Carga = table_Carga.where(pd.notnull(table_Carga), None)

cnxn = f.connection()
cursor = cnxn.cursor()
# Inserir Dataframe no SQL Server:
for index, row in table_Carga.iterrows():
    try:
        cursor.execute("""INSERT INTO carga_fato(IDCarga,IDAtracacao,Origem,Destino,CDMercadoria,
                    [Tipo Operação da Carga],[Carga Geral Acondicionamento],ConteinerEstado,
                    [Tipo Navegação],FlagAutorizacao,FlagCabotagem,FlagCabotagemMovimentacao,
                    FlagConteinerTamanho,FlagLongoCurso,FlagMCOperacaoCarga,FlagOffshore,
                    FlagTransporteViaInterioir,[Percurso Transporte em vias Interiores],
                    [Percurso Transporte Interiores],STNaturezaCarga,STSH2,STSH4,[Natureza da Carga],
                    Sentido,TEU,QTCarga,VLPesoCargaBruta,Ano,Mês,[Porto Atracação],SGUF,
                    VLPesoCargaConteinerizada)
                    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    row.IDCarga,row.IDAtracacao,row.Origem,row.Destino,row.CDMercadoria,
                    row.Tipo_Operação_da_Carga,row.Carga_Geral_Acondicionamento,row.ConteinerEstado,
                    row.Tipo_Navegação,row.FlagAutorizacao,row.FlagCabotagem,row.FlagCabotagemMovimentacao,
                    row.FlagConteinerTamanho,row.FlagLongoCurso,row.FlagMCOperacaoCarga,row.FlagOffshore,
                    row.FlagTransporteViaInterioir,row.Percurso_Transporte_em_vias_Interiores,
                    row.Percurso_Transporte_Interiores,row.STNaturezaCarga,row.STSH2,row.STSH4,
                    row.Natureza_da_Carga,row.Sentido,row.TEU,row.QTCarga,row.VLPesoCargaBruta,row.Ano,
                    row.Mes,row.Porto_Atracação,row.SGUF,row.VLPesoCargaConteinerizada)
        #print(f"Registro inserido com sucesso: {index}")
    except Exception as e:
        print(f"Não foi possível inserir o registro {index}. O erro encontrado foi: {e}")
        break
cnxn.commit()
cursor.close()