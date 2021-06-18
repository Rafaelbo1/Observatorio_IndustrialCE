# Informações do Teste - Observatório IndustrialCE. 


### Questão 1a):

   A decisão de utilização estratégica relacionada a ciência de dados quanto a utilização de um  data lake é diferente da decisão de mesmo fim para o uso do SQL, uma vez que ambos os tipos de bancos de dados podem ser usados em conjunto para produzir os melhores resultados possíveis.
   Olhando para os dados disponibilizados pode-se perceber que a melhor opção de utilização é um banco de dados SQL. Apesar de ter-se muita dados categóricos, o que possibilitaria o uso de um NoSQL, os principais objetivos requeridos são de tratamentos de dados para obter valores int (inteiros) ou flaot(reais) que produzam respostas, insights, ideação, etc, para a otimização de tomadas de decisões a nível operacional, tático e estratégico para as organizações envolvidos.
   Por fim, recomenda-se o uso do SQL e o investimento em um Data lake, o que favorecerá os resultados do time de data science, engenheiros de machine  e engenheiro de dados, como já utilizam diversas empresas no mercado, as quais tem obtido bons resultados com estas tecnologias.

Para as atividade a seguir, disponibilizei um conjunto de bibliotecas/funções naturais de Python que contribuem para o desenvolvimento do que foi solicitado. As bibliotecas estão no requirements.txt
As funções desenvolvidas para uso nos scripts que geram as respostas estão no arquivo functons.py

### Questão 1b):
		
Desenvolva um script em python que extraia os dados do anuário, e transforme-os em duas tabelas fato, atracacao_fato e carga_fato, com as respectivas colunas abaixo(coluna disponíve no PDF enviado).
A base de dados foi tratada com os nomes das variáveis e das categorias originais.
A partir da base de dados, foram desenvolvidas as atividades com a descrição do "passo a passo" nos scripts dos códigos de cada demanda.

#### Tabela atracacao_fato: sript(main.py)
Após o carregamentos dos dados necessários (tabelas: ____Atracacao.txt e ____TemposAtracacao.txt ) a tabela atracacao_fato é criada na variável "table_Atracacao", em seguida o código faz uma conexão com o banco de dados para enviar a tabela para uma tabela no SQL Server com as mesmas colunas da tabela de origem (para enviar para o seu banco de dados basta colocar suas configurações de acesso )

  1 Coloque os dados dentro de uma pasta com o nome de "dados" no mesmo lugar onde estão os scripts "functions.py","main.py" e "main2.py", dentro da pasta criada descompacte as pastas dos arquivos baixados do link que foi enviado.
  
  2 Verifique seu tipo de acesso ao SQL Server, caso possua uma senha para conectar-se, você deve colocar senha e password na função "connection" como recomenda a microsoft em  cnxn = pyodbc.connect()
        
        Driver= 'yourdrive'
        server = 'servername' 
        database = 'AdventureWorks' 
        username = 'yourusername' 
        password = 'databasename'
        
  3 Crie no SQL Server um banco de dados nomeado como "Observatorio_IndustriaCE"
  
  4 Mude o nome do Server='RAFABO\SQLEXPRESS' na linha 23 do "functions.py" para o Server do seu SQL Server
  
  5 Crie uma tabela no SQL Server com os nomes (IDAtracacao,CDTUP,IDBerco,Berço,[Porto Atracação],
                    [Apelido Instalação Portuária],[Complexo Portuário],[Tipo da Autoridade Portuária],
                    [Data Atracação],[Data Chegada],[Data Desatracação],[Data Início Operação],
                    [Data Término Operação],Ano,Mês,[Tipo de Operação],[Tipo de Navegação da Atracação],
                    [Nacionalidade do Armador],FlagMCOperacaoAtracacao,Terminal,Município,UF,SGUF,
                    [Região Geográfica],[Nº da Capitania],[Nº do IMO],TEsperaAtracacao,
                    TEsperaInicioOp,TOperacao,TEsperaDesatracacao,TAtracado,TEstadia)
     Considere o tipo de cada variável, fator importante para o processo de ETL realiado com SQL e python
     
   6 Após isto é so executar o "main.py" com as bibliotecas necessárias já instaladas.
   
   O resuultado é uma tabela com o nome "atracacao_fato" no SQL Server, como solicitado nesta questão do test.
   
#### Tabela carga_fato: sript(main2.py)

   1 Com os passos de 1 a 5 realizados para a criação da tabela anterior, basta seguir para...
   
   2 Crie uma tabela no SQL Server com os nomes (IDCarga,IDAtracacao,Origem,Destino,CDMercadoria,
                    [Tipo Operação da Carga],[Carga Geral Acondicionamento],ConteinerEstado,
                    [Tipo Navegação],FlagAutorizacao,FlagCabotagem,FlagCabotagemMovimentacao,
                    FlagConteinerTamanho,FlagLongoCurso,FlagMCOperacaoCarga,FlagOffshore,
                    FlagTransporteViaInterioir,[Percurso Transporte em vias Interiores],
                    [Percurso Transporte Interiores],STNaturezaCarga,STSH2,STSH4,[Natureza da Carga],
                    Sentido,TEU,QTCarga,VLPesoCargaBruta,Ano,Mês,[Porto Atracação],SGUF,
                    VLPesoCargaConteinerizada)
     Considere o tipo de cada variável, fator importante para o processo de ETL realiado com SQL e python
     
   3 Após isto é so executar o "main2.py" com as bibliotecas necessárias já instaladas.
   
   O resuultado é uma tabela com o nome "carga_fato" no SQL Server, como solicitado nesta questão do test.
   
   
   
   
### Questão 1c):

Criar Tabelas com dados do Ceará, Nordeste e Brasil contendo número de atracações, para cada localidade, bem como tempo de espera para atracar e tempo atracado por meses nos anos de 2018 e 2019. Segundo tabela de referência no PDF.
  
   1 Para esta quesão foi desenvolvido o script "SQL_python.py", de simples execução, uma vez configurado a função "connection" em "functions.py", basta executar o código que ele fara a aquisição da tabela atracacao_fato do SQL Server e construirá as tabelas solicitadas com os nomes "tabela_ceara.xlsx" , "tabela_Ndeste.xlsx" , "tabela_BR.xlsx". Em seguida e de forma automática o código salva as tabelas em arquivos Excel (.xlsx).
  
 ### Questão Bônus:
 
 O tempo não permitiu fazer, porém imaginou-se Apache Airflow com um fluxo simples para a execução dos work's deste desafio com a  DAG:

   1 Load_data >> [criar_atracacao_fatoSQL(main.py) , criar_carga_fatoSQL(main2.py)] >> Criar_tabelasExcel(SQL_python).
