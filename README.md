# Documentação do Projeto de Integração de Dados com Spark e Databricks
1. Introdução
Este documento descreve o processo de integração e análise de dados utilizando Apache Spark e Databricks. Os dados originam-se de diferentes fontes e são combinados para responder a várias perguntas de negócios.

2. Fontes de Dados
JDBC: uma fonte de dados relacional contendo informações de usuários.
Arquivos Parquet: contêm informações sobre apartamentos e reservas.
3. Etapas de Processamento
3.1. Carregamento de Dados
   ![WhatsApp Image 2023-08-28 at 11 02 50](https://github.com/alexandrepino/Teste_Pyspark/assets/60200989/562e4e95-22c1-49c4-8997-bd1988ac9f6f)

Os dados dos usuários foram carregados a partir de uma fonte JDBC.
Os dados sobre apartamentos e reservas foram carregados a partir de arquivos Parquet.
3.2. Transformações e Análises
![WhatsApp Image 2023-08-28 at 11 03 19](https://github.com/alexandrepino/Teste_Pyspark/assets/60200989/7f55bf91-3737-4990-af96-94e794e5d1a4)
![WhatsApp Image 2023-08-28 at 11 03 55](https://github.com/alexandrepino/Teste_Pyspark/assets/60200989/aba42d61-b5c4-41d9-85cc-a8884ddf6efd)
![WhatsApp Image 2023-08-28 at 11 04 05](https://github.com/alexandrepino/Teste_Pyspark/assets/60200989/fef7fff4-2afa-4de6-8bcc-ec67f88765ad)

Os dados foram combinados usando operações de join.
Foi realizada a análise para identificar o país com o maior número de itens cancelados.
Foi realizada a análise para determinar o faturamento da linha de produto mais vendida em 2005.
4. Resultados
Os resultados das análises foram salvos em formatos específicos, como DataFrames Spark, e foram feitas conversões para o formato Delta para otimização e consulta mais eficiente.

5. Integração com GitHub
Foi discutida a possibilidade de integrar diretamente o Databricks com o GitHub.
Foram abordados potenciais erros e soluções durante essa integração.
6. Conclusão e Próximos Passos
Este projeto demonstrou a capacidade de integrar, transformar e analisar dados usando Spark e Databricks. A integração direta com o GitHub pode ser explorada mais a fundo para otimizar o processo de versionamento de código.

