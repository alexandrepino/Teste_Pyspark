from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Inicializando a Spark Session
spark = SparkSession.builder \
    .appName("Spark PostgreSQL JDBC Example") \
    .getOrCreate()

# Defina suas informações de conexão JDBC
database_url = "jdbc:postgresql://psql-mock-database-cloud.postgres.database.azure.com:5432/booking1692887182201ysjfejewsglhoucg"
database_properties = {
    "user": "tazxhuoqsmqehhxjalbdvsuj@psql-mock-database-cloud",
    "password": "ygxquvphswjbglbiugcuwdxt",
    "driver": "org.postgresql.Driver"
}

# Definindo o diretório pai
base_path = "dbfs:/your/desired/directory/"

# Lista das tabelas para salvar
tables = ["worker", "poc_worker", "gceuser_report", "company", "worker_type", "address", "users", "appartments", "bookings"]

# Iterando sobre cada tabela, lendo do banco e salvando como Parquet
for table in tables:
    df = spark.read.jdbc(url=database_url, table=table, properties=database_properties)
    df.write.parquet(base_path + table + "/")
base_path = "dbfs:/your/desired/directory/"

# Lista das tabelas para realizar o merge
tables = ["worker", "poc_worker", "gceuser_report", "company", "worker_type", "address", "users", "appartments", "bookings"]

# Mapeando tabelas para suas colunas de chave primária
primary_keys = {
    "worker": "id",
    "poc_worker": "id",
    "gceuser_report": "gceuser_report_id",
    "company": "id",
    "worker_type": "id",
    "address": "id",
    "users": "id",
    "appartments": "id",
    "bookings": "id"
}

# Dicionário para armazenar os DataFrames após o merge
merged_dataframes = {}

# Iterando sobre cada tabela, lendo do banco e realizando o merge com os dados salvos em Parquet
for table in tables:
    # Identificando a chave primária da tabela atual
    primary_key = primary_keys[table]
    
    # Ler a tabela do PostgreSQL
    jdbc_df = spark.read.jdbc(url=database_url, table=table, properties=database_properties)
    
    # Ler os arquivos Parquet salvos anteriormente
    parquet_df = spark.read.parquet(base_path + table + "/")
    
    # Registros que existem em ambos os DataFrames
    updates = jdbc_df.alias("jdbc").join(parquet_df.alias("parquet"), primary_key).select(["jdbc.*"])
    
    # Registros que só existem no DataFrame do JDBC
    inserts = jdbc_df.alias("jdbc").join(parquet_df.alias("parquet"), primary_key, "left_anti").select(["jdbc.*"])
    
    # Registros que só existem no DataFrame Parquet (aqui você pode decidir se deseja excluir estes ou mantê-los)
    only_in_parquet = parquet_df.join(jdbc_df, primary_key, "left_anti")
    
    # Combinando os DataFrames
    merged_df = updates.union(inserts).union(only_in_parquet)
    
    # Adicionando o DataFrame merged ao dicionário
    merged_dataframes[table] = merged_df

# Caminhos para os dados
bookings_path = base_path + "bookings/"
appartments_path = base_path + "appartments/"

# Carregar os DataFrames
bookings_df = spark.read.parquet(bookings_path)
appartments_df = spark.read.parquet(appartments_path)

# Filtrar os registros que foram cancelados
cancelled_bookings = bookings_df.filter(col("confirmed") == 1)

# JOIN entre bookings e appartments usando a chave correta
joined_df = cancelled_bookings.join(appartments_df, cancelled_bookings.apartment_id == appartments_df.id)

# Agrupar por país e contar as reservas canceladas
country_counts = joined_df.groupBy("country").agg(count("apartment_id").alias("cancelled_count"))

# Ordenar em ordem decrescente de contagem e pegar o país no topo
top_country = country_counts.orderBy(col("cancelled_count").desc()).first()

print(f"O país com a maior quantidade de itens cancelados é {top_country['country']} com {top_country['cancelled_count']} itens cancelados.")

# Caminho onde você deseja salvar o formato Delta
delta_path = "/path/to/your/delta/directory"

# Salvar o merged_df no formato Delta
merged_df.write.format("delta").mode("overwrite").save(delta_path)
