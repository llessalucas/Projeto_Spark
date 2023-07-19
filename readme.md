
# Lendo o arquivo CSV
%python
df_supermercado = spark.read.format('csv').options(header='true',inferSchema='true',delimiter=',').load('/FileStore/tables/carga/supermarket_sales___Sheet1.csv')
display(df_supermercado)


# Alterando nome das colunas -  Import col method from pyspark.sql.functions 
from pyspark.sql.functions import col
df_supermercado = df_supermercado.select(col("Invoice ID").alias('ID')
                            ,col("Branch").alias('Filial')
                            ,col("City").alias('Cidade')
                            ,col("Customer Type").alias('Tipo_Cliente')
                            ,col("Gender").alias('Genero')
                            ,col("Product line").alias('Linha_Produto')
                            ,col("Unit price").alias('Preco_Unitario')
                            ,col("Quantity").alias('Quantidade')
                            ,col("Tax 5%").alias('Taxa5')
                            ,col("Total")
                            ,col("Date")
                            ,col("Time")
                            ,col("Payment").alias('Tipo_Pagamento')
                            ,col("cogs").alias('CMV')
                            ,col("gross margin percentage").alias('Percc_Margem_Bruta')
                            ,col("gross income").alias('Receita_Bruta')
                                                       )
                 
 
# Print the dataframe
display(df_supermercado)

# Alterando o valor da coluna Genero e Tipo Cliente
from pyspark.sql.functions import regexp_replace
df= df_supermercado.withColumn("Genero",regexp_replace("Genero","Female","F")).withColumn("Genero",regexp_replace("Genero","Male","M")).withColumn("Tipo_Cliente",regexp_replace("Tipo_Cliente","Member","Associado")).withColumn("Tipo_Cliente",regexp_replace("Tipo_Cliente","Normal","Nao Associado"))
display (df)

# Alterando o nome da linha do produto
from pyspark.sql.functions import regexp_replace
df2= df.withColumn("Linha_Produto",regexp_replace("Linha_Produto","Home and lifestyle","Casa e estilo de vida")).withColumn("Linha_Produto",regexp_replace("Linha_Produto","Fashion accessories","Acessorios de moda")).withColumn("Linha_Produto",regexp_replace("Linha_Produto","Health and beauty","Saude e Beleza")).withColumn("Linha_Produto",regexp_replace("Linha_Produto","Electronic accessories","Acessorios Eletrônicos")).withColumn("Linha_Produto",regexp_replace("Linha_Produto","Food and beverages","Alimentos e Bebidas")).withColumn("Linha_Produto",regexp_replace("Linha_Produto","Sports and travel","Esportes e Viagens"))
display (df2)

# Alterando o nome do Tipo de Pagamento
from pyspark.sql.functions import regexp_replace
df3= df2.withColumn("Tipo_Pagamento",regexp_replace("Tipo_Pagamento","Cash","Dinheiro")).withColumn("Tipo_Pagamento",regexp_replace("Tipo_Pagamento","Ewallet","Carteira Digital")).withColumn("Tipo_Pagamento",regexp_replace("Tipo_Pagamento","Credit card","Cartao de Credito"))
display (df3)

Exportando Tabela
df3.write.saveAsTable("BD_VENDAS")
