from pyspark.sql import SparkSession 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
from array import array
import pandas as pd




scSpark = SparkSession \
    .builder \
    .appName("Ejercicio 1") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sdfData = scSpark.read.csv("/home/oswald/data/all_data.csv", header=True, sep=",")
sdfData.createOrReplaceTempView("profeco")


#Conteo de registros
conteoTotal = scSpark.sql("SELECT count(1) from profeco")
conteoTotal.show()


#Conteo de categorías
conteoCategorias = scSpark.sql("SELECT categoria, count(distinct producto) as conteo from profeco group by categoria order by conteo asc")
conteoCategorias = conteoCategorias.toPandas()


ax = conteoCategorias.plot.barh(x='categoria', y='conteo', legend=True, rot = 0)
plt.tight_layout()
plt.rcParams['legend.title_fontsize'] = 'xx-small'
plt.savefig('categoriasBarChart.png')


#Cadenas comerciales monitoreadas
conteoComercial = scSpark.sql("SELECT cadenaComercial, count(distinct producto) as conteo from profeco group by cadenaComercial")
conteoComercial = conteoComercial.toPandas()
conteoComercial
ax = conteoComercial.plot.barh(x='cadenaComercial', y='conteo', legend=True, rot = 0)
plt.tight_layout()
plt.rcParams['legend.title_fontsize'] = 'xx-small'
plt.savefig('comercialBarChart.png')

conteoComercial.head()

#Productos mas monitoreados por cadena
conteoProducto = scSpark.sql("SELECT estado, producto, count(1) as conteo from profeco group by estado, producto order by conteo desc")
conteoProducto = conteoProducto.toPandas()

conteProductoEstado = conteoProducto.groupby('estado').head(5)


fecha = scSpark.sql("SELECT count (distinct fechaRegistro) as fecha from profeco")
fecha = fecha.toPandas()

