from pyspark.sql.functions import from_json
from pyspark.sql.types import LongType, StructType, StructField, StringType, FloatType, TimestampType, IntegerType, LongType

bucket = "projebitirme"
spark.conf.set("temporaryGcsBucket", bucket)
spark.conf.set("parentProject", "inspiring-bonus-396515")

kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "104.198.214.251:9092").option("subscribe","ornek").load()

schema = StructType([
     StructField("pair", StringType()), 
     StructField("pairNormalized", StringType()),
     StructField("timestamp", LongType()),
     StructField("last", FloatType()),
     StructField("high", FloatType()),
     StructField("low", FloatType()),
     StructField("bid", FloatType()),   
     StructField("ask", FloatType()),
     StructField("open", FloatType()),
     StructField("volume", FloatType()),
     StructField("average", FloatType()),
     StructField("daily", FloatType()),
     StructField("dailyPercent", FloatType()),
     StructField("denominatorSymbol", StringType()),
     StructField("numeratorSymbol", StringType())
])


activationDF = kafkaDF.select(from_json(kafkaDF["value"].cast("string"), schema).alias("activation"))
