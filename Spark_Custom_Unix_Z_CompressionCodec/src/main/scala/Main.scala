object Main {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Custom Spark")
      .config("spark.hadoop.io.compression.codecs", "com.custom.compressioncodec.ZzipCodec")
      .getOrCreate()

    val f = this.getClass.getResource("/compressedFile/Source_CSVFile.csv.Z")
    val df = spark.read.csv(f.getPath)
    df.show(10)
  }

}
