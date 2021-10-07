import org.apache.spark.sql.SparkSession

object Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Example").getOrCreate();
    val df = spark.range(1000000).toDF("Amount");
    df.show();
  }
}
