import org.apache.spark.sql.SparkSession

object Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Example").getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    val df = spark.range(1000000).toDF("Amount");
    println(df.count())
    df.show();


    val dfVideos = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/Users/yassine/videos_by_tag_year.csv")

    dfVideos.createTempView("videos")

    // En utilisant SQL
    val dfTags = spark.sql("SELECT tag, count(*) as occurrences FROM videos GROUP BY tag ORDER BY count(*) DESC")

    dfTags.explain()

    dfTags.write.json("output/tags.json")

    // En utilisant les dataframes

    import org.apache.spark.sql.functions._

    dfVideos
      .groupBy("tag")
      .count()
      .write
      .csv("output/tags.csv")




  }
}
