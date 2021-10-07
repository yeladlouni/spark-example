import org.apache.spark.sql.SparkSession

object Covid {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dfCovid = spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .csv("lab/covid_vaccination_vs_death_ratio.csv")

    dfCovid.printSchema()

    dfCovid.createTempView("covid")

    dfCovid
      .groupBy("country")
      .sum("total_vaccinations")
      .show()

    // Première question

    spark
      .sql("SELECT country, sum(total_vaccinations) vaccinted FROM covid GROUP BY country ORDER BY sum(total_vaccinations) DESC")
      .show()

    // Deuxième question

    val top4 = spark
      .sql("SELECT country, sum(total_vaccinations) vaccinted FROM covid GROUP BY country ORDER BY sum(total_vaccinations) DESC LIMIT 4 ")
      .select("country")
      .show(truncate = false)

    // 3e question

    val deaths = spark
      .sql("SELECT country, sum(New_deaths) deaths FROM covid GROUP BY country ORDER BY sum(New_deaths) DESC")
      .show()

    // 4e Question

    val vaccinatedPerMonth = spark
      .sql("SELECT substr(date, 6,2) month, sum(total_vaccinations) vaccinated FROM covid GROUP BY substr(date, 6,2) ORDER BY sum(total_vaccinations) DESC")
      .show()

  }
}
