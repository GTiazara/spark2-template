package sql_practice

import org.apache.spark.sql.functions.{explode, _}
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demoFrDF = spark.read.json("data/input/demographie_par_commune.json");

    val depFrDF = spark.read.csv("data/input/departements.txt")


    println(demoFrDF
      .agg(sum("Population"))
      .show()
    )

    println(demoFrDF.groupBy("Departement")
      .agg(sum("Population")
        .as("sum_pop"))
      .orderBy($"sum_pop".desc).show()
    )

    println(demoFrDF.groupBy("Departement")
      .agg(sum("Population")
        .as("sum_pop"))
      .orderBy($"sum_pop".desc).join(depFrDF, demoFrDF("Departement") === depFrDF("_c1"), "inner" ).show()
    )

  }


  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._


    val s07 = spark.read.format("csv")
      .option("header", false)
      .option("sep", "\t")
      .load("data/input/sample_07")
    .toDF("id", "description07", "number07", "salary07");

    val s08 = spark.read.format("csv")
      .option("header", false)
      .option("sep", "\t")
      .load("data/input/sample_08")
      .toDF("id", "description08", "number08", "salary08");


    println(s07.show()
    )

    println(s07.select("salary07", "description07").where(col("salary07")>1000000).orderBy($"salary07".desc).show()
    )

    println(
      s07.join(s08, s07("id") === s08("id"), "inner")
        .select(col("description07"), (col("salary08") - col("salary07")).as("Growth"))
        .where((col("salary08") - col("salary07"))>0).show()
    )

    println(
      s07.join(s08, s07("id") === s08("id"), "inner")
        .select(col("description07"), (col("salary08") - col("salary07")).as("Growth"), (col("number08") - col("number07")).as("Jobloss"))
        .where((col("salary08") - col("salary07")) > 0 and (col("number08") - col("number07"))<0).show()
    )
  }

  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)



  }

}
