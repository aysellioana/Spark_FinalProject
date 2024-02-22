import org.apache.log4j._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.sql.Timestamp

object Amounts {

  case class Warehouse(id: Long, warehouse: String, product: String, eventTime: Timestamp)
  case class Amount(id: Long, amount: BigDecimal, eventTime: Timestamp )

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

      //spark session
    val spark = SparkSession
      .builder
      .appName("Warehouse")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val warehouseSchema = new StructType()
      .add("id", LongType, nullable = true)
      .add("warehouse", StringType, nullable = true)
      .add("product", StringType, nullable = true)
      .add("eventTime", LongType, nullable = true)
    println(warehouseSchema)

    val amountSchema = new StructType()
      .add("id", LongType, nullable = true)
      .add("amount", DecimalType(10, 2), nullable = true)
      .add("eventTime", LongType, nullable = true)



    val warehouseDS = spark.read
      .schema(warehouseSchema)
      .csv("data/warehouse.csv")
      .as[Warehouse]


    val amount = spark.read
      .schema(amountSchema)
      .csv("data/amount.csv")
      .as[Amount]

    val warehouse = warehouseDS.toDF().withColumnRenamed("eventTime", "eventTimeWarehouse")
    amount.show()

    val joinedDF = warehouse.join(amount, Seq("id"))
    joinedDF.show()

    import org.apache.spark.sql.functions._

    val currentAmountWithMaxTime = joinedDF.select("id", "warehouse", "product", "amount", "eventTime")
      .filter(warehouse("eventTimeWarehouse") <= amount("eventTime"))
      .groupBy("id", "warehouse", "product")
      .agg(
        max(amount("eventTime")).alias("currentEventTime"),
      )
      .orderBy("id")

    val finalCurrentAmount = currentAmountWithMaxTime.join(amount.as("amountAlias"),
        currentAmountWithMaxTime("id") === amount("id") &&
          currentAmountWithMaxTime("currentEventTime") === amount("eventTime"))
      .select(currentAmountWithMaxTime("id"), currentAmountWithMaxTime("warehouse"),
        currentAmountWithMaxTime("product"),
        $"amountAlias.amount").orderBy("id")


    println("# current amount for each position, warehouse, product")
    //currentAmountWithMaxTime.show()
    finalCurrentAmount.show()

    val maxAmount = amount.join(warehouse, Seq("id"))
      .groupBy($"warehouse", $"product")
      .agg(max($"amount").alias("max_amount")).orderBy("warehouse")

    println("Max amount for each warehouse and product:")
    maxAmount.show()

    val minAmount = amount.join(warehouse, Seq("id"))
      .groupBy($"warehouse", $"product")
      .agg(min($"amount").alias("max_amount")).orderBy("warehouse")

    println("Min amount for each warehouse and product:")
    minAmount.show()

    val avgAmount = amount.join(warehouse, Seq("id"))
      .groupBy($"warehouse", $"product")
      .agg(round(avg($"amount"),3).alias("max_amount")).orderBy("warehouse")

    println("Avg amount for each warehouse and product:")
    avgAmount.show()


    println("Max, min, avg amounts for each warehouse and product:")
    maxAmount.join(minAmount, Seq("warehouse", "product"))
      .join(avgAmount, Seq("warehouse", "product")).orderBy("warehouse", "product")
      .show()

  }
}
