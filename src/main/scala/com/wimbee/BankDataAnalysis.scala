import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.udf
import org.jfree.chart.ChartFactory
import org.jfree.chart.ChartPanel
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
import javax.swing.JFrame

case class Transaction(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)
case class CustomerTransactionFrequency(customer_id: Int, transaction_frequency: Long)
case class TransactionPattern(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

object BankDataAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Load CSV Data")
      .master("local")
      .getOrCreate()

    val transactions = loadDataFromCSV(spark, "src/main/resources/transactions.csv")
    val optimizedData = optimizePerformance(transactions)
    exploreDataset(transactions)
    val filteredData = handleMissingData(transactions)
    exploreDataset(filteredData)
    calculateBasicStatistics(filteredData)
    println("**************************************************************")
    val transactionFrequencyDF = customerTransactionFrequency(filteredData)
    transactionFrequencyDF.show()
    println("*******************************************************************")
    val dailyData = groupByTransactionDate(filteredData, "day")
    val monthlyData = groupByTransactionDate(filteredData, "month")
    dailyData.show()
    monthlyData.show()
    plotTransactionTrends(filteredData, "day")
    println("****************************************************************")
    val segmentedCustomersDF = customerSegmentation(filteredData)
    segmentedCustomersDF.show()
    val avgTransactionAmountDF = calculateAvgTransactionAmount(filteredData, segmentedCustomersDF)
    avgTransactionAmountDF.show()
    val transactionPatternsDF = identifyTransactionPatterns(filteredData)
    transactionPatternsDF.show()
    val transactionPatterns = identifyTransactionPatterns(filteredData)
    visualizeTransactionPatterns(transactionPatterns)
    val fraudulentTransactions = detectFraudulentTransactions(filteredData)
    fraudulentTransactions.show()
  }

  def loadDataFromCSV(spark: SparkSession, filename: String): DataFrame = {
    val dataDF = spark.read.option("header", "true").option("inferSchema", "true").csv(filename)
    dataDF
  }

  def exploreDataset(data: DataFrame): Unit = {
    data.printSchema()
    data.show()
  }

  def handleMissingData(data: DataFrame): DataFrame = {
    data.filter(col("amount") > 0 && col("transaction_date").isNotNull && col("transaction_type").isNotNull && col("customer_id").isNotNull)
  }

  def calculateBasicStatistics(data: DataFrame): Unit = {
    val totalDeposits = data.filter(col("transaction_type") === "deposit").agg(sum("amount")).head().getDouble(0)
    val totalWithdrawals = data.filter(col("transaction_type") === "withdrawal").agg(sum("amount")).head().getDouble(0)
    val averageTransactionAmount = data.agg(avg("amount")).head().getDouble(0)
    println(s"Total Deposits: $totalDeposits")
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

  def customerTransactionFrequency(data: DataFrame): DataFrame = {
    val transactionFrequencyDF = data.groupBy("customer_id")
      .agg(count("*").alias("transaction_frequency"))
    transactionFrequencyDF
  }

  def parseDate(dateStr: String): LocalDate = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]")
    LocalDate.parse(dateStr, formatter)
  }


  def groupByTransactionDate(data: DataFrame, timeUnit: String): DataFrame = {
    val spark = data.sparkSession
    import spark.implicits._
    val parseDateUDF = udf(parseDate _)
    val dataWithTimeUnit = data.withColumn("transaction_date_parsed", parseDateUDF($"transaction_date"))
      .withColumn("transaction_date_truncated", date_trunc(timeUnit, $"transaction_date_parsed"))
    val aggregatedData = dataWithTimeUnit.groupBy("transaction_date_truncated")
      .agg(count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        min("amount").alias("min_amount"),
        max("amount").alias("max_amount"))
    aggregatedData
  }

  def plotTransactionTrends(data: DataFrame, timeUnit: String): Unit = {
    val spark = data.sparkSession
    import spark.implicits._
    val parseDateUDF = udf(parseDate _)
    val dataWithTimeUnit = data.withColumn("transaction_date_parsed", parseDateUDF($"transaction_date"))
      .withColumn("transaction_date_truncated", date_trunc(timeUnit, $"transaction_date_parsed"))
    val aggregatedData = dataWithTimeUnit.groupBy("transaction_date_truncated", "transaction_type")
      .agg(sum("amount").alias("transaction_amount"))
    val pivotedData = aggregatedData.groupBy("transaction_date_truncated")
      .pivot("transaction_type", Seq("deposit", "withdrawal"))
      .agg(coalesce(sum("transaction_amount"), lit(0.0)))
    val timeColumn = timeUnit match {
      case "day" => $"transaction_date_truncated".cast("string")
      case "month" => date_format($"transaction_date_truncated", "yyyy-MM")
    }
    val depositSeries = pivotedData.select(timeColumn, col("deposit"))
      .withColumnRenamed("deposit", "Deposits")
      .orderBy(timeColumn)
      .collect()
      .map(row => (row.getAs[String](0), row.getAs[Double]("Deposits")))
    val withdrawalSeries = pivotedData.select(timeColumn, col("withdrawal"))
      .withColumnRenamed("withdrawal", "Withdrawals")
      .orderBy(timeColumn)
      .collect()
      .map(row => (row.getAs[String](0), row.getAs[Double]("Withdrawals")))
    val dataset = new DefaultCategoryDataset()
    depositSeries.foreach { case (date, depositAmount) =>
      dataset.addValue(depositAmount, "Deposits", date)
    }
    withdrawalSeries.foreach { case (date, withdrawalAmount) =>
      dataset.addValue(withdrawalAmount, "Withdrawals", date)
    }
    val chart = ChartFactory.createLineChart("Transaction Trends", "Date", "Amount", dataset, PlotOrientation.VERTICAL, true, true, false)
    val frame = new JFrame("Transaction Trends")
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.add(new ChartPanel(chart))
    frame.pack()
    frame.setVisible(true)
  }

  def customerSegmentation(data: DataFrame): DataFrame = {
    val spark = data.sparkSession
    import spark.implicits._
    val customerSummary = data.groupBy("customer_id")
      .agg(
        sum("amount").alias("total_transaction_amount"),
        count("*").alias("transaction_frequency")
      )
    val highValueAmountThreshold = 1000.0
    val frequentTransactionThreshold = 10
    val segmentLabelUDF = udf((totalAmount: Double, frequency: Long) => {
      if (totalAmount >= highValueAmountThreshold && frequency >= frequentTransactionThreshold) "High-Value & Frequent"
      else if (totalAmount >= highValueAmountThreshold) "High-Value"
      else if (frequency >= frequentTransactionThreshold) "Frequent"
      else "Inactive"
    })
    val segmentedCustomers = customerSummary.withColumn("segment", segmentLabelUDF($"total_transaction_amount", $"transaction_frequency"))
    segmentedCustomers.select("customer_id", "segment")
  }

  def calculateAvgTransactionAmount(data: DataFrame, segments: DataFrame): DataFrame = {
    val dataWithSegment = data.join(segments, Seq("customer_id"))
    val avgTransactionAmountDF = dataWithSegment.groupBy("segment")
      .agg(avg("amount").alias("avg_transaction_amount"))
    avgTransactionAmountDF
  }

  def identifyTransactionPatterns(data: DataFrame): DataFrame = {
    val spark = data.sparkSession
    import spark.implicits._
    val parseDateUDF = udf(parseDate _)
    val dataWithParsedDate = data.withColumn("transaction_date_parsed", parseDateUDF($"transaction_date"))
    val significantDepositWithdrawalData = dataWithParsedDate.filter(
      (col("transaction_type") === "deposit" && col("amount") >= 1000.0) ||
        (col("transaction_type") === "withdrawal" && col("amount") >= 1000.0)
    )
    val windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date_parsed")
    val consecutiveTransactionData = significantDepositWithdrawalData.withColumn(
      "prev_transaction_type",
      lag("transaction_type", 1).over(windowSpec)
    ).withColumn(
      "prev_amount",
      lag("amount", 1).over(windowSpec)
    )
    val transactionPatternsDF = consecutiveTransactionData.filter(
      col("transaction_type") === "withdrawal" &&
        col("prev_transaction_type") === "deposit" &&
        col("amount") >= 1000.0 &&
        col("prev_amount") >= 1000.0
    ).select(
      $"customer_id",
      $"transaction_date",
      $"prev_transaction_type".alias("previous_transaction_type"),
      $"prev_amount".alias("previous_amount"),
      $"transaction_type",
      $"amount"
    )
    transactionPatternsDF
  }

  def visualizeTransactionPatterns(data: DataFrame): Unit = {
    val spark = data.sparkSession
    import spark.implicits._
    val parseDateUDF = udf(parseDate _)
    val dataWithParsedDate = data.withColumn("transaction_date_parsed", parseDateUDF($"transaction_date"))
    val aggregatedData = dataWithParsedDate.groupBy("customer_id", "transaction_date_parsed")
      .agg(sum(when($"transaction_type" === "deposit", $"amount").otherwise(0)).alias("deposit_amount"),
        sum(when($"transaction_type" === "withdrawal", $"amount").otherwise(0)).alias("withdrawal_amount"))

    val depositSeries = aggregatedData.select($"transaction_date_parsed", $"deposit_amount")
      .orderBy($"transaction_date_parsed")
      .collect()
      .map(row => (row.getAs[LocalDate](0), row.getAs[Double]("deposit_amount")))

    val withdrawalSeries = aggregatedData.select($"transaction_date_parsed", $"withdrawal_amount")
      .orderBy($"transaction_date_parsed")
      .collect()
      .map(row => (row.getAs[LocalDate](0), row.getAs[Double]("withdrawal_amount")))
    val dataset = new DefaultCategoryDataset()

    depositSeries.foreach { case (date, depositAmount) =>
      dataset.addValue(depositAmount, "Deposits", date)
    }
    withdrawalSeries.foreach { case (date, withdrawalAmount) =>
      dataset.addValue(withdrawalAmount, "Withdrawals", date)
    }
    val chart = ChartFactory.createLineChart(
      "Transaction Patterns",
      "Date",
      "Amount",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )
    val frame = new JFrame("Transaction Patterns")
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.add(new ChartPanel(chart))
    frame.pack()
    frame.setVisible(true)
  }

  def detectFraudulentTransactions(data: DataFrame): DataFrame = {
    val withdrawalThreshold = 1000.0
    val flaggedData = data.withColumn("is_fraudulent",
      when(col("transaction_type") === "withdrawal" && col("amount") >= withdrawalThreshold, true)
        .otherwise(false))
    flaggedData
  }

  def optimizePerformance(data: DataFrame): DataFrame = {
    val tunedSpark = SparkSession.builder()
      .appName("Optimize Performance")
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "4")
      .config("spark.executor.instances", "2")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()
    val cachedData = data.cache()
    cachedData.count()
    val repartitionedData = cachedData.repartition(200)
    repartitionedData
  }

}
