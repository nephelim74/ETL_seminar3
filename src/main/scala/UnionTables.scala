import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties

object SalesAnalysis {
  def main(args: Array[String]): Unit = {
    // Инициализация SparkSession
    val spark = SparkSession.builder()
      .appName("Sales Analysis")
      .master("local[*]") // Используем все доступные ядра
      .getOrCreate()

    // Параметры подключения к MySQL
    val jdbcUrl = "jdbc:mysql://localhost:3306/spark"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "Ig04Nat30")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // Чтение данных из таблиц
    val advertisersDF = spark.read.jdbc(jdbcUrl, "advertisers", connectionProperties)
    val campaignsDF = spark.read.jdbc(jdbcUrl, "campaigns", connectionProperties)
    val salesDF = spark.read.jdbc(jdbcUrl, "sales", connectionProperties)

    // Агрегация данных
    val summaryDF = advertisersDF
      .join(campaignsDF, "advertiser_id") // Объединяем advertisers и campaigns
      .join(salesDF, "campaign_id") // Объединяем с sales
      .groupBy("advertiser_id", "advertiser_name") // Группируем по рекламодателю
      .agg(
        countDistinct("campaign_id").as("total_campaigns"), // Количество уникальных кампаний
        count("sale_id").as("total_sales"), // Общее количество продаж
        sum("sale_amount").as("total_sale_amount") // Общая сумма продаж
      )

    // Сохранение результатов в таблицу advertiser_summary
    summaryDF.write
      .mode("overwrite") // Перезаписываем таблицу, если она существует
      .jdbc(jdbcUrl, "advertiser_summary", connectionProperties)

    // Показать результат
    summaryDF.show()

    // Остановка SparkSession
    spark.stop()
  }
}