import org.apache.spark.sql.SparkSession
import java.util.Properties

object DenormalizeTables {
  def main(args: Array[String]): Unit = {
    // Инициализация SparkSession
    val spark = SparkSession.builder()
      .appName("Denormalize Tables")
      .master("local[*]") // Используем все доступные ядра
      .getOrCreate()

    // Параметры подключения к MySQL
    val jdbcUrl = "jdbc:mysql://localhost:3306/spark"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "Ig04Nat30")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // Создание таблиц в MySQL
    createTables(spark, jdbcUrl, connectionProperties)

    // Денормализация данных
    denormalizeData(spark, jdbcUrl, connectionProperties)

    // Остановка SparkSession
    spark.stop()
  }

  def createTables(spark: SparkSession, jdbcUrl: String, connectionProperties: Properties): Unit = {
    // Создание таблицы advertisers
    spark.read
      .jdbc(jdbcUrl, """
    (SELECT 1 AS advertiser_id, 'Advertiser A' AS advertiser_name UNION ALL
     SELECT 2 AS advertiser_id, 'Advertiser B' AS advertiser_name UNION ALL
     SELECT 3 AS advertiser_id, 'Advertiser C' AS advertiser_name) AS tmp
  """, connectionProperties)
      .write
      .mode("overwrite")
      .jdbc(jdbcUrl, "advertisers", connectionProperties)

    // Создание таблицы campaigns
    spark.read
      .jdbc(jdbcUrl, """
        (SELECT 1 AS campaign_id, 1 AS advertiser_id, 'Campaign 1' AS campaign_name UNION ALL
         SELECT 2 AS campaign_id, 1 AS advertiser_id, 'Campaign 2' UNION ALL
         SELECT 3 AS campaign_id, 2 AS advertiser_id, 'Campaign 3') AS tmp
      """, connectionProperties)
      .write
      .mode("overwrite")
      .jdbc(jdbcUrl, "campaigns", connectionProperties)

    // Создание таблицы sales
    spark.read
      .jdbc(jdbcUrl, """
        (SELECT 1 AS sale_id, 1 AS campaign_id, 100 AS sale_amount UNION ALL
         SELECT 2 AS sale_id, 1 AS campaign_id, 200 UNION ALL
         SELECT 3 AS sale_id, 2 AS campaign_id, 150 UNION ALL
         SELECT 4 AS sale_id, 3 AS campaign_id, 300) AS tmp
      """, connectionProperties)
      .write
      .mode("overwrite")
      .jdbc(jdbcUrl, "sales", connectionProperties)
  }

  def denormalizeData(spark: SparkSession, jdbcUrl: String, connectionProperties: Properties): Unit = {
    // SQL-запрос для денормализации данных
    val query = """
      SELECT
          a.advertiser_id,
          a.advertiser_name,
          COUNT(DISTINCT c.campaign_id) AS total_campaigns,
          COUNT(s.sale_id) AS total_sales,
          COALESCE(SUM(s.sale_amount), 0) AS total_sale_amount
      FROM
          advertisers a
      LEFT JOIN
          campaigns c ON a.advertiser_id = c.advertiser_id
      LEFT JOIN
          sales s ON c.campaign_id = s.campaign_id
      GROUP BY
          a.advertiser_id, a.advertiser_name
    """

    // Выполнение запроса и сохранение результатов
    val denormalizedDF = spark.read
      .jdbc(jdbcUrl, s"($query) AS tmp", connectionProperties)

    denormalizedDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "advertiser_summary", connectionProperties)

    // Показать результат
    denormalizedDF.show()
  }
}