import org.apache.spark.sql.SparkSession
import java.util.Properties
import scala.util.Random

object CustomerCountryAnalysis {
  def main(args: Array[String]): Unit = {
    // Инициализация SparkSession
    val spark = SparkSession.builder()
      .appName("Customer Country Analysis")
      .master("local[*]") // Используем все доступные ядра
      .getOrCreate()

    // Параметры подключения к MySQL
    val jdbcUrl = "jdbc:mysql://localhost:3306/spark"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "Ig04Nat30")
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // Генерация случайных данных для таблицы countries
    val countriesData = (1 to 10).map { i =>
      (i, s"Country ${('A' + i - 1).toChar}", s"Region ${i % 3 + 1}", Random.nextInt(10000000))
    }.toList

    // Генерация случайных данных для таблицы customers
    val customersData = (1 to 10).map { i =>
      (i, s"Customer ${('A' + i - 1).toChar}", Random.nextInt(10) + 1)
    }.toList

    // Создание DataFrame для countries
    import spark.implicits._
    val countriesDF = countriesData.toDF("country_id", "country_name", "region", "population")

    // Создание DataFrame для customers
    val customersDF = customersData.toDF("customer_id", "customer_name", "country_id")

    // Запись данных в таблицы MySQL
    countriesDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "countries", connectionProperties)

    customersDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "customers", connectionProperties)

    // Объединение данных
    val summaryDF = customersDF
      .join(countriesDF, "country_id") // Объединяем customers и countries
      .select(
        "customer_id",
        "customer_name",
        "country_id",
        "country_name",
        "region",
        "population"
      )

    // Сохранение результатов в таблицу customer_summary
    summaryDF.write
      .mode("overwrite") // Перезаписываем таблицу, если она существует
      .jdbc(jdbcUrl, "customer_summary", connectionProperties)

    // Показать результат
    println("Таблица customers:")
    customersDF.show()

    println("Таблица countries:")
    countriesDF.show()

    println("Денормализованная таблица customer_summary:")
    summaryDF.show()

    // Остановка SparkSession
    spark.stop()
  }
}