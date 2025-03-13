import org.apache.spark.sql.SparkSession

object MinimumWordCountBatch {
  def main(args: Array[String]): Unit = {
    // Iniciar sesión de Spark
    val spark = SparkSession.builder()
      .appName("MinimumWordCountBatch")
      .getOrCreate()

    // Cargar el archivo de texto
    val inputDF = spark.read.text("P5_spark_batch/books")

    // Contar palabras y filtrar aquellas que aparecen más de 100 veces
    import spark.implicits._
    val wordCounts = inputDF
      .flatMap(_.getString(0).split("\\W+"))  // Dividir texto en palabras
      .filter(_.nonEmpty)                      // Filtrar palabras no vacías
      .groupByKey(_.toLowerCase)               // Agrupar por palabra (ignorando mayúsculas)
      .count()                                 // Contar la frecuencia de cada palabra
      .filter(_._2 > 100)                      // Filtrar palabras que aparecen más de 100 veces

    // Calcular la suma total de las frecuencias de estas palabras
    val totalSum = wordCounts
      .map(_._2)                               // Obtener la frecuencia de cada palabra
      .reduce(_ + _)                           // Sumar todas las frecuencias

    // Imprimir el resultado
    println("La suma total de las palabras que aparecen más de 100 veces es: " + totalSum)

    // Detener la sesión de Spark
    spark.stop()
  }
}

