package examen_estructura

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._



object Examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
    // Ejercicio1
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {

      // Muestra el esquema del DataFrame:
      estudiantes.printSchema()

      // Filtrar estudiantes con calificación > 8 (side effect demo):
      val filtrados = estudiantes.filter(col("calificacion")> 8)
      filtrados.show()

      // Seleccionar nombres y ordenarlos en forma descendente
      val nombreDesc = estudiantes
        .select("nombre")
        .orderBy(col("calificacion").desc)


      // Mostrar resultados
      nombreDesc.show()

      nombreDesc

    }


  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._


    // Defino UDF
    val udfParOImpar = udf((n: Int) => if  (n % 2 == 0) "Par" else "Impar")

    //aplicamos udf, covierto el dataset a [String] y luego lo convierto a una lista
    numeros
      .withColumn("par_o_impar", udfParOImpar(col("numero")))
      .select("par_o_impar")
  }


  /** Ejercicio 3: Joins y agregaciones
   * Pregunta: Dado dos DataFrames,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante")) // Se declara la columna sobre la cual
                                                                                   // Se va a llevar a cabo el join
      .groupBy(estudiantes("id"), estudiantes("nombre"))  // Agrupa el dataframe por las columnas "id" y "nombre" del
                                                          // dataframe "estudiantes"
      .agg(avg("calificacion").as("promedio"))  // Se aplica una agregación de promedio sobre la columna
                                                                  // "calificacion", y llama la columna resultante
                                                                  // "promedio"
      .orderBy($"id")                                    // ordena el dataframe resultante por la columna "id"


  }


  /** Ejercicio 4: Uso de RDDs
   * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */

  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {

    val rdd = spark.sparkContext.parallelize(palabras) // convierte la lista en un RDD

    val palabrasContadas = rdd
      .map(palabra => (palabra,1)) // realiza un map sobre el RDD, y asigna a cada palabra el valor de llave 1
      .reduceByKey(_ + _) // suma los valores de llave sobre la misma palabra

    palabrasContadas
  }
  /**
   * Ejercicio 5: Procesamiento de archivos
   * Pregunta: Carga un archivo CSV que contenga información sobre
   * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(path: String)(implicit spark: SparkSession): DataFrame = {

  import spark.implicits._

    val ventas = spark.read
      .option("header","true") // añadimos las cabeceras de las columnas
      // .option("inferSchema","false") // detecta los tipos de columna (lo comento según la recomendación del profesor,
                                        // aunque al contrario de a él, este cédigo no pasa el test con o sin esta linea)
      .option("delimiter",",") // detecta el delimitador elegido
      .csv(path)

    ventas
      .withColumn("ingreso total", col("cantidad") * col("precio_unitario"))
      .groupBy($"id_producto")
      .agg(sum("ingreso total").as("ingreso total"))
  }

}
