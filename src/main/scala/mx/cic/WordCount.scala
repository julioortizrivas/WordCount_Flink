package mx.cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import scala.collection.mutable.ListBuffer
import java.io._
/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
  // Esta funcion lista los archivos de la carpeta Data
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  // La siguiente función hace el WordCount
  def contar(archivo: String):DataSet[(String,Int)] = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = env.readTextFile(archivo)
    val conteoPalabras = input.flatMap { _.toLowerCase.split("\\W+") }
      .map { (_,1) }
      .groupBy(0)
      .sum(1)
    conteoPalabras
  }

  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment


    //Listar archivos de un directorio
    val archivos = getListOfFiles(args(0))

    //Leer archivos en el directorio uno por uno y hacer el wordcount
    /*
     * Variables para armar un String al final que se mandara al archivo generado
     * var nombres se usa para guardar los nombres de los archivos
     * var palabrasPorArchivo guarda el total de palabras en cada archivo
     * var totalPalabras guarda el total de palabras contadas en los n archivos
     * var todas guarda las palabras contadas por archivo y el nombre del archivo
     * val nArchivos guarda el numero de archivos que se leeran
     *
     * val separador es solo el separador entre secciones en el archivo
     *
     */

    var nombres = ""
    var palabraPorArchivo = ""
    var totalPalabras = 0
    var todas = ""
    val separador = "\n****************************************************\n"
    val nArchivos = archivos.length

    // Leemos cada archivo
    for(archivo <- archivos){
      nombres += archivo + ", "
      val cuenta = contar(archivo.toString) //Hace el conteo de las palabras al archivo enviado
      palabraPorArchivo += cuenta.sum(1).collect()(0)._2 + ", "
      totalPalabras += cuenta.sum(1).collect()(0)._2
      todas += archivo.toString + "\n" + cuenta.collect() + "\n"
    }

    // Crea el String que será enviado al archivo
    val salida = nombres + " (" + nArchivos + ")" + separador +
                  palabraPorArchivo + separador +
                  totalPalabras + separador +
                  todas

    /*
     * Crea el archivo de texto
     * val writer crea el archivo y abre el escritor del archivo
     * writer.write(salida) escribe en el archivo el String salida
     * writer.close() cierra el escritor del archivo
     */
    val writer = new PrintWriter(new File("WordCount.txt"))
    writer.write(salida)
    writer.close()
  }
}

