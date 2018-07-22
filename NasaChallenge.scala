/*
 * Autor: Douglas T. S. Leite
 * Data: 22/07/2018
 * Classe responsável por extrair as informações dos arquivos de log e apresentá-las
*/

package com.nasachallenge.spark

import java.time.format.DateTimeFormatter
import java.time.LocalDate
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import com.nasachallenge.spark.ParseLog

object NasaChallenge {
  
  private val formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z")

  def main(args: Array[String]) {

    // define o nível de log para a aplicação
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // cria o contexto do Spark para utilizar todos os cores da máquina local e define o nome
    val sc = new SparkContext("local[*]", "NasaChallenge")

    // carrega os arquivos de dados, nosso dataset
    val NASA_access_log_Aug95 = sc.textFile("../nasa_logs/NASA_access_log_Aug95")
    val NASA_access_log_Jul95 = sc.textFile("../nasa_logs/NASA_access_log_Jul95")
    
    // une os dois arquivos
    val nasaLogsFull = NASA_access_log_Aug95.union(NASA_access_log_Jul95)
    
    // cria o cache pois o mesmo dataset será usado várias vezes dentro da aplicação, isso irá melhoar nossa performance
    nasaLogsFull.cache()
    
    // cria o parse para que possamos utilizar as colunas necessárias
    val parsedLog = nasaLogsFull.map(ParseLog.fromLine _)
    
    // cria o map para os objetos que representam as colunas do Log
    val nasa_logs = parsedLog.filter(_.isRight).map(_.right.get)
    
    // 1 - Quantidade de hosts únicos
    val hosts_unicos = nasa_logs.map(_.host).distinct()
    println(s"Quantidade de hosts únicos: ${hosts_unicos.count()}")
    
    // 2. Total de erros 404
    val erros_404 = nasa_logs.filter(_.code == "404")
    println(s"Total de erros 404: ${erros_404.count()}")
    
    // 3 - Os 5 URLs que mais causaram erros
    val total_404 = erros_404
                                .map(log => (log.url, 1))
                                 .reduceByKey(_ + _)
    val top_5_404 = total_404
                      .sortBy(p => p._2, ascending = false)
                      .take(5)

    println(s"\nTop 5 urls com erros 404: \n${top_5_404.mkString("\n")}\n")
    
    // 4 - Total de erros por dia
    def getDayOfMonth = (s: String) => LocalDate.parse(s, formatter)

    val erro_404_dia = erros_404
                      .map(log => (getDayOfMonth(log.timestamp), 1))
                      .reduceByKey(_ + _)

    val media_404 = erro_404_dia.map(_._2).mean()

    printf(s"Média de erros 404 por dia: %.2f\n", media_404)    
    
    // 5 - Total de bytes retornados
    val total_bytes = nasa_logs.map(_.bytes).reduce(_ + _)
    println(s"Total de bytes returnados: $total_bytes")
    
    
    sc.stop()
    
  }
 
}