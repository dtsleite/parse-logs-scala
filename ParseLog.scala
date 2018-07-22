/*
 * Autor: Douglas T. S. Leite
 * Data: 22/07/2018
 * Classe responsável por realizar a transformação do arquivo de log e retornar dados já tratados
*/

package com.nasachallenge.spark

import scala.Left
import scala.Right

// usando um case class pois vamos trabalhar com dados imutáveis
case class ParseLog(host: String, timestamp: String, url: String, code: String, bytes: Long)

object ParseLog {
    
    //	define os splits, em gurpos,  para cada dado no arquivo de log
    private val LOG_ENTRY_PATTERN = """^(\S+) - - \[(.*?)\] "(.*?)" (\d{3}) (\S+)""".r
  
    // retorna as linhas que estão de acordo com o nosso pattern
    def fromLine(line: String): Either[String, ParseLog] = {
        line match {
          case LOG_ENTRY_PATTERN(host, ts, req, code, b) =>
            val url = getUrlFromRequest(req)
            val bytes = toLong(b)
            Right(ParseLog(host, ts, url.getOrElse(req), code, bytes.getOrElse(0)))
          case _ => Left(line)
        }
    }
    
    // obtém o url do campo de request
    private def getUrlFromRequest(url: String): Option[String] = {
        val i = url.indexOf("/")
        if (i > -1) {
          val subs = url.slice(i, url.length)
          return Some(subs.split(" ").head)
        }
        return None
    }

    private def toLong(b: String): Option[Long] = {
        try {
          Some(b.toLong)
        } catch {
          case e: NumberFormatException => None
        }
    }
    
 
  
}