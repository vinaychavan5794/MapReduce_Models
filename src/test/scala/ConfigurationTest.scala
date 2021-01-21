

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}
import scala.io.Source

class ConfigurationTest extends FunSuite with BeforeAndAfter{

  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /*This test checks if the config file is correctly read
   * by comparing the properties of the config file with their actual values  */

  test("ConfigInput") {

    val jobName0 = configuration.getString("appconfiguration.jobName0")
    assert(jobName0.equals("publications_with_one_author"))
  }


  /*This test checks if the dblp xml file is correctly read
   * by comparing the author size with the actual number of authors in the xml file*/

  test("dblpInput") {

    val source_html = Source.fromResource("dblp1.xml").mkString
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${source_html}</dblp>"""
    val xmlValue = scala.xml.XML.loadString(xmlComp)
    val authors = (xmlValue \\ "author")
    //LOG.info("author size"+authors.size)
    assert(authors.size==3)

  }

  /*This test checks the publication without any co author for venue meltdownattack.com
  * and matches it with the actual publication
  * from the input xml*/

  test("PublicationWithOneAuthor") {

    val source_html = Source.fromResource("dblp2.xml").mkString
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${source_html}</dblp>"""

    val xmlValue = scala.xml.XML.loadString(xmlComp)
    val dblp = (xmlValue \\ "dblp")
    for(article <- dblp \\ "article"){
      val authors = (article \\ "author")
      if(authors.size==1){
        val publication=( article \\ "title").text
        assert(publication.equals("A 'RISC' Object Model for Object System Interoperation: Concepts and Applications."))

      }

    }

  }




}
