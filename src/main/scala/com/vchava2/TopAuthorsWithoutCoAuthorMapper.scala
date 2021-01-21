package com.vchava2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}


class TopAuthorsWithoutCoAuthorMapper extends Mapper[Object, Text, Text, IntWritable]{

  val authorKey = new Text
  val one = new IntWritable(1)


  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /*This map function returns tuple of the format (Author, 1) for the job
  top_authors_without_coauthors. It is the first step in processing job top_authors_without_coauthors
  * */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {

    //Get dtd resource on filesystem
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    //This is used to correctly handle the parsing of tags and escaped entities in the XML file.
    //We encapsulate the xml input fragment into <dblp> tags and provide its dtd for parsing.
    //https://stackoverflow.com/questions/54851274/error-while-loading-the-string-as-xml-in-scala
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML
    val xml = scala.xml.XML.loadString(xmlComp)

    //Look for author tags
    val authors = (xml \\ "author")

    // return without adding tuples if no authors or more than 1 authors
    if (authors.isEmpty || authors.size >1) {
      return
    }

    authorKey.set(authors.text)
    context.write(authorKey, one)


  }



}
