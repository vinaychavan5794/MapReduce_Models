package com.vchava2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}


class PublicationsWithHighestAuthorsMapper extends Mapper[Object, Text, Text, Text] {


  val publication = new Text
  val venueKey = new Text
  val authorSize = new IntWritable(1)
  val tuple=new Text



  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)



  /**
   *
   * This Mapper function returns tuples of the format: (Venue, AuthorSize+Splitter+ publication)
   * Here value is Author size concatenated with the corresponding  publication name seperated by a splitter string
   * It is used as the first step of the publications_with_highest_author job,
   * in the Reducer actual computation will then be done on a per-author basis.
   *
   */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,Text]#Context): Unit = {

    //Get dtd resource on filesystem
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    //This is used to correctly handle the parsing of tags and escaped entities in the XML file.
    //We encapsulate the xml input fragment into <dblp> tags and provide its dtd for parsing.
    //https://stackoverflow.com/questions/54851274/error-while-loading-the-string-as-xml-in-scala
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML format
    val xml = scala.xml.XML.loadString(xmlComp)

    //Look for author tags
    val authors = (xml \\ "author")
    val pub = (xml \\ "title")
    val venue =(xml \\ "journal")

    // Return without adding tuples if no authors
    if (authors.isEmpty || venue.isEmpty) {
      return
    }

    publication.set(pub.text)
    authorSize.set(authors.size)
    venueKey.set(venue.text)
    val tup= publication+Constants.SPLITTER+authorSize
    tuple.set(tup)
    context.write(venueKey,tuple)



  }

}
