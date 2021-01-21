package com.vchava2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io. Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

class PublicationsOneAuthorMapper extends Mapper[Object, Text, Text, Text]{
  val venueKey = new Text()
  val publicationValue = new Text()

  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /**
   *
   * This Mapper is responsible for creating tuples for the publications_with_one_author job.
   * The created tuples is of the format (venue, publication) where key is venue and
   * value is publication with one author
   */
  override def map(key:Object, value:Text, context:Mapper[Object, Text, Text, Text]#Context): Unit = {

    //Get dtd resource on filesystem
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    //This is used to correctly handle the parsing of tags and escaped entities in the XML file.
    //We encapsulate the xml input fragment into <dblp> tags and provide its dtd for parsing.
    //https://stackoverflow.com/questions/54851274/error-while-loading-the-string-as-xml-in-scala
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML format
    val xml = scala.xml.XML.loadString(xmlComp)

    //check for author tags
    val authors = xml \\ "author"

    val publication = xml \\ "title"
    val venue = (xml \\ "journal")

    // return if publication has more than 1 authors or null publication or venue is empty
    if (authors.size > 1 || publication==null || authors.isEmpty || venue.isEmpty) {
      return
    }

    publicationValue.set(publication.text)

    venueKey.set(venue.text)
    context.write(venueKey, publicationValue)
  }
}


