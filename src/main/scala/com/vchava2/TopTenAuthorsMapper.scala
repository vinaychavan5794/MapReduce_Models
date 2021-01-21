package com.vchava2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

class TopTenAuthorsMapper extends Mapper[Object, Text, Text, Text]{
  val authorValue = new Text
  val venueKey = new Text()


  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)


  /*This function  writes tuple of the format (Venue, AuthorName ).
  * It is invoked only when the input publication has more than 1 authors*/
  def getCoAuthors(author: String, venue: String, context:Mapper[Object,Text,Text,Text]#Context): Unit = {

    authorValue.set(author)
    venueKey.set(venue)
    context.write(venueKey, authorValue)

  }

  /**
   *
   * This Mapper function returns tuples of the format: (Venue, AuthorName)
   * It is used as the first step of the top_ten_authors_venue job,
   * in the Reducer actual computation will then be done on a per-author basis.
   */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,Text]#Context): Unit = {

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
    val venue = (xml \\ "journal")

    // return  without adding tuples if no authors or no venue
    if (authors.isEmpty || venue.isEmpty) {
      return
    }
    if(authors.size==1){
      authorValue.set(authors.text)
      venueKey.set(venue.text)
      context.write(venueKey, authorValue)
    }else{
      authors.foreach(author => getCoAuthors(author.text, venue.text,  context))
    }



  }
}
