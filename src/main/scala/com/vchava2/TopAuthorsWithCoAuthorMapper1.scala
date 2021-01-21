package com.vchava2

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.mutable



class TopAuthorsWithCoAuthorMapper1 extends Mapper[Object, Text, Text, Text]{

  val authorKey:Text = new Text
  val coAuthorsValue:Text = new Text()


  /*This map function returns tuple of the format (Author, set of Co-Authors). It is
  the first step in processing the job top_authors_with_coauthors
  * */
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    //Get dtd resource on filesystem
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString
    val inputSet: mutable.Set[String] = new mutable.HashSet[String]()

    //This is used to correctly handle the parsing of tags and escaped entities in the XML file.
    //We encapsulate the xml input fragment into <dblp> tags and provide its dtd for parsing.
    //https://stackoverflow.com/questions/54851274/error-while-loading-the-string-as-xml-in-scala
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML Format
    val xml = scala.xml.XML.loadString(xmlComp)

    val authors = (xml \\ "author")

    // return  without adding tuples if no authors
    if (authors.isEmpty) return

    if(authors.size>1){
      authors.foreach(author=>{
        inputSet.add(author.text)
      })
    }
    authors.foreach(author=>{
      inputSet.remove(author.text)
      authorKey.set(author.text)
      coAuthorsValue.set(inputSet.toString())
      context.write(authorKey,coAuthorsValue)
      inputSet.add(author.text)
    })

  }
}
