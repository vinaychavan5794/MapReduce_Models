package com.vchava2


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class AuthorsWithoutInterruptionMapper extends Mapper[Object, Text, Text, IntWritable]{

  var hashMap: mutable.HashMap[String, scala.collection.mutable.SortedSet[Int]] = _
  val numYears = new IntWritable(1)
  val authorKey = new Text


  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)



  override def setup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

    hashMap = new mutable.HashMap[String, scala.collection.mutable.SortedSet[Int]]()
  }



  /**
   * This function takes the author name as a String(key) and the publication year (value)
   * and adds them to the hashMap.
   *
   */
  def getCoAuthors(author: String, num: Int): Unit = {


    if(hashMap.contains(author)){
      hashMap(author).add(num)

    }else{
      val set = scala.collection.mutable.TreeSet[Int](num)
      hashMap.put(author,set)

    }


  }

  /*
   * This Mapper function returns hashMap with key as author and value as set of unique years
   * in which the author made publications
   */
  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {

    //LOG.debug("HashMap:"+ hashMap.toString())

    //Get dtd resource on filesystem
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString

    //This is used to correctly handle the parsing of tags and escaped entities in the XML file.
    //We encapsulate the xml input fragment into <dblp> tags and provide its dtd for parsing.
    //https://stackoverflow.com/questions/54851274/error-while-loading-the-string-as-xml-in-scala
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${value.toString}</dblp>"""

    //Convert to XML format
    val xml = scala.xml.XML.loadString(xmlComp)

    // Check for author tags
    val authors = (xml \\ "author")
    val year = xml \\ "year"

    // Return without adding tuples if no authors
    if (authors.isEmpty) {
      return
    }

    // Return without adding tuples if no year information is available
    if (year.isEmpty || year.text == null || year.text.trim.length==0) {
      return
    }

    if(authors.size ==1){

      if(hashMap.contains(authors.text)){
        hashMap(authors.text).add(year.text.toInt)

      }else{
        val set = scala.collection.mutable.TreeSet[Int](year.text.toInt)
        hashMap.put(authors.text,set)

      }
    }else{
      authors.foreach(author => getCoAuthors(author.text, year.text.toInt))
    }


  }

  /*It is used as the first step of the author_without_interruption  job, in the Reducer further processing
  * will then be computed on a per-author basis.
  * This override cleanup method is executed at the end and it determines if the author has published
  * for 10+ years consecutively by checking if the set of years corresponding to the author
  *  is in Arithmetic progression (AP) with difference as 1. If the set is in AP then the
  * author along with year count is written for the reducer to process it further */
  override def cleanup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

    for ((key, value) <- hashMap) {
      val name: String = key
      val count: Int = value.size

      if(count >=10){
        val lastValue=value.firstKey +(value.size-1)
        if(value.last==lastValue){
          numYears.set(count)
          authorKey.set(name)
          context.write(authorKey, numYears)
        }

      }

    }

  }
}
