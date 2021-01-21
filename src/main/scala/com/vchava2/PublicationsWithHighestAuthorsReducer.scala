package com.vchava2



import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class PublicationsWithHighestAuthorsReducer extends Reducer[Text, Text, Text, Text]{


  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)
  val inputList:mutable.ListBuffer[(Int,Text)] = new ListBuffer[(Int, Text)]

  /*This reduce method outputs tuple of the format ( publication,venue)
  * here value is the publication with highest authors. It compares the author size using
  * a custom comparator.
  * */
  override def reduce(key: Text, values: java.lang.Iterable[Text],
                      context:Reducer[Text, Text, Text, Text]#Context): Unit = {


    inputList.clear
    values.forEach(value=>{

      val input:Array[String] = value.toString.split(Constants.SPLITTER)

      inputList.addOne((input.tail.head.toInt,
        new Text(input.head)))

    })

    val listResult:mutable.ListBuffer[(Int,Text)]= inputList.sortWith((i, j)=>{
      i._1 > j._1
    })

    //LOG.debug("List after sorting:"+ listResult.toString())
    val maxAuthorCount:Int = listResult.head._1
    listResult.foreach(value=>{
      if (maxAuthorCount.equals(value._1)) {
        context.write(value._2,key)
      }

    })


  }
}
