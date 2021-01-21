package com.vchava2


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TopAuthorsWithCoAuthorMapper2 extends Mapper[Object, Text, Text, IntWritable]{

  var sortedList: mutable.ListBuffer[(Int, String)] = _
  val numCoauthors = new IntWritable(1)
  val authorKey = new Text
  var count:Int=0

  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)



  override def setup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

    sortedList = new ListBuffer[(Int, String)]
    count=0
  }





  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context): Unit = {

    val inputTuple:List[String] = value.toString.split(Constants.COMMA).toList
    sortedList.addOne((inputTuple.head.toInt,inputTuple.tail.head))

  }


  /*This cleanup function returns tuple of the format (Author, Count of Number of Co-Authors). It is
    the second step in processing job top_authors_with_coauthors
    * */
  override def cleanup(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {


    sortedList = sortedList.sortWith((i,j)=>{
      i._1 > j._1
    })

      for ((key, value) <- sortedList) {
        if(count!=Constants.SIZE){
          numCoauthors.set(key)
          authorKey.set(value)
          context.write(authorKey, numCoauthors)
          count+=1
        }else{
          return
        }

      }

  }


}
