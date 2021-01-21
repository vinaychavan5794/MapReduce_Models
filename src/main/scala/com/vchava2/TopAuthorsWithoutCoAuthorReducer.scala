package com.vchava2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.javaapi.CollectionConverters.asScala

class TopAuthorsWithoutCoAuthorReducer extends Reducer[Text, IntWritable, Text, IntWritable]{

  var sortedList: mutable.ListBuffer[(Int, String)] = _
  val numberPublication = new IntWritable(1)
  val authorKey = new Text
  var count:Int=0


  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)



  override def setup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    sortedList = new ListBuffer[(Int, String)]
    count=0
  }


  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                      context:Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = asScala(values).foldLeft(0) { (t,i) => t + i.get }

    sortedList.addOne((sum,key.toString))
  }

  /*This cleanup function returns tuple of the format (Author, number of publications).
  * */

  override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    sortedList = sortedList.sortWith((i,j)=>{
      i._1 > j._1
    })

    for ((key, value) <- sortedList) {
      if(count!=Constants.SIZE){
        numberPublication.set(key)
        authorKey.set(value)
        context.write(authorKey, numberPublication)
        count+=1
      }else{
        return
      }

    }

  }
}
