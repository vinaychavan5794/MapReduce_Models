
package com.vchava2

import java.util
import java.util.Collections

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TopTenAuthorsReducer extends Reducer[Text, Text, Text, Text]{


  // Maintains top 10 authors for each venue
  var treeMap: mutable.TreeMap[Int, String] = _
  val authorValue = new Text


  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)



  override def setup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    treeMap = new mutable.TreeMap[Int, String]()(Ordering[Int].reverse)

  }

  implicit class Count[T](list: List[T]) {
    def count(n: T): Int = list.count(_.equals(n) )
  }

  /*This function returns tuple of the format (Venue, list of top 10 Authors).
  * */
  override def reduce(key: Text, values: java.lang.Iterable[Text],
                      context:Reducer[Text, Text, Text, Text]#Context): Unit = {


    val inputSet: mutable.Set[String] = new mutable.HashSet[String]()

    //val inputList:mutable.ListBuffer[(String)] = new ListBuffer[( String)]

    val list: util.List[String] =new util.ArrayList[String]()
    values.forEach(value=>{
      //inputList.addOne(value.toString)
      inputSet.add(value.toString)
      list.add(value.toString)
    })

    inputSet.foreach(value => {

      val occurrences: Int = Collections.frequency(list, value)
      //val occurrences: Int = inputList.groupBy(identity).view.mapValues(_.size)(value)

      if(treeMap.size==10){
        if(treeMap.lastKey < occurrences){
          treeMap.remove(treeMap.lastKey)
          treeMap.put(occurrences,value)
        }
      }else{
        treeMap.put(occurrences,value)
      }

    })

    for ((k, value) <- treeMap) {
      //name=name+Constants.COMMA+value
      authorValue.set(value)
      context.write(key,authorValue)
    }



  }


}

