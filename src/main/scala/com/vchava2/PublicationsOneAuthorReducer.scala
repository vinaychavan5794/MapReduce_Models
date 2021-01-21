package com.vchava2

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

class PublicationsOneAuthorReducer extends Reducer[Text, Text, Text, Text]{
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /* This reduce method writes output tuple of form (venue, publication name with one author)
  * */
  override def reduce(key: Text, values: java.lang.Iterable[Text],
                      context:Reducer[Text, Text, Text,Text]#Context): Unit = {


    values.forEach(value=>{
      context.write(key,value)
    })

  }

}
