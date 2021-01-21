package com.vchava2

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.jdk.javaapi.CollectionConverters.asScala

class AuthorsWithoutInterruptionReducer extends Reducer[Text,IntWritable,Text,IntWritable]{

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                      context:Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    //All values corresponding to the key are added
    val sum = asScala(values).foldLeft(0) { (t,i) => t + i.get }

    //Write output tuple i.e. (author name, count of years published consecutively))
    context.write(key, new IntWritable(sum))
  }
}
