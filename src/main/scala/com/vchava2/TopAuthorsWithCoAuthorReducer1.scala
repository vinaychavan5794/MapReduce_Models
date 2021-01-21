package com.vchava2
import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.mutable
class TopAuthorsWithCoAuthorReducer1 extends Reducer[Text,Text,IntWritable,Text] {
  val coAuthorSet: mutable.HashSet[Text] = new mutable.HashSet[Text]()
  val authorKey: Text = new Text()


  /*This reduce function returns tuple of the format (count of Co-Authors, Author). It is
  the first step in processing job top_authors_with_coauthors
  * */

  override def reduce(key: Text, values: lang.Iterable[Text],
                      context: Reducer[Text, Text, IntWritable,Text]#Context): Unit = {
    coAuthorSet.clear()

    values.forEach(value=>{
      coAuthorSet.add(value)
    })

    context.write(new IntWritable(coAuthorSet.size),key)

  }
}
