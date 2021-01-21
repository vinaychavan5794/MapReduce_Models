import java.util
import java.util.Collections

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source


class MapReduceTest extends FunSuite with BeforeAndAfter{

  val LOG: Logger = LoggerFactory.getLogger(getClass)

  /*This test finds the top author for venue meltdownattack.com with highesh publications
  * and matches it with the actual author*/
  test("AuthorsWithHighestPublicationsPerVenue") {

    val source_html = Source.fromResource("dblp2.xml").mkString
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${source_html}</dblp>"""

    val treeMap = new mutable.TreeMap[Int, String]()(Ordering[Int].reverse)
    val hashMap = new mutable.HashMap[String, ListBuffer[String]]()

    val xmlValue = scala.xml.XML.loadString(xmlComp)
    val dblp = (xmlValue \\ "dblp")
    for(article <- dblp \\ "article"){
      val authors = (article \\ "author")
      val venue=  (article \\ "journal")
      if(authors.size>1){
        authors.foreach(author=>{
          if(hashMap.contains(venue.text)){
            hashMap(venue.text).addOne(author.text)
          }else{
            hashMap.put(venue.text,new mutable.ListBuffer[String])
            hashMap(venue.text).addOne(author.text)
          }
        })
      }else{
        if(hashMap.contains(venue.text)){
          hashMap(venue.text).addOne(authors.text)
        }else{
          hashMap.put(venue.text,new ListBuffer[String])
          hashMap(venue.text).addOne(authors.text)
        }
      }
    }

    val inputSet: mutable.Set[String] = new mutable.HashSet[String]()
    for ((key, value) <- hashMap) {
      val list: util.List[String] =new util.ArrayList[String]()
      value.foreach(valu=>{
        inputSet.add(valu)
      })

      value.foreach(input=>{
        list.add(input)
      })

      inputSet.foreach(inputString => {

        val occurrences: Int = Collections.frequency(list, inputString)
        if(treeMap.size==1){
          if(treeMap.lastKey < occurrences){
            treeMap.remove(treeMap.lastKey)
            treeMap.put(occurrences,inputString)
          }
        }else{
          treeMap.put(occurrences,inputString)
        }

      })

    }

    treeMap.values.foreach(result=>{
      assert(result.equals("Paul Kocher"))
    })
  }

  /*This test outputs list of Authors with most Co Authors and compares the list
  * with the actual list of Authors with most Co Authors*/

  test("AuthorsWithMaxCoAuthors") {

    var max: Int = 0
    var sum: Int = 0
    val source_html = Source.fromResource("dblp2.xml").mkString
    val inputSet: mutable.Set[String] = new mutable.HashSet[String]()
    val res = getClass.getClassLoader.getResource("dblp.dtd").toURI.toString
    val xmlComp =  s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$res"><dblp>${source_html}</dblp>"""
    val xmlValue = scala.xml.XML.loadString(xmlComp)
    val dblp = (xmlValue \\ "dblp")
    val hashMap = new mutable.HashMap[String, ListBuffer[Int]]()
    for(article <- dblp \\ "article"){
      val authors = (article \\ "author")
      if(authors.size>1){
        authors.foreach(author=>{
          if(hashMap.contains(author.text)){
            hashMap(author.text).addOne(authors.size-1)
          }else{
            hashMap.put(author.text,new mutable.ListBuffer[Int])
            hashMap(author.text).addOne(authors.size-1)
          }
        })
      }
    }

    LOG.debug("Input set"+ hashMap)
    for ((key, value) <- hashMap) {


      sum=0
      value.foreach(input=>{
        sum=sum+input

      })
      if(max<sum){
        max=sum
        inputSet.clear()
        inputSet.add(key)
      }else if (max==sum){
        inputSet.add(key)
      }
    }

    assert(inputSet.contains("Paul Kocher"))
    assert(inputSet.contains("Daniel Genkin"))
    assert(inputSet.contains("Werner Haas 0004"))

  }


}
