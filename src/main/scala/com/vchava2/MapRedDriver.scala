package com.vchava2

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ IntWritable, Text}
import org.slf4j.{Logger, LoggerFactory}

object MapRedDriver {
  //Initialize Config and Logger objects
  val configuration: Config = ConfigFactory.load("appconfiguration.conf")
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  //Read input and output file paths from app configuration file
  val inputXMLFile: String = configuration.getString("appconfiguration.inputXMLFile")
  val outputFile: String = configuration.getString("appconfiguration.outputFile")

  def main(args: Array[String]): Unit = {

    val startTime = System.nanoTime
    LOG.info("---- Starting MapReduce Jobs ----")

    val verbose = configuration.getBoolean("appconfiguration.verbose")
    val startTags = configuration.getString("appconfiguration.startTags")
    val endTags = configuration.getString("appconfiguration.endTags")

    val jobName0 = configuration.getString("appconfiguration.jobName0")
    val jobName1 = configuration.getString("appconfiguration.jobName1")
    val jobName2 = configuration.getString("appconfiguration.jobName2")
    val jobName3 = configuration.getString("appconfiguration.jobName3")
    val jobName4 = configuration.getString("appconfiguration.jobName4")
    val jobName5 = configuration.getString("appconfiguration.jobName5")
    val jobName6 = configuration.getString("appconfiguration.jobName6")

    //Delete output_dir every time the map/reduce is executed
    FileUtils.deleteDirectory(new File(outputFile));
    LOG.info("Deleted output directory")
    val conf: Configuration = new Configuration()

    //Set start and end tags for CustomXMLInputFormat
    conf.set(CustomXMLInputFormat.STARTING_TAGS, startTags)
    conf.set(CustomXMLInputFormat.ENDING_TAGS, endTags)

    //Format as CSV output
    conf.set("mapred.textoutputformat.separator", Constants.COMMA)

    // Job to compute list of publications with one author (per venue)
    val job0 = Job.getInstance(conf, jobName0)
    job0.setJarByClass(classOf[CustomXMLInputFormat])
    job0.setMapperClass(classOf[PublicationsOneAuthorMapper])
    job0.setReducerClass(classOf[PublicationsOneAuthorReducer])
    job0.setInputFormatClass(classOf[CustomXMLInputFormat])
    job0.setOutputKeyClass(classOf[Text])
    job0.setOutputValueClass(classOf[Text])
    job0.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job0, new Path(inputXMLFile))
    FileOutputFormat.setOutputPath(job0, new Path((outputFile+Constants.SLASH+jobName0)))

    //job to compute top 100 authors with max co-authors (Stage 1)
    val job1 = Job.getInstance(conf, jobName1)
    job1.setJarByClass(classOf[CustomXMLInputFormat])
    job1.setMapperClass(classOf[TopAuthorsWithCoAuthorMapper1])
    job1.setReducerClass(classOf[TopAuthorsWithCoAuthorReducer1])
    job1.setInputFormatClass(classOf[CustomXMLInputFormat])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[Text])
    job1.setOutputKeyClass(classOf[IntWritable])
    job1.setOutputValueClass(classOf[Text])
    job1.setOutputFormatClass(classOf[TextOutputFormat[IntWritable, Text]])
    FileInputFormat.addInputPath(job1, new Path(inputXMLFile))
    FileOutputFormat.setOutputPath(job1, new Path((outputFile+Constants.SLASH+jobName1)))

    //job to compute top 100 authors without any co-authors
    val job2 = Job.getInstance(conf, jobName2)
    job2.setJarByClass(classOf[CustomXMLInputFormat])
    job2.setMapperClass(classOf[TopAuthorsWithoutCoAuthorMapper])
    job2.setReducerClass(classOf[TopAuthorsWithoutCoAuthorReducer])
    job2.setInputFormatClass(classOf[CustomXMLInputFormat])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job2, new Path(inputXMLFile))
    FileOutputFormat.setOutputPath(job2, new Path((outputFile+Constants.SLASH+jobName2)))


    //job to compute list of authors that have published without interruption for 10+ years
    val job3 = Job.getInstance(conf, jobName3)
    job3.setJarByClass(classOf[CustomXMLInputFormat])
    job3.setMapperClass(classOf[AuthorsWithoutInterruptionMapper])
    job3.setReducerClass(classOf[AuthorsWithoutInterruptionReducer])
    job3.setInputFormatClass(classOf[CustomXMLInputFormat])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])
    job3.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job3, new Path(inputXMLFile))
    FileOutputFormat.setOutputPath(job3, new Path((outputFile+Constants.SLASH+jobName3)))


    //job to compute top 10 published authors per venue
    val job4 = Job.getInstance(conf, jobName4)
    job4.setJarByClass(classOf[CustomXMLInputFormat])
    job4.setMapperClass(classOf[TopTenAuthorsMapper])
    job4.setReducerClass(classOf[TopTenAuthorsReducer])
    job4.setInputFormatClass(classOf[CustomXMLInputFormat])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[Text])
    job4.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job4, new Path(inputXMLFile))
    FileOutputFormat.setOutputPath(job4, new Path((outputFile+Constants.SLASH+jobName4)))

    //job to compute list of publications for each venue that contain the highest number of authors
    val job5 = Job.getInstance(conf, jobName5)
    job5.setJarByClass(classOf[CustomXMLInputFormat])
    job5.setMapperClass(classOf[PublicationsWithHighestAuthorsMapper])
    job5.setReducerClass(classOf[PublicationsWithHighestAuthorsReducer])
    job5.setInputFormatClass(classOf[CustomXMLInputFormat])
    job5.setMapOutputKeyClass(classOf[Text]);
    job5.setMapOutputValueClass(classOf[Text]);
    job5.setOutputKeyClass(classOf[Text])
    job5.setOutputValueClass(classOf[Text])
    job5.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])
    FileInputFormat.addInputPath(job5, new Path(inputXMLFile))
    FileOutputFormat.setOutputPath(job5, new Path((outputFile+Constants.SLASH+jobName5)))

    //job to compute top 100 authors with max co-authors (Stage 2)
    val job6 = Job.getInstance(conf, jobName6)
    job6.setMapperClass(classOf[TopAuthorsWithCoAuthorMapper2])
    job6.setReducerClass(classOf[TopAuthorsWithCoAuthorReducer2])
    job6.setOutputKeyClass(classOf[Text])
    job6.setOutputValueClass(classOf[IntWritable])
    job6.setNumReduceTasks(1)
    job6.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job6, new Path(outputFile+Constants.SLASH+jobName1))
    FileOutputFormat.setOutputPath(job6, new Path((outputFile+Constants.SLASH+jobName6)))


    LOG.info("-------- Starting All the Job(s) -------")
    if (job0.waitForCompletion(verbose) && job1.waitForCompletion(verbose)
      && job6.waitForCompletion(verbose) && job2.waitForCompletion(verbose)
      && job3.waitForCompletion(verbose)&& job4.waitForCompletion(verbose)
      && job5.waitForCompletion(verbose))

    {
      val endTime = System.nanoTime
      val totalTime = endTime - startTime
      LOG.info("--- FINISHED with Execution time: "+totalTime/1_000_000_000+" seconds ----")
    } else {
      LOG.info("--- FAILED Due to Some Error ---")
    }
  }
}
