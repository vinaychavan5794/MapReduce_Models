## Introduction

This project consist of creating a distributed program for parallel processing of the publically available DBLP dataset that contains entries for various publications at many different venues (e.g., conferences and journals). The hadoop framework has been extensively used as part of this homework.

## Requirements

* dblp.xml file is available in the input_dir folder which will be used as input for all Map Reduce jobs
* Hadoop framework must be configured in the system before executing any of the Jobs.


## Instructions on how to run the jobs

The recommended procedure is to run the Jobs is by using IntellJ IDEA with the Scala plugin installed.

1. Open IntellJ IDEA, a welcome screen will be shown, select Check out from Version Control  and then Git.
2. Enter the following URL and click Clone : https://vinaychavan5794@bitbucket.org/cs441-fall2020/vinay_indrajit_chavan_hw2.git
3. When prompted confirm with Yes
4. src/main/scala/com.vchava2/ is where you will find the Map/reduce driver class: MapRedDriver. A run configuration is automatically created when you execute the Driver class.



## Alternative Way To Execute Jobs: SBT from Command Line Interface

1. Execute the command : git clone https://vinaychavan5794@bitbucket.org/cs441-fall2020/vinay_indrajit_chavan_hw2.git
2. Run the command: "sbt clean compile run"
3. In order to run test cases , execute command "sbt clean compile test"

Note: (input data): The dblp.xml file needs to be copied in the input_dir folder before executing any job
Note: "sbt clean compile run" command executes map/reduce jobs locally.

## Creating a JAR

In order to create a Jar file to run the map/reduce jobs on AWS EMR or on a virtual machine that provides a Hadoop installation, follow below steps:

1. Execute the command : git clone https://vinaychavan5794@bitbucket.org/cs441-fall2020/vinay_indrajit_chavan_hw2.git
2. Run the command: "sbt clean compile assembly"
3. A Jar will be created in the target/scala-2.13/ folder with the following name: vinay_indrajit_chavan_hw2.jar.

Note: The Java version present on the system you compile the code must match the version on the system you execute it on. Java JDK 1.8 is recommended.



## Map/Reduce jobs

This homework consist of a total of 6 map/reduce jobs.

* publications_with_one_author: This job outputs list of publications with one author for each venue. The output format is (Venue, Publication Name). Refer PublicationsOneAuthorMapper and PublicationsOneAuthorReducer class files for more details.

* top_authors_with_coauthors: This job finds top 100 authors who have published with max co-authors. The output format is (Author Name, Total Number of Co-Authors). This Map/Reduce job works in two stages (pipeline). In the first stage list of Authors along with their total co-author count is computed. Refer TopAuthorsWithCoAuthorMapper1 and TopAuthorsWithCoAuthorReducer1 class files for more details. The Hadoop framework sorts the keys of the tuples in descending order by default during the shuffling operation (between Map and Reduce). Therefore, in the Stage 1 Reducer function we flip the tuple key with its value and viceversa. e.g. (Key, Value) -> (Value, Key); while in Stage 2 Mapper function we perform the flip again, restoring the correct placement of keys and values. In the second stage top 100 authors are shown as the output. Refer TopAuthorsWithCoAuthorMapper2 and TopAuthorsWithCoAuthorReducer2 class files for more details. Furthermore by imposing a restriction on the number of reducers = 1 in Stage 2, we obtained a single ordered list in csv format. (instead of multiple part* files)

* top_authors_without_coauthors: This job computes top 100 authors who have published without any co-author. The output tuples have the following format: (AuthorName, Publication Count). Refer the class files TopAuthorsWithoutCoAuthorMapper and TopAuthorsWithoutCoAuthorReducer for more details.

* author_without_interruption: This job finds a list of authors who have published for a consecutive 10 + years. The output format is (Author Name, Number of consecutive years).
For each author, the set of years which is formed is checked for Arithmetic progression with difference as 1. If the set of years is in AP then that Author along with year count is sent to reducer for further processing. Refer class files AuthorsWithoutInterruptionMapper and AuthorsWithoutInterruptionReducer for more details.

* top_ten_authors_venue: This job produces top 10 authors per venue. The output format is (Venue Name, Author Name)
Refer class files TopTenAuthorsMapper and TopTenAuthorsReducer for more details

* publications_with_highest_author: This job produces list of publications per venue with highest number of authors. The output format is ( Publication Name, Venue Name).
Refer PublicationsWithHighestAuthorsMapper and PublicationsWithHighestAuthorsReducer class files for more details.

## Test Cases

*ConfigurationTest Class has 3 test cases :

1. ConfigInput : This test checks if the config file is correctly read by comparing the properties of the config file with their actual values
2. dblpInput : This test checks if the dblp xml file is correctly read by comparing the author size with the actual number of authors in the xml file
3. PublicationWithOneAuthor: This test checks the publication without any co author for venue meltdownattack.com and matches it with the actual publication from the input xml

*MapReduceTest Class has 2 test cases :

1. AuthorsWithHighestPublicationsPerVenue: This test finds the top author for venue meltdownattack.com with highesh publications and matches it with the actual author
2. AuthorsWithMaxCoAuthors: This test outputs list of Authors with most Co Authors and compares the list with the actual list of Authors with most Co Authors


## File splitting

The input XML file is split in data chunks which is done on a filesystem level by HDFS with default chunk size being 128 MB.

An implementation by Apache Mahout is present at the following URL: https://github.com/apache/mahout/blob/758cfada62556d679c445416dff9d9fb2a3c4e59/community/mahout-mr/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java

The Hadoop framework do provide an InputSplit concept, however, the CustomXMLInputFormat class (Above Apache Mahout is used as a starting point, but is modified a bit as the implementation does not support multiple starting tags and it therefore required extensive changes) embeds the logic to correctly split XML files.


The default TextInputFormat provided by the framework splits files line-by-line, this is not acceptable for XML as the minimum logical unit of an XML file is a tag.
The CustomXMLInputFormat class enforces that by having splits to prevent XML tags getting cut in different parts.

The way the CustomXMLInputFormat works is by allowing splits as small as a single XML outer tag (e.g. phdthesis, article etc..).

However, this doesn't mean that the number of mappers equal to the number of XML outer tags present in the file.
This is because the number of mappers started, by default, actually depends on how the data is split on the filesystem level. (e.g. 2.8 GB is roughly about 21 to 23 mappers: 2800/128 = 21 to 23)


## Output results in the repository

Below are the location of the results for each of the Map/Reduce tasks. The .txt files are in csv format.

* publications_with_one_author: output_dir/publications_with_one_author/publications_with_one_author.txt

* top_authors_with_coauthors: output_dir/top_authors_with_coauthors/top_authors_with_coauthors.txt

* top_authors_without_coauthors: output_dir/top_authors_without_coauthors/top_authors_without_coauthors.txt

* author_without_interruption: output_dir/author_without_interruption/author_without_interruption.txt

* top_ten_authors_venue: output_dir/top_ten_authors_venue/top_ten_authors_venue.txt

* publications_with_highest_author: output_dir/publications_with_highest_author/publications_with_highest_author.txt


## AWS EMR Deployment Process

A Link to the YouTube video explaining the AWS EMR deployment process is available here: https://youtu.be/tnKJG0eneKc




