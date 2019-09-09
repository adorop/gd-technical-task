# GD Technical Task

## Domain
Ecommerce site with products divided into categories like toys, electronics etc. We receive events like product was seen (impression), product page was opened, product was purchased etc. 

## Tasks
#### 1
Enrich incoming data with user sessions. Definition of a session: for each user, it contains consecutive events that belong to a single category  and are not more than 5 minutes away from each other. Output should look like this (session columns are in bold):
eventTime, eventType, category, userId, …, **sessionId**, **sessionStartTime**, **sessionEndTime**  
Implement it using 
1) sql window functions and 
2) Spark aggregator **- Not implemented**

#### 2
Compute the following statistics:
* For each category find median session duration
* For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins
* For each category find top 10 products ranked by time spent by users on product pages - this may require different type of sessions. For this particular task, session lasts until the user is looking at particular product. When particular user switches to another product the new session starts. **- Not Implemented**

General notes:
* Ideally tasks should be implemented using pure SQL on top of Spark DataFrame API.
* Spark version 2.2 or higher

## Build
Maven build can be triggered with the following profiles:

* *local*: includes Spark dependency in the final uber jar(allows application to be run locally)
* *statistics*: defines [StatisticsApp](src/main/scala/aliaksei/darapiyevich/statistics/StatisticsApp.scala) as a main class of application jar([ImpressionEventEnrichmentApp](src/main/scala/aliaksei/darapiyevich/impression/enrichment/ImpressionEventEnrichmentApp.scala) is default)

## Run
Application accepts 4 command line arguments:
* --input-format
* --input-location
* --output-format
* --output-location

*--help* or *-h* can be used to get more information

## Design
[EtlJob](/home/aliaksei/Documents/gd-technical-task/src/main/scala/aliaksei/darapiyevich/EtlJob.scala) is a facade for running Spark jobs.
It consists of 3 components:
* *trait Extract[I]* - responsible for reading data
* *trait Transform[I, O]* - responsible for queries 
* *trait Load[O]* - responsible for writing output

Main classes of applications leverage behavior of *EtlJobs* by constructing them with different sets of components' implementations

## Test
*Scala Test* tests are included in build lifecycle.
* [UnitTestSpec](src/test/scala/aliaksei/darapiyevich/UnitTestSpec.scala) is trait for pure unit tests. All the dependencies are mocked using *Mockito*
* [SparkTransformationsSpec](src/test/scala/aliaksei/darapiyevich/SparkTransformationsSpec.scala) is trait for transformations testing. Child specs read test and expected data in csv format from *resources* folder
