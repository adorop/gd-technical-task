package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.JobDefinition

object StatisticsAppJobDefinition {

  implicit class StatisticsAppJobDefinitionOps(jobDefinition: JobDefinition) {
    def withOutputSubfolder(subfolderName: String): JobDefinition = {
      val givenOutputDefinition = jobDefinition.outputDefinition
      val outputBasePath = givenOutputDefinition.location
      val finalPath = s"$outputBasePath/$subfolderName"
      jobDefinition.copy(outputDefinition = givenOutputDefinition.copy(location = finalPath))
    }
  }

}
