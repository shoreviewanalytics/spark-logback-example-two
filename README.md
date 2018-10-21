sparkjobspecificlogging2

Another Spark job specific log file example using Logback that can be run on a single node DataStax Enterprise (DSE) cluster with Analytics enabled or a multi-node (DSE) cluster with Analytics enabled.  

This example doesn't require the use of --driver-java-options "=Dlogback.configurationFile=" option with spark-submit.  Also there's an ability to reference to a specific logback.xml file that can be shared between applications or could be a custom logback.xml file per application. In this example, the logback.xml file is configured with a parameter called ${app}, to accept a value that is set within the application / job using System.setProperty. 

To run:

dse -u username -p password spark-submit --class com.java.spark.LoggingSample --master dse://? /path/LoggingSample1.0.jar

