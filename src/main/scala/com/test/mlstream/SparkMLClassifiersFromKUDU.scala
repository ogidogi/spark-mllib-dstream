package com.test.mlstream

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, sql}
import org.slf4j.{Logger, LoggerFactory}

object SparkMLClassifiersFromKUDU {

    val LOGGER: Logger = LoggerFactory.getLogger(SparkMLClassifiersFromKUDU.getClass.getName)

    def main(args: Array[String]): Unit = {

        // Load values form the Config file(application.json)
        val config: Config = ConfigFactory.load("application.json")

        val SPARK_MASTER = config.getString("spark.master")
        val SPARK_APP_NAME = config.getString("ml.app_name")
        val SPARK_BATCH_DURATION: Int = config.getInt("spark.batch_duration")

        val ML_CLASSIFIER = config.getString("ml.classifier")
        val MODEL_LOCATION = config.getString("ml.model_location")

        val KUDU_MASTER: String = config.getString("kudu.master")
        val KUDU_TABLE_NAME: String = "impala::" + config.getString("kudu.table_name")

        val sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER)
        val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(SPARK_BATCH_DURATION))
        val sc = sparkStreamingContext.sparkContext

        val spark = SparkSession.builder.config(sparkConf).getOrCreate()
        import spark.implicits._

        val kuduOptions = Map(
            "kudu.table" ->  KUDU_TABLE_NAME,
            "kudu.master" -> KUDU_MASTER)

        // Load Model from HDFS
        val model = PipelineModel.load(MODEL_LOCATION + "/" + ML_CLASSIFIER)

        LOGGER.info("Loaded Model from HDFS")

        var df = spark.read.options(kuduOptions).format("org.apache.kudu.spark.kudu").load

        df.show()

        df = df.withColumnRenamed("sepal_length", "SepalLength")
                .withColumnRenamed( "petal_length","PetalLength")
                .withColumnRenamed("sepal_width", "SepalWidth")
                .withColumnRenamed("petal_width","PetalWidth")

        df = df.withColumn("SepalLength", $"SepalLength".cast(sql.types.DoubleType))
                .withColumn("PetalLength", $"PetalLength".cast(sql.types.DoubleType))
                .withColumn("SepalWidth", $"SepalWidth".cast(sql.types.DoubleType))
                .withColumn("PetalWidth", $"PetalWidth".cast(sql.types.DoubleType))

        val assembler = new VectorAssembler()
                .setInputCols(Array("PetalLength", "PetalWidth", "SepalLength", "SepalWidth"))
                .setOutputCol("features")

        val transformed_data = assembler.transform(df).drop("PetalLength", "PetalWidth", "SepalLength", "SepalWidth")

        transformed_data.show()
        transformed_data.printSchema()

        // Make predictions
        val predictions = model.transform(transformed_data)

        predictions.show()
        LOGGER.info("Count: " + predictions.count().toString)

        val evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedFlower")
                .setPredictionCol("prediction")
                .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)

        println("Accuracy: " + accuracy)
        println("Test Error = " + (1.0 - accuracy))




    }



}
