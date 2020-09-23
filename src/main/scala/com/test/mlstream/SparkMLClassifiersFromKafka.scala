package com.test.mlstream

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, sql}
import org.slf4j.{Logger, LoggerFactory}

object SparkMLClassifiersFromKafka {

    val LOGGER: Logger = LoggerFactory.getLogger(SparkMLClassifiersFromKafka.getClass.getName)

    def main(args: Array[String]): Unit = {

        // Load values form the Config file(application.json)
        val config: Config = ConfigFactory.load("application.json")

        val SPARK_APP_NAME: String = config.getString("spark.app_name")
        val SPARK_MASTER: String = config.getString("spark.master")
        val SPARK_BATCH_DURATION: Int = config.getInt("spark.batch_duration")

        val KAFKA_TOPICS: String = config.getString("kafka.consumer_topic")
        val KAFKA_BROKERS: String = config.getString("kafka.brokers")
        val KAFKA_GROUP_ID: String = config.getString("kafka.group_id")
        val KAFKA_OFFSET_RESET: String = config.getString("kafka.auto_offset_reset")

        val ML_CLASSIFIER = config.getString("ml.classifier")
        val MODEL_LOCATION = config.getString("ml.model_location")

        val sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER)
        val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(SPARK_BATCH_DURATION))

        val spark = SparkSession.builder.config(sparkConf).getOrCreate()
        import spark.implicits._

        val kafkaParams = Map[String, Object]("bootstrap.servers" -> KAFKA_BROKERS,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> KAFKA_GROUP_ID,
            "auto.offset.reset" -> KAFKA_OFFSET_RESET,
            "enable.auto.commit" -> (false: java.lang.Boolean))

        val topicsSet = KAFKA_TOPICS.split(",").toSet

        val kafkaStream = KafkaUtils.createDirectStream[String, String](sparkStreamingContext, PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

        // Load Model from HDFS
        val model = PipelineModel.load(MODEL_LOCATION + "/" + ML_CLASSIFIER)

        kafkaStream.map(message => {
            message.value().toString
        }).foreachRDD(rdd => {
            if (!rdd.isEmpty()) {

                val data = spark.read.json(rdd.toDS())

                data.printSchema()
                data.show()

                val df = data.withColumn("PetalLength", $"PetalLength".cast(sql.types.DoubleType))
                        .withColumn("PetalWidth", $"PetalWidth".cast(sql.types.DoubleType))
                        .withColumn("SepalLength", $"SepalLength".cast(sql.types.DoubleType))
                        .withColumn("SepalWidth", $"SepalWidth".cast(sql.types.DoubleType))

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
        })

        sparkStreamingContext.start()
        sparkStreamingContext.awaitTermination()

    }

}
