import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.javaapi.consumer.ConsumerConnector

object ConsumerGroupExample {
    def createConsumerConfig(a_zookeeper : String, a_groupId : String) : ConsumerConfig = {
        val properties = new Properties()
        properties.put("zookeeper.connect", a_zookeeper)
        properties.put("group.id", a_groupId)
        properties.put("zookeeper.session.timeout", "400")
        properties.put("zookepeper.sync.time.ms", "200")
        properties.put("auto.commit.interval.ms", "1000")

        new ConsumerConfig(properties)
    }

    def main(args : Array[String]) : Unit = {
        val zookeeper = "localhost:2181"
        val groupId = "ninja-turtles"
        val topic = "pizza"
        val  threads = Integer.valueOf(3)

        val example = new ConsumerGroupExample(zookeeper, groupId, topic)
        example.run(threads)

        try {
            Thread.sleep(20000)
        } catch {
            case e : InterruptedException =>
                e.printStackTrace()
        } finally {
            example.shutdown()
        }

    }
}

class ConsumerGroupExample {
    import ConsumerGroupExample._

    private var consumer: ConsumerConnector = _
    private var topic: String = _
    private var executor: ExecutorService = _

    def this(a_zookeeper: String, a_groupId: String, a_topic: String) {
        this()
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
            createConsumerConfig(a_zookeeper, a_groupId)
        )
        topic = a_topic
    }

    def shutdown(): Unit = {
        if (consumer != null) {
            consumer.shutdown()
        }
        if (executor != null) {
            executor.shutdown()
        }

        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                println("Timed out waiting for consumer threads to shut down, exiting uncleanly")
            }
        } catch {
            case e: InterruptedException =>
                println("Interrupted during shutdown, exiting uncleanly")
        }
    }

    def run(a_numThreads : Int): Unit = {
        val topicCountMap : java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]()
        topicCountMap.put(topic, Integer.valueOf(a_numThreads))
        val consumerMap = consumer.createMessageStreams(topicCountMap)
        val streams = consumerMap.get(topic).toArray()

        executor = Executors.newFixedThreadPool(a_numThreads)

        var threadNumber = 0
        for(stream <- streams) {
            executor.submit(new ConsumerTest(stream.asInstanceOf[KafkaStream[Array[Byte], Array[Byte]]], threadNumber))
            threadNumber += 1
        }
    }
}
