import kafka.consumer.KafkaStream

class ConsumerTest extends Runnable {
    var m_stream : KafkaStream[Array[Byte], Array[Byte]] = _
    var m_threadNumber : Int = _

    def this(a_stream : KafkaStream[Array[Byte], Array[Byte]], a_threadNumber : Int) {
        this()
        m_threadNumber = a_threadNumber
        m_stream = a_stream
    }

    def run() : Unit = {
        val it = m_stream.iterator()
        while(it.hasNext()) {
            println(s"Thread ${m_threadNumber}: ${it.next().message().map(c => c.toChar).mkString}")
        }
        println(s"Shutting down thread ${m_threadNumber}")
    }
}
