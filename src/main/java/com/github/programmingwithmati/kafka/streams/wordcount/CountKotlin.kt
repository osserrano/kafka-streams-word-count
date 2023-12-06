package com.github.programmingwithmati.kafka.streams.wordcount

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import java.util.*


object CountKotlin {
    @JvmStatic
    fun main(args: Array<String>) {
        val props = config
        val streamsBuilder = StreamsBuilder()
        // Build the Topology
        streamsBuilder.stream<String, String>("sentences")
            .flatMapValues { _: String?, value: String ->
                listOf(
                    *value.lowercase(Locale.getDefault())
                        .split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                )
            }
            .groupBy { _: String?, value: String? -> value }
            .count(Materialized.with(Serdes.String(), Serdes.Long()))
            .toStream()
            .to("word-count", Produced.with(Serdes.String(), Serdes.Long()))
        // Create the Kafka Streams Application
        val kafkaStreams = KafkaStreams(streamsBuilder.build(), props)
        // Start the application
        kafkaStreams.start()

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(Thread { kafkaStreams.close() })
    }

    private val config: Properties
        private get() {
            val properties = Properties()
            properties[StreamsConfig.APPLICATION_ID_CONFIG] = "word-count-app-kotlin"
            properties[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:29092"
            properties[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
            properties[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
            properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            properties[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = "0"
            return properties
        }
}

