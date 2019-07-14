package org.springframework.fu.kofu.amqp

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.springframework.boot.autoconfigure.amqp.RabbitProperties
import org.springframework.context.support.GenericApplicationContext
import org.springframework.fu.kofu.AbstractDsl
import org.springframework.fu.kofu.ConfigurationDsl
import java.nio.charset.Charset

open class RabbitMqDsl(private val init: RabbitMqDsl.() -> Unit) : AbstractDsl() {

    protected val properties = RabbitProperties()

    var host: String
        get() = properties.host
        set(value) {
            properties.host = value
        }

    lateinit var connection: Connection
    lateinit var channel: Channel

    internal val exchanges: MutableList<ExchangeDsl> = mutableListOf()

    override fun initialize(context: GenericApplicationContext) {

        init()

        connection = ConnectionFactory().also { c ->
            c.host = properties.host
            c.password = properties.password
            c.port = properties.port
            c.username = properties.username

        }.newConnection()
        channel = connection.createChannel()

        context.registerBean(Connection::class.java, { connection })
        context.registerBean(Channel::class.java, { channel })

        exchanges.forEach {
            it.channel = channel
            it.initialize(context)
        }
    }
}

fun RabbitMqDsl.exchange(
    name: String = "defaultExchange", type: ExchangeDsl.ExchangeTypes = ExchangeDsl.ExchangeTypes.fanout,
    dsl: ExchangeDsl.() -> Unit
): ExchangeDsl {
    val exchangeDsl = ExchangeDsl(name, type)
    exchanges += exchangeDsl
    exchangeDsl.apply(dsl)
    return exchangeDsl
}

class ExchangeDsl(
    var name: String,
    var type: ExchangeTypes = ExchangeTypes.fanout

) : AbstractDsl() {

    internal val queues = mutableListOf<QueueDsl>()
    lateinit var channel: Channel

    override fun initialize(context: GenericApplicationContext) {
        super.initialize(context)

        channel.exchangeDeclare(name, type.name)

        queues.forEach {
            it.channel = channel
            it.initialize(context)
        }
    }

    enum class ExchangeTypes { direct, topic, fanout, headers, system }
}

fun ExchangeDsl.queue(
    name: String? = null, dsl: QueueDsl.() -> Unit
): QueueDsl {

    val queueDsl = QueueDsl(name = name, exchange = this.name)
    queueDsl.apply(dsl)
    queues += queueDsl
    return queueDsl
}

class QueueDsl(
    var name: String?,
    var exchange: String?,
    var routingKey: String = "",
    var durable: Boolean = false,
    var exclusive: Boolean = true,
    var autoDelete: Boolean = true,
    var arguments: MutableMap<String, Any> = mutableMapOf()

) : AbstractDsl() {

    lateinit var channel: Channel
    internal val consumer = mutableListOf<(String) -> Unit>()
    internal val publishers = mutableListOf<PublisherDsl>()

    override fun initialize(context: GenericApplicationContext) {
        super.initialize(context)

        val queue =
            if (name == null) channel.queueDeclare()
            else channel.queueDeclare(name, durable, exclusive, autoDelete, arguments)

        channel.queueBind(queue.queue, exchange, routingKey)

        consumer.forEach {
            channel.basicConsume(queue.queue,
                { _, message ->
                    it.invoke(message.body.toString(charset = Charset.defaultCharset()))
                },
                { _ -> })
        }
        publishers.forEach {
            it.channel = channel
            it.initialize(context)
        }
    }
}


class PublisherDsl(
    val exchange: String?,
    private val function: PublisherDsl.() -> Unit
) : AbstractDsl() {

    lateinit var channel: Channel

    override fun initialize(context: GenericApplicationContext) {
        super.initialize(context)
        function.invoke(this)
    }

    fun publish(message: String, routingKey: String = "") {
        channel.basicPublish(exchange, routingKey, null, message.toByteArray())
    }
}

fun QueueDsl.publisher(f: PublisherDsl.() -> Unit) {
    publishers += PublisherDsl(exchange, f)
}

fun QueueDsl.consumer(f: (String) -> Unit) {
    consumer += f
}

fun ConfigurationDsl.rabbitMq(dsl: RabbitMqDsl.() -> Unit) {
    RabbitMqDsl(dsl).initialize(context)
}