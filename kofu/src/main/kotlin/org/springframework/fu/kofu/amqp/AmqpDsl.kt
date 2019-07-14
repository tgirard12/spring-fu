package org.springframework.fu.kofu.amqp

import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.annotation.Queue
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint
import org.springframework.amqp.rabbit.connection.*
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration
import org.springframework.boot.autoconfigure.amqp.RabbitProperties
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties
import org.springframework.boot.context.properties.PropertyMapper
import org.springframework.context.support.GenericApplicationContext
import org.springframework.fu.kofu.AbstractDsl
import org.springframework.fu.kofu.cassandra.CassandraDsl
import java.time.Duration
import java.util.function.Consumer
import java.util.function.Supplier

open class AmqpDsl(private val init: AmqpDsl.() -> Unit) : AbstractDsl() {

    protected val properties = RabbitProperties()


    var host: String
        get() = properties.host
        set(value) {
            properties.host = value
        }

    @RabbitListener()
    fun rabbitListener(queues: List<Queue>, listener: (Message) -> Unit) {

        RabbitListenerEndpointRegistry().apply {
            this.registerListenerContainer(
                SimpleRabbitListenerEndpoint().apply {
                    this.setMessageListener { listener.invoke(it) }
                },
                RabbitListenerContainerFactory { }
            )
        }

    }

    @RabbitListener
    override fun initialize(context: GenericApplicationContext) {
        super.initialize(context)
        init()

        val map = PropertyMapper.get()
        val factory = CachingConnectionFactory(
            getRabbitConnectionFactoryBean(properties).getObject()
        )
        map.from { properties.determineAddresses() }.to { factory.setAddresses(it) }
        map.from { properties.isPublisherConfirms }.to { factory.isPublisherConfirms = it }
        map.from { properties.isPublisherReturns }.to { factory.isPublisherReturns = it }
        val channel = properties.cache.channel
        map.from { channel.size }.whenNonNull().to { factory.channelCacheSize = it }
        map.from { channel.checkoutTimeout }.whenNonNull().`as`<Long> { it.toMillis() }
            .to { factory.setChannelCheckoutTimeout(it) }
        val connection = properties.cache.connection
        map.from { connection.mode }.whenNonNull().to { factory.cacheMode = it }
        map.from { connection.size }.whenNonNull().to { factory.connectionCacheSize = it }
        // TODO map.from(connectionNameStrategy::getIfUnique).whenNonNull().to(factory::setConnectionNameStrategy);


        context.registerBean<ConnectionFactory>(ConnectionFactory::class.java, { factory }, null)
    }

    @Throws(Exception::class)
    private fun getRabbitConnectionFactoryBean(properties: RabbitProperties): RabbitConnectionFactoryBean {
        val map = PropertyMapper.get()
        val factory = RabbitConnectionFactoryBean()
        map.from { properties.determineHost() }.whenNonNull().to { factory.setHost(it) }
        map.from { properties.determinePort() }.to { factory.setPort(it) }
        map.from { properties.determineUsername() }.whenNonNull().to { factory.setUsername(it) }
        map.from { properties.determinePassword() }.whenNonNull().to { factory.setPassword(it) }
        map.from { properties.determineVirtualHost() }.whenNonNull().to { factory.setVirtualHost(it) }
        map.from { properties.requestedHeartbeat }.whenNonNull()
            .asInt<Long> { it.getSeconds() }
            .to { factory.setRequestedHeartbeat(it) }
        val ssl = properties.ssl
        if (ssl.isEnabled) {
            factory.setUseSSL(true)
            map.from { ssl.algorithm }.whenNonNull().to { factory.setSslAlgorithm(it) }
            map.from { ssl.keyStoreType }.to { factory.setKeyStoreType(it) }
            map.from { ssl.keyStore }.to { factory.setKeyStore(it) }
            map.from { ssl.keyStorePassword }.to { factory.setKeyStorePassphrase(it) }
            map.from { ssl.trustStoreType }.to { factory.setTrustStoreType(it) }
            map.from { ssl.trustStore }.to { factory.setTrustStore(it) }
            map.from { ssl.trustStorePassword }.to { factory.setTrustStorePassphrase(it) }
            map.from { ssl.isValidateServerCertificate }
                .to { validate -> factory.isSkipServerCertificateValidation = !validate }
            map.from { ssl.verifyHostname }.to { factory.setEnableHostnameVerification(it) }
        }
        map.from { properties.connectionTimeout }.whenNonNull().asInt<Long> { it.toMillis() }
            .to { factory.setConnectionTimeout(it) }
        factory.afterPropertiesSet()
        return factory
    }
}

fun AmqpDsl.amqp(dsl: AmqpDsl.() -> Unit) {
    AmqpDsl(dsl).initialize(context)
}