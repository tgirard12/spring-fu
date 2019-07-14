package org.springframework.fu.kofu.amqp

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.boot.WebApplicationType
import org.springframework.fu.kofu.application
import org.springframework.util.SocketUtils
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

internal class RabbitMqDslTest {

    lateinit var cdl: CountDownLatch

    val log = LoggerFactory.getLogger(RabbitMqDslTest::class.java)

    @BeforeEach
    fun before() {
        cdl = CountDownLatch(1)
    }

    @Test
    fun `enable amqp configuration`() {
        val port = SocketUtils.findAvailableTcpPort()
        val app = application(WebApplicationType.NONE) {
            beans {
            }
            rabbitMq {
                exchange {
                    queue {
                        consumer {
                            log.info("receive '$it'")
                        }
                        publisher {
                            Flux.just(1, 2, 3, 4, 5)
                                .delayElements(Duration.ofSeconds(1))
                                .subscribe {
                                    publish("Message $it")
                                }
                        }
                    }
                }
            }
        }
        with(app.run()) {
            cdl.await(10000, TimeUnit.MILLISECONDS)
            close()
        }
    }
}