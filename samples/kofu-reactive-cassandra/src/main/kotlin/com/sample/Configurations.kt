package com.sample

import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.fu.kofu.cassandra.reactiveCassandra
import org.springframework.fu.kofu.configuration
import org.springframework.fu.kofu.webflux.mustache
import org.springframework.fu.kofu.webflux.webFlux

fun dataConfig(cassandraHost: String,
			   cassandraPort: Int) = configuration {
	beans {
		bean<UserRepository>()
	}
	listener<ApplicationReadyEvent> {
		ref<UserRepository>().init()
	}
	reactiveCassandra {
		keyspaceName = "Kofu"
		port = cassandraPort
		contactPoints = listOf(cassandraHost)
	}
}

fun webConfig(serverPort: Int) = configuration {
	beans {
		bean<UserHandler>()
		bean(::routes)
	}
	webFlux {
		port = serverPort
		mustache()
		codecs {
			string()
			jackson()
		}
	}
}
