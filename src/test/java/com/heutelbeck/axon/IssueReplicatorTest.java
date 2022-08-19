package com.heutelbeck.axon;

import static org.axonframework.messaging.responsetypes.ResponseTypes.instanceOf;
import static reactor.test.StepVerifier.create;

import java.time.Duration;

import org.axonframework.queryhandling.QueryGateway;
import org.axonframework.queryhandling.QueryHandler;
import org.axonframework.queryhandling.QueryUpdateEmitter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.heutelbeck.axon.IssueReplicatorTest.TestScenarioConfiguration;

import reactor.core.publisher.Flux;

@SpringBootTest
@Testcontainers
@Import(TestScenarioConfiguration.class)
public class IssueReplicatorTest {
	private static final int  AXON_SERVER_GRPC_PORT          = 8124;
	private static final long TIMEOUT_FOR_AXON_SERVER_SPINUP = 40L;

	@Container
	static GenericContainer<?> axonServer = new GenericContainer<>(DockerImageName.parse("axoniq/axonserver"))
			.withExposedPorts(8024, 8124).waitingFor(Wait.forHttp("/actuator/info").forPort(8024))
			.withStartupTimeout(Duration.ofSeconds(TIMEOUT_FOR_AXON_SERVER_SPINUP));

	@Autowired
	QueryGateway queryGateway;

	@Autowired
	QueryUpdateEmitter emitter;

	@DynamicPropertySource
	static void registerAxonProperties(DynamicPropertyRegistry registry) {
		registry.add("axon.axonserver.servers",
				() -> axonServer.getHost() + ":" + axonServer.getMappedPort(AXON_SERVER_GRPC_PORT));
	}

	@Test
	void testForIssue2331() {
		var emitIntervallMs        = 200L;
		var numberOfEmittedUpdates = 2;

		// Query expects Integer and String
		var result = queryGateway.subscriptionQuery("demo", null, instanceOf(Integer.class), instanceOf(String.class));

		// Updates which match for the correct update message type.
		Flux.interval(Duration.ofMillis(emitIntervallMs)).doOnNext(i -> emitter.emit(
				query -> String.class.isAssignableFrom(query.getUpdateResponseType().responseMessagePayloadType()),
				"update-" + i)).take(Duration.ofMillis(emitIntervallMs * numberOfEmittedUpdates + emitIntervallMs / 2L))
				.subscribe();

		// Updates which match for the wrong update message type.
		Flux.interval(Duration.ofMillis(emitIntervallMs)).doOnNext(i -> emitter.emit(
				query -> Integer.class.isAssignableFrom(query.getUpdateResponseType().responseMessagePayloadType()), i))
				.take(Duration.ofMillis(emitIntervallMs * numberOfEmittedUpdates + emitIntervallMs / 2L)).subscribe();

		// Verify initial response
		create(result.initialResult().timeout(Duration.ofSeconds(10L))).expectNext(1234).verifyComplete();

		// Verify updates. All Updates should come from the Flux checking for String as
		// update response type of the respective queries.
		create(result.updates().take(2).timeout(Duration.ofSeconds(10L))).expectNext("update-0", "update-1")
				.verifyComplete();

		result.cancel();
	}

	public static class Projection {
		@QueryHandler(queryName = "demo")
		public Integer handleDemoQuery() {
			return 1234;
		}
	}

	@Configuration
	static class TestScenarioConfiguration {
		@Bean
		Projection projection() {
			return new Projection();
		}
	}

	@SpringBootApplication
	static class TestApplication {
		public static void main(String[] args) {
			SpringApplication.run(TestApplication.class, args);
		}
	}
}
