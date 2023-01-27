package org.backglite.examples.dlqhandling;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@EnableAutoConfiguration
@TestPropertySource(locations = "classpath:test.properties")
@ContextConfiguration(locations = {"classpath*:**/spring/dlqh-*.xml",
		"classpath*:**/spring/DlqHandlingApplicationTests-context.xml"},
		initializers = DlqHandlingApplicationTests.RabbitMQConnectionInitializer.class)
public class DlqHandlingApplicationTests {
	private static final int ADDITIONAL_WAIT_TIMEOUT = 1000;
	private static final int RABBITMQ_PORT = 5672;

	private static final GenericContainer<?> rabbitMqContainer = new GenericContainer<>("rabbitmq:3.7")
			.withExposedPorts(RABBITMQ_PORT)
			.withEnv("RABBITMQ_DEFAULT_USER", "guest")
			.withEnv("RABBITMQ_DEFAULT_PASS", "guest")
            .waitingFor(Wait.forListeningPort())
            .waitingFor(Wait.forLogMessage(".*?Setting permissions for 'guest'.*\n", 1));

	@Value("${dlq.max_retries}")
	private int dlqMaxRetries;

	@Value("${dlq.retries_delay}")
	private int dlqRetriesDelay;

	@Autowired
	@Qualifier(value = "testOutputObjectChannel")
	MessageChannel testOutputObjectChannel;

	@SpyBean
	@Qualifier(value = "processObjectChannel")
	MessageChannel processObjectChannel;

	@SpyBean
	@Qualifier(value = "outputRetryChannel")
	MessageChannel outputRetryChannel;

	@SpyBean
	@Qualifier(value = "outputParkingLotChannel")
	MessageChannel outputParkingLotChannel;

	@Test
	public void expectedObjectHandling() {
		Message<SimplePOJO> message = MessageBuilder.withPayload(new SimplePOJO("foo")).build();
		testOutputObjectChannel.send(message);

		verify(processObjectChannel, timeout(ADDITIONAL_WAIT_TIMEOUT)).send(any());
	}

	@Test
	public void unexpectedObjectHandling() {
		Message<OtherPOJO> message = MessageBuilder.withPayload(new OtherPOJO("foo")).build();
		testOutputObjectChannel.send(message);

		verify(outputRetryChannel,
				timeout(dlqRetriesDelay * dlqMaxRetries + ADDITIONAL_WAIT_TIMEOUT).times(dlqMaxRetries))
				.send(any());
		verify(outputParkingLotChannel, timeout(ADDITIONAL_WAIT_TIMEOUT)).send(any());
		verify(processObjectChannel, after(ADDITIONAL_WAIT_TIMEOUT).never()).send(any());
	}

	public static class RabbitMQConnectionInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

		@Override
		public void initialize(@NotNull ConfigurableApplicationContext applicationContext) {
            rabbitMqContainer.start();
			TestPropertySourceUtils.addInlinedPropertiesToEnvironment(applicationContext,
					"spring.rabbitmq.host=" + rabbitMqContainer.getContainerIpAddress(),
					"spring.rabbitmq.port=" + rabbitMqContainer.getMappedPort(RABBITMQ_PORT),
					"spring.rabbitmq.username=guest",
					"spring.rabbitmq.password=guest");
		}
	}
}
