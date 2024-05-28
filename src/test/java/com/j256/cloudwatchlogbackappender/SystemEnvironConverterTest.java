package com.j256.cloudwatchlogbackappender;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.LoggingEvent;

import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.util.Collections;

public class SystemEnvironConverterTest extends BaseConverterTest {

	@Test(timeout = 5000)
	public void testStuff() throws InterruptedException {

		String envName = "SHELL";
		String envValue = System.getenv(envName);

		CloudWatchLogsClient awsLogClient = createMock(CloudWatchLogsClient.class);
		appender.setAwsLogsClient(awsLogClient);

		String prefix = "logstream-";
		appender.setLogStream(prefix + "%systemEnviron{" + envName + "}");
		final String expectedLogStream = prefix + envValue;
		PatternLayout layout = new PatternLayout();
		layout.setPattern("%msg");
		layout.setContext(LOGGER_CONTEXT);
		layout.start();
		appender.setLayout(layout);

		LoggingEvent event = new LoggingEvent();
		event.setTimeStamp(System.currentTimeMillis());
		event.setLoggerName("name");
		event.setLevel(Level.DEBUG);
		event.setMessage("message");
		event.setMDCPropertyMap(Collections.emptyMap());

		final PutLogEventsResponse response = PutLogEventsResponse.builder().build();
		expect(awsLogClient.putLogEvents(isA(PutLogEventsRequest.class))).andAnswer(() -> {
			PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
			assertEquals(LOG_GROUP, request.logGroupName());
			assertEquals(expectedLogStream, request.logStreamName());
			return response;
		});
		awsLogClient.close();

		// =====================================

		replay(awsLogClient);
		appender.start();
		appender.append(event);
		while (appender.getEventsWrittenCount() < 1) {
			Thread.sleep(10);
		}
		appender.stop();
		verify(awsLogClient);
	}

	@Test(timeout = 5000)
	public void testNoEnvNameSpecified() throws InterruptedException {

		CloudWatchLogsClient awsLogClient = createMock(CloudWatchLogsClient.class);
		appender.setAwsLogsClient(awsLogClient);

		String prefix = "logstream-";
		appender.setLogStream(prefix + "%systemEnviron");
		final String expectedLogStream = prefix + null;
		PatternLayout layout = new PatternLayout();
		layout.setPattern("%msg");
		layout.setContext(LOGGER_CONTEXT);
		layout.start();
		appender.setLayout(layout);

		LoggingEvent event = new LoggingEvent();
		event.setTimeStamp(System.currentTimeMillis());
		event.setLoggerName("name");
		event.setLevel(Level.DEBUG);
		event.setMessage("message");
		event.setMDCPropertyMap(Collections.emptyMap());

		final PutLogEventsResponse response = PutLogEventsResponse.builder().build();
		expect(awsLogClient.putLogEvents(isA(PutLogEventsRequest.class))).andAnswer(() -> {
			PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
			assertEquals(LOG_GROUP, request.logGroupName());
			assertEquals(expectedLogStream, request.logStreamName());
			return response;
		});
		awsLogClient.close();

		// =====================================

		replay(awsLogClient);
		appender.start();
		appender.append(event);
		while (appender.getEventsWrittenCount() < 1) {
			Thread.sleep(10);
		}
		appender.stop();
		verify(awsLogClient);
	}

	@Test(timeout = 5000)
	public void testUnknownEnvNameSpecified() throws InterruptedException {

		CloudWatchLogsClient awsLogClient = createMock(CloudWatchLogsClient.class);
		appender.setAwsLogsClient(awsLogClient);

		String prefix = "logstream-";
		appender.setLogStream(prefix + "%systemEnviron{NOTKNOWNVARIABLE}");
		final String expectedLogStream = prefix + null;
		PatternLayout layout = new PatternLayout();
		layout.setPattern("%msg");
		layout.setContext(LOGGER_CONTEXT);
		layout.start();
		appender.setLayout(layout);

		LoggingEvent event = new LoggingEvent();
		event.setTimeStamp(System.currentTimeMillis());
		event.setLoggerName("name");
		event.setLevel(Level.DEBUG);
		event.setMessage("message");
		event.setMDCPropertyMap(Collections.emptyMap());

		final PutLogEventsResponse response = PutLogEventsResponse.builder().build();
		expect(awsLogClient.putLogEvents(isA(PutLogEventsRequest.class))).andAnswer(() -> {
			PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
			assertEquals(LOG_GROUP, request.logGroupName());
			assertEquals(expectedLogStream, request.logStreamName());
			return response;
		});
		awsLogClient.close();

		// =====================================

		replay(awsLogClient);
		appender.start();
		appender.append(event);
		while (appender.getEventsWrittenCount() < 1) {
			Thread.sleep(10);
		}
		appender.stop();
		verify(awsLogClient);
	}
}
