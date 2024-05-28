package com.j256.cloudwatchlogbackappender;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.*;

import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.LoggingEvent;

import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.util.Collections;

public class Ec2InstanceIdConverterTest extends BaseConverterTest {

	@Test(timeout = 10000)
	public void testInstanceId() throws InterruptedException {

		String instanceId = "fewhwehpewpf";
		Ec2InstanceIdConverter.setInstanceId(instanceId);

		CloudWatchLogsClient awsLogClient = createMock(CloudWatchLogsClient.class);
		appender.setAwsLogsClient(awsLogClient);

		String prefix = "logstream-";
		appender.setLogStream(prefix + "%instanceId");
		final String expectedLogStream = prefix + instanceId;
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

	@Test(timeout = 10000)
	public void testInstanceNameUnknown() throws InterruptedException {

		Ec2InstanceIdConverter.setInstanceId(null);

		CloudWatchLogsClient awsLogClient = createMock(CloudWatchLogsClient.class);
		appender.setAwsLogsClient(awsLogClient);

		String prefix = "logstream-";
		appender.setLogStream(prefix + "%instanceId");
		final String expectedLogStream = prefix + "unknown";
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
