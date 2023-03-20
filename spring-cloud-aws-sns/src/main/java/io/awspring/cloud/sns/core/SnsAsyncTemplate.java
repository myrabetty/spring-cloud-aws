/*
 * Copyright 2013-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sns.core;

import static io.awspring.cloud.sns.core.SnsHeaders.NOTIFICATION_SUBJECT_HEADER;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.core.AbstractMessageSendingTemplate;
import org.springframework.messaging.core.DestinationResolvingMessageSendingOperations;
import org.springframework.messaging.core.MessagePostProcessor;
import org.springframework.util.Assert;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

/**
 * Helper class that simplifies asynchronous sending of notifications to SNS. The only mandatory field is
 * {@link SnsAsyncClient}.
 *
 * @author Elisabetta Semboloni
 */
public class SnsAsyncTemplate extends AbstractMessageSendingTemplate<TopicAsyncMessageChannel>
		implements DestinationResolvingMessageSendingOperations<TopicAsyncMessageChannel>, SnsOperations {

	private final SnsAsyncClient snsClient;
	private final TopicArnResolver topicArnResolver;

	public SnsAsyncTemplate(SnsAsyncClient snsClient) {
		this(snsClient, null);
	}

	public SnsAsyncTemplate(SnsAsyncClient snsClient, @Nullable MessageConverter messageConverter) {
		this(snsClient, new CachingTopicArnResolver(new DefaultAsyncTopicArnResolver(snsClient)), messageConverter);
	}

	public SnsAsyncTemplate(SnsAsyncClient snsAsyncClient, TopicArnResolver topicArnResolver,
			@Nullable MessageConverter messageConverter) {
		Assert.notNull(snsAsyncClient, "SnsClient must not be null");
		Assert.notNull(topicArnResolver, "topicArnResolver must not be null");
		this.topicArnResolver = topicArnResolver;
		this.snsClient = snsAsyncClient;

		if (messageConverter != null) {
			this.setMessageConverter(initMessageConverter(messageConverter));
		}
	}

	public void setDefaultDestinationName(@Nullable String defaultDestination) {
		super.setDefaultDestination(
				defaultDestination == null ? null : resolveMessageChannelByTopicName(defaultDestination));
	}

	@Override
	protected void doSend(TopicAsyncMessageChannel destination, Message<?> message) {
		destination.send(message);
	}

	protected CompletableFuture<Boolean> doAsyncSend(TopicAsyncMessageChannel destination, Message<?> message) {
		return destination.sendAsync(message);
	}

	@Override
	public void send(String destination, Message<?> message) throws MessagingException {
		doSend(resolveMessageChannelByTopicName(destination), message);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload) throws MessagingException {
		this.convertAndSend(destination, payload, null, null);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload, @Nullable Map<String, Object> headers)
			throws MessagingException {
		this.convertAndSend(destination, payload, headers, null);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload, @Nullable MessagePostProcessor postProcessor)
			throws MessagingException {
		this.convertAndSend(destination, payload, null, postProcessor);
	}

	@Override
	public <T> void convertAndSend(String destination, T payload, @Nullable Map<String, Object> headers,
			@Nullable MessagePostProcessor postProcessor) throws MessagingException {
			convertAndSend(resolveMessageChannelByTopicName(destination), payload, headers, postProcessor);
	}

	/**
	 * Convenience method that sends a synchronous notification with the given {@literal message} and {@literal subject} to the
	 * {@literal destination}. The {@literal subject} is sent as header as defined in the
	 * <a href="https://docs.aws.amazon.com/sns/latest/dg/json-formats.html">SNS message JSON formats</a>.
	 * @param destinationName The logical name of the destination
	 * @param message The message to send
	 * @param subject The subject to send
	 */
	public void sendNotification(String destinationName, Object message, @Nullable String subject) {
		this.convertAndSend(destinationName, message, Collections.singletonMap(NOTIFICATION_SUBJECT_HEADER, subject));
	}

	/**
	 * Convenience method that sends a synchronous notification with the given {@literal message} and {@literal subject} to the
	 * {@literal destination}. The {@literal subject} is sent as header as defined in the
	 * <a href="https://docs.aws.amazon.com/sns/latest/dg/json-formats.html">SNS message JSON formats</a>. The
	 * configured default destination will be used.
	 * @param message The message to send
	 * @param subject The subject to send
	 */
	public void sendNotification(Object message, @Nullable String subject) {
		this.convertAndSend(getRequiredDefaultDestination(), message,
				Collections.singletonMap(NOTIFICATION_SUBJECT_HEADER, subject));
	}

	@Override
	public void sendNotification(String topic, SnsNotification<?> notification) {
		this.convertAndSend(topic, notification.getPayload(), notification.getHeaders());
	}

	private TopicAsyncMessageChannel resolveMessageChannelByTopicName(String topicName) {
			Arn topicArn = this.topicArnResolver.resolveTopicArn(topicName);
			return new TopicAsyncMessageChannel(this.snsClient, topicArn);

	}

	private static CompositeMessageConverter initMessageConverter(@Nullable MessageConverter messageConverter) {
		List<MessageConverter> converters = new ArrayList<>();

		StringMessageConverter stringMessageConverter = new StringMessageConverter();
		stringMessageConverter.setSerializedPayloadClass(String.class);
		converters.add(stringMessageConverter);

		if (messageConverter != null) {
			converters.add(messageConverter);
		}

		return new CompositeMessageConverter(converters);
	}

	public <T> CompletableFuture<Boolean> convertAndSendAsync(String destination, T payload) throws MessagingException {
		return this.convertAndSendAsync(destination, payload, null, null);
	}

	public <T> CompletableFuture<Boolean> convertAndSendAsync(String destination, T payload, @Nullable Map<String, Object> headers)
		throws MessagingException {
		return this.convertAndSendAsync(destination, payload, headers, null);
	}

	public <T> CompletableFuture<Boolean> convertAndSendAsync(TopicAsyncMessageChannel destination, T payload, @Nullable Map<String, Object> headers,
		@Nullable MessagePostProcessor postProcessor) throws MessagingException {
		Message<?> message = doConvert(payload, headers, postProcessor);
		return doAsyncSend(destination, message);
	}

	public <T> CompletableFuture<Boolean> convertAndSendAsync(String destination, T payload, @Nullable Map<String, Object> headers,
		@Nullable MessagePostProcessor postProcessor) throws MessagingException {
		return this.convertAndSendAsync(resolveMessageChannelByTopicName(destination), payload, headers, postProcessor);
	}


	/**
	 * Convenience method that sends asynchronously a notification with the given {@literal message} and {@literal subject} to the
	 * {@literal destination}. The {@literal subject} is sent as header as defined in the
	 * <a href="https://docs.aws.amazon.com/sns/latest/dg/json-formats.html">SNS message JSON formats</a>.
	 * @param destinationName The logical name of the destination
	 * @param message The message to send
	 * @param subject The subject to send
	 */
	public CompletableFuture<Boolean> sendNotificationAsync(String destinationName, Object message, @Nullable String subject) {
		return this.convertAndSendAsync(destinationName, message, Collections.singletonMap(NOTIFICATION_SUBJECT_HEADER, subject));
	}

	public CompletableFuture<Boolean> sendNotificationAsync(String topic, SnsNotification<?> notification) {
		return this.convertAndSendAsync(topic, notification.getPayload(), notification.getHeaders());
	}
}
