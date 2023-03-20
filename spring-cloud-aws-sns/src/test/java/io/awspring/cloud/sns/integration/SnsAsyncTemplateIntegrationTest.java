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
package io.awspring.cloud.sns.integration;

import static io.awspring.cloud.sns.core.SnsHeaders.MESSAGE_DEDUPLICATION_ID_HEADER;
import static io.awspring.cloud.sns.core.SnsHeaders.MESSAGE_GROUP_ID_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SNS;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import io.awspring.cloud.sns.Person;
import io.awspring.cloud.sns.core.SnsAsyncTemplate;
import io.awspring.cloud.sns.core.SnsTemplate;
import io.awspring.cloud.sns.core.TopicsAsyncListingTopicArnResolver;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * Integration tests for {@link SnsAsyncTemplate}.
 *
 * @author Elisabetta Semboloni
 */
@Testcontainers
class SnsAsyncTemplateIntegrationTest {

	private static final String TOPIC_NAME = "my_topic_name";
	private static final ObjectMapper objectMapper = new ObjectMapper();
	@Container
	static LocalStackContainer localstack = new LocalStackContainer(
		DockerImageName.parse("localstack/localstack:1.4.0")).withServices(SNS).withServices(SQS).withReuse(true);
	private static SnsAsyncTemplate snsAsyncTemplate;
	private static SnsAsyncClient snsAsyncClient;
	private static SqsClient sqsClient;

	@BeforeAll
	public static void createSnsTemplate() {
		snsAsyncClient = SnsAsyncClient.builder().endpointOverride(localstack.getEndpointOverride(SNS))
			.region(Region.of(localstack.getRegion()))
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("noop", "noop")))
			.build();
		sqsClient = SqsClient.builder().endpointOverride(localstack.getEndpointOverride(SQS))
			.region(Region.of(localstack.getRegion()))
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("noop", "noop")))
			.build();
		MappingJackson2MessageConverter mappingJackson2MessageConverter = new MappingJackson2MessageConverter();
		mappingJackson2MessageConverter.setSerializedPayloadClass(String.class);
		snsAsyncTemplate = new SnsAsyncTemplate(snsAsyncClient, mappingJackson2MessageConverter);
	}

	@Nested
	class FifoTopics {

		private static String queueUrl;
		private static String queueArn;

		@BeforeAll
		public static void init() {
			queueUrl = sqsClient
				.createQueue(
					r -> r.queueName("my-queue.fifo").attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true")))
				.queueUrl();
			queueArn = sqsClient
				.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
				.attributes().get(QueueAttributeName.QUEUE_ARN);
		}

		@Test
		void sendAsync_validTextMessage_usesFifoChannel_send_arn_read_by_sqs()
			throws ExecutionException, InterruptedException {
			String topicName = "my_topic_name.fifo";
			Map<String, String> topicAttributes = new HashMap<>();
			topicAttributes.put("FifoTopic", String.valueOf(true));
			String topicArn = snsAsyncClient
				.createTopic(CreateTopicRequest.builder().name(topicName).attributes(topicAttributes).build())
				.get().topicArn();
			snsAsyncClient.subscribe(r -> r.topicArn(topicArn).protocol("sqs").endpoint(queueArn));

			final var message = snsAsyncTemplate.convertAndSendAsync(topicName, "message",
				Map.of(MESSAGE_GROUP_ID_HEADER, "group-id", MESSAGE_DEDUPLICATION_ID_HEADER, "deduplication-id"));
			await().untilAsserted(() -> {
				ReceiveMessageResponse response = sqsClient.receiveMessage(r -> r.queueUrl(queueUrl));
				assertThat(response.hasMessages()).isTrue();
				JsonNode body = objectMapper.readTree(response.messages().get(0).body());
				assertThat(body.get("Message").asText()).isEqualTo("message");
				assertThat(message.isDone()).isTrue();
			});
		}

		@Test
		void sendAsync_validTextMessage_usesFifoChannel_deliveryFails() {
			String topicName = "my_topic_name.fifo";
			final var result = snsAsyncTemplate.convertAndSendAsync(topicName, "message",
				Map.of(MESSAGE_GROUP_ID_HEADER, "group-id", MESSAGE_DEDUPLICATION_ID_HEADER, "deduplication-id"));
			assertThat(result.isDone()).isFalse();
		}

		@AfterEach
		public void purgeQueue() {
			sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
		}
	}

	@Nested
	class NonFifoTopics {

		private static String queueUrl;
		private static String queueArn;

		@BeforeAll
		public static void init() {
			queueUrl = sqsClient.createQueue(r -> r.queueName("my-queue")).queueUrl();
			queueArn = sqsClient
				.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
				.attributes().get(QueueAttributeName.QUEUE_ARN);
		}

		@Test
		void sendAsync_validTextMessage_usesTopicChannel_send_arn_read_by_sqs()
			throws ExecutionException, InterruptedException {
			String topicArn = snsAsyncClient.createTopic(CreateTopicRequest.builder().name(TOPIC_NAME).build()).get()
				.topicArn();
			snsAsyncClient.subscribe(r -> r.topicArn(topicArn).protocol("sqs").endpoint(queueArn));

			final var result = snsAsyncTemplate.convertAndSendAsync(topicArn, "message");

			await().untilAsserted(() -> {
				ReceiveMessageResponse response = sqsClient.receiveMessage(r -> r.queueUrl(queueUrl));
				assertThat(response.hasMessages()).isTrue();
				JsonNode body = objectMapper.readTree(response.messages().get(0).body());
				assertThat(body.get("Message").asText()).isEqualTo("message");
				assertThat(result.isDone()).isTrue();
			});
		}

		@Test
		void sendAsync_validPersonObject_usesTopicChannel_send_arn_read_sqs()
			throws ExecutionException, InterruptedException {
			String topic_arn = snsAsyncClient.createTopic(CreateTopicRequest.builder().name(TOPIC_NAME).build()).get()
				.topicArn();

			snsAsyncClient.subscribe(r -> r.topicArn(topic_arn).protocol("sqs").endpoint(queueArn));

			snsAsyncTemplate.convertAndSend(topic_arn, new Person("foo"));

			await().untilAsserted(() -> {
				ReceiveMessageResponse response = sqsClient.receiveMessage(r -> r.queueUrl(queueUrl));
				assertThat(response.hasMessages()).isTrue();
				Person person = objectMapper.readValue(
					objectMapper.readTree(response.messages().get(0).body()).get("Message").asText(), Person.class);
				assertThat(person.getName()).isEqualTo("foo");
			});
		}

		@AfterEach
		public void purgeQueue() {
			sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
		}
	}

	@Nested
	class TopicsListingTopicArnResolverTest {

		@BeforeAll
		public static void beforeAll() {
			createTopics();
		}

		private static void createTopics() {
			for (int i = 0; i < 101; i++) {
				snsAsyncClient.createTopic(CreateTopicRequest.builder().name(TOPIC_NAME + i).build());
			}
		}

		@Test
		void sendAsync_test_message_for_existing_topic_name_trigger_trigger_iteration() {
			TopicsAsyncListingTopicArnResolver topicsListingTopicArnResolver = new TopicsAsyncListingTopicArnResolver(
				snsAsyncClient);
			SnsAsyncTemplate snsTemplateTestCache = new SnsAsyncTemplate(snsAsyncClient, topicsListingTopicArnResolver,
				null);
			snsTemplateTestCache.sendNotificationAsync(TOPIC_NAME + 100, "message content", "subject");
		}

		@Test
		void sendAsync_test_message_for_not_existing_topic_name() {
			TopicsAsyncListingTopicArnResolver topicsListingTopicArnResolver = new TopicsAsyncListingTopicArnResolver(
				snsAsyncClient);
			SnsAsyncTemplate snsTemplateTestCache = new SnsAsyncTemplate(snsAsyncClient, topicsListingTopicArnResolver,
				null);
			assertThatThrownBy(
				() -> snsTemplateTestCache.sendNotificationAsync("Some_random_topic", "message content", "subject"))
				.isInstanceOf(CompletionException.class);
		}
	}

}
