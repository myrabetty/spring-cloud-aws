/*
 * Copyright 2013-2023 the original author or authors.
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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.ListTopicsRequest;
import software.amazon.awssdk.services.sns.model.ListTopicsResponse;
import software.amazon.awssdk.services.sns.model.Topic;

/**
 * Basic implementation for resolving ARN from topicName. It is listing all topics using {@link SnsAsyncClient#listTopics()}
 * to determine topicArn for topicName.
 *
 * @author Elisabetta Semboloni
 */
public class TopicsAsyncListingTopicArnResolver implements AsyncTopicArnResolver {

	private final SnsAsyncClient snsAsyncClient;

	public TopicsAsyncListingTopicArnResolver(SnsAsyncClient snsClient) {
		Assert.notNull(snsClient, "SnsClient cannot be null!");
		this.snsAsyncClient = snsClient;
	}

	@Override
	public CompletableFuture<Arn> resolveTopicArnAsync(String topicName) {
		final var topicsFuture = snsAsyncClient.listTopics();
		return checkIfArnIsInList(topicName, topicsFuture);
	}

	private CompletableFuture<Arn> doRecursiveCall(@Nullable String token, String topicName) {
		if (token != null) {
				CompletableFuture<ListTopicsResponse> topicsResponse = snsAsyncClient
					.listTopics(ListTopicsRequest.builder().nextToken(token).build());
				return checkIfArnIsInList(topicName, topicsResponse);
		} else {
			throw new TopicNotFoundException("Topic does not exist for given topic name!");
		}
	}

	private CompletableFuture<Arn> checkIfArnIsInList(String topicName, CompletableFuture<ListTopicsResponse> listTopicsResponseFuture) {
		return listTopicsResponseFuture.thenCompose(listTopicsResponse ->  {
			Optional<String> arn = listTopicsResponse.topics().stream().map(Topic::topicArn)
				.filter(ta -> ta.contains(topicName)).findFirst();
			return arn.map(s -> CompletableFuture.completedFuture(Arn.fromString(s)))
				.orElseGet(() -> doRecursiveCall(listTopicsResponse.nextToken(), topicName));
		});
	}

}
