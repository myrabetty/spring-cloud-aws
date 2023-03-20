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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.util.Assert;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;

/**
 * Default implementation of {@link AsyncTopicArnResolver} used to determine topic ARN by name asynchronously.
 *
 * @author Elisabette Semboloni
 */
public class DefaultAsyncTopicArnResolver implements AsyncTopicArnResolver {

	private final SnsAsyncClient snsClient;

	public DefaultAsyncTopicArnResolver(SnsAsyncClient snsClient) {
		Assert.notNull(snsClient, "snsClient is required");
		this.snsClient = snsClient;
	}

	@Override
	public CompletableFuture<Arn> resolveTopicArnAsync(String topicName) {
		Assert.notNull(topicName, "topicName must not be null");
		if (topicName.toLowerCase().startsWith("arn:")) {
			return CompletableFuture.completedFuture(Arn.fromString(topicName));
		} else {
			CreateTopicRequest.Builder builder = CreateTopicRequest.builder().name(topicName);

			// fix for https://github.com/awspring/spring-cloud-aws/issues/707
			if (topicName.endsWith(".fifo")) {
				builder.attributes(Map.of("FifoTopic", "true"));
			}

			// if topic exists, createTopic returns successful response with topic arn
			final var topicFuture = this.snsClient.createTopic(builder.build());
			return topicFuture.thenApply(topic -> Arn.fromString(topic.topicArn()));
		}
	}
}
