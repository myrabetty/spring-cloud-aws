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

import java.util.concurrent.CompletableFuture;
import org.springframework.messaging.Message;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

/**
 * Implementation of {@link AbstractAsyncMessageChannel} which is used for converting and sending messages via
 * {@link SnsAsyncClient} to SNS in an asynchronous fashion.
 *
 * @author Elisabetta Semboloni
 */
public final class TopicAsyncMessageChannel extends AbstractAsyncMessageChannel {

	private final SnsAsyncClient snsAsyncClient;

	private final Arn topicArn;

	public TopicAsyncMessageChannel(SnsAsyncClient snsAsyncClient, Arn topicArn) {
		this.snsAsyncClient = snsAsyncClient;
		this.topicArn = topicArn;
	}

	protected CompletableFuture<Boolean> sendInternalAsync(Message<?> message) {
		return this.snsAsyncClient.publish(TopicMessageChannelUtils.getBuilder(message, topicArn).build()).thenApply(ignored -> true);
	}


}
