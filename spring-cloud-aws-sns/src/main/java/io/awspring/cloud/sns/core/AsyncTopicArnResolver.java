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
import software.amazon.awssdk.arns.Arn;

/**
 * Resolves topic ARN by name.
 *
 * @author Elisabetta Semboloni
 */
public interface AsyncTopicArnResolver extends TopicArnResolver {

	CompletableFuture<Arn> resolveTopicArnAsync(String topicName);

	/**
	 * Resolves topic ARN by topic name. If topicName is already an ARN, it returns {@link Arn}. If topicName is just a
	 * string with a topic name, it attempts to create a topic or if topic already exists, just returns its ARN.
	 */
	@Override
	default Arn resolveTopicArn(String topicName){
			return resolveTopicArnAsync(topicName).join();
	}
}
