/*
 * Copyright 2002-2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.InterceptableChannel;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Abstract base class for {@link AbstractAsyncMessageChannel} implementations. Replicates {@link
 * org.springframework.messaging.support.AbstractMessageChannel}
 *
 * @author Elisabette Semboloni
 */
public abstract class AbstractAsyncMessageChannel
    implements MessageChannel, InterceptableChannel, BeanNameAware {
  private static final int MAX_TIMEOUT_MILLIS = 1000;
  protected Log logger = LogFactory.getLog(getClass());

  private String beanName;

  private final List<ChannelInterceptor> interceptors = new ArrayList<>(5);

  protected AbstractAsyncMessageChannel() {
    this.beanName = getClass().getSimpleName() + "@" + ObjectUtils.getIdentityHexString(this);
  }

  /**
   * Set an alternative logger to use than the one based on the class name.
   *
   * @param logger the logger to use
   * @since 5.1
   */
  public void setLogger(Log logger) {
    this.logger = logger;
  }

  /**
   * Return the currently configured Logger.
   *
   * @since 5.1
   */
  public Log getLogger() {
    return logger;
  }

  /** A message channel uses the bean name primarily for logging purposes. */
  @Override
  public void setBeanName(String name) {
    this.beanName = name;
  }

  /** Return the bean name for this message channel. */
  public String getBeanName() {
    return this.beanName;
  }

  @Override
  public void setInterceptors(List<ChannelInterceptor> interceptors) {
    Assert.noNullElements(interceptors, "'interceptors' must not contain null elements");
    this.interceptors.clear();
    this.interceptors.addAll(interceptors);
  }

  @Override
  public void addInterceptor(ChannelInterceptor interceptor) {
    Assert.notNull(interceptor, "'interceptor' must not be null");
    this.interceptors.add(interceptor);
  }

  @Override
  public void addInterceptor(int index, ChannelInterceptor interceptor) {
    Assert.notNull(interceptor, "'interceptor' must not be null");
    this.interceptors.add(index, interceptor);
  }

  @Override
  public List<ChannelInterceptor> getInterceptors() {
    return Collections.unmodifiableList(this.interceptors);
  }

  @Override
  public boolean removeInterceptor(ChannelInterceptor interceptor) {
    return this.interceptors.remove(interceptor);
  }

  @Override
  public ChannelInterceptor removeInterceptor(int index) {
    return this.interceptors.remove(index);
  }

  /**
   * Implements a send with a default timeout which will be sent asynchronously but returned in a
   * synchronous fashion.
   */
  @Override
  public final boolean send(Message<?> message) {
    return send(message, MAX_TIMEOUT_MILLIS);
  }

  /**
   * Implements a send with a custom time-out. Send is performed asynchronously but returned in a
   * synchronous fashion.
   */
  @Override
  public final boolean send(Message<?> message, long timeout) {
    try {
      return sendAsync(message).get(timeout, TimeUnit.MILLISECONDS);
    }  catch (InterruptedException | TimeoutException e) {
		// Interrupted/ timed-out while waiting for the result.
		throw new RuntimeException(e);
	} catch (ExecutionException e) {
		// ExecutionException is a wrapper the get() puts over the real exception.
		if (e.getCause() instanceof RuntimeException) {
			// The future failed with a runtime exception, just throw that.
			throw (RuntimeException)e.getCause();
		} else {
			// The future failed with a non-runtime exception, but the interface only permits runtime.
			throw new RuntimeException(e.getCause());
		}
	}
  }

  /**
   * Send a message in an asynchronous fashion returning a {@link CompletableFuture} which contain a
   * boolean indicating if the message was sent or not.
   *
   * @param message
   * @return
   */
  public CompletableFuture<Boolean> sendAsync(Message<?> message) {
    Assert.notNull(message, "Message must not be null");
    ChannelInterceptorChain chain = new ChannelInterceptorChain();
    CompletableFuture<Boolean> sentFuture = CompletableFuture.completedFuture(false);
    try {
      message = chain.applyPreSend(message, this);
      Message<?> messageToUse = message;
      if (messageToUse == null) {
        return CompletableFuture.completedFuture(false);
      }
      sentFuture =
          sendInternalAsync(messageToUse)
              .thenApply(
                  sent -> {
                    chain.applyPostSend(messageToUse, this, sent);
                    chain.triggerAfterSendCompletion(messageToUse, this, sent, null);
                    return sent;
                  });

      return sentFuture;
    } catch (Exception ex) {
      message = chain.applyPreSend(message, this);
      Message<?> messageToUse = message;
      sentFuture.thenAccept(sent -> chain.triggerAfterSendCompletion(messageToUse, this, sent, ex));
      if (ex instanceof MessagingException) {
        throw (MessagingException) ex;
      }
      throw new MessageDeliveryException(message, "Failed to send message to " + this, ex);
    } catch (Throwable err) {
      message = chain.applyPreSend(message, this);
      Message<?> messageToUse = message;
      MessageDeliveryException ex2 =
          new MessageDeliveryException(message, "Failed to send message to " + this, err);
      sentFuture.thenAccept(
          sent -> chain.triggerAfterSendCompletion(messageToUse, this, sent, ex2));
      throw ex2;
    }
  }

  protected abstract CompletableFuture<Boolean> sendInternalAsync(Message<?> message);

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + this.beanName + "]";
  }

  /** Assists with the invocation of the configured channel interceptors. */
  protected class ChannelInterceptorChain {

    private int sendInterceptorIndex = -1;

    private int receiveInterceptorIndex = -1;

    @Nullable
    public Message<?> applyPreSend(Message<?> message, MessageChannel channel) {
      Message<?> messageToUse = message;
      for (ChannelInterceptor interceptor : interceptors) {
        Message<?> resolvedMessage = interceptor.preSend(messageToUse, channel);
        if (resolvedMessage == null) {
          String name = interceptor.getClass().getSimpleName();
          if (logger.isDebugEnabled()) {
            logger.debug(name + " returned null from preSend, i.e. precluding the send.");
          }
          triggerAfterSendCompletion(messageToUse, channel, false, null);
          return null;
        }
        messageToUse = resolvedMessage;
        this.sendInterceptorIndex++;
      }
      return messageToUse;
    }

    public void applyPostSend(Message<?> message, MessageChannel channel, boolean sent) {
      for (ChannelInterceptor interceptor : interceptors) {
        interceptor.postSend(message, channel, sent);
      }
    }

    public void triggerAfterSendCompletion(
        Message<?> message, MessageChannel channel, boolean sent, @Nullable Exception ex) {

      for (int i = this.sendInterceptorIndex; i >= 0; i--) {
        ChannelInterceptor interceptor = interceptors.get(i);
        try {
          interceptor.afterSendCompletion(message, channel, sent, ex);
        } catch (Throwable ex2) {
          logger.error("Exception from afterSendCompletion in " + interceptor, ex2);
        }
      }
    }

    public boolean applyPreReceive(MessageChannel channel) {
      for (ChannelInterceptor interceptor : interceptors) {
        if (!interceptor.preReceive(channel)) {
          triggerAfterReceiveCompletion(null, channel, null);
          return false;
        }
        this.receiveInterceptorIndex++;
      }
      return true;
    }

    @Nullable
    public Message<?> applyPostReceive(Message<?> message, MessageChannel channel) {
      Message<?> messageToUse = message;
      for (ChannelInterceptor interceptor : interceptors) {
        messageToUse = interceptor.postReceive(messageToUse, channel);
        if (messageToUse == null) {
          return null;
        }
      }
      return messageToUse;
    }

    public void triggerAfterReceiveCompletion(
        @Nullable Message<?> message, MessageChannel channel, @Nullable Exception ex) {

      for (int i = this.receiveInterceptorIndex; i >= 0; i--) {
        ChannelInterceptor interceptor = interceptors.get(i);
        try {
          interceptor.afterReceiveCompletion(message, channel, ex);
        } catch (Throwable ex2) {
          if (logger.isErrorEnabled()) {
            logger.error("Exception from afterReceiveCompletion in " + interceptor, ex2);
          }
        }
      }
    }
  }
}
