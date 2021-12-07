/*
 * Copyright (c) 2018-2019 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *        The Eclipse Public License is available at
 *        http://www.eclipse.org/legal/epl-v10.html
 *
 *        The Apache License v2.0 is available at
 *        http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.amqp;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.concurrent.*;

abstract public class FutureHandler<T, X> implements Future<T>, Handler<X> {

  protected ExecutionException exception;
  protected T result;
  protected CountDownLatch latch = new CountDownLatch(1);

  public static <T> FutureHandler<T, T> simple() {
    return new FutureHandler<T, T>() {
      @Override
      synchronized public void handle(T t) {
        result = t;
        latch.countDown();
      }
    };
  }

  public static <T> FutureHandler<T, AsyncResult<T>> asyncResult() {
    return new FutureHandler<T, AsyncResult<T>>() {
      @Override
      synchronized public void handle(AsyncResult<T> t) {
        if (t.succeeded()) {
          result = t.result();
        } else {
          exception = new ExecutionException(t.cause());
        }
        latch.countDown();
      }
    };
  }

  @Override
  abstract public void handle(X t);

  @Override
  public T get() throws InterruptedException, ExecutionException {
    latch.await();
    return result();
  }

  private T result() throws ExecutionException {
    synchronized (this) {
      if (exception != null) {
        throw exception;
      }
      return result;
    }
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
    if (latch.await(timeout, unit)) {
      return result();
    } else {
      throw new TimeoutException();
    }
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return false;
  }
}
