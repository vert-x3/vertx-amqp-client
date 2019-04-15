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

import io.vertx.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

public class ConnectionTest extends ArtemisTestBase {

  private Vertx vertx;

  @Before
  public void init() {
    vertx = Vertx.vertx();
  }

  @After
  public void destroy() {
    vertx.close();
  }

  @Test
  public void testConnectionSuccessWithDetailsPassedInOptions() {
    AtomicBoolean done = new AtomicBoolean();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)

    ).connect(
      ar -> done.set(ar.succeeded())
    );

    await().untilAtomic(done, is(true));
  }

  @Test
  public void testConnectionSuccessWithDetailsPassedAsSystemVariables() {
    System.setProperty("amqp-client-host", host);
    System.setProperty("amqp-client-port", Integer.toString(port));
    AtomicBoolean done = new AtomicBoolean();
    client = AmqpClient.create(new AmqpClientOptions()).connect(
      ar -> {
        if (ar.failed()) {
          ar.cause().printStackTrace();
        }
        done.set(ar.succeeded());
      }
    );

    await().untilAtomic(done, is(true));

    System.clearProperty("amqp-client-host");
    System.clearProperty("amqp-client-port");
  }

  @Test
  public void testConnectionFailedBecauseOfBadHost() {
    AtomicBoolean done = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost("org.acme")
      .setPort(port)
    ).connect(
      ar -> {
        failure.set(ar.cause());
        done.set(true);
      }
    );

    await().pollInterval(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS)
      .untilAtomic(done, is(true));
    assertThat(failure.get()).isNotNull();
  }
}
