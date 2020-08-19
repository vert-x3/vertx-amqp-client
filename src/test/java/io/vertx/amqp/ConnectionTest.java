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

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

public class ConnectionTest extends BareTestBase {

  @Test
  public void testConnectionSuccessWithDetailsPassedInOptions() throws Exception {
    AtomicBoolean serverConnectionOpen = new AtomicBoolean();

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnectionOpen.set(true);
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });
    });

    try {
      AtomicBoolean done = new AtomicBoolean();
      client = AmqpClient.create(vertx, new AmqpClientOptions()
          .setHost("localhost")
          .setPort(server.actualPort()));

      client.connect(ar -> done.set(ar.succeeded()));

      await().untilAtomic(serverConnectionOpen, is(true));
      await().untilAtomic(done, is(true));
    } finally {
      server.close();
    }
  }

  @Test
  public void testConnectionSuccessWithDetailsPassedAsSystemVariables() throws Exception {
    AtomicBoolean serverConnectionOpen = new AtomicBoolean();

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnectionOpen.set(true);
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });
    });

    System.setProperty("amqp-client-host", "localhost");
    System.setProperty("amqp-client-port", Integer.toString(server.actualPort()));
    try {
      AtomicBoolean done = new AtomicBoolean();
      client = AmqpClient.create(vertx, new AmqpClientOptions());

      client.connect(ar -> {
        if (ar.failed()) {
          ar.cause().printStackTrace();
        }
        done.set(ar.succeeded());
      });

      await().untilAtomic(serverConnectionOpen, is(true));
      await().untilAtomic(done, is(true));
    }
    finally {
      System.clearProperty("amqp-client-host");
      System.clearProperty("amqp-client-port");
      server.close();
    }
  }

  @Test
  public void testConnectionFailedBecauseOfBadHost() throws Exception {
    AtomicBoolean done = new AtomicBoolean();
    AtomicBoolean serverConnectionOpen = new AtomicBoolean();

    MockServer server = new MockServer(vertx, serverConnection -> {
      // [Dont] expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnectionOpen.set(true);
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });
    });

    try {
      AtomicReference<Throwable> failure = new AtomicReference<>();
      client = AmqpClient.create(vertx, new AmqpClientOptions()
          .setHost("org.acme")
          .setPort(server.actualPort()));

      client.connect(ar -> {
        failure.set(ar.cause());
        done.set(true);
      });

      await().pollInterval(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAtomic(done, is(true));
      assertThat(failure.get()).isNotNull();
      assertThat(serverConnectionOpen.get()).isFalse();
    } finally {
      server.close();
    }
  }
}
