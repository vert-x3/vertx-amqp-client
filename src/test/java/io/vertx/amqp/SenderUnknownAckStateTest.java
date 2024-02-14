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

import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class SenderUnknownAckStateTest extends BareTestBase {

  private AmqpConnection connection;
  private String address;
  private MockServer server;

  @Before
  public void init() throws Exception {
    server = setupMockServer();

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<AmqpConnection> reference = new AtomicReference<>();
    client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort()))
      .connect(connection -> {
        reference.set(connection.result());
        if (connection.failed()) {
          connection.cause().printStackTrace();
        }
        latch.countDown();
      });

    assertThat(latch.await(6, TimeUnit.SECONDS)).isTrue();
    this.connection = reference.get();
    assertThat(connection).isNotNull();
    this.address = UUID.randomUUID().toString();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    server.close();
  }

  @Test(timeout = 10000)
  public void test(TestContext context) throws Exception {
    connection.createSender(address, context.asyncAssertSuccess(sender -> {
      AmqpMessage msg = AmqpMessage.create().withBooleanAsBody(true).build();
      sender
        .write(msg)
        .onComplete(context.asyncAssertFailure(expected -> {
        // Expected
      }));
    }));
  }

  private MockServer setupMockServer() throws Exception {
    return new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.receiverOpenHandler(serverReceiver-> {
        serverReceiver.handler((delivery, message) -> {
          // Triggers message state to be null
          delivery.settle();
        });

        serverReceiver.open();
      });
    });
  }
}
