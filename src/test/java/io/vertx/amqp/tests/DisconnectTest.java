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
package io.vertx.amqp.tests;

import java.util.UUID;

import io.vertx.amqp.AmqpClient;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.impl.AmqpClientImpl;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class DisconnectTest extends BareTestBase {

  @Test(timeout = 20000)
  public void testUseConnectionAfterDisconnect(TestContext ctx) throws Exception {
    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });
    });

    String queue = UUID.randomUUID().toString();
    client = AmqpClient.create(vertx, new AmqpClientOptions()
        .setHost("localhost")
        .setPort(server.actualPort()));

    Async handlerFired = ctx.async();
    client.connect().onComplete(ctx.asyncAssertSuccess(conn -> {
      conn.exceptionHandler(err -> {
        conn.createSender(queue).onComplete(ctx.asyncAssertFailure(sender -> {
        }));
        conn.createAnonymousSender().onComplete(ctx.asyncAssertFailure(sender -> {
        }));
        conn.createReceiver("some-address").onComplete(ctx.asyncAssertFailure(sender -> {
        }));
        conn.createReceiver("some-address", new AmqpReceiverOptions()).onComplete(ctx.asyncAssertFailure(sender -> {
        }));
        conn.createDynamicReceiver().onComplete(ctx.asyncAssertFailure(sender -> {
        }));

        handlerFired.complete();
      });

      server.close();
    }));

    handlerFired.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectionsCleanupOnDisconnect(TestContext ctx) throws Exception {
    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnection.open();
      });
    });

    client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort()));

    Async handlerFired = ctx.async();
    client.connect().onComplete(ctx.asyncAssertSuccess(conn -> {
      conn.closeFuture().onComplete(ar -> {
        ctx.assertEquals(0, ((AmqpClientImpl)client).numConnections());
        handlerFired.complete();
      });
      server.close();
    }));

    handlerFired.awaitSuccess();
  }
}
