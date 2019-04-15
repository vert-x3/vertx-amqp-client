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

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ReceiverDurabilityTest extends BareTestBase {

  @Test(timeout = 10000)
  public void testNotDurableNotCustom(TestContext context) throws ExecutionException, InterruptedException {
    durableReceiverTestImpl(context, false, UUID.randomUUID().toString(), false);
  }

  @Test(timeout = 10000)
  public void testDurableNotCustom(TestContext context) throws ExecutionException, InterruptedException {
    durableReceiverTestImpl(context, true, "my link name", false);
  }

  @Test(timeout = 10000)
  public void testNotDurableWithCustom(TestContext context) throws ExecutionException, InterruptedException {
    durableReceiverTestImpl(context, false, "my link name", true);
  }

  @Test(timeout = 10000)
  public void testDurableWithCustom(TestContext context) throws ExecutionException, InterruptedException {
    durableReceiverTestImpl(context, true, UUID.randomUUID().toString(), true);
  }

  private void durableReceiverTestImpl(TestContext context, boolean durable, String linkName, boolean custom)
    throws InterruptedException, ExecutionException {

    final Async clientLinkOpenAsync = context.async();
    final Async serverLinkOpenAsync = context.async();
    final Async clientLinkCloseAsync = context.async();
    MockServer server = null;
    try {
      server = new MockServer(vertx, serverConnection -> {
        serverConnection.openHandler(result -> serverConnection.open());
        serverConnection.sessionOpenHandler(ProtonSession::open);

        serverConnection.senderOpenHandler(serverSender -> {
          serverSender.closeHandler(res -> {
            context.assertFalse(durable, "unexpected link close for durable sub");
            serverSender.close();
          });

          serverSender.detachHandler(res -> {
            context.assertTrue(durable, "unexpected link detach for non-durable sub");
            serverSender.detach();
          });

          serverSender.open();

          // Verify the link details used were as expected
          context.assertEquals(linkName, serverSender.getName(), "unexpected link name");
          context.assertNotNull(serverSender.getRemoteSource(), "source should not be null");
          org.apache.qpid.proton.amqp.messaging.Source source = (org.apache.qpid.proton.amqp.messaging.Source) serverSender
            .getRemoteSource();
          if (durable) {
            context.assertEquals(TerminusExpiryPolicy.NEVER, source.getExpiryPolicy(), "unexpected expiry");
            context.assertEquals(TerminusDurability.UNSETTLED_STATE, source.getDurable(), "unexpected durability");
          }
          Symbol[] capabilities = source.getCapabilities();
          if (custom) {
            context.assertTrue(Arrays
                .equals(new Symbol[] { Symbol.valueOf("custom") }, capabilities),
              "Unexpected capabilities: " + Arrays.toString(capabilities));
          }

          serverLinkOpenAsync.complete();
        });
      });

      // ===== Client Handling =====

      AmqpClient client = AmqpClient.create(vertx,
        new AmqpClientOptions().setPort(server.actualPort()).setHost("localhost"));
      client.connect(res -> {
        context.assertTrue(res.succeeded());
        AmqpConnection connection = res.result();

        // Create publisher with given link name
        AmqpReceiverOptions options = new AmqpReceiverOptions().setLinkName(linkName);
        if (durable) {
          options.setDurable(true);
        }
        if (custom) {
          options.addCapability("custom");
        }

        connection.createReceiver("myAddress", options, x -> {
        }, receiver -> {
          context.assertTrue(receiver.succeeded());
          clientLinkOpenAsync.complete();
          receiver.result()
            .exceptionHandler(context::fail);
          receiver.result().close(x -> clientLinkCloseAsync.complete());
        });
      });

      serverLinkOpenAsync.awaitSuccess();
      clientLinkOpenAsync.awaitSuccess();

      clientLinkCloseAsync.awaitSuccess();
    } finally {
      if (server != null) {
        server.close();
      }
    }
  }
}
