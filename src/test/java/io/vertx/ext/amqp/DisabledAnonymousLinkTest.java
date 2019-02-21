/**
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
package io.vertx.ext.amqp;

import io.vertx.ext.amqp.impl.AmqpConnectionImpl;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSession;
import io.vertx.proton.impl.ProtonServerImpl;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class DisabledAnonymousLinkTest extends BareTestBase {

  @Test(timeout = 20000)
  public void testConnectionToServerWithoutAnonymousSenderLinkSupport(TestContext context) throws Exception {
    Async asyncShutdown = context.async();
    AtomicBoolean linkOpened = new AtomicBoolean();

    MockServer server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(x -> serverConnection.open());
      serverConnection.closeHandler(x -> serverConnection.close());
      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.receiverOpenHandler(serverReceiver -> {
        linkOpened.set(true);
        serverReceiver.setCondition(ProtonHelper.condition(AmqpError.PRECONDITION_FAILED, "Expected no links"));
        serverReceiver.close();
      });
      serverConnection.senderOpenHandler(serverSender -> {
        linkOpened.set(true);
        serverSender.setCondition(ProtonHelper.condition(AmqpError.PRECONDITION_FAILED, "Expected no links"));
        serverSender.close();
      });
    });
    ((ProtonServerImpl) server.getProtonServer()).setAdvertiseAnonymousRelayCapability(false);

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
      .setReplyEnabled(true);

    this.client = AmqpClient.create(vertx, options).connect(res -> {
      context.assertTrue(res.succeeded(), "Expected start to succeed with not reply manager");
      context.assertFalse(((AmqpConnectionImpl) res.result()).replyManager().isReplySupported());
      res.result().close(shutdownRes -> {
        context.assertTrue(shutdownRes.succeeded());
        asyncShutdown.complete();
      });
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }

    context.assertFalse(linkOpened.get());
  }
}
