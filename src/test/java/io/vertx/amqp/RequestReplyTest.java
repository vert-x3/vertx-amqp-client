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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonSender;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test the request-reply use case, specifically creating a dynamic address receiver
 * for consuming responses, and anonymous sender for sending replies.
 */
@RunWith(VertxUnitRunner.class)
public class RequestReplyTest extends BareTestBase {

  private static final String REQUEST = "what's your name?";
  private static final String RESPONSE = "my name is Neo";

  private Future<Void> prepareRequestReceiverAndAnonymousSenderResponder(TestContext context, AmqpConnection connection, String requestQueue, String replyAddress) {
    Promise<Void> future = Promise.promise();
    connection.createReceiver(requestQueue).onComplete(d -> {
      d.result().handler(msg -> {
        context.assertEquals(REQUEST, msg.bodyAsString());
        context.assertEquals(replyAddress, msg.replyTo());
        connection.createAnonymousSender().onComplete(sender ->
          sender.result().send(AmqpMessage.create().address(msg.replyTo()).withBody(RESPONSE).build()));
      });
      future.handle(d.mapEmpty());
    });
    return future.future();
  }

  private Future<AmqpReceiver> prepareDynamicReplyReceiver(TestContext context, AmqpConnection connection, Async done) {
    Promise<AmqpReceiver> future = Promise.promise();
    connection.createDynamicReceiver().onComplete(context.asyncAssertSuccess(receiver -> {
      context.assertNotNull(receiver.address());
      receiver.handler(message -> {
        context.assertEquals(message.bodyAsString(), RESPONSE);
        done.complete();
      });
      future.complete(receiver);
    }));
    return future.future();
  }

  private Future<Void> prepareSenderAndSendRequestMessage(TestContext context, AmqpConnection connection, String address, String replyAddress) {
    Promise<Void> future = Promise.promise();
    connection.createSender(address).onComplete(context.asyncAssertSuccess(res -> {
      res.sendWithAck(
        AmqpMessage.create()
          .replyTo(replyAddress)
          .withBody(REQUEST).build()).onComplete(future);
    }));
    return future.future();
  }

  @Test(timeout = 10000)
  public void testRequesting(TestContext context) throws Exception {
    final String requestQueue = "requestQueue";
    final String dynamicResponseAddress = UUID.randomUUID() + "dynamicAddress";

    MockServer server = createServerForRequestorTestImpl(context, requestQueue, dynamicResponseAddress);

    try {
      Async done = context.async();

      client = AmqpClient.create(vertx, new AmqpClientOptions()
        .setHost("localhost").setPort(server.actualPort()));
      client.connect().onComplete(context.asyncAssertSuccess(conn -> {

        prepareDynamicReplyReceiver(context, conn, done)
          .compose(dr -> prepareSenderAndSendRequestMessage(context, conn, requestQueue, dr.address()));
      }));

      done.awaitSuccess();
    } finally {
      server.close();
    }
  }


  private MockServer createServerForRequestorTestImpl(TestContext context, String requestQueue, String dynamicResponseAddress) throws Exception {
    AtomicReference<ProtonSender> senderRef = new AtomicReference<>();
    MockServer server = new MockServer(vertx, serverConnection -> {

      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.senderOpenHandler(serverSender -> {
        serverSender.closeHandler(res -> {
          serverSender.close();
        });

        // Verify the remote terminus details used were as expected
        context.assertNotNull(serverSender.getRemoteSource(), "source should not be null");
        Source remoteSource = (org.apache.qpid.proton.amqp.messaging.Source) serverSender.getRemoteSource();
        context.assertTrue(remoteSource.getDynamic(), "expected dynamic source to be requested");
        context.assertNull(remoteSource.getAddress(), "expected no source address to be set");

        // Set the local terminus details
        Source source = (org.apache.qpid.proton.amqp.messaging.Source) remoteSource.copy();
        source.setAddress(dynamicResponseAddress);
        serverSender.setSource(source);

        serverSender.open();

        senderRef.set(serverSender);
      });

      serverConnection.receiverOpenHandler(serverReceiver -> {
        serverReceiver.closeHandler(res -> {
          serverReceiver.close();
        });

        // Verify the remote terminus details used were as expected
        context.assertNotNull(serverReceiver.getRemoteTarget(), "target should not be null");
        Target remoteTarget = (org.apache.qpid.proton.amqp.messaging.Target) serverReceiver.getRemoteTarget();
        context.assertFalse(remoteTarget.getDynamic(), "should not be requested to be dynamic target");
        context.assertEquals(requestQueue, remoteTarget.getAddress(), "expected request queue address to be set");

        // Set the local terminus details
        Target target = (org.apache.qpid.proton.amqp.messaging.Target) remoteTarget.copy();
        target.setAddress(requestQueue);
        serverReceiver.setTarget(target);

        serverReceiver.handler((delivery, msg) -> {
          Section body = msg.getBody();

          context.assertNotNull(body);
          context.assertTrue(body instanceof AmqpValue);
          context.assertEquals(REQUEST, ((AmqpValue) body).getValue());

          Message replyMsg = Message.Factory.create();
          replyMsg.setBody(new AmqpValue(RESPONSE));

          senderRef.get().send(replyMsg);
        });

        serverReceiver.open();
      });
    });
    return server;
  }

  @Test(timeout = 10000)
  public void testResponder(TestContext context) throws Exception {
    final String requestQueue = "requestQueue";
    final String responseAddress = UUID.randomUUID() + "replyToAddress";
    Async done = context.async();
    MockServer server = createServerForResponderTestImpl(context, requestQueue, responseAddress, done);

    try {
      client = AmqpClient.create(vertx, new AmqpClientOptions()
        .setHost("localhost").setPort(server.actualPort()));

      client.connect().onComplete(context.asyncAssertSuccess(conn -> {
        prepareRequestReceiverAndAnonymousSenderResponder(context, conn, requestQueue, responseAddress);
      }));

      done.awaitSuccess();
    } finally {
      server.close();
    }
  }

  private MockServer createServerForResponderTestImpl(TestContext context, String requestQueue, String replyToAddress, Async done) throws Exception {
    MockServer server = new MockServer(vertx, serverConnection -> {

      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.receiverOpenHandler(serverReceiver -> {
        serverReceiver.closeHandler(res -> {
          serverReceiver.close();
        });

        // Verify the remote terminus details used were as expected
        context.assertNotNull(serverReceiver.getRemoteTarget(), "target should not be null");
        Target remoteTarget = (org.apache.qpid.proton.amqp.messaging.Target) serverReceiver.getRemoteTarget();
        context.assertFalse(remoteTarget.getDynamic(), "should not be requested to be dynamic target");
        context.assertNull(remoteTarget.getAddress(), "expected address to be null, to reflect using the anonymous terminus");

        // Set the local terminus details
        Target target = (org.apache.qpid.proton.amqp.messaging.Target) remoteTarget.copy();
        serverReceiver.setTarget(target);

        serverReceiver.handler((delivery, msg) -> {
          Section body = msg.getBody();

          context.assertNotNull(body);
          context.assertTrue(body instanceof AmqpValue);
          context.assertEquals(RESPONSE, ((AmqpValue) body).getValue());
          context.assertEquals(replyToAddress, msg.getAddress());

          done.complete();
        });

        serverReceiver.open();
      });

      serverConnection.senderOpenHandler(serverSender -> {
        serverSender.closeHandler(res -> {
          serverSender.close();
        });

        // Verify the remote terminus details used were as expected
        context.assertNotNull(serverSender.getRemoteSource(), "source should not be null");
        Source remoteSource = (org.apache.qpid.proton.amqp.messaging.Source) serverSender.getRemoteSource();
        context.assertFalse(remoteSource.getDynamic(), "should not be requesting dynamic source");
        context.assertEquals(requestQueue, remoteSource.getAddress(), "expected source address to be request queue");

        // Set the local terminus details
        Source source = (org.apache.qpid.proton.amqp.messaging.Source) remoteSource.copy();
        source.setAddress(requestQueue);
        serverSender.setSource(source);

        serverSender.open();

        // Just buffer a send of a request message
        Message requestMsg = Message.Factory.create();
        requestMsg.setBody(new AmqpValue(REQUEST));
        requestMsg.setReplyTo(replyToAddress);

        serverSender.send(requestMsg);
      });

    });
    return server;
  }
}
