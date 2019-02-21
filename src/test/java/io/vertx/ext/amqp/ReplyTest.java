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

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.*;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ReplyTest extends ArtemisTestBase {

  private Vertx vertx;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test(timeout = 20000)
  public void testReplyHandlingDisabledOnProducerSide(TestContext context) {
    Async asyncSend = context.async();
    Async asyncSendWithReply = context.async();
    Async asyncShutdown = context.async();

    String destinationName = name.getMethodName();
    String content = "myStringContent" + destinationName;

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password);
    client = AmqpClient.create(vertx, options)
      .connect(sr -> {
        context.assertTrue(sr.succeeded());

        sr.result().sender(destinationName, ms -> {
          context.assertTrue(ms.succeeded());
          // Try send with a reply handler, expect to fail.
          ms.result().send(AmqpMessage.create().withBody(content).build(), reply -> {
            context.assertTrue(reply.failed());
            asyncSendWithReply.complete();
          });

          // Try send without reply handler.
          ms.result().send(AmqpMessage.create().withBody(content).build(), ack -> asyncSend.complete());

          sr.result().close(shutdownRes -> {
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });
      });

    asyncSend.awaitSuccess();
    asyncSendWithReply.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReplyHandlingDisabledOnConsumerSide(TestContext context) {
    Async asyncSendMsg = context.async();
    Async asyncRequest = context.async();
    Async asyncShutdown = context.async();
    String destinationName = name.getMethodName();
    String content = "myStringContent" + destinationName;
    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password);
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect(startResult -> {
      context.assertTrue(startResult.succeeded());
      startResult.result().receiver(destinationName,
        msg -> {
          context.assertEquals(content, msg.bodyAsString(), "unexpected msg content");
          context.assertNotNull(msg.replyTo(), "reply address was not set on the request");

          // Try to reply.
          try {
            msg.reply(AmqpMessage.create().withBody("my response").build());
            context.fail("Expected exception to be thrown");
          } catch (IllegalStateException e) {
            // Expected reply disabled.
          }
          asyncRequest.complete();

          startResult.result().close(shutdownRes -> {
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        },
        res -> {
          context.assertTrue(res.succeeded());

          // Send a message from a regular AMQP client, with reply-to set
          ProtonClient protonClient = ProtonClient.create(vertx);
          protonClient.connect(host, port, username, password, x -> {
            context.assertTrue(x.succeeded());
            org.apache.qpid.proton.message.Message protonMsg = Proton.message();
            protonMsg.setBody(new AmqpValue(content));
            protonMsg.setReplyTo(destinationName);

            ProtonConnection conn = x.result().open();

            ProtonSender sender = conn.createSender(destinationName).open();
            sender.send(protonMsg, delivery -> {
              context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
              context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
              context.assertTrue(delivery.remotelySettled(), "message was not settled");
              conn.closeHandler(closeResult -> conn.disconnect()).close();
              asyncSendMsg.complete();
            });
          });
        });
    });

    asyncSendMsg.awaitSuccess();
    asyncRequest.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReply(TestContext context) {
    Async gotReplyAsync = context.async();
    String destinationName = name.getMethodName();
    String content = "myStringContent";
    String replyContent = "myStringReply";

    this.client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost(host).setPort(port).setUsername(username).setPassword(password));
    client.connect(startResult -> {
      context.assertTrue(startResult.succeeded());

      startResult.result().receiver(destinationName,
        msg -> {
          context.assertNotNull(msg.bodyAsString(), "expected msg body but none found");
          context.assertEquals(content, msg.bodyAsString(), "unexpected msg content");
          context.assertNotNull(msg.replyTo(), "reply address was not set on the request");
          AmqpMessage reply = AmqpMessage.create().withBody(replyContent).build();
          msg.reply(reply);
        },
        done ->
          startResult.result().sender(destinationName, sender -> {
            context.assertTrue(sender.succeeded());
            sender.result().send(
              AmqpMessage.create().withBody(content).build(),
              reply -> {
                context.assertTrue(reply.succeeded());
                AmqpMessage replyMessage = reply.result();
                context.assertEquals(replyContent, replyMessage.bodyAsString(), "unexpected reply msg content");
                context.assertNotNull(replyMessage.address(), "address was not set on the reply");
                context.assertNull(replyMessage.replyTo(), "reply address was set on the reply");
                gotReplyAsync.complete();
              }
            );
          })
        );
    });
    gotReplyAsync.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReplyToOriginalReply(TestContext context) {
    Async requestReceivedAsync = context.async();
    Async replyReceivedAsync = context.async();
    Async replyToReplyAsync = context.async();

    String destinationName = name.getMethodName();
    String content = "myStringContent";
    String replyContent = "myStringReply";
    String replyToReplyContent = "myStringReplyToReply";

    this.client = AmqpClient.create(vertx, new AmqpClientOptions()
    .setHost(host).setPort(port).setUsername(username).setPassword(password));
    client.connect(startResult -> {
      context.assertTrue(startResult.succeeded());

      startResult.result().receiver(destinationName,
        msg -> {
          context.assertNotNull(msg.bodyAsString(), "expected msg body but none found");
          context.assertEquals(content, msg.bodyAsString(), "unexpected msg content");
          context.assertNotNull(msg.replyTo(), "reply address was not set on the request");

          AmqpMessage reply = AmqpMessage.create().withBody(replyContent).build();
          msg.reply(reply, replyToReply -> {
            context.assertTrue(replyToReply.succeeded());
            context.assertEquals(replyToReplyContent, replyToReply.result().bodyAsString(),
              "unexpected 2nd reply msg content");
            context.assertNull(replyToReply.result().replyTo(), "reply address was unexpectedly set on 2nd reply");

            replyToReplyAsync.complete();
          });
        },
        done -> {
          startResult.result().sender(destinationName, sender -> {
            context.assertTrue(sender.succeeded());
            sender.result().send(
              AmqpMessage.create().withBody(content).build(),
              reply -> {
                context.assertTrue(reply.succeeded());
                AmqpMessage replyMessage = reply.result();
                context.assertEquals(replyContent, replyMessage.bodyAsString(), "unexpected reply msg content");
                context.assertNotNull(replyMessage.address(), "address was not set on the reply");
                context.assertNotNull(replyMessage.replyTo(), "reply address was not set on the reply");
                replyReceivedAsync.complete();

                AmqpMessage response = AmqpMessage.create().withBody(replyToReplyContent).build();
                replyMessage.reply(response);

                requestReceivedAsync.complete();
              }
            );
          });
        });
      });
    requestReceivedAsync.awaitSuccess();
    replyReceivedAsync.awaitSuccess();
    replyToReplyAsync.awaitSuccess();
  }
}
