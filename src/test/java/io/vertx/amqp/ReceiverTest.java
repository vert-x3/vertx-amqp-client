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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonDelivery;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ReceiverTest extends BareTestBase {

  private MockServer server;

  @After
  @Override
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if(server !=null) {
      server.close();
    }
  }

  @Test(timeout = 10000)
  public void testReceiveMessageWithApplicationProperties(TestContext context) throws Exception {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;
    String propKey = "appPropKey";
    String propValue = "appPropValue";

    Async asyncRecvMsg = context.async();
    Async asyncSendMsg = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.senderOpenHandler(serverSender-> {
        Message protonMsg = Message.Factory.create();
        protonMsg.setBody(new AmqpValue(sentContent));

        Map<String, Object> props = new HashMap<>();
        props.put(propKey, propValue);
        ApplicationProperties appProps = new ApplicationProperties(props);
        protonMsg.setApplicationProperties(appProps);

        serverSender.open();

        serverSender.send(protonMsg, delivery -> {
          context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
          context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
          context.assertTrue(delivery.remotelySettled(), "message was not settled");
          asyncSendMsg.complete();
        });
      });
    });

    client = AmqpClient.create(new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().createReceiver(testName, done -> {
        done.result().handler(msg -> {
          context.assertNotNull(msg, "message was null");
          context.assertNotNull(msg.bodyAsString(), "amqp message body content was null");
          context.assertEquals(sentContent, msg.bodyAsString(), "amqp message body was not as expected");

          // Check the application property was present
          context.assertTrue(msg.applicationProperties() != null, "application properties element not present");
          JsonObject appProps = msg.applicationProperties();
          context.assertTrue(appProps.containsKey(propKey), "expected property key element not present");
          context.assertEquals(propValue, appProps.getValue(propKey), "app property value not as expected");
          context.assertEquals(1, appProps.size(), "unexpected app properties");
          asyncRecvMsg.complete();
        });
      });
    });

    asyncSendMsg.awaitSuccess();
    asyncRecvMsg.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testReceptionWithAutoAccept(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(msgCount);

    server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
      msgsAcked.countDown();
    });

    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
    ).connect(connResult -> {
      context.assertTrue(connResult.succeeded());
      AmqpConnection connection = connResult.result();

      connection.createReceiver(queue, recResult -> {
        context.assertTrue(recResult.succeeded());
        AmqpReceiver receiver = recResult.result();

        receiver.handler(message -> list.add(message.bodyAsString()));
      });
    });

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test(timeout = 10000)
  public void testReceptionWithManuallyAcceptedMessages(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(msgCount);

    server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
      msgsAcked.countDown();
    });

    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
    ).connect(connResult -> {
      context.assertTrue(connResult.succeeded());
      AmqpConnection connection = connResult.result();

      AmqpReceiverOptions options = new AmqpReceiverOptions().setAutoAcknowledgement(false);
      connection.createReceiver(queue, options, recResult -> {
        context.assertTrue(recResult.succeeded());
        AmqpReceiver receiver = recResult.result();

        receiver.handler(message -> {
          list.add(message.bodyAsString());
          message.accepted();
        });
      });
    });

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test(timeout = 10000)
  public void testReceptionWithManuallyRejectedMessages(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(msgCount);

    server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Rejected.class, "state was not rejected");
      acks.add(i);
      msgsAcked.countDown();
    });

    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
    ).connect(connResult -> {
      context.assertTrue(connResult.succeeded());
      AmqpConnection connection = connResult.result();

      AmqpReceiverOptions options = new AmqpReceiverOptions().setAutoAcknowledgement(false);
      connection.createReceiver(queue, options, recResult -> {
        context.assertTrue(recResult.succeeded());
        AmqpReceiver receiver = recResult.result();

        receiver.handler(message -> {
          list.add(message.bodyAsString());
          message.rejected();
        });
      });
    });

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test(timeout = 10000)
  public void testReceptionWithManuallyModifiedFailedMessages(TestContext context) throws Exception {
    doReceptionWithManuallyModifiedMessagesTestImpl(context, false);
  }

  @Test(timeout = 10000)
  public void testReceptionWithManuallyModifiedFailedUndeliverableHereMessages(TestContext context) throws Exception {
    doReceptionWithManuallyModifiedMessagesTestImpl(context, true);
  }

  private void doReceptionWithManuallyModifiedMessagesTestImpl(TestContext context, boolean undeliverable) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(msgCount);

    server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Modified.class, "state was not modified");

      context.assertTrue(((Modified) state).getDeliveryFailed());
      context.assertEquals(undeliverable, ((Modified) state).getUndeliverableHere());

      acks.add(i);
      msgsAcked.countDown();
    });

    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
    ).connect(connResult -> {
      context.assertTrue(connResult.succeeded());
      AmqpConnection connection = connResult.result();

      AmqpReceiverOptions options = new AmqpReceiverOptions().setAutoAcknowledgement(false);
      connection.createReceiver(queue, options, recResult -> {
        context.assertTrue(recResult.succeeded());
        AmqpReceiver receiver = recResult.result();

        receiver.handler(message -> {
          list.add(message.bodyAsString());
          message.modified(true, undeliverable);
        });
      });
    });

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }


  @Test(timeout = 10000)
  public void testReceptionCreatingReceiverWithoutConnection(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(msgCount);

    server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
      msgsAcked.countDown();
    });

    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
    ).createReceiver(queue, recResult -> {
        context.assertTrue(recResult.succeeded());
        AmqpReceiver receiver = recResult.result();

        receiver.handler(message -> list.add(message.bodyAsString()));
      }
    );

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  private MockServer setupMockServer(TestContext context, int msgCount, BiConsumer<ProtonDelivery, Integer> stateCheck) throws Exception {
    AtomicInteger sent = new AtomicInteger();

    return new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.senderOpenHandler(serverSender-> {
        serverSender.sendQueueDrainHandler(x-> {
          while(sent.get() < msgCount && !serverSender.sendQueueFull()) {
            Message m = Proton.message();
            final int i = sent.getAndIncrement();
            m.setBody(new AmqpValue(String.valueOf(i)));

            serverSender.send(m , delivery -> {
              context.assertNotNull(delivery.getRemoteState(), "message had no state set");
              stateCheck.accept(delivery, i);
              context.assertTrue(delivery.remotelySettled(), "message was not settled");
            });
          }
        });

        serverSender.open();
      });
    });
  }

  @Test(timeout = 15000)
  public void testReceptionWhenDemandChangesWhileHandlingMessages(TestContext context) throws Exception {
    final int msgCount = 2000;
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(msgCount);

    final String queue = UUID.randomUUID().toString();
    final List<String> list = new CopyOnWriteArrayList<>();
    final Promise<AmqpReceiver> receiverCreationPromise = Promise.promise();
    final Future<AmqpReceiver> receiverCreationFuture = receiverCreationPromise.future();

    server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
      msgsAcked.countDown();
    });

    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort()))
      .connect(connection -> connection.result().createReceiver(queue, recResult -> {
        context.assertTrue(recResult.succeeded());
        AmqpReceiver receiver = recResult.result();

        receiver.pause();
        receiver.handler(amqpMessage -> list.add(amqpMessage.bodyAsString()));
        receiverCreationPromise.complete(receiver);
      }));

    await().until(receiverCreationFuture::succeeded);

    AmqpReceiver amqpReceiver = receiverCreationFuture.result();

    amqpReceiver.fetch(400);

    await().pollInterval(20, TimeUnit.MILLISECONDS).until(() -> list.size() == 400);

    amqpReceiver.fetch(1600);

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();

    assertThat(list).containsAll(IntStream.range(0, msgCount).mapToObj(String::valueOf).collect(Collectors.toList()));
    assertThat(acks).containsAll(IntStream.range(0, msgCount).boxed().collect(Collectors.toList()));
  }

  @Test(timeout = 20000)
  public void testReceiveMultipleMessageAfterDelayedHandlerAddition(TestContext context) throws Exception {
    final int msgCount = 5;
    final List<String> list = new CopyOnWriteArrayList<>();
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(msgCount);

    server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
      msgsAcked.countDown();
    });

    AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      // Set up a consumer using the client but DONT register the handler
      res.result().createReceiver(name.getMethodName(), done -> {
        context.assertTrue(done.succeeded());

        // Add the handler after a delay
        vertx.setTimer(250, x -> {
          done.result().handler(msg -> {
            list.add(msg.bodyAsString());
          });
        }); // timer
      }); // receiver
    }); // connect

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly("0", "1", "2", "3", "4");
    assertThat(acks).containsExactly(0, 1, 2, 3, 4);
  }

  @Test(timeout = 20000)
  public void testReceiveMultipleMessageAfterPause(TestContext context) throws Exception {
    final int totalMsgCount = 5;
    final int pauseCount = 2;
    final List<String> list = new CopyOnWriteArrayList<>();
    final List<Integer> acks = new CopyOnWriteArrayList<>();
    CountDownLatch msgsAcked = new CountDownLatch(totalMsgCount);

    server = setupMockServer(context, totalMsgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
      msgsAcked.countDown();
    });

    AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      final AtomicInteger received = new AtomicInteger();
      final AtomicLong pauseStartTime = new AtomicLong();
      final int delay = 250;

      // Set up a consumer using the client
      res.result().createReceiver(name.getMethodName(), done -> {
          context.assertTrue(done.succeeded());
          AmqpReceiver receiver = done.result();
          receiver.handler(msg -> {
            int msgNum = received.getAndIncrement();

            String content = msg.bodyAsString();
            context.assertNotNull(content, "amqp message " + msgNum + " body content was null");
            context.assertEquals(String.valueOf(msgNum), content, "amqp message " + msgNum + " body not as expected");
            list.add(content);

            // Pause once we get initial messages
            if (msgNum == pauseCount) {
              receiver.pause();
              pauseStartTime.set(System.currentTimeMillis());
              // Resume after a delay
              vertx.setTimer(delay, x -> receiver.resume());
            }

            // Verify subsequent deliveries occur after the expected delay
            if (msgNum > pauseCount) {
              context.assertTrue(pauseStartTime.get() > 0, "pause start not initialised before receiving msg" + msgNum);
              context.assertTrue(System.currentTimeMillis() >= pauseStartTime.get() + delay, "delivery occurred before expected");
            }
          });
        });
    });

    assertThat(msgsAcked.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly("0", "1", "2", "3", "4");
    assertThat(acks).containsExactly(0, 1, 2, 3, 4);
  }
}
