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
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonDelivery;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ReceiverTest extends BareTestBase {

  @Test
  public void testReceptionWithAutoAccept(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();

    MockServer server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
    });

    try {
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

      await().until(() -> list.size() == msgCount);
      assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

      await().until(() -> acks.size() == msgCount);
      assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    } finally {
      server.close();
    }
  }

  @Test
  public void testReceptionWithManuallyAcceptedMessages(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();

    MockServer server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
    });

    try {
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

      await().until(() -> list.size() == msgCount);
      assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

      await().until(() -> acks.size() == msgCount);
      assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    } finally {
      server.close();
    }
  }

  @Test
  public void testReceptionWithManuallyRejectedMessages(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();

    MockServer server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Rejected.class, "state was not rejected");
      acks.add(i);
    });

    try {
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

      await().until(() -> list.size() == msgCount);
      assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

      await().until(() -> acks.size() == msgCount);
      assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    } finally {
      server.close();
    }
  }

  @Test
  public void testReceptionWithManuallyModifiedFailedMessages(TestContext context) throws Exception {
    doReceptionWithManuallyModifiedMessagesTestImpl(context, false);
  }

  @Test
  public void testReceptionWithManuallyModifiedFailedUndeliverableHereMessages(TestContext context) throws Exception {
    doReceptionWithManuallyModifiedMessagesTestImpl(context, true);
  }

  private void doReceptionWithManuallyModifiedMessagesTestImpl(TestContext context, boolean undeliverable) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();

    MockServer server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Modified.class, "state was not modified");

      context.assertTrue(((Modified) state).getDeliveryFailed());
      context.assertEquals(undeliverable, ((Modified) state).getUndeliverableHere());

      acks.add(i);
    });

    try {
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

      await().until(() -> list.size() == msgCount);
      assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

      await().until(() -> acks.size() == msgCount);
      assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    } finally {
      server.close();
    }
  }


  @Test
  public void testReceptionCreatingReceiverWithoutConnection(TestContext context) throws Exception {
    final int msgCount = 10;
    final List<Integer> acks = new CopyOnWriteArrayList<>();

    MockServer server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
    });

    try {
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

      await().until(() -> list.size() == msgCount);
      assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");

      await().until(() -> acks.size() == msgCount);
      assertThat(acks).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    } finally {
      server.close();
    }
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

  @Test
  public void testReceptionWhenDemandChangesWhileHandlingMessages(TestContext context) throws Exception {
    final int msgCount = 2000;
    final List<Integer> acks = new CopyOnWriteArrayList<>();

    final String queue = UUID.randomUUID().toString();
    final List<String> list = new CopyOnWriteArrayList<>();
    final Promise<AmqpReceiver> receiverCreationPromise = Promise.promise();
    final Future<AmqpReceiver> receiverCreationFuture = receiverCreationPromise.future();

    MockServer server = setupMockServer(context, msgCount, (delivery, i) -> {
      DeliveryState state = delivery.getRemoteState();
      context.assertEquals(state.getClass(), Accepted.class, "state was not accepted");
      acks.add(i);
    });

    try {
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

      await().until(() -> list.size() == 400);

      amqpReceiver.fetch(1600);

      await("All messages should be handled").atMost(20, TimeUnit.SECONDS).untilAsserted(() -> assertThat(list)
        .containsAll(IntStream.range(0, msgCount).mapToObj(String::valueOf).collect(Collectors.toList())));

      await("All messages should be acknowledged").atMost(20, TimeUnit.SECONDS).untilAsserted(() -> assertThat(acks)
        .containsAll(IntStream.range(0, msgCount).boxed().collect(Collectors.toList())));
    } finally {
      server.close();
    }
  }

}
