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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

public class SenderTypeTest extends BareTestBase {

  private AmqpConnection connection;
  private String address;
  private MockServer server;
  private List<Object> list;
  private AtomicReference<Consumer<Message>> msgCheckRef;

  @Before
  public void init() throws Exception {
    list = new CopyOnWriteArrayList<>();
    msgCheckRef = new AtomicReference<>();
    server = setupMockServer(msgCheckRef, list);

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
  public void testBoolean(TestContext context) throws Exception {
    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withBooleanAsBody(true).build());
        sender.send(AmqpMessage.create().withBooleanAsBody(Boolean.FALSE).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(true, false);
  }

  @Test(timeout = 10000)
  public void testByte(TestContext context) throws Exception {
    byte valueA = Byte.MAX_VALUE;
    byte valueB = Byte.MIN_VALUE;

    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withByteAsBody(valueA).build());
        sender.send(AmqpMessage.create().withByteAsBody(Byte.valueOf(valueB)).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(valueA, valueB);
  }

  @Test(timeout = 10000)
  public void testShort(TestContext context) throws Exception {
    short valueA = Short.MAX_VALUE;
    short valueB = Short.MIN_VALUE;

    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withShortAsBody(valueA).build());
        sender.send(AmqpMessage.create().withShortAsBody(Short.valueOf(valueB)).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(valueA, valueB);
  }

  @Test(timeout = 10000)
  public void testInteger(TestContext context) throws Exception {
    int valueA = Integer.MAX_VALUE;
    int valueB = Integer.MIN_VALUE;

    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withIntegerAsBody(valueA).build());
        sender.send(AmqpMessage.create().withIntegerAsBody(Integer.valueOf(valueB)).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(valueA, valueB);
  }

  @Test(timeout = 10000)
  public void testLong(TestContext context) throws Exception {
    long valueA = Long.MAX_VALUE;
    long valueB = Long.MIN_VALUE;

    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withLongAsBody(valueA).build());
        sender.send(AmqpMessage.create().withLongAsBody(Long.valueOf(valueB)).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(valueA, valueB);
  }

  @Test(timeout = 10000)
  public void testFloat(TestContext context) throws Exception {
    float valueA = Float.MAX_VALUE;
    float valueB = Float.MIN_VALUE;

    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withFloatAsBody(valueA).build());
        sender.send(AmqpMessage.create().withFloatAsBody(Float.valueOf(valueB)).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(valueA, valueB);
  }

  @Test(timeout = 10000)
  public void testDouble(TestContext context) throws Exception {
    double valueA = Double.MAX_VALUE;
    double valueB = Double.MIN_VALUE;

    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withDoubleAsBody(valueA).build());
        sender.send(AmqpMessage.create().withDoubleAsBody(Double.valueOf(valueB)).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(valueA, valueB);
  }

  @Test(timeout = 10000)
  public void testCharacter(TestContext context) throws Exception {
    char valueA = 'a';
    char valueB = 'Z';

    CountDownLatch msgsRecieved = new CountDownLatch(2);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withCharAsBody(valueA).build());
        sender.send(AmqpMessage.create().withCharAsBody(Character.valueOf(valueB)).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(valueA, valueB);
  }

  @Test(timeout = 10000)
  public void testTimestamp(TestContext context) throws Exception {
    // We avoid using Instant.now() in the test since its precision is
    // variable and JDK + platform dependent, while timestamp is always
    // millisecond precision. A mismatch throws off the equality comparison.
    long currentTimeMillis = System.currentTimeMillis();
    Instant instant = Instant.ofEpochMilli(currentTimeMillis);
    Date timestamp = new Date(currentTimeMillis);

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withInstantAsBody(instant).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(timestamp);
  }

  @Test(timeout = 10000)
  public void testUUID(TestContext context) throws Exception {
    UUID value = UUID.randomUUID();

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withUuidAsBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(value);
  }

  @Test(timeout = 10000)
  public void testBinary(TestContext context) throws Exception {
    String textString = "this is a message";

    Buffer value = Buffer.buffer(textString);
    Binary dataContents = new Binary(textString.getBytes(StandardCharsets.UTF_8));

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof Data);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withBufferAsBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(dataContents);
  }

  @Test(timeout = 10000)
  public void testString(TestContext context) throws Exception {
    String value = "this is a message";

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(value);
  }

  @Test(timeout = 10000)
  public void testSymbol(TestContext context) throws Exception {
    String value = "Newton";

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withSymbolAsBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(Symbol.valueOf(value));
  }

  @Test(timeout = 10000)
  public void testList(TestContext context) throws Exception {
    List<Object> value = new ArrayList<>();
    value.add("foo");
    value.add(1);
    value.add(true);

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withListAsBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(value);
  }

  @Test(timeout = 10000)
  @SuppressWarnings("unchecked")
  public void testMap(TestContext context) throws Exception {
    Map<String, String> value = new HashMap<>();
    value.put("1", "foo");
    value.put("2", "bar");
    value.put("3", "baz");

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof AmqpValue);
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withMapAsBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat((Map<String, String>) list.get(0)).containsAllEntriesOf(value);
  }

  @Test(timeout = 10000)
  public void testJsonObject(TestContext context) throws Exception {
    JsonObject value = new JsonObject().put("data", "message").put("number", 1)
      .put("array", new JsonArray().add(1).add(2).add(3));
    Binary dataContents = new Binary(value.encode().getBytes(StandardCharsets.UTF_8));

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof Data);
      context.assertEquals("application/json", msg.getContentType());
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withJsonObjectAsBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(dataContents);
  }

  @Test(timeout = 10000)
  public void testJsonArray(TestContext context) throws Exception {
    JsonArray value = new JsonArray().add(1).add(2).add(3);
    Binary dataContents = new Binary(value.encode().getBytes(StandardCharsets.UTF_8));

    CountDownLatch msgsRecieved = new CountDownLatch(1);
    msgCheckRef.set(msg -> {
      context.assertTrue(msg.getBody() instanceof Data);
      context.assertEquals("application/json", msg.getContentType());
      msgsRecieved.countDown();
    });

    connection.createSender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withJsonArrayAsBody(value).build());
      }
    });

    assertThat(msgsRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(dataContents);
  }

  private MockServer setupMockServer(AtomicReference<Consumer<Message>> msgCheckRef, List<Object> payloads) throws Exception {
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
          delivery.disposition(Accepted.getInstance(), true);

          Section body = message.getBody();
          if(body instanceof AmqpValue) {
            payloads.add(((AmqpValue) body).getValue());
          } else if(body instanceof AmqpSequence) {
            payloads.add(((AmqpSequence) body).getValue());
          } else if(body instanceof Data) {
            payloads.add(((Data) body).getValue());
          }

          Consumer<Message> msgCheck = msgCheckRef.get();
          msgCheck.accept(message);

        });

        serverReceiver.open();
      });
    });
  }
}
