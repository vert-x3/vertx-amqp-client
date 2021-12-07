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
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class ReceptionTypeTest extends BareTestBase {

  private AmqpConnection connection;
  private AtomicReference<Object> msgPayloadRef;
  private MockServer server;

  @Before
  public void init() throws Exception {
    msgPayloadRef = new AtomicReference<>();
    server = setupMockServer(msgPayloadRef);

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
  }

  @After
  @Override
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if(server != null) {
      server.close();
    }
  }

  private <T> void testType(Object payload, Function<AmqpMessage, T> extractor, T expected) throws Exception {
    assertThat(msgPayloadRef.compareAndSet(null, payload)).isTrue();
    CountDownLatch latch = new CountDownLatch(1);
    List<T> list = new CopyOnWriteArrayList<>();

    connection.createReceiver(UUID.randomUUID().toString(), done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      done.result().handler(message -> {
        list.add(extractor.apply(message));
        latch.countDown();
      });
    });

    assertThat(latch.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(list).containsExactly(expected);
  }

  @Test(timeout = 10000)
  public void testNoBody() throws Exception {
    testType(null, AmqpMessage::isBodyNull, true);
  }

  @Test(timeout = 10000)
  public void testNull() throws Exception {
    testType(new AmqpValue(null), AmqpMessage::isBodyNull, true);
  }

  @Test(timeout = 10000)
  public void testBooleanTrue() throws Exception {
    boolean b = Boolean.TRUE;
    testType(b, AmqpMessage::bodyAsBoolean, b);
  }

  @Test(timeout = 10000)
  public void testBooleanFalse() throws Exception {
    boolean b = Boolean.FALSE;
    testType(b, AmqpMessage::bodyAsBoolean, b);
  }

  @Test(timeout = 10000)
  public void testByte() throws Exception {
    byte b = 1;
    testType(b, AmqpMessage::bodyAsByte, b);
  }

  @Test(timeout = 10000)
  public void testShort() throws Exception {
    short s = 2;
    testType(s, AmqpMessage::bodyAsShort, s);
  }

  @Test(timeout = 10000)
  public void testInteger() throws Exception {
    int i = 3;
    testType(i, AmqpMessage::bodyAsInteger, i);
  }

  @Test(timeout = 10000)
  public void testLong() throws Exception {
    long l = Long.MAX_VALUE - 1;
    testType(l, AmqpMessage::bodyAsLong, l);
  }

  @Test(timeout = 10000)
  public void testFloat() throws Exception {
    float f = 12.34f;
    testType(f, AmqpMessage::bodyAsFloat, f);
  }

  @Test(timeout = 10000)
  public void testDouble() throws Exception {
    double d = 56.78;
    testType(d, AmqpMessage::bodyAsDouble, d);
  }

  @Test(timeout = 10000)
  public void testCharacter() throws Exception {
    char c = 'c';
    testType(c, AmqpMessage::bodyAsChar, c);
  }

  @Test(timeout = 10000)
  public void testTimestamp() throws Exception {
    // We avoid using Instant.now() in the test since its precision is
    // variable and JDK + platform dependent, while timestamp is always
    // millisecond precision. A mismatch throws off the equality comparison.
    long currentTimeMillis = System.currentTimeMillis();
    Instant instant = Instant.ofEpochMilli(currentTimeMillis);
    testType(new Date(currentTimeMillis), AmqpMessage::bodyAsTimestamp, instant);
  }

  @Test(timeout = 10000)
  public void testUUID() throws Exception {
    UUID uuid = UUID.randomUUID();
    testType(uuid, AmqpMessage::bodyAsUUID, uuid);
  }

  @Test(timeout = 10000)
  public void testBinary() throws Exception {
    Buffer buffer = Buffer.buffer("hello !!!");
    testType(new Data(new Binary(buffer.getBytes())), AmqpMessage::bodyAsBinary, buffer);
  }

  @Test(timeout = 10000)
  public void testString() throws Exception {
    String string = "hello !";
    testType(string, AmqpMessage::bodyAsString, string);
  }

  @Test(timeout = 10000)
  public void testSymbol() throws Exception {
    String string = "my-symbol";
    testType(Symbol.valueOf(string), AmqpMessage::bodyAsSymbol, string);
  }

  @Test(timeout = 10000)
  public void testList() throws Exception {
    List<Object> l = new ArrayList<>();
    l.add("foo");
    l.add(1);
    l.add(true);
    testType(l, AmqpMessage::bodyAsList, l);
  }

  @Test(timeout = 10000)
  public void testListPassedAsAmqpSequence() throws Exception {
    List<Object> l = new ArrayList<>();
    l.add("sequence");
    l.add(2);
    l.add(true);
    testType(new AmqpSequence(l), AmqpMessage::bodyAsList, l);
  }

  @Test(timeout = 10000)
  public void testMap() throws Exception {
    Map<String, String> map = new HashMap<>();
    map.put("1", "hello");
    map.put("2", "bonjour");
    testType(map, AmqpMessage::bodyAsMap, map);
  }

  @Test(timeout = 10000)
  public void testJsonObject() throws Exception {
    JsonObject json = new JsonObject().put("data", "message").put("number", 1)
      .put("array", new JsonArray().add(1).add(2).add(3));
    Data data = new Data(new Binary(json.toBuffer().getBytes()));
    testType(data, AmqpMessage::bodyAsJsonObject, json);
  }

  @Test(timeout = 10000)
  public void testJsonArray() throws Exception {
    JsonArray array = new JsonArray().add(1).add(2).add(3);
    Data data = new Data(new Binary(array.toBuffer().getBytes()));
    testType(data, AmqpMessage::bodyAsJsonArray, array);
  }

  private MockServer setupMockServer(AtomicReference<Object> msgPayloadSupplier) throws Exception {
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
        serverSender.open();

        Message msg = Message.Factory.create();

        Object payload = msgPayloadSupplier.get();
        if (payload instanceof Section) {
          msg.setBody((Section) payload);
        } else if (payload != null) {
          msg.setBody(new AmqpValue(payload));
        } else {
          // Don't set a body.
        }

        serverSender.send(msg);
      });
    });
  }
}
