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

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class ReceptionTypeTest extends ArtemisTestBase {

  private Vertx vertx;
  private AmqpConnection connection;
  private String address;

  @Before
  public void init() {
    vertx = Vertx.vertx();
    AtomicReference<AmqpConnection> reference = new AtomicReference<>();
    client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password))
      .connect(connection -> {
        reference.set(connection.result());
        if (connection.failed()) {
          connection.cause().printStackTrace();
        }
      });

    await().untilAtomic(reference, is(notNullValue()));
    this.connection = reference.get();
    this.address = UUID.randomUUID().toString();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    vertx.close();
  }

  private <T> void testType(Handler<AmqpUsage> producer, Function<AmqpMessage, T> extractor, T... expected) {
    List<T> list = new CopyOnWriteArrayList<>();
    connection.createReceiver(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      done.result().handler(message -> {
        list.add(extractor.apply(message));
      });
      CompletableFuture.runAsync(() -> {
        try {
          producer.handle(usage);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == expected.length);
    assertThat(list).containsExactly(expected);
  }

  @Test
  public void testNull() {
    testType(usage -> {
      // Send Amqpvalue(null)
      usage.produce(address, 1, null, () -> new AmqpValue(null));
      // Send no body
      usage.produce(address, 1, null, () -> null);
    }, AmqpMessage::isBodyNull, true, true);
  }

  @Test
  public void testBoolean() {
    testType(usage -> {
      usage.produce(address, 1, null, () -> true);
      usage.produce(address, 1, null, () -> false);
      usage.produce(address, 1, null, () -> Boolean.TRUE);
      usage.produce(address, 1, null, () -> Boolean.FALSE);
    }, AmqpMessage::bodyAsBoolean, true, false, true, false);
  }

  @Test
  public void testByte() {
    byte b = 1;
    testType(usage -> {
      usage.produce(address, 1, null, () -> b);
      usage.produce(address, 1, null, () -> Byte.valueOf(b));
    }, AmqpMessage::bodyAsByte, b, b);
  }

  @Test
  public void testShort() {
    short s = 2;
    testType(usage -> {
      usage.produce(address, 1, null, () -> s);
      usage.produce(address, 1, null, () -> Short.valueOf(s));
    }, AmqpMessage::bodyAsShort, s, s);
  }

  @Test
  public void testInteger() {
    int i = 3;
    testType(usage -> {
      usage.produce(address, 1, null, () -> i);
      usage.produce(address, 1, null, () -> Integer.valueOf(i));
    }, AmqpMessage::bodyAsInteger, i, i);
  }

  @Test
  public void testLong() {
    long l = Long.MAX_VALUE - 1;
    testType(usage -> {
      usage.produce(address, 1, null, () -> l);
      usage.produce(address, 1, null, () -> Long.valueOf(l));
    }, AmqpMessage::bodyAsLong, l, l);
  }

  @Test
  public void testFloat() {
    float f = 12.34f;
    testType(usage -> {
      usage.produce(address, 1, null, () -> f);
      usage.produce(address, 1, null, () -> Float.valueOf(f));
    }, AmqpMessage::bodyAsFloat, f, f);
  }

  @Test
  public void testDouble() {
    double d = 56.78;
    testType(usage -> {
      usage.produce(address, 1, null, () -> d);
      usage.produce(address, 1, null, () -> Double.valueOf(d));
    }, AmqpMessage::bodyAsDouble, d, d);
  }

  @Test
  public void testCharacter() {
    char c = 'c';
    testType(usage -> {
      usage.produce(address, 1, null, () -> c);
      usage.produce(address, 1, null, () -> Character.valueOf(c));
    }, AmqpMessage::bodyAsChar, c, c);
  }

  @Test
  public void testTimestamp() {
    // We avoid using Instant.now() in the test since its precision is
    // variable and JDK + platform dependent, while timestamp is always
    // millisecond precision. A mismatch throws off the equality comparison.
    Instant instant = Instant.ofEpochMilli(System.currentTimeMillis());
    testType(usage -> {
      usage.produce(address, 1, null, () -> Date.from(instant));
    }, AmqpMessage::bodyAsTimestamp, instant);
  }

  @Test
  public void testUUID() {
    UUID uuid = UUID.randomUUID();
    testType(usage -> {
      usage.produce(address, 1, null, () -> uuid);
    }, AmqpMessage::bodyAsUUID, uuid);
  }

  @Test
  public void testBinary() {
    Buffer buffer = Buffer.buffer("hello !!!");
    testType(usage -> {
      usage.produce(address, 1, null, () -> new Data(new Binary(buffer.getBytes())));
    }, AmqpMessage::bodyAsBinary, buffer);
  }

  @Test
  public void testString() {
    String string = "hello !";
    testType(usage -> {
      usage.produce(address, 1, null, () -> string);
    }, AmqpMessage::bodyAsString, string);
  }

  @Test
  public void testSymbol() {
    String string = "my-symbol";
    testType(usage -> {
      usage.produce(address, 1, null, () -> Symbol.getSymbol("my-symbol"));
    }, AmqpMessage::bodyAsSymbol, string);
  }

  @Test
  public void testListPassedAsAmqpSequence() {
    List<Object> l = new ArrayList<>();
    l.add("foo");
    l.add(1);
    l.add(true);
    testType(usage -> {
      usage.produce(address, 1, null, () -> new AmqpSequence(l));
    }, AmqpMessage::bodyAsList, l);
  }

  @Test
  public void testListPassedAsAmqpValue() {
    List<Object> l = new ArrayList<>();
    l.add("foo");
    l.add(1);
    l.add(true);
    testType(usage -> {
      usage.produce(address, 1, null, () -> new AmqpValue(l));
    }, AmqpMessage::bodyAsList, l);
  }

  @Test
  public void testMap() {
    Map<String, String> map = new HashMap<>();
    map.put("1", "hello");
    map.put("2", "bonjour");
    testType(usage -> {
      usage.produce(address, 1, null, () -> new AmqpValue(map));
    }, AmqpMessage::bodyAsMap, map);
//    assertThat(list.get(0)).containsAllEntriesOf(map);
  }

  @Test
  public void testJsonObject() {
    JsonObject json = new JsonObject().put("data", "message").put("number", 1)
      .put("array", new JsonArray().add(1).add(2).add(3));
    testType(usage -> {
      usage.produce(address, 1, null, () -> new Data(new Binary(json.toBuffer().getBytes())));
    }, AmqpMessage::bodyAsJsonObject, json);
  }

  @Test
  public void testJsonArray() {
    JsonArray array = new JsonArray().add(1).add(2).add(3);
    testType(usage -> {
      usage.produce(address, 1, null, () -> new Data(new Binary(array.toBuffer().getBytes())));
    }, AmqpMessage::bodyAsJsonArray, array);
  }
}
