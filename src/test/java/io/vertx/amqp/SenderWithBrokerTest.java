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

import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.RepeatRule;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class SenderWithBrokerTest extends ArtemisTestBase {

  //TODO Test the error with bad credentials

  @Rule
  public TestName name = new TestName();
  @Rule
  public RepeatRule repeat = new RepeatRule();
  private Vertx vertx;

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

  @Test
  @Repeat(10)
  public void testThatMessagedAreSent() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    usage.consumeStrings(queue, 2, 1, TimeUnit.MINUTES, null, list::add);
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().createSender(queue, done -> {
          if (done.failed()) {
            done.cause().printStackTrace();
          } else {
            // Sending a few messages
            done.result().send(AmqpMessage.create().withBody("hello").address(queue).build());
            done.result().send(AmqpMessage.create().withBody("world").address(queue).build());
          }
        });
      }
    );

    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly("hello", "world");
  }

  @Test
  @Repeat(10)
  public void testThatMessagedAreSentWithASenderCreatedFromClient() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    usage.consumeStrings(queue, 2, 1, TimeUnit.MINUTES, null, list::add);
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).createSender(queue, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        // Sending a few messages
        done.result().send(AmqpMessage.create().withBody("hello").address(queue).build());
        done.result().send(AmqpMessage.create().withBody("world").address(queue).build());
      }
    });

    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly("hello", "world");
  }

  @Test
  @Repeat(10)
  public void testThatMessagedAreSentWithASenderCreatedFromClientWithOptions() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    usage.consumeStrings(queue, 2, 1, TimeUnit.MINUTES, null, list::add);
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).createSender(queue, new AmqpSenderOptions().setDynamic(false), done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        // Sending a few messages
        done.result().send(AmqpMessage.create().withBody("hello").address(queue).build());
        done.result().send(AmqpMessage.create().withBody("world").address(queue).build());
      }
    });

    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly("hello", "world");
  }

  @Test
  @Repeat(10)
  public void testThatMessagedAreAcknowledged() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    AtomicInteger acks = new AtomicInteger();
    usage.consumeStrings(queue, 2, 1, TimeUnit.MINUTES, null, list::add);
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().createSender(queue, done -> {
          if (done.failed()) {
            done.cause().printStackTrace();
          } else {
            // Sending a few messages
            done.result().sendWithAck(AmqpMessage.create().withBody("hello").address(queue).build(), x -> {
              if (x.succeeded()) {
                acks.incrementAndGet();
                done.result().sendWithAck(AmqpMessage.create().withBody("world").address(queue).build(), y -> {
                  if (y.succeeded()) {
                    acks.incrementAndGet();
                  }
                });
              }
            });
          }
        });
      }
    );

    await().until(() -> acks.get() == 2);
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly("hello", "world");
  }
}
