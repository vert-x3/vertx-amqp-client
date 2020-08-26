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

import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.Sasl.SaslOutcome;
import org.junit.Test;

import io.vertx.core.Handler;
import io.vertx.core.net.NetSocket;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.sasl.ProtonSaslAuthenticator;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectionTest extends BareTestBase {

  private static final String USER = "MY_USER";
  private static final String PASSWD = "PASSWD_VALUE";
  private static final String BAD_PASSWD = "WRONG_VALUE";

  private static final String PLAIN = "PLAIN";

  @Test(timeout = 10000)
  public void testConnectionSuccessWithDetailsPassedInOptions() throws Exception {
    doConnectionWithDetailsPassedInOptionsTestImpl(true);
  }

  @Test(timeout = 10000)
  public void testConnectionFailureWithDetailsPassedInOptions() throws Exception {
    doConnectionWithDetailsPassedInOptionsTestImpl(false);
  }

  private void doConnectionWithDetailsPassedInOptionsTestImpl(boolean succeed) throws Exception {
    CountDownLatch done = new CountDownLatch(1);
    String password = succeed ? PASSWD : BAD_PASSWD;
    AtomicBoolean serverConnectionOpen = new AtomicBoolean();

    MockServer server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnectionOpen.set(true);
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });
    });

    TestAuthenticator authenticator = new TestAuthenticator(PLAIN, succeed);
    server.getProtonServer().saslAuthenticatorFactory(() -> authenticator);

    try {
      client = AmqpClient.create(vertx, new AmqpClientOptions()
          .setHost("localhost")
          .setPort(server.actualPort())
          .setUsername(USER)
          .setPassword(password));

      client.connect(ar -> {
        if (ar.failed() && succeed) {
          ar.cause().printStackTrace();
        } else {
          done.countDown();
        }
      });

      assertThat(done.await(6, TimeUnit.SECONDS)).isTrue();
      assertThat(serverConnectionOpen.get()).isEqualTo(succeed);
    } finally {
      server.close();
    }

    assertThat(authenticator.getChosenMech()).isEqualTo(PLAIN);
    assertThat(authenticator.getInitialResponse()).isEqualTo(getPlainInitialResponse(USER, password));
  }

  @Test(timeout = 10000)
  public void testConnectionSuccessWithDetailsPassedAsSystemVariables() throws Exception {
    doConnectionWithDetailsPassedAsSystemVariablesTestImpl(true);
  }

  @Test(timeout = 10000)
  public void testConnectionFailureWithDetailsPassedAsSystemVariables() throws Exception {
    doConnectionWithDetailsPassedAsSystemVariablesTestImpl(false);
  }

  private void doConnectionWithDetailsPassedAsSystemVariablesTestImpl(boolean succeed) throws Exception {
    CountDownLatch done = new CountDownLatch(1);
    AtomicBoolean serverConnectionOpen = new AtomicBoolean();
    String password = succeed ? PASSWD : BAD_PASSWD;

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnectionOpen.set(true);
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });
    });

    TestAuthenticator authenticator = new TestAuthenticator(PLAIN, succeed);
    server.getProtonServer().saslAuthenticatorFactory(() -> authenticator);

    System.setProperty("amqp-client-host", "localhost");
    System.setProperty("amqp-client-port", Integer.toString(server.actualPort()));
    System.setProperty("amqp-client-username", USER);
    System.setProperty("amqp-client-password", password);
    try {
      client = AmqpClient.create(vertx, new AmqpClientOptions());

      client.connect(ar -> {
        if (ar.failed() && succeed) {
          ar.cause().printStackTrace();
        } else {
          done.countDown();
        }
      });

      assertThat(done.await(6, TimeUnit.SECONDS)).isTrue();
      assertThat(serverConnectionOpen.get()).isEqualTo(succeed);
    }
    finally {
      System.clearProperty("amqp-client-host");
      System.clearProperty("amqp-client-port");
      System.clearProperty("amqp-client-username");
      System.clearProperty("amqp-client-password");
      server.close();
    }
  }

  @Test(timeout = 10000)
  public void testConnectionFailedBecauseOfBadHost() throws Exception {
    CountDownLatch done = new CountDownLatch(1);
    AtomicBoolean serverConnectionOpen = new AtomicBoolean();

    MockServer server = new MockServer(vertx, serverConnection -> {
      // [Dont] expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnectionOpen.set(true);
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });
    });

    try {
      AtomicReference<Throwable> failure = new AtomicReference<>();
      client = AmqpClient.create(vertx, new AmqpClientOptions()
          .setHost("org.acme")
          .setPort(server.actualPort()));

      client.connect(ar -> {
        failure.set(ar.cause());
        done.countDown();
      });

      assertThat(done.await(6, TimeUnit.SECONDS)).isTrue();
      assertThat(failure.get()).isNotNull();
      assertThat(serverConnectionOpen.get()).isFalse();
    } finally {
      server.close();
    }
  }

  private static byte[] getPlainInitialResponse(String username, String password) {
    Objects.requireNonNull(username);
    Objects.requireNonNull(password);

    byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
    byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);

    byte[] data = new byte[usernameBytes.length + passwordBytes.length + 2];
    System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
    System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);

    return data;
  }

  private static final class TestAuthenticator implements ProtonSaslAuthenticator {
    private Sasl sasl;
    private boolean succeed;
    private String offeredMech;
    String chosenMech = null;
    byte[] initialResponse = null;

    public TestAuthenticator(String offeredMech, boolean succeed){
      this.offeredMech = offeredMech;
      this.succeed = succeed;
    }

    @Override
    public void init(NetSocket socket, ProtonConnection protonConnection, Transport transport) {
      this.sasl = transport.sasl();
      sasl.server();
      sasl.allowSkip(false);
      sasl.setMechanisms(offeredMech);
    }

    @Override
    public void process(Handler<Boolean> processComplete) {
      boolean done = false;
      String[] remoteMechanisms = sasl.getRemoteMechanisms();
      if (remoteMechanisms.length > 0) {
        chosenMech = remoteMechanisms[0];

        initialResponse = new byte[sasl.pending()];
        sasl.recv(initialResponse, 0, initialResponse.length);

        if (succeed) {
          sasl.done(SaslOutcome.PN_SASL_OK);
        } else {
          sasl.done(SaslOutcome.PN_SASL_AUTH);
        }

        done = true;
      }

      processComplete.handle(done);
    }

    @Override
    public boolean succeeded() {
      return succeed;
    }

    public String getChosenMech() {
      return chosenMech;
    }

    public byte[] getInitialResponse() {
      return initialResponse;
    }
  }
}
