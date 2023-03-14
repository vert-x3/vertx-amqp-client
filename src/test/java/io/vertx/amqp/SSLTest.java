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

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.core.spi.tls.SslContextFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.Target;
import org.junit.After;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.concurrent.ExecutionException;

public class SSLTest extends BareTestBase {

  private static final String PASSWORD = "password";
  private static final String KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
  private static final String WRONG_HOST_KEYSTORE = "src/test/resources/broker-wrong-host-pkcs12.keystore";
  private static final String TRUSTSTORE = "src/test/resources/client-pkcs12.truststore";
  private static final String KEYSTORE_CLIENT = "src/test/resources/client-pkcs12.keystore";
  private static final String OTHER_CA_TRUSTSTORE = "src/test/resources/other-ca-pkcs12.truststore";
  private static final String VERIFY_HTTPS = "HTTPS";
  private static final String NO_VERIFY = "";

  private MockServer server;

  @After
  public void cleanup() {
    if (server != null) {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConnectWithSslSucceeds(TestContext context) throws Exception {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    server = new MockServer(vertx, conn -> handleStartupProcess(conn, context), serverOptions);

    PfxOptions clientPfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    AmqpClientOptions options = new AmqpClientOptions()
      .setSsl(true)
      .setHost("localhost")
      .setPort(server.actualPort())
      .setPfxTrustOptions(clientPfxOptions);

    AmqpClient.create(vertx, options).connect().onComplete(context.asyncAssertSuccess(res -> {
      // Expect start to succeed
      async.complete();
    }));

    async.awaitSuccess();
  }

  // This test is here to cover a WildFly use case for passing in an SSLContext for which there are no
  // configuration options.
  // This is currently done by casing to AmqpClientImpl and calling setSuppliedSSLContext().
  @Test(timeout = 20000)
  public void testConnectWithSuppliedSslContextSucceeds(TestContext context) throws Exception {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    server = new MockServer(vertx, conn -> handleStartupProcess(conn, context), serverOptions);

    Path tsPath = Paths.get(".").resolve(TRUSTSTORE);
    TrustManagerFactory tmFactory;
    try (InputStream trustStoreStream = Files.newInputStream(tsPath, StandardOpenOption.READ)){
      KeyStore trustStore = KeyStore.getInstance("pkcs12");
      trustStore.load(trustStoreStream, PASSWORD.toCharArray());
      tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmFactory.init(trustStore);
    }

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
      null,
      tmFactory.getTrustManagers(),
      null
    );

    AmqpClientOptions options = new AmqpClientOptions()
      .setSsl(true)
      .setHost("localhost")
      .setPort(server.actualPort())
      .setSslEngineOptions(new JdkSSLEngineOptions() {
        @Override
        public SslContextFactory sslContextFactory() {
          return new SslContextFactory() {
            @Override
            public SslContext create() throws SSLException {
              return new JdkSslContext(
                sslContext,
                true,
                null,
                IdentityCipherSuiteFilter.INSTANCE,
                ApplicationProtocolConfig.DISABLED,
                io.netty.handler.ssl.ClientAuth.NONE,
                null,
                false);
            }
          };
        }
      });

    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
        // Expect start to succeed
        async.complete();
      }));

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithSslToNonSslServerFails(TestContext context) throws Exception {
    Async async = context.async();

    // Create a server that doesn't use ssl
    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(false);

    server = new MockServer(vertx, conn -> handleStartupProcess(conn, context), serverOptions);

    PfxOptions clientPfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    AmqpClientOptions options = new AmqpClientOptions()
      .setSsl(true)
      .setHost("localhost")
      .setPort(server.actualPort())
      .setPfxTrustOptions(clientPfxOptions);

    AmqpClient.create(vertx, options).connect().onComplete(context.asyncAssertFailure(err -> {
      async.complete();
    }));

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithSslToServerWithUntrustedKeyFails(TestContext context) throws Exception {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    server = new MockServer(vertx, conn -> handleStartupProcess(conn, context), serverOptions);

    AmqpClientOptions options = new AmqpClientOptions();
    PfxOptions pfxOptions = new PfxOptions().setPath(OTHER_CA_TRUSTSTORE).setPassword(PASSWORD);
    options.setSsl(true)
      .setHost("localhost")
      .setPort(server.actualPort())
      .setPfxTrustOptions(pfxOptions);

    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertFailure(err -> {
      // Expect start to fail due to remote peer not being trusted
      async.complete();
    }));

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithSslToServerWhileUsingTrustAll(TestContext context) throws Exception {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    server = new MockServer(vertx, conn -> handleStartupProcess(conn, context), serverOptions);

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
      .setSsl(true)
      .setTrustAll(true);

    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
      // Expect start to succeed
      async.complete();
    }));

    async.awaitSuccess();
  }

  private void handleStartupProcess(ProtonConnection serverConnection, TestContext context) {
    // Expect a connection
    serverConnection.openHandler(serverSender -> serverConnection.open());
    serverConnection.closeHandler(x -> serverConnection.close());

    // Expect a session to open, when the sender/receiver is created by the client startup
    serverConnection.sessionOpenHandler(ProtonSession::open);

    // Expect a receiver link open for the anonymous-relay reply sender
    serverConnection.receiverOpenHandler(serverReceiver -> {
      Target remoteTarget = serverReceiver.getRemoteTarget();
      context.assertNotNull(remoteTarget, "target should not be null");
      context.assertNull(remoteTarget.getAddress(), "expected null address");

      serverReceiver.setTarget(remoteTarget);

      serverReceiver.open();
    });

    // Expect a sender link open for a dynamic address
    serverConnection.senderOpenHandler(serverSender -> {
      Source remoteSource = (Source) serverSender.getRemoteSource();
      context.assertNotNull(remoteSource, "source should not be null");
      context.assertTrue(remoteSource.getDynamic(), "source should be dynamic");
      context.assertNull(remoteSource.getAddress(), "expected null address");

      Source source = (Source) remoteSource.copy();
      source.setAddress("should-be-random-generated-address");
      serverSender.setSource(source);

      serverSender.open();
    });
  }

  @Test(timeout = 20000)
  public void testConnectWithSslWithoutRequiredClientKeyFails(TestContext context) throws Exception {
    doClientCertificateTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testConnectWithSslWithRequiredClientKeySucceeds(TestContext context) throws Exception {
    doClientCertificateTestImpl(context, true);
  }

  private void doClientCertificateTestImpl(TestContext context, boolean supplyClientCert) throws InterruptedException,
    ExecutionException {
    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    serverOptions.setClientAuth(ClientAuth.REQUIRED);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    PfxOptions pfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    serverOptions.setPfxTrustOptions(pfxOptions);

    server = new MockServer(vertx, conn -> handleStartupProcess(conn, context), serverOptions);

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
      .setSsl(true)
      .setPfxTrustOptions(pfxOptions);

    if (supplyClientCert) {
      PfxOptions clientKeyPfxOptions = new PfxOptions().setPath(KEYSTORE_CLIENT).setPassword(PASSWORD);
      options.setPfxKeyCertOptions(clientKeyPfxOptions);
    }

    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(res -> {
      if (supplyClientCert) {
        // Expect start to succeed
        context.assertTrue(res.succeeded(), "expected start to succeed due to supplying client certs");
      } else {
        // Expect start to fail
        context.assertFalse(res.succeeded(), "expected start to fail due to withholding client cert");
      }
      async.complete();
    });

    async.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConnectWithHostnameVerification(TestContext context) throws Exception {
    doHostnameVerificationTestImpl(context, true);
  }

  @Test(timeout = 20000)
  public void testConnectWithoutHostnameVerification(TestContext context) throws Exception {
    doHostnameVerificationTestImpl(context, false);
  }

  private void doHostnameVerificationTestImpl(TestContext context, boolean verifyHost) throws Exception {

    Async async = context.async();

    ProtonServerOptions serverOptions = new ProtonServerOptions();
    serverOptions.setSsl(true);
    PfxOptions serverPfxOptions = new PfxOptions().setPath(WRONG_HOST_KEYSTORE).setPassword(PASSWORD);
    serverOptions.setPfxKeyCertOptions(serverPfxOptions);

    server = new MockServer(vertx, conn -> {
      handleStartupProcess(conn, context);
    }, serverOptions);

    PfxOptions clientPfxOptions = new PfxOptions().setPath(TRUSTSTORE).setPassword(PASSWORD);
    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort())
      .setSsl(true)
      .setPfxTrustOptions(clientPfxOptions);

    // Verify/update the hostname verification settings
    context.assertEquals(VERIFY_HTTPS, options.getHostnameVerificationAlgorithm(),
      "expected host verification to be on by default");
    if (!verifyHost) {
      options.setHostnameVerificationAlgorithm(NO_VERIFY);
    }

    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(res -> {
        if (verifyHost) {
          // Expect start to fail
          context.assertFalse(res.succeeded(), "expected start to fail due to server cert not matching hostname");
        } else {
          // Expect start to succeed
          context.assertTrue(res.succeeded(), "expected start to succeed due to not verifying server hostname");
        }
        async.complete();
      });

    async.awaitSuccess();
  }

}
