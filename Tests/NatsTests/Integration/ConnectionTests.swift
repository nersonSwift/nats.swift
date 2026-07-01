// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation
import Logging
import NIO
import Nats
import NatsServer
import Testing

@Suite(.serialized) final class CoreNatsTests {

    var natsServer = NatsServer()

    deinit {
        natsServer.stop()
    }

    @Test func testRtt() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let rtt: TimeInterval = try await client.rtt()
        #expect(rtt > 0, "should have RTT")

        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testPublish() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")

        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                confirm()
            }
        }
        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testSuspendAndResume() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")

        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                confirm()
            }
        }
        let suspended = AsyncStream<Void>.makeStream()
        let reconnected = AsyncStream<Void>.makeStream()
        client.on([.suspended, .connected]) { event in
            if event.kind() == .suspended {
                suspended.continuation.yield(())
            }
            if event.kind() == .connected {
                reconnected.continuation.yield(())
            }
        }
        try await client.suspend()
        try await confirmation { confirm in
            var it = suspended.stream.makeAsyncIterator()
            _ = await it.next()
            confirm()
        }
        try await client.resume()
        try await confirmation { confirm in
            var it = reconnected.stream.makeAsyncIterator()
            _ = await it.next()
            confirm()
        }
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                confirm()
            }
        }
        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testForceReconnect() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")

        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                confirm()
            }
        }
        let suspended = AsyncStream<Void>.makeStream()
        let reconnected = AsyncStream<Void>.makeStream()
        client.on([.suspended, .connected]) { event in
            if event.kind() == .suspended {
                suspended.continuation.yield(())
            }
            if event.kind() == .connected {
                reconnected.continuation.yield(())
            }
        }
        try await client.reconnect()
        try await confirmation { confirm in
            var it = suspended.stream.makeAsyncIterator()
            _ = await it.next()
            confirm()
        }
        try await confirmation { confirm in
            var it = reconnected.stream.makeAsyncIterator()
            _ = await it.next()
            confirm()
        }
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                confirm()
            }
        }
        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testConnectMultipleURLsOneIsValid() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .urls([
                URL(string: natsServer.clientURL)!, URL(string: "nats://localhost:4344")!,
                URL(string: "nats://localhost:4343")!,
            ])
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")

        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                confirm()
            }
        }
        try await client.close()
    }

    @Test func testConnectMultipleURLsRetainOrder() async throws {
        natsServer.start()
        let natsServer2 = NatsServer()
        natsServer2.start()
        logger.logLevel = .critical
        for _ in 0..<10 {
            let client = NatsClientOptions()
                .urls([URL(string: natsServer2.clientURL)!, URL(string: natsServer.clientURL)!])
                .retainServersOrder()
                .build()
            try await client.connect()
            #expect(client.connectedUrl == URL(string: natsServer2.clientURL))
            try await client.close()
        }
    }

    @Test func testConnectDNSError() async throws {
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .urls([URL(string: "nats://invalid:1234")!])
            .build()
        do {
            try await client.connect()
        } catch NatsError.ConnectError.dns(_) {
            return
        } catch {
            Issue.record("Expeted dns lookup error; got: \(error)")
        }
        Issue.record("Expeted dns lookup error")
    }

    @Test func testConnectNIOError() async throws {
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .urls([URL(string: "nats://localhost:4321")!])
            .build()
        do {
            // should be connection refused error
            try await client.connect()
        } catch NatsError.ConnectError.io(_) {
            return
        } catch {
            Issue.record("Expeted IO lookup error; got: \(error)")
        }
        Issue.record("Expeted io lookup error")
    }

    @Test(.timeLimit(.minutes(1))) func testRetryOnFailedConnect() async throws {
        let client = NatsClientOptions()
            .url(URL(string: "nats://localhost:4321")!)
            .reconnectWait(1)
            .retryOnfailedConnect()
            .build()

        let connected = AsyncStream<Void>.makeStream()
        client.on(.connected) { event in
            connected.continuation.yield(())
        }

        try await client.connect()
        natsServer.start(port: 4321)

        try await confirmation { confirm in
            var it = connected.stream.makeAsyncIterator()
            _ = await it.next()
            confirm()
        }

    }

    @Test(.timeLimit(.minutes(1))) func testPublishWithReply() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")

        try await client.publish("msg".data(using: .utf8)!, subject: "test", reply: "reply")
        let iter = sub.makeAsyncIterator()
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                #expect(msg.replySubject == "reply")
                confirm()
            }
        }
    }

    @Test(.timeLimit(.minutes(1))) func testPublishWithReplyOnCustomInbox() async throws {
        natsServer.start()
        logger.logLevel = .debug
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .inboxPrefix("_INBOX_foo")
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")

        try await client.publish(
            "msg".data(using: .utf8)!, subject: "test", reply: client.newInbox())
        let iter = sub.makeAsyncIterator()
        try await confirmation { confirm in
            if let msg = try await iter.next() {
                #expect(msg.subject == "test")
                #expect(msg.replySubject?.starts(with: "_INBOX_foo.") == true)
                confirm()
            }
        }
    }

    @Test func testSubscribe() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        let message = try await iter.next()
        #expect(message?.payload == "msg".data(using: .utf8)!)
    }

    @Test(.timeLimit(.minutes(1))) func testQueueGroupSubscribe() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let sub1 = try await client.subscribe(subject: "test", queue: "queueGroup")
        let sub2 = try await client.subscribe(subject: "test", queue: "queueGroup")

        try await client.publish("msg".data(using: .utf8)!, subject: "test")

        try await withThrowingTaskGroup(of: NatsMessage?.self) { group in
            group.addTask { try await sub1.makeAsyncIterator().next() }
            group.addTask { try await sub2.makeAsyncIterator().next() }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(1_000_000_000))
                return nil
            }

            var msgReceived = false
            var timeoutReceived = false
            for try await result in group {
                if let _ = result {
                    if msgReceived == true {
                        Issue.record("received 2 messages")
                        return
                    }
                    msgReceived = true
                } else {
                    if !msgReceived {
                        Issue.record("timeout received before getting any messages")
                        return
                    }
                    timeoutReceived = true
                }
                if msgReceived && timeoutReceived {
                    break
                }
            }
            group.cancelAll()
            try await sub1.unsubscribe()
            try await sub2.unsubscribe()
            return
        }
    }

    @Test func testUnsubscribe() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        var message = try await iter.next()
        #expect(message?.payload == "msg".data(using: .utf8)!)

        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        try await sub.unsubscribe()

        message = try await iter.next()
        #expect(message == nil)

        do {
            try await sub.unsubscribe()
        } catch NatsError.SubscriptionError.subscriptionClosed {
            return
        }
        Issue.record("Expected subscription closed error")
    }

    @Test func testUnsubscribeAfter() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")
        try await sub.unsubscribe(after: 3)
        for _ in 0..<5 {
            try await client.publish("msg".data(using: .utf8)!, subject: "test")
        }

        var i = 0
        for try await _ in sub {
            i += 1
        }
        #expect(i == 3, "Expected 3 messages to be delivered")
        try await client.close()
    }

    @Test func testConnect() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()
    }

    @Test(.timeLimit(.minutes(1))) func testReconnect() async throws {
        natsServer.start()
        let port = natsServer.port!
        logger.logLevel = .critical

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .reconnectWait(1)
            .build()

        try await client.connect()

        // Payload to publish
        let payload = "hello".data(using: .utf8)!

        var messagesReceived = 0
        let sub = try! await client.subscribe(subject: "foo")

        // publish some messages
        Task {
            for _ in 0..<10 {
                try await client.publish(payload, subject: "foo")
            }
        }

        // make sure sub receives messages
        for try await _ in sub {
            messagesReceived += 1
            if messagesReceived == 10 {
                break
            }
        }
        let connected = AsyncStream<Void>.makeStream()
        client.on(.connected) { event in
            connected.continuation.yield(())
        }

        // restart the server
        natsServer.stop()
        sleep(1)
        natsServer.start(port: port)
        try await confirmation { confirm in
            var it = connected.stream.makeAsyncIterator()
            _ = await it.next()
            confirm()
        }

        // publish more messages, sub should receive them
        Task {
            for _ in 0..<10 {
                try await client.publish(payload, subject: "foo")
            }
        }

        for try await _ in sub {
            messagesReceived += 1
            if messagesReceived == 20 {
                break
            }
        }

        // Check if the total number of messages received matches the number sent
        #expect(20 == messagesReceived, "Mismatch in the number of messages sent and received")
        try await client.close()
    }

    @Test func testUsernameAndPassword() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        natsServer.start(cfg: bundle.url(forResource: "creds", withExtension: "conf")!.relativePath)

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .usernameAndPassword("derek", "s3cr3t")
            .maxReconnects(5)
            .build()
        try await client.connect()
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(subject: "test")

        // Test if client with bad credentials throws an error
        let badCertsClient = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .usernameAndPassword("derek", "badpassword")
            .maxReconnects(5)
            .build()

        do {
            try await badCertsClient.connect()
            Issue.record("Should have thrown an error")
        } catch NatsError.ServerError.authorizationViolation {
            // success
            return
        } catch {
            Issue.record("Expected auth error; got: \(error)")
        }
        Issue.record("Expected error from connect")
    }

    @Test func testTokenAuth() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        natsServer.start(cfg: bundle.url(forResource: "token", withExtension: "conf")!.relativePath)

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .token("s3cr3t")
            .maxReconnects(5)
            .build()
        try await client.connect()
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(subject: "test")

        // Test if client with bad credentials throws an error
        let badCertsClient = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .token("badtoken")
            .maxReconnects(5)
            .build()

        do {
            try await badCertsClient.connect()
            Issue.record("Should have thrown an error")
        } catch NatsError.ServerError.authorizationViolation {
            return
        } catch {
            Issue.record("Expected auth error; got: \(error)")
        }
        Issue.record("Expected error from connect")
    }

    @Test func testCredentialsAuth() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        natsServer.start(cfg: bundle.url(forResource: "jwt", withExtension: "conf")!.relativePath)

        let creds = bundle.url(forResource: "TestUser", withExtension: "creds")!

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).credentialsFile(
            creds
        ).build()
        try await client.connect()
        let subscribe = try await client.subscribe(subject: "foo").makeAsyncIterator()
        try await client.publish("data".data(using: .utf8)!, subject: "foo")
        _ = try await subscribe.next()
    }

    @Test func testNkeyAuth() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        natsServer.start(cfg: bundle.url(forResource: "nkey", withExtension: "conf")!.relativePath)

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .nkey("SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM")
            .build()
        try await client.connect()
        let subscribe = try await client.subscribe(subject: "foo").makeAsyncIterator()
        try await client.publish("data".data(using: .utf8)!, subject: "foo")
        _ = try await subscribe.next()
    }

    @Test func testNkeyAuthFile() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        natsServer.start(cfg: bundle.url(forResource: "nkey", withExtension: "conf")!.relativePath)

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .nkeyFile(bundle.url(forResource: "nkey", withExtension: "")!)
            .build()
        try await client.connect()
        let subscribe = try await client.subscribe(subject: "foo").makeAsyncIterator()
        try await client.publish("data".data(using: .utf8)!, subject: "foo")
        _ = try await subscribe.next()

        // Test if passing both nkey and nkeyPath throws an error
        let badClient = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .nkeyFile(bundle.url(forResource: "nkey", withExtension: "")!)
            .nkey("SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM")
            .build()

        do {
            try await badClient.connect()
            Issue.record("Should have thrown an error")
        } catch let error as NatsError.ConnectError {
            if case .invalidConfig(_) = error {
                #expect(
                    error.description
                        == "nats: invalid client configuration: cannot use both nkey and nkeyPath")
                return
            }
            Issue.record("Expected auth error; got: \(error)")
        } catch {
            Issue.record("Expected auth error; got: \(error)")
        }
        Issue.record("Expected error from connect")
    }

    @Test func testMutualTls() async throws {
        let bundle = Bundle.module
        logger.logLevel = .critical
        let serverCert = bundle.url(forResource: "server-cert", withExtension: "pem")!.relativePath
        let serverKey = bundle.url(forResource: "server-key", withExtension: "pem")!.relativePath
        let rootCA = bundle.url(forResource: "rootCA", withExtension: "pem")!.relativePath
        let cfgFile = try createConfigFileFromTemplate(
            templateURL: bundle.url(forResource: "tls", withExtension: "conf")!,
            args: [serverCert, serverKey, rootCA])
        natsServer.start(cfg: cfgFile.relativePath)

        let certsURL = bundle.url(forResource: "rootCA", withExtension: "pem")!
        let clientCert = bundle.url(forResource: "client-cert", withExtension: "pem")!
        let clientKey = bundle.url(forResource: "client-key", withExtension: "pem")!

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .requireTls()
            .rootCertificates(certsURL)
            .clientCertificate(
                clientCert,
                clientKey
            )
            .build()
        try await client.connect()
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(subject: "test")
    }

    @Test func testTlsFirst() async throws {
        let bundle = Bundle.module
        logger.logLevel = .critical
        let serverCert = bundle.url(forResource: "server-cert", withExtension: "pem")!.relativePath
        let serverKey = bundle.url(forResource: "server-key", withExtension: "pem")!.relativePath
        let rootCA = bundle.url(forResource: "rootCA", withExtension: "pem")!.relativePath
        let cfgFile = try createConfigFileFromTemplate(
            templateURL: bundle.url(forResource: "tls_first", withExtension: "conf")!,
            args: [serverCert, serverKey, rootCA])
        natsServer.start(cfg: cfgFile.relativePath)

        let certsURL = bundle.url(forResource: "rootCA", withExtension: "pem")!
        let clientCert = bundle.url(forResource: "client-cert", withExtension: "pem")!
        let clientKey = bundle.url(forResource: "client-key", withExtension: "pem")!

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .requireTls()
            .rootCertificates(certsURL)
            .clientCertificate(
                clientCert,
                clientKey
            )
            .withTlsFirst()
            .build()
        try await client.connect()
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        try await client.flush()
        _ = try await client.subscribe(subject: "test")
    }

    @Test func testInvalidCertificate() async throws {
        let bundle = Bundle.module
        logger.logLevel = .critical
        let serverCert = bundle.url(forResource: "server-cert", withExtension: "pem")!.relativePath
        let serverKey = bundle.url(forResource: "server-key", withExtension: "pem")!.relativePath
        let rootCA = bundle.url(forResource: "rootCA", withExtension: "pem")!.relativePath
        let cfgFile = try createConfigFileFromTemplate(
            templateURL: bundle.url(forResource: "tls", withExtension: "conf")!,
            args: [serverCert, serverKey, rootCA])
        natsServer.start(cfg: cfgFile.relativePath)

        let certsURL = bundle.url(forResource: "rootCA", withExtension: "pem")!
        let invalidCert = bundle.url(forResource: "client-cert-invalid", withExtension: "pem")!
        let invalidKey = bundle.url(forResource: "client-key-invalid", withExtension: "pem")!

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .requireTls()
            .rootCertificates(certsURL)
            .clientCertificate(
                invalidCert,
                invalidKey
            )
            .build()
        do {
            try await client.connect()
        } catch NatsError.ConnectError.tlsFailure(_) {
            return
        } catch {
            Issue.record("Expected tls error; got: \(error)")
        }
        Issue.record("Expected error from connect")
    }

    @Test func testWebsocket() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        natsServer.start(cfg: bundle.url(forResource: "ws", withExtension: "conf")!.relativePath)

        let client = NatsClientOptions().url(URL(string: natsServer.clientWebsocketURL)!).build()

        try await client.connect()
        let sub = try await client.subscribe(subject: "test")
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        let message = try await iter.next()
        #expect(message?.payload == "msg".data(using: .utf8)!)

        try await client.close()
    }

    @Test func testWebsocketTLS() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        let serverCert = bundle.url(forResource: "server-cert", withExtension: "pem")!.relativePath
        let serverKey = bundle.url(forResource: "server-key", withExtension: "pem")!.relativePath
        let rootCA = bundle.url(forResource: "rootCA", withExtension: "pem")!.relativePath
        let cfgFile = try createConfigFileFromTemplate(
            templateURL: bundle.url(forResource: "wss", withExtension: "conf")!,
            args: [serverCert, serverKey, rootCA])
        natsServer.start(cfg: cfgFile.relativePath)

        let certsURL = bundle.url(forResource: "rootCA", withExtension: "pem")!
        let clientCert = bundle.url(forResource: "client-cert", withExtension: "pem")!
        let clientKey = bundle.url(forResource: "client-key", withExtension: "pem")!

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientWebsocketURL)!)
            .rootCertificates(certsURL)
            .clientCertificate(
                clientCert,
                clientKey
            )
            .build()

        try await client.connect()
        let sub = try await client.subscribe(subject: "test")
        try await client.publish("msg".data(using: .utf8)!, subject: "test")
        let iter = sub.makeAsyncIterator()
        let message = try await iter.next()
        #expect(message?.payload == "msg".data(using: .utf8)!)

        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testLameDuckMode() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()

        let lameDuck = AsyncStream<NatsEventKind>.makeStream()
        client.on(.lameDuckMode) { event in
            lameDuck.continuation.yield(event.kind())
        }
        try await client.connect()

        natsServer.sendSignal(.lameDuckMode)
        try await confirmation { confirm in
            var it = lameDuck.stream.makeAsyncIterator()
            let kind = await it.next()
            #expect(kind == NatsEventKind.lameDuckMode)
            confirm()
        }
        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testRequest() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let service = try await client.subscribe(subject: "service")
        Task {
            for try await msg in service {
                try await client.publish(
                    "reply".data(using: .utf8)!, subject: msg.replySubject!, reply: "reply")
            }
        }
        let response = try await client.request("request".data(using: .utf8)!, subject: "service")
        #expect(response.payload == "reply".data(using: .utf8)!)

        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testRequestCustomInbox() async throws {
        natsServer.start()
        logger.logLevel = .debug

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .inboxPrefix("_INBOX_bar.foo")
            .build()
        try await client.connect()

        let service = try await client.subscribe(subject: "service")
        Task {
            for try await msg in service {
                try await client.publish(
                    "reply".data(using: .utf8)!, subject: msg.replySubject!, reply: "reply")
            }
        }
        let response = try await client.request("request".data(using: .utf8)!, subject: "service")
        #expect(response.payload == "reply".data(using: .utf8)!)

        try await client.close()
    }

    @Test func testRequest_noResponders() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        do {
            _ = try await client.request("request".data(using: .utf8)!, subject: "service")
        } catch NatsError.RequestError.noResponders {
            try await client.close()
            return
        }

        Issue.record("Expected no responders")
    }

    @Test(.timeLimit(.minutes(1))) func testRequest_timeout() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let service = try await client.subscribe(subject: "service")
        Task {
            for try await msg in service {
                sleep(2)
                try await client.publish(
                    "reply".data(using: .utf8)!, subject: msg.replySubject!, reply: "reply")
            }
        }
        do {
            _ = try await client.request(
                "request".data(using: .utf8)!, subject: "service", timeout: 1)
        } catch NatsError.RequestError.timeout {
            try await service.unsubscribe()
            try await client.close()
            return
        }

        Issue.record("Expected timeout")
    }

    @Test func testRequest_permissionDenied() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        let templateURL = bundle.url(forResource: "permissions", withExtension: "conf")!
        let cfgFile = try createConfigFileFromTemplate(
            templateURL: templateURL,
            args: ["deny", "_INBOX.*"])
        natsServer.start(cfg: cfgFile.relativePath)

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        do {
            _ = try await client.request("request".data(using: .utf8)!, subject: "service")
        } catch NatsError.RequestError.permissionDenied {
            try await client.close()
            return
        }

        Issue.record("Expected permission denied")
    }

    @Test func testPublishOnClosedConnection() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let rtt: TimeInterval = try await client.rtt()
        #expect(rtt > 0, "should have RTT")

        try await client.close()
        do {
            try await client.publish("msg".data(using: .utf8)!, subject: "test")
        } catch NatsError.ClientError.connectionClosed {
            return
        } catch {
            Issue.record("Expected connection closed error; got: \(error)")
        }
        Issue.record("Expected connection closed error")
    }

    @Test func testCloseClosedConnection() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let rtt: TimeInterval = try await client.rtt()
        #expect(rtt > 0, "should have RTT")

        try await client.close()
        do {
            try await client.close()
        } catch NatsError.ClientError.connectionClosed {
            return
        } catch {
            Issue.record("Expected connection closed error; got: \(error)")
        }
        Issue.record("Expected connection closed error")
    }

    @Test func testSuspendClosedConnection() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let rtt: TimeInterval = try await client.rtt()
        #expect(rtt > 0, "should have RTT")

        try await client.close()
        do {
            try await client.suspend()
        } catch NatsError.ClientError.connectionClosed {
            return
        } catch {
            Issue.record("Expected connection closed error; got: \(error)")
        }
        Issue.record("Expected connection closed error")
    }

    @Test func testReconnectOnClosedConnection() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let rtt: TimeInterval = try await client.rtt()
        #expect(rtt > 0, "should have RTT")

        try await client.close()
        do {
            try await client.reconnect()
        } catch NatsError.ClientError.connectionClosed {
            return
        } catch {
            Issue.record("Expected connection closed error; got: \(error)")
        }
        Issue.record("Expected connection closed error")
    }

    @Test func testSubscribeMissingPermissions() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        let cfgFile = try createConfigFileFromTemplate(
            templateURL: bundle.url(forResource: "permissions", withExtension: "conf")!,
            args: ["deny", "events.>"])
        natsServer.start(cfg: cfgFile.relativePath)

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()

        try await client.connect()
        var sub = try await client.subscribe(subject: "events.A")
        var isError = false
        do {
            for try await _ in sub {
                Issue.record("Expected no message)")
            }
        } catch NatsError.SubscriptionError.permissionDenied {
            // success
            isError = true
        }
        if !isError {
            Issue.record("Expected missing permissions error")
        }

        sub = try await client.subscribe(subject: "events.*")
        isError = false
        do {
            for try await _ in sub {
                Issue.record("Expected no message)")
            }
        } catch NatsError.SubscriptionError.permissionDenied {
            // success
            isError = true
        }
        if !isError {
            Issue.record("Expected missing permissions error")
        }
    }

    @Test func testSubscribePermissionsRevoked() async throws {
        logger.logLevel = .critical
        let bundle = Bundle.module
        let templateURL = bundle.url(forResource: "permissions", withExtension: "conf")!
        var cfgFile = try createConfigFileFromTemplate(
            templateURL: templateURL,
            args: ["allow", "events.>"])
        natsServer.start(cfg: cfgFile.relativePath)

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()

        try await client.connect()
        let sub = try await client.subscribe(subject: "events.A")

        let iter = sub.makeAsyncIterator()
        try await client.publish("msg".data(using: .utf8)!, subject: "events.A")

        _ = try await iter.next()

        cfgFile = try createConfigFileFromTemplate(
            templateURL: templateURL, args: ["deny", "events.>"], destination: cfgFile)

        // reload config with
        natsServer.sendSignal(.reload)

        do {
            _ = try await iter.next()
        } catch NatsError.SubscriptionError.permissionDenied {
            // success
            return
        }
        Issue.record("Expected permission denied error")
    }

    /// Test that multiple connect() calls on the same client throw an error
    @Test func testMultipleConnectCallsThrowError() async throws {
        natsServer.start()

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()

        // First connect should succeed
        try await client.connect()

        // Second connect should throw alreadyConnected error
        do {
            try await client.connect()
            Issue.record("Second connect() should have thrown an error")
        } catch NatsError.ClientError.alreadyConnected {
            // Expected behavior
        } catch {
            Issue.record("Expected alreadyConnected error, got: \(error)")
        }

        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testUnsubscribeAfterWithWaitingConsumer() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")
        try await sub.unsubscribe(after: 3)

        let consumed = Task { () -> Int in
            var i = 0
            for try await _ in sub {
                i += 1
            }
            return i
        }
        for _ in 0..<5 {
            try await client.publish("msg".data(using: .utf8)!, subject: "test")
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        let i = try await consumed.value
        #expect(i == 3, "Expected exactly 3 delivered before auto-unsubscribe")
        try await client.close()
    }

    @Test(.timeLimit(.minutes(1))) func testQueueGroupSurvivesReconnect() async throws {
        natsServer.start()
        let port = natsServer.port!
        logger.logLevel = .critical

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .reconnectWait(1)
            .build()
        try await client.connect()

        let sub1 = try await client.subscribe(subject: "q.subject", queue: "workers")
        let sub2 = try await client.subscribe(subject: "q.subject", queue: "workers")
        _ = try await client.rtt()

        let reconnected = AsyncStream<Void>.makeStream()
        client.on(.connected) { _ in reconnected.continuation.yield(()) }
        natsServer.stop()
        sleep(1)
        natsServer.start(port: port)
        try await confirmation { confirm in
            var it = reconnected.stream.makeAsyncIterator()
            _ = await it.next()
            confirm()
        }
        _ = try await client.rtt()

        try await confirmation(expectedCount: 10) { delivered in
            let received = AsyncStream<Void>.makeStream()
            let collector1 = Task {
                for try await _ in sub1 {
                    delivered()
                    received.continuation.yield(())
                }
            }
            let collector2 = Task {
                for try await _ in sub2 {
                    delivered()
                    received.continuation.yield(())
                }
            }

            let payload = "x".data(using: .utf8)!
            for _ in 0..<10 {
                try await client.publish(payload, subject: "q.subject")
            }
            try await client.flush()

            var it = received.stream.makeAsyncIterator()
            for _ in 0..<10 {
                _ = await it.next()
            }
            _ = try await client.rtt()

            collector1.cancel()
            collector2.cancel()
            _ = try? await collector1.value
            _ = try? await collector2.value
        }
        try await client.close()
    }

    func createConfigFileFromTemplate(
        templateURL: URL, args: [String], destination: URL? = nil
    ) throws -> URL {
        let templateContent = try String(contentsOf: templateURL, encoding: .utf8)
        let config = String(format: templateContent, arguments: args.map { $0 as CVarArg })

        let tempDirectoryURL = FileManager.default.temporaryDirectory

        let tempFileURL: URL

        if let destination {
            tempFileURL = destination
        } else {
            tempFileURL = tempDirectoryURL.appendingPathComponent(UUID().uuidString)
                .appendingPathExtension("conf")
        }

        // Write the filled content to the temp file
        try config.write(to: tempFileURL, atomically: true, encoding: .utf8)

        // Return the URL of the newly created temp file
        return tempFileURL
    }
}
