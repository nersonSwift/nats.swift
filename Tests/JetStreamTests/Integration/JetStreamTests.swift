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
import JetStream
import Logging
import NIO
import Nats
import NatsServer
import Testing

@Suite(.serialized) final class JetStreamTests {

    var natsServer = NatsServer()

    deinit {
        natsServer.stop()
    }

    @Test func testJetStreamContext() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        _ = JetStreamContext(client: client)
        _ = JetStreamContext(client: client, prefix: "$JS.API")
        _ = JetStreamContext(client: client, domain: "STREAMS")
        _ = JetStreamContext(client: client, timeout: 10)
        let ctx = JetStreamContext(client: client)

        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        let data = stream.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.STREAM.CREATE.FOO")
        let ack = try await ctx.publish("foo", message: "Hello, World!".data(using: .utf8)!)
        _ = try await ack.wait()

        try await client.close()
    }

    @Test func testJetStreamContextWithPrefix() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "prefix", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let clientA = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .usernameAndPassword("a", "a")
            .build()
        try await clientA.connect()

        let clientI = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .usernameAndPassword("i", "i")
            .build()
        try await clientI.connect()

        let jsA = JetStreamContext(client: clientA)
        let jsI = JetStreamContext(client: clientI, prefix: "fromA")

        _ = try await jsI.createStream(cfg: StreamConfig(name: "TEST", subjects: ["foo"]))
        _ = try await jsA.getStream(name: "TEST")
    }

    @Test func testJetStreamContextWithDomain() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "domain", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let js = JetStreamContext(client: client, domain: "ABC")
        _ = try await js.createStream(cfg: StreamConfig(name: "TEST", subjects: ["foo"]))
    }

    @Test func testJetStreamNotEnabled() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        do {
            _ = try await ctx.createStream(cfg: StreamConfig(name: "test"))
        } catch JetStreamError.RequestError.noResponders {
            // success
            return
        }
    }

    @Test func testJetStreamNotEnabledForAccount() async throws {
        natsServer.start()
        logger.logLevel = .critical
        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        do {
            _ = try await ctx.createStream(cfg: StreamConfig(name: "test"))
        } catch JetStreamError.RequestError.noResponders {
            // success
            return
        }
    }

    @Test func testStreamCRUD() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        // minimal config
        var cfg = StreamConfig(name: "test", subjects: ["foo"])
        let stream = try await ctx.createStream(cfg: cfg)

        var expectedConfig = StreamConfig(
            name: "test", description: nil, subjects: ["foo"], retention: .limits, maxConsumers: -1,
            maxMsgs: -1, maxBytes: -1, discard: .old, discardNewPerSubject: nil,
            maxAge: NanoTimeInterval(0), maxMsgsPerSubject: -1, maxMsgSize: -1, storage: .file,
            replicas: 1, noAck: nil, duplicates: NanoTimeInterval(120), placement: nil, mirror: nil,
            sources: nil, sealed: false, denyDelete: false, denyPurge: false, allowRollup: false,
            compression: StoreCompression.none, firstSeq: nil, subjectTransform: nil,
            rePublish: nil, allowDirect: false, mirrorDirect: false,
            consumerLimits: StreamConsumerLimits(inactiveThreshold: nil, maxAckPending: nil),
            metadata: nil)

        // we need to set the metadata to whatever was set by the server as it contains e.g. server version
        expectedConfig.metadata = stream.info.config.metadata

        #expect(expectedConfig == stream.info.config)

        // attempt overwriting existing stream
        var errOk = false
        do {
            _ = try await ctx.createStream(
                cfg: StreamConfig(name: "test", description: "cannot update with create"))
        } catch JetStreamError.StreamError.streamNameExist(_) {
            errOk = true
            // success
        }
        #expect(errOk, "Expected stream not found error")

        // get a stream
        guard var stream = try await ctx.getStream(name: "test") else {
            Issue.record("Expected a stream, got nil")
            return
        }
        #expect(expectedConfig == stream.info.config)

        // get a non-existing stream
        errOk = false
        if let _ = try await ctx.getStream(name: "bad") {
            Issue.record("Expected stream not found, go: \(stream)")
        }

        // update the stream
        cfg.description = "updated"
        stream = try await ctx.updateStream(cfg: cfg)
        expectedConfig.description = "updated"

        #expect(expectedConfig == stream.info.config)

        // attempt to update illegal stream property
        cfg.storage = .memory
        // attempt updating non-existing stream
        errOk = false
        do {
            _ = try await ctx.updateStream(cfg: cfg)
        } catch JetStreamError.StreamError.invalidConfig(_) {
            // success
            errOk = true
        }

        // attempt updating non-existing stream
        errOk = false
        do {
            _ = try await ctx.updateStream(cfg: StreamConfig(name: "bad"))
        } catch JetStreamError.StreamError.streamNotFound(_) {
            // success
            errOk = true
        }
        #expect(errOk, "Expected stream not found error")

        // delete the stream
        try await ctx.deleteStream(name: "test")

        // make sure the stream no longer exists
        if let _ = try await ctx.getStream(name: "test") {
            Issue.record("Expected stream not found")
        }
    }

    @Test func testStreamConfig() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        var cfg = StreamConfig(
            name: "full", description: "desc", subjects: ["bar"], retention: .interest,
            maxConsumers: 50, maxMsgs: 100, maxBytes: 1000, discard: .new,
            discardNewPerSubject: true, maxAge: NanoTimeInterval(300), maxMsgsPerSubject: 50,
            maxMsgSize: 100, storage: .memory, replicas: 1, noAck: true,
            duplicates: NanoTimeInterval(120), placement: Placement(cluster: "cluster"),
            mirror: nil, sources: [StreamSource(name: "source")], sealed: false, denyDelete: false,
            denyPurge: true, allowRollup: false, compression: .s2, firstSeq: 10,
            subjectTransform: nil, rePublish: nil, allowDirect: false, mirrorDirect: false,
            consumerLimits: StreamConsumerLimits(inactiveThreshold: NanoTimeInterval(10)),
            metadata: ["key": "value"])

        let stream = try await ctx.createStream(cfg: cfg)

        // we need to set the metadata to whatever was set by the server as it contains e.g. server version
        cfg.metadata = stream.info.config.metadata

        #expect(stream.info.config == cfg)
    }

    @Test func testStreamInfo() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        // minimal config
        let cfg = StreamConfig(name: "test", subjects: ["foo"])
        let stream = try await ctx.createStream(cfg: cfg)

        let info = try await stream.info()
        #expect(info.config.name == "test")

        // simulate external update of stream
        let updateJSON = """
            {
                "name": "test",
                "subjects": ["foo"],
                "description": "updated"
            }
            """
        let data = updateJSON.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.STREAM.UPDATE.test")

        #expect(stream.info.config.description == nil)

        let newInfo = try await stream.info()
        #expect(newInfo.config.description == "updated")
        #expect(stream.info.config.description == "updated")
    }

    @Test func testListStreams() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        for i in 0..<260 {
            let subPrefix = i % 2 == 0 ? "foo" : "bar"
            let cfg = StreamConfig(name: "STREAM-\(i)", subjects: ["\(subPrefix).\(i)"])
            let _ = try await ctx.createStream(cfg: cfg)
        }

        // list all streams
        var streams = await ctx.streams()

        var i = 0
        for try await _ in streams {
            i += 1
        }
        #expect(i == 260)

        var names = await ctx.streamNames()
        i = 0
        for try await _ in names {
            i += 1
        }
        #expect(i == 260)

        // list streams with subject foo.*
        streams = await ctx.streams(subject: "foo.*")

        i = 0
        for try await stream in streams {
            #expect(stream.config.subjects!.first!.starts(with: "foo."))
            i += 1
        }
        #expect(i == 130)

        names = await ctx.streamNames(subject: "foo.*")
        i = 0
        for try await _ in names {
            i += 1
        }
        #expect(i == 130)

        // list streams with subject not matching any
        streams = await ctx.streams(subject: "baz.*")

        i = 0
        for try await stream in streams {
            Issue.record("should return 0 streams, got: \(stream.config)")
        }
        #expect(i == 0)

        names = await ctx.streamNames(subject: "baz.*")
        i = 0
        for try await _ in names {
            i += 1
        }
        #expect(i == 0)
    }

    @Test func testGetMessage() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: cfg)

        var hm = NatsHeaderMap()
        hm[try! NatsHeaderName("foo")] = NatsHeaderValue("bar")
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm[try! NatsHeaderName("key")] = NatsHeaderValue("val")
        for i in 1...100 {
            let msg = "\(i)".data(using: .utf8)!
            let subj = i % 2 == 0 ? "foo.A" : "foo.B"
            let ack = try await ctx.publish(subj, message: msg, headers: hm)
            _ = try await ack.wait()
        }

        // get by sequence
        var msg = try await stream.getMessage(sequence: 50)
        #expect(msg!.payload == "50".data(using: .utf8)!)

        // get by sequence and subject
        msg = try await stream.getMessage(sequence: 50, subject: "foo.B")
        // msg with sequence 50 is on subject foo.A, so we expect the next message which should be on foo.B
        #expect(msg!.payload == "51".data(using: .utf8)!)
        #expect(msg!.headers == hm)

        // get first message from a subject
        msg = try await stream.getMessage(firstForSubject: "foo.A")
        #expect(msg!.payload == "2".data(using: .utf8)!)
        #expect(msg!.headers == hm)

        // get last message from subject
        msg = try await stream.getMessage(lastForSubject: "foo.B")
        #expect(msg!.payload == "99".data(using: .utf8)!)
        #expect(msg!.headers == hm)

        // message not found
        msg = try await stream.getMessage(sequence: 200)
        #expect(msg == nil)
    }

    @Test func testGetMessageDirect() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"], allowDirect: true)
        let stream = try await ctx.createStream(cfg: cfg)

        var hm = NatsHeaderMap()
        hm[try! NatsHeaderName("foo")] = NatsHeaderValue("bar")
        hm.append(try! NatsHeaderName("foo"), NatsHeaderValue("baz"))
        hm[try! NatsHeaderName("key")] = NatsHeaderValue("val")
        for i in 1...100 {
            let msg = "\(i)".data(using: .utf8)!
            let subj = i % 2 == 0 ? "foo.A" : "foo.B"
            let ack = try await ctx.publish(subj, message: msg, headers: hm)
            _ = try await ack.wait()
        }

        // get by sequence
        var msg = try await stream.getMessageDirect(sequence: 50)
        #expect(msg!.payload == "50".data(using: .utf8)!)

        // get by sequence and subject
        msg = try await stream.getMessageDirect(sequence: 50, subject: "foo.B")
        // msg with sequence 50 is on subject foo.A, so we expect the next message which should be on foo.B
        #expect(msg!.payload == "51".data(using: .utf8)!)

        // get first message from a subject
        msg = try await stream.getMessageDirect(firstForSubject: "foo.A")
        #expect(msg!.payload == "2".data(using: .utf8)!)

        // get last message from subject
        msg = try await stream.getMessageDirect(lastForSubject: "foo.B")
        #expect(msg!.payload == "99".data(using: .utf8)!)

        // message not found
        msg = try await stream.getMessageDirect(sequence: 200)
        #expect(msg == nil)
    }

    @Test func testDeleteMessage() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: cfg)

        for i in 1...10 {
            let msg = "\(i)".data(using: .utf8)!
            let subj = "foo.A"
            let ack = try await ctx.publish(subj, message: msg)
            _ = try await ack.wait()
        }

        // get by sequence to make sure the msg is available
        var msg = try await stream.getMessage(sequence: 5)
        #expect(msg!.payload == "5".data(using: .utf8)!)

        // delete
        try await stream.deleteMessage(sequence: 5)

        msg = try await stream.getMessage(sequence: 5)
        #expect(msg == nil)

        // try deleting the msg again
        var errOk = false
        do {
            try await stream.deleteMessage(sequence: 5)
        } catch JetStreamError.StreamMessageError.deleteSequenceNotFound(_) {
            // success
            errOk = true
        }
        #expect(errOk, "Expected sequence not found error")

        // now do the same with secure delete
        // we cannot easily test whether the operation actually overwritten the value from unit test
        msg = try await stream.getMessage(sequence: 7)
        #expect(msg!.payload == "7".data(using: .utf8)!)

        // delete
        try await stream.deleteMessage(sequence: 7, secure: true)

        msg = try await stream.getMessage(sequence: 7)
        #expect(msg == nil)
    }

    @Test func testPurge() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: cfg)

        let data = "hello".data(using: .utf8)!
        for _ in 0..<3 {
            let subj = "foo.A"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }
        for _ in 0..<4 {
            let subj = "foo.B"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }
        for _ in 0..<5 {
            let subj = "foo.C"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }

        // purge foo.B
        var purged = try await stream.purge(subject: "foo.B")
        #expect(purged == 4)
        var info = try await stream.info()
        #expect(info.state.messages == 8)

        // purge rest of the messages
        purged = try await stream.purge()
        #expect(purged == 8)
        info = try await stream.info()
        #expect(info.state.messages == 0)
    }

    @Test func testPurgeSequence() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: cfg)

        let data = "hello".data(using: .utf8)!
        for _ in 0..<10 {
            let subj = "foo.A"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }
        for _ in 0..<20 {
            let subj = "foo.B"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }
        for _ in 0..<30 {
            let subj = "foo.C"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }

        // purge "foo.B" with sequence 15
        // This should remove only the first 4 messages on foo.B
        var purged = try await stream.purge(sequence: 15, subject: "foo.B")
        #expect(purged == 4)
        var info = try await stream.info()
        #expect(info.state.messages == 56)

        // purge with sequence 41, no filter
        // This should remove the first 36 (after previous purge) messages
        // and leave us with 20 messages
        purged = try await stream.purge(sequence: 41)
        #expect(purged == 36)
        info = try await stream.info()
        #expect(info.state.messages == 20)
    }

    @Test func testPurgeKeep() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let cfg = StreamConfig(name: "STREAM", subjects: ["foo.*"])
        let stream = try await ctx.createStream(cfg: cfg)

        let data = "hello".data(using: .utf8)!
        for _ in 0..<10 {
            let subj = "foo.A"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }
        for _ in 0..<20 {
            let subj = "foo.B"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }
        for _ in 0..<30 {
            let subj = "foo.C"
            let ack = try await ctx.publish(subj, message: data)
            _ = try await ack.wait()
        }

        // purge "foo.B" retaining 50 messages
        // This should remove 15 messages from "foo.B"
        var purged = try await stream.purge(keep: 5, subject: "foo.B")
        #expect(purged == 15)
        var info = try await stream.info()
        #expect(info.state.messages == 45)

        // purge with keep 10, no filter
        // This should remove all but 10 messages from the stream
        purged = try await stream.purge(keep: 10)
        #expect(purged == 35)
        info = try await stream.info()
        #expect(info.state.messages == 10)
    }

    @Test func testStreamConsumerCRUD() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo"])
        let stream = try await ctx.createStream(cfg: streamCfg)

        var expectedConfig = ConsumerConfig(
            name: "test", durable: nil, description: nil, deliverPolicy: .all, optStartSeq: nil,
            optStartTime: nil, ackPolicy: .explicit, ackWait: NanoTimeInterval(30), maxDeliver: -1,
            backOff: nil, filterSubject: nil, filterSubjects: nil, replayPolicy: .instant,
            rateLimit: nil,
            sampleFrequency: nil, maxWaiting: 512, maxAckPending: 1000, headersOnly: nil,
            maxRequestBatch: nil, maxRequestExpires: nil, maxRequestMaxBytes: nil,
            inactiveThreshold: NanoTimeInterval(5), replicas: 1, memoryStorage: nil, metadata: nil)
        // minimal config
        var cfg = ConsumerConfig(name: "test")
        _ = try await stream.createConsumer(cfg: cfg)

        // attempt overwriting existing consumer
        var errOk = false
        do {
            _ = try await stream.createConsumer(
                cfg: ConsumerConfig(name: "test", description: "cannot update with create"))
        } catch JetStreamError.ConsumerError.consumerNameExist(_) {
            errOk = true
            // success
        }
        #expect(errOk, "Expected consumer exists error")

        // get a consumer
        guard var cons = try await stream.getConsumer(name: "test") else {
            Issue.record("Expected a stream, got nil")
            return
        }

        // we need to set the metadata to whatever was set by the server as it contains e.g. server version
        expectedConfig.metadata = cons.info.config.metadata

        #expect(expectedConfig == cons.info.config)

        // get a non-existing consumer
        if let cons = try await stream.getConsumer(name: "bad") {
            Issue.record("Expected consumer not found, got: \(cons)")
        }

        // update the stream
        cfg.description = "updated"
        cons = try await stream.updateConsumer(cfg: cfg)
        expectedConfig.description = "updated"

        #expect(expectedConfig == cons.info.config)

        // attempt to update illegal consumer property; nats-server 2.14 rejects a storage
        // type change on an existing consumer, 2.10 accepts it
        cfg.memoryStorage = true
        do {
            let updated = try await stream.updateConsumer(cfg: cfg)
            #expect(
                updated.info.config.memoryStorage == true,
                "Expected the accepted storage type change to be applied")
        } catch let err as JetStreamError.APIError {
            #expect(err.errorCode == ErrorCode.consumerCreate)
        }

        // attempt updating non-existing consumer
        errOk = false
        do {
            _ = try await stream.updateConsumer(cfg: ConsumerConfig(name: "bad"))
        } catch JetStreamError.ConsumerError.consumerDoesNotExist(_) {
            // success
            errOk = true
        }
        #expect(errOk, "Expected consumer not found error")

        // delete the consumer
        try await stream.deleteConsumer(name: "test")

        // make sure the consumer no longer exists
        if let _ = try await stream.getConsumer(name: "test") {
            Issue.record("Expected consumer not found")
        }
    }

    @Test func testJetStreamContextConsumerCRUD() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let streamCfg = StreamConfig(name: "test", subjects: ["foo"])
        _ = try await ctx.createStream(cfg: streamCfg)

        var expectedConfig = ConsumerConfig(
            name: "test", durable: nil, description: nil, deliverPolicy: .all, optStartSeq: nil,
            optStartTime: nil, ackPolicy: .explicit, ackWait: NanoTimeInterval(30), maxDeliver: -1,
            backOff: nil, filterSubject: nil, filterSubjects: nil, replayPolicy: .instant,
            rateLimit: nil,
            sampleFrequency: nil, maxWaiting: 512, maxAckPending: 1000, headersOnly: nil,
            maxRequestBatch: nil, maxRequestExpires: nil, maxRequestMaxBytes: nil,
            inactiveThreshold: NanoTimeInterval(5), replicas: 1, memoryStorage: nil,
            metadata: nil)
        // minimal config
        var cfg = ConsumerConfig(name: "test")
        _ = try await ctx.createConsumer(stream: "test", cfg: cfg)

        // attempt overwriting existing consumer
        var errOk = false
        do {
            _ = try await ctx.createConsumer(
                stream: "test",
                cfg: ConsumerConfig(name: "test", description: "cannot update with create"))
        } catch JetStreamError.ConsumerError.consumerNameExist(_) {
            errOk = true
            // success
        }
        #expect(errOk, "Expected consumer exists error")

        // get a consumer
        guard var cons = try await ctx.getConsumer(stream: "test", name: "test") else {
            Issue.record("Expected a stream, got nil")
            return
        }
        // we need to set the metadata to whatever was set by the server as it contains e.g. server version
        expectedConfig.metadata = cons.info.config.metadata
        #expect(expectedConfig == cons.info.config)

        // get a non-existing consumer
        if let cons = try await ctx.getConsumer(stream: "test", name: "bad") {
            Issue.record("Expected consumer not found, got: \(cons)")
        }

        // update the stream
        cfg.description = "updated"
        cons = try await ctx.updateConsumer(stream: "test", cfg: cfg)
        expectedConfig.description = "updated"

        #expect(expectedConfig == cons.info.config)

        // attempt to update illegal consumer property; nats-server 2.14 rejects a storage
        // type change on an existing consumer, 2.10 accepts it
        cfg.memoryStorage = true
        do {
            let updated = try await ctx.updateConsumer(stream: "test", cfg: cfg)
            #expect(
                updated.info.config.memoryStorage == true,
                "Expected the accepted storage type change to be applied")
        } catch let err as JetStreamError.APIError {
            #expect(err.errorCode == ErrorCode.consumerCreate)
        }

        // attempt updating non-existing consumer
        errOk = false
        do {
            _ = try await ctx.updateConsumer(stream: "test", cfg: ConsumerConfig(name: "bad"))
        } catch JetStreamError.ConsumerError.consumerDoesNotExist(_) {
            // success
            errOk = true
        }
        #expect(errOk, "Expected consumer not found error")

        // delete the consumer
        try await ctx.deleteConsumer(stream: "test", name: "test")

        // make sure the consumer no longer exists
        if let _ = try await ctx.getConsumer(stream: "test", name: "test") {
            Issue.record("Expected consumer not found")
        }
    }

    @Test func testConsumerConfig() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        var cfg = ConsumerConfig(
            name: "test", durable: nil, description: "consumer", deliverPolicy: .byStartSequence,
            optStartSeq: 10, optStartTime: nil, ackPolicy: .none, ackWait: NanoTimeInterval(5),
            maxDeliver: 100, backOff: [NanoTimeInterval(5), NanoTimeInterval(10)],
            filterSubject: "FOO.A", filterSubjects: nil, replayPolicy: .original, rateLimit: nil,
            sampleFrequency: "50",
            maxWaiting: 20, maxAckPending: 20, headersOnly: true, maxRequestBatch: 5,
            maxRequestExpires: NanoTimeInterval(120), maxRequestMaxBytes: 1024,
            inactiveThreshold: NanoTimeInterval(30), replicas: 1, memoryStorage: true,
            metadata: ["a": "b"])

        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "stream", subjects: ["FOO.*"]))

        let cons = try await stream.createConsumer(cfg: cfg)
        cfg.metadata = cons.info.config.metadata

        #expect(cfg == cons.info.config)
    }

    @Test func testCreateEphemeralConsumer() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)
        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "stream", subjects: ["FOO.*"]))

        let cons = try await stream.createConsumer(cfg: ConsumerConfig())

        #expect(cons.info.name.count == 8)
    }

    @Test func testConsumerInfo() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let stream = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo"]))

        let cfg = ConsumerConfig(name: "cons")
        let consumer = try await stream.createConsumer(cfg: cfg)

        let info = try await consumer.info()
        #expect(info.config.name == "cons")

        // simulate external update of consumer
        let updateJSON = """
            {
                "stream_name": "test",
                "config": {
                    "name": "cons",
                    "description": "updated",
                    "ack_policy": "explicit"
                },
                "action": "update"
            }
            """
        let data = updateJSON.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.CONSUMER.CREATE.test.cons")

        #expect(consumer.info.config.description == nil)

        let newInfo = try await consumer.info()
        #expect(newInfo.config.description == "updated")
        #expect(consumer.info.config.description == "updated")
    }

    @Test func testConsumerInfoWithCustomInbox() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .debug

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .inboxPrefix("_INBOX_custom.baz")
            .build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let stream = try await ctx.createStream(cfg: StreamConfig(name: "test", subjects: ["foo"]))

        let cfg = ConsumerConfig(name: "cons")
        let consumer = try await stream.createConsumer(cfg: cfg)

        let info = try await consumer.info()
        #expect(info.config.name == "cons")

        // simulate external update of consumer
        let updateJSON = """
            {
                "stream_name": "test",
                "config": {
                    "name": "cons",
                    "description": "updated",
                    "ack_policy": "explicit"
                },
                "action": "update"
            }
            """
        let data = updateJSON.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.CONSUMER.CREATE.test.cons")

        #expect(consumer.info.config.description == nil)

        let newInfo = try await consumer.info()
        #expect(newInfo.config.description == "updated")
        #expect(consumer.info.config.description == "updated")
    }

    @Test func testListConsumers() async throws {
        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let stream = try await ctx.createStream(
            cfg: StreamConfig(name: "test", subjects: ["foo.*"]))

        for i in 0..<260 {
            let cfg = ConsumerConfig(name: "CONSUMER-\(i)")
            let _ = try await stream.createConsumer(cfg: cfg)
        }

        // list all consumers
        var consumers = await stream.consumers()

        var i = 0
        for try await _ in consumers {
            i += 1
        }
        #expect(i == 260)

        let names = await stream.consumerNames()
        i = 0
        for try await _ in names {
            i += 1
        }
        #expect(i == 260)

        // list consumers on non-existing stream: the reply carries an error alongside an
        // empty page, and an error reply is an error
        consumers = await ctx.consumers(stream: "bad")
        do {
            for try await _ in consumers {
                Issue.record("Expected stream not found, got a consumer")
            }
            Issue.record("Expected stream not found error")
        } catch let err as JetStreamError.APIError {
            #expect(err.errorCode == ErrorCode.streamNotFound)
        }

        let badNames = await ctx.consumerNames(stream: "bad")
        do {
            for try await _ in badNames {
                Issue.record("Expected stream not found, got a consumer name")
            }
            Issue.record("Expected stream not found error")
        } catch let err as JetStreamError.APIError {
            #expect(err.errorCode == ErrorCode.streamNotFound)
        }
    }

}
