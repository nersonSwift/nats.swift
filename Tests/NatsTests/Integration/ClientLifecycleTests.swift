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
import NatsServer
import Testing

@testable import Nats

@Suite(.serialized) struct ClientLifecycleTests {

    @Test(.timeLimit(.minutes(1)))
    func testAbandonedReconnectingClientIsReleased() async throws {
        logger.logLevel = .critical
        let server = NatsServer()
        server.start()
        defer { server.stop() }

        weak var weakHandler: ConnectionHandler?
        do {
            let client = NatsClientOptions().url(URL(string: server.clientURL)!).build()
            try await client.connect()
            weakHandler = client.connectionHandler
            // Drop the server so the client enters its reconnect loop, then release it.
            server.stop()
        }
        for _ in 0..<100 where weakHandler != nil {
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        #expect(weakHandler == nil, "abandoned reconnecting client was leaked")
    }

    @Test(.timeLimit(.minutes(1)))
    func testClientReleasedAfterMaxReconnectsGiveUp() async throws {
        logger.logLevel = .critical
        let server = NatsServer()
        server.start()
        defer { server.stop() }

        weak var weakHandler: ConnectionHandler?
        do {
            let client = NatsClientOptions()
                .url(URL(string: server.clientURL)!)
                .reconnectWait(0.05)
                .maxReconnects(2)
                .build()
            try await client.connect()
            weakHandler = client.connectionHandler
            let closed = AsyncStream<Void>.makeStream()
            client.on([.closed]) { _ in closed.continuation.yield(()) }
            // Drop the server and wait until the client exhausts maxReconnects and
            // gives up, then release it: a wedged give-up would keep the handler alive.
            server.stop()
            var events = closed.stream.makeAsyncIterator()
            _ = await events.next()
        }
        for _ in 0..<100 where weakHandler != nil {
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        #expect(weakHandler == nil, "client leaked after maxReconnects give-up")
    }

    @Test(.timeLimit(.minutes(1)))
    func testClientWithActiveSubscriptionIsReleased() async throws {
        logger.logLevel = .critical
        let server = NatsServer()
        server.start()
        defer { server.stop() }

        weak var weakHandler: ConnectionHandler?
        do {
            let client = NatsClientOptions().url(URL(string: server.clientURL)!).build()
            try await client.connect()
            weakHandler = client.connectionHandler
            // Register a subscription and drop it: the handler holds it strongly and
            // the subscription holds the handler back, so releasing the client must
            // break that cycle.
            _ = try await client.subscribe(subject: "test")
        }
        for _ in 0..<100 where weakHandler != nil {
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        #expect(weakHandler == nil, "client with an active subscription was leaked")
    }

    @Test(.timeLimit(.minutes(1)))
    func testActiveSubscriptionEndsOnMaxReconnectsGiveUp() async throws {
        logger.logLevel = .critical
        let server = NatsServer()
        server.start()
        defer { server.stop() }

        let client = NatsClientOptions()
            .url(URL(string: server.clientURL)!)
            .reconnectWait(0.05)
            .maxReconnects(2)
            .build()
        try await client.connect()
        let sub = try await client.subscribe(subject: "test")

        // Drop the server so the client exhausts maxReconnects and gives up while the
        // subscription is still held: its iterator must terminate rather than hang.
        server.stop()
        for try await _ in sub {}
    }
}
