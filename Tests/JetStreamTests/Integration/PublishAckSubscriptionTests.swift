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
import NatsServer
import Testing

@testable import JetStream
@testable import Nats

@Suite(.serialized) final class PublishAckSubscriptionTests {

    var natsServer = NatsServer()
    private var clientCID: UInt64?

    deinit {
        natsServer.stop()
    }

    @Test(.timeLimit(.minutes(1))) func publishAndWaitDoesNotAccumulateSubscriptions()
        async throws
    {
        let ctx = try await makeStreamContext()
        let handler = try #require(ctx.client.connectionHandler)
        let baseline = try baselineSubscriptionCounts(handler)

        for _ in 0..<50 {
            _ = try await ctx.publish("foo", message: "hi".data(using: .utf8)!).wait()
        }

        let local = await poll(
            until: { $0 == baseline.local }, value: { handler.subscriptionCount })
        #expect(local == baseline.local, "publish+wait leaked inbox subscriptions on the client")

        let conn = try await poll(
            until: { $0.subscriptions == baseline.server }, value: { try self.clientConnection() })
        #expect(
            conn.subscriptions == baseline.server,
            "publish+wait leaked inbox subscriptions on the server: \(conn.subscriptionsList ?? [])"
        )
    }

    @Test(.timeLimit(.minutes(1))) func discardedAckFutureReleasesItsSubscription() async throws {
        let ctx = try await makeStreamContext()
        let handler = try #require(ctx.client.connectionHandler)
        let baseline = try baselineSubscriptionCounts(handler)

        for _ in 0..<50 {
            _ = try await ctx.publish("foo", message: "hi".data(using: .utf8)!)
        }

        let local = await poll(
            until: { $0 == baseline.local }, value: { handler.subscriptionCount })
        #expect(local == baseline.local, "a discarded AckFuture leaked its subscription")

        let conn = try await poll(
            until: { $0.subscriptions == baseline.server }, value: { try self.clientConnection() })
        #expect(
            conn.subscriptions == baseline.server,
            "a discarded AckFuture leaked its subscription: \(conn.subscriptionsList ?? [])")
    }

    @Test(.timeLimit(.minutes(1))) func waitReportsCancellationAsCancellation() async throws {
        let ctx = try await makeStreamContext()
        let handler = try #require(ctx.client.connectionHandler)
        let baseline = try baselineSubscriptionCounts(handler)

        let future = try await makeNeverAckedFuture(on: ctx.client, timeout: 30)
        let waiting = Task { try await future.wait() }
        try await Task.sleep(nanoseconds: 200_000_000)
        let cancelledAt = ProcessInfo.processInfo.systemUptime
        waiting.cancel()

        do {
            _ = try await waiting.value
            Issue.record("expected wait() to report cancellation")
        } catch is CancellationError {
        } catch {
            Issue.record("expected CancellationError, got \(error)")
        }
        #expect(
            ProcessInfo.processInfo.systemUptime - cancelledAt < 5,
            "cancellation was reported only after the 30s timeout elapsed")

        let local = await poll(
            until: { $0 == baseline.local }, value: { handler.subscriptionCount })
        withExtendedLifetime(future) {
            #expect(local == baseline.local, "the cancelled wait() left its subscription behind")
        }
    }

    @Test(.timeLimit(.minutes(1))) func waitStillTimesOutWithoutCancellation() async throws {
        let ctx = try await makeStreamContext()
        let handler = try #require(ctx.client.connectionHandler)
        let baseline = try baselineSubscriptionCounts(handler)

        let future = try await makeNeverAckedFuture(on: ctx.client, timeout: 0.5)

        do {
            _ = try await future.wait()
            Issue.record("expected wait() to time out")
        } catch JetStreamError.RequestError.timeout {
        } catch {
            Issue.record("expected RequestError.timeout, got \(error)")
        }

        let local = await poll(
            until: { $0 == baseline.local }, value: { handler.subscriptionCount })
        withExtendedLifetime(future) {
            #expect(local == baseline.local, "the timed out wait() left its subscription behind")
        }
    }

    @Test(.timeLimit(.minutes(1))) func waitReportsAClosedConnectionAsTimeout() async throws {
        let ctx = try await makeStreamContext()
        let future = try await makeNeverAckedFuture(on: ctx.client, timeout: 30)

        let waiting = Task { try await future.wait() }
        try await Task.sleep(nanoseconds: 200_000_000)
        let closedAt = ProcessInfo.processInfo.systemUptime
        try await ctx.client.close()

        do {
            _ = try await waiting.value
            Issue.record("expected wait() to fail once the connection closed")
        } catch JetStreamError.RequestError.timeout {
        } catch {
            Issue.record("expected RequestError.timeout, got \(error)")
        }
        #expect(
            ProcessInfo.processInfo.systemUptime - closedAt < 5,
            "the closed connection was reported only after the 30s timeout elapsed")
    }

    // MARK: - Fixture

    private func makeStreamContext() async throws -> JetStreamContext {
        natsServer.start(
            cfg: Bundle.module.url(forResource: "jetstream_monitor", withExtension: "conf")!
                .relativePath)
        logger.logLevel = .critical
        try #require(
            !natsServer.monitoringURL.isEmpty,
            "nats-server did not report a monitoring port")

        let client = NatsClientOptions().url(try #require(URL(string: natsServer.clientURL)))
            .build()
        try await client.connect()
        clientCID = try natsServer.soleClientConnection().cid

        let ctx = JetStreamContext(client: client)
        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        _ = try await client.request(
            try #require(stream.data(using: .utf8)),
            subject: "$JS.API.STREAM.CREATE.FOO")
        return ctx
    }

    /// An ``AckFuture`` over a private inbox nobody ever publishes to, so its `wait()` can only
    /// end in a timeout or in cancellation.
    private func makeNeverAckedFuture(
        on client: NatsClient, timeout: TimeInterval
    ) async throws -> AckFuture {
        let sub = try await client.subscribe(subject: client.newInbox())
        return AckFuture(sub: sub, timeout: timeout)
    }

    private func baselineSubscriptionCounts(
        _ handler: ConnectionHandler
    ) throws -> (local: Int, server: Int) {
        return (handler.subscriptionCount, try clientConnection().subscriptions)
    }

    // MARK: - Monitoring endpoint

    /// The suite's own connection, addressed by `cid` so that a stray client reconnecting from
    /// another suite onto the same ephemeral port cannot be mistaken for it.
    private func clientConnection() throws -> NatsMonitoredConnection {
        return try natsServer.monitoredConnection(cid: try #require(clientCID))
    }

    // MARK: - Polling

    /// Re-reads `value` every 50ms until `isSettled` holds or 5s elapse, then returns the last
    /// read. Both the local map removal and the `UNSUB` write are asynchronous, so a single
    /// read right after the loop would be a race.
    private func poll<T>(
        until isSettled: (T) -> Bool, value: () async throws -> T
    ) async rethrows -> T {
        var current = try await value()
        for _ in 0..<100 where !isSettled(current) {
            try? await Task.sleep(nanoseconds: 50_000_000)
            current = try await value()
        }
        return current
    }
}
