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

@testable import Nats

@Suite(.serialized) final class SlowConsumerTests {

    var natsServer = NatsServer()

    deinit {
        natsServer.stop()
    }

    @Test(.timeLimit(.minutes(1)))
    func testSlowConsumerEventFiresOnOverflow() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        // Capacity 2 and we never read from the subscription, so once more than two
        // messages are delivered the buffer overflows and must fire a slow consumer
        // event.
        await confirmation("slow consumer event was not fired") { confirmed in
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let once = ResumeOnce()
                client.on(.error) { event in
                    guard case .error(let err) = event,
                        let subErr = err as? NatsError.SubscriptionError,
                        case .slowConsumer = subErr
                    else { return }
                    once.run {
                        confirmed()
                        continuation.resume()
                    }
                }
                Task {
                    _ = try? await client.subscribe(subject: "foo", capacity: 2)
                    _ = try? await client.rtt()
                    let payload = "x".data(using: .utf8)!
                    for _ in 0..<10 {
                        try? await client.publish(payload, subject: "foo")
                    }
                    try? await client.flush()
                }
            }
        }
        try await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func testSlowConsumerReArmsAfterDrain() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        // Overflow once, drain below capacity/2, overflow again: the slow-consumer
        // signal must re-arm and fire a second time.
        await confirmation("slow consumer did not re-arm", expectedCount: 2) { confirmed in
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let twice = ResumeAfter(2)
                client.on(.error) { event in
                    guard case .error(let err) = event,
                        let subErr = err as? NatsError.SubscriptionError,
                        case .slowConsumer = subErr
                    else { return }
                    confirmed()
                    twice.tick { continuation.resume() }
                }
                Task {
                    let payload = "x".data(using: .utf8)!
                    let sub = try? await client.subscribe(subject: "foo", capacity: 2)
                    _ = try? await client.rtt()

                    for _ in 0..<6 {
                        try? await client.publish(payload, subject: "foo")
                    }
                    try? await client.flush()
                    _ = try? await client.rtt()

                    if let sub {
                        let iter = sub.makeAsyncIterator()
                        _ = try? await iter.next()
                        _ = try? await iter.next()
                    }

                    for _ in 0..<6 {
                        try? await client.publish(payload, subject: "foo")
                    }
                    try? await client.flush()
                }
            }
        }
        try await client.close()
    }
}
