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
import Nats
import NatsServer
import Testing

@Suite(.serialized)
struct StressTests {

    @Test(.timeLimit(.minutes(1)))
    func testConcurrentSubscriptionCreation() async throws {
        let natsServer = NatsServer()
        natsServer.start()
        defer { natsServer.stop() }

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .build()
        try await client.connect()

        let tasks = (0..<10).map { i in
            Task {
                try await client.subscribe(subject: "concurrent.test.\(i)")
            }
        }
        for task in tasks {
            _ = try await task.value
        }

        try await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func testByteBufferReinitialization() async throws {
        let natsServer = NatsServer()
        natsServer.start()
        defer { natsServer.stop() }

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .reconnectWait(0.01)
            .maxReconnects(100)
            .build()
        try await client.connect()

        let sub = try await client.subscribe(subject: "test.buffer.race")

        let publishTask = Task {
            for i in 0..<1000 {
                let payload = String(repeating: "X", count: i % 1000 + 1)
                try? await client.publish(payload.data(using: .utf8)!, subject: "test.buffer.race")
                if i % 10 == 0 {
                    try? await Task.sleep(nanoseconds: 1000)
                }
            }
        }

        let reconnectTask = Task {
            for _ in 0..<20 {
                try? await Task.sleep(nanoseconds: 50_000_000)
                try? await client.reconnect()
            }
        }

        let consumeTask = Task {
            var count = 0
            for try await _ in sub {
                count += 1
                if count > 100 {
                    break
                }
            }
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { await publishTask.value }
            group.addTask { await reconnectTask.value }
            group.addTask { try await consumeTask.value }
            _ = try await group.waitForAll()
            group.cancelAll()
        }

        try await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func testConcurrentChannelActiveAndRead() async throws {
        let natsServer = NatsServer()
        natsServer.start()
        defer { natsServer.stop() }

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .reconnectWait(0.01)
            .maxReconnects(50)
            .build()
        try await client.connect()

        let sub = try await client.subscribe(subject: "test.concurrent.>")

        let publishTask = Task {
            for i in 0..<500 {
                let subjects = ["test.concurrent.a", "test.concurrent.b", "test.concurrent.c"]
                let subject = subjects[i % subjects.count]
                let payload = String(repeating: "D", count: (i * 7) % 2048 + 100)
                try? await client.publish(payload.data(using: .utf8)!, subject: subject)
            }
        }

        let reconnectTask = Task {
            for i in 0..<10 {
                try await Task.sleep(nanoseconds: 100_000_000)
                try await client.reconnect()
                for j in 0..<10 {
                    try? await client.publish(
                        "RECONNECT-\(i)-\(j)".data(using: .utf8)!,
                        subject: "test.concurrent.reconnect")
                }
            }
        }

        let consumeTask = Task {
            var count = 0
            for try await _ in sub {
                count += 1
                if count > 200 {
                    break
                }
                if count % 50 == 0 {
                    try await Task.sleep(nanoseconds: 1_000_000)
                }
            }
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { await publishTask.value }
            group.addTask { try await reconnectTask.value }
            group.addTask { try await consumeTask.value }
            _ = try await group.waitForAll()
            group.cancelAll()
        }

        try await client.close()
    }
}
