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
import Nats
import NatsServer
import Testing

@Suite(.serialized) final class NatsEventsTests {

    var natsServer = NatsServer()

    deinit {
        natsServer.stop()
    }

    @Test(.timeLimit(.minutes(1)))
    func testClientConnectedEvent() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()

        try await confirmation("client was not notified of connection established event") {
            connected in
            client.on(.connected) { event in
                #expect(event.kind() == NatsEventKind.connected)
                connected()
            }
            try await client.connect()
        }
        try await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func testClientClosedEvent() async throws {
        natsServer.start()
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()

        try await confirmation("client was not notified of connection closed event") { closed in
            client.on(.closed) { event in
                #expect(event.kind() == NatsEventKind.closed)
                closed()
            }
            try await client.connect()
            try await client.close()
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func testClientReconnectEvent() async throws {
        natsServer.start()
        let port = natsServer.port!
        logger.logLevel = .critical

        let client = NatsClientOptions()
            .url(URL(string: natsServer.clientURL)!)
            .reconnectWait(1)
            .build()

        try await client.connect()

        try await confirmation("client was not notified of disconnection event") { disconnected in
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let once = ResumeOnce()
                client.on(.disconnected) { event in
                    once.run {
                        #expect(event.kind() == NatsEventKind.disconnected)
                        disconnected()
                        continuation.resume()
                    }
                }
                natsServer.stop()
            }
        }

        try await confirmation("client was not notified of reconnection event") { reconnected in
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let once = ResumeOnce()
                client.on(.connected) { event in
                    once.run {
                        #expect(event.kind() == NatsEventKind.connected)
                        reconnected()
                        continuation.resume()
                    }
                }
                natsServer.start(port: port)
            }
        }

        try await client.close()
    }
}
