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
import Testing

@testable import Nats

@Suite struct NatsClientOptionsTests {

    @Test func testDefaultInboxPrefix() {
        let client = NatsClientOptions().build()
        let inbox = client.newInbox()
        #expect(inbox.hasPrefix("_INBOX."), "Default inbox prefix should be '_INBOX.'")
        #expect(inbox.count == "_INBOX.".count + 22, "Inbox should have prefix plus NUID")
    }

    @Test func testCustomInboxPrefix() {
        let customPrefix = "_INBOX_abc123."
        let client = NatsClientOptions().inboxPrefix(customPrefix).build()
        let inbox = client.newInbox()
        #expect(inbox.hasPrefix(customPrefix), "Inbox should use custom prefix")
        #expect(
            inbox.count == customPrefix.count + 22, "Inbox should have custom prefix plus NUID")
    }

    @Test func testDefaultPortsInjection() {
        let options = NatsClientOptions()

        let natsUrl = URL(string: "nats://localhost")!
        let tlsUrl = URL(string: "tls://demo.nats.io")!
        let wsUrl = URL(string: "ws://127.0.0.1")!
        let wssUrl = URL(string: "wss://echo.websocket.org")!
        let customPortUrl = URL(string: "nats://localhost:9999")!

        // Apply the URLs
        _ = options.urls([natsUrl, tlsUrl, wsUrl, wssUrl, customPortUrl])

        // Swift's Reflection (Mirror) allows us to read private variables (like 'urls') during tests
        let mirror = Mirror(reflecting: options)
        guard
            let internalUrls = mirror.children.first(where: { $0.label == "urls" })?.value as? [URL]
        else {
            Issue.record("Could not extract urls from options")
            return
        }

        #expect(internalUrls[0].port == 4222, "nats:// should default to 4222")
        #expect(internalUrls[1].port == 4222, "tls:// should default to 4222")
        #expect(internalUrls[2].port == 80, "ws:// should default to 80")
        #expect(internalUrls[3].port == 443, "wss:// should default to 443")
        #expect(internalUrls[4].port == 9999, "Custom ports should not be overwritten")
    }

    @Test func testDefaultPortsInjectionWithSingleUrl() {
        let options = NatsClientOptions()

        let natsUrl = URL(string: "nats://localhost")!

        _ = options.url(natsUrl)

        let mirror = Mirror(reflecting: options)
        guard
            let internalUrls = mirror.children.first(where: { $0.label == "urls" })?.value as? [URL]
        else {
            Issue.record("Could not extract urls from options")
            return
        }

        #expect(internalUrls.count == 1, "Single URL should produce exactly one entry")
        #expect(internalUrls[0].port == 4222, "nats:// should default to 4222")
    }
}
