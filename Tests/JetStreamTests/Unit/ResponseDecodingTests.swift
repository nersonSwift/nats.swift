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

@testable import JetStream

/// Decoding contract for ``Response``, over payloads captured verbatim from a live
/// `nats:2.10 -js` server.
///
/// The branch is chosen by the presence of a non-null `error` key, never by which of the
/// two types happens to decode. Both directions of that rule have a live payload behind
/// them: a publish error reply carries `stream` and `seq: 0` alongside `error`, so it
/// decodes cleanly as the success type ``Ack``; and the `CONSUMER.LIST` / `CONSUMER.NAMES`
/// replies carry a complete, decodable payload alongside `error`.
@Suite struct ResponseDecodingTests {

    private func decodeAck(_ json: String) throws -> Response<Ack> {
        try JSONDecoder().decode(Response<Ack>.self, from: Data(json.utf8))
    }

    /// Mirrors the private page type `Consumers` decodes `CONSUMER.LIST` into.
    private struct ConsumersPage: Codable {
        let total: Int
        let consumers: [ConsumerInfo]?
    }

    private func decodeConsumersPage(_ json: String) throws -> Response<ConsumersPage> {
        try JSONDecoder().decode(Response<ConsumersPage>.self, from: Data(json.utf8))
    }

    @Test func overQuotaPublishReplyDecodesAsError() throws {
        let response = try decodeAck(
            #"{"error":{"code":503,"err_code":10077,"description":"maximum messages exceeded"},"stream":"PROBE","seq":0}"#
        )

        guard case .error(let apiResponse) = response else {
            Issue.record("expected .error, got \(response)")
            return
        }
        #expect(apiResponse.type == nil)
        #expect(apiResponse.error.code == 503)
        #expect(apiResponse.error.errorCode == ErrorCode.streamStoreFailed)
        #expect(apiResponse.error.description == "maximum messages exceeded")
    }

    @Test func storedPublishReplyDecodesAsSuccess() throws {
        let response = try decodeAck(#"{"stream":"PROBE", "seq":2}"#)

        guard case .success(let ack) = response else {
            Issue.record("expected .success, got \(response)")
            return
        }
        #expect(ack.stream == "PROBE")
        #expect(ack.seq == 2)
        #expect(ack.duplicate == false)
    }

    @Test func duplicatePublishReplyDecodesAsSuccessWithOriginalSequence() throws {
        let response = try decodeAck(#"{"stream":"PROBE", "seq":2,"duplicate": true}"#)

        guard case .success(let ack) = response else {
            Issue.record("expected .success, got \(response)")
            return
        }
        #expect(ack.seq == 2)
        #expect(ack.duplicate == true)
    }

    /// `CONSUMER.LIST` on a missing stream answers with an error *and* a fully decodable
    /// page in one document — byte-identical on nats-server 2.10.22 and 2.14.4. The reply
    /// carries an error, so it is an error, and listing consumers of a missing stream
    /// throws instead of yielding an empty sequence.
    @Test func consumerListReplyWithErrorDecodesAsError() throws {
        let response = try decodeConsumersPage(
            #"{"type":"io.nats.jetstream.api.v1.consumer_list_response","error":{"code":404,"err_code":10059,"description":"stream not found"},"total":0,"offset":0,"limit":0,"consumers":[]}"#
        )

        guard case .error(let apiResponse) = response else {
            Issue.record("expected .error, got \(response)")
            return
        }
        #expect(apiResponse.type == "io.nats.jetstream.api.v1.consumer_list_response")
        #expect(apiResponse.error.code == 404)
        #expect(apiResponse.error.errorCode == ErrorCode.streamNotFound)
    }

    @Test func consumerListReplyWithoutErrorDecodesAsSuccess() throws {
        let response = try decodeConsumersPage(
            #"{"type":"io.nats.jetstream.api.v1.consumer_list_response","total":0,"offset":0,"limit":0,"consumers":[]}"#
        )

        guard case .success(let page) = response else {
            Issue.record("expected .success, got \(response)")
            return
        }
        #expect(page.total == 0)
        #expect(page.consumers?.isEmpty == true)
    }

    /// `error` is `omitempty` on the wire, so a null is not a shape the server sends — but
    /// a null error is the absence of an error, and must never be read as one.
    @Test func replyWithNullErrorDecodesAsSuccess() throws {
        let response = try decodeConsumersPage(
            #"{"type":"io.nats.jetstream.api.v1.consumer_list_response","error":null,"total":0,"offset":0,"limit":0,"consumers":[]}"#
        )

        guard case .success(let page) = response else {
            Issue.record("expected .success, got \(response)")
            return
        }
        #expect(page.consumers?.isEmpty == true)
    }

    /// The classification rule, pinned where the two candidate readings disagree: a reply
    /// that carries an error the client cannot fully parse is still an error, and must be
    /// refused rather than reported as a successful ack. `err_code` and `description` are
    /// `omitempty` in the server's wire type, so an error object narrower than
    /// ``JetStreamError/APIError`` is the shape this guards against.
    @Test func replyWithUnparsableErrorIsNotReportedAsSuccess() throws {
        #expect(throws: DecodingError.self) {
            try decodeAck(#"{"error":{"code":503},"stream":"PROBE","seq":0}"#)
        }
    }

    @Test func streamCreateResponseDecodesAsSuccess() throws {
        let json = #"""
            {"type":"io.nats.jetstream.api.v1.stream_create_response","config":{"name":"PROBE","subjects":["probe.x"],"retention":"workqueue","max_consumers":-1,"max_msgs":3,"max_bytes":-1,"max_age":0,"max_msgs_per_subject":-1,"max_msg_size":-1,"discard":"new","storage":"file","num_replicas":1,"duplicate_window":120000000000,"compression":"none","allow_direct":false,"mirror_direct":false,"sealed":false,"deny_delete":false,"deny_purge":false,"allow_rollup_hdrs":false,"consumer_limits":{}},"created":"2026-08-11T19:33:50.034964009Z","state":{"messages":0,"bytes":0,"first_seq":0,"first_ts":"0001-01-01T00:00:00Z","last_seq":0,"last_ts":"0001-01-01T00:00:00Z","consumer_count":0},"ts":"2026-08-11T19:33:50.037861342Z","did_create":true}
            """#
        let response = try JSONDecoder().decode(Response<StreamInfo>.self, from: Data(json.utf8))

        guard case .success(let info) = response else {
            Issue.record("expected .success, got \(response)")
            return
        }
        #expect(info.config.name == "PROBE")
        #expect(info.state.messages == 0)
    }
}
