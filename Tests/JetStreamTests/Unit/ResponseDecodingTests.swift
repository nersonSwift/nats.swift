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
/// A JetStream publish error reply carries `stream` and `seq: 0` alongside `error`
/// and carries no `type`, so it decodes cleanly as the success type ``Ack``. If the
/// error branch is not recognised first, a rejected publish is reported to the
/// caller as a successful ack.
@Suite struct ResponseDecodingTests {

    private func decodeAck(_ json: String) throws -> Response<Ack> {
        try JSONDecoder().decode(Response<Ack>.self, from: Data(json.utf8))
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
