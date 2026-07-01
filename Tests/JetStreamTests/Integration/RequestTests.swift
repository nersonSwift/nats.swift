//
//  File.swift
//
//
//  Created by Piotr Piotrowski on 05/06/2024.
//

import Foundation
import Logging
import NIO
import Nats
import NatsServer
import Testing

@testable import JetStream

@Suite(.serialized) final class RequestTests {

    var natsServer = NatsServer()

    deinit {
        natsServer.stop()
    }

    @Test func testRequest() async throws {

        let bundle = Bundle.module
        natsServer.start(
            cfg: bundle.url(forResource: "jetstream", withExtension: "conf")!.relativePath)
        logger.logLevel = .critical

        let client = NatsClientOptions().url(URL(string: natsServer.clientURL)!).build()
        try await client.connect()

        let ctx = JetStreamContext(client: client)

        let stream = """
            {
                "name": "FOO",
                "subjects": ["foo"]
            }
            """
        let data = stream.data(using: .utf8)!

        _ = try await client.request(data, subject: "$JS.API.STREAM.CREATE.FOO")

        let info: Response<AccountInfo> = try await ctx.request("INFO", message: Data())

        guard case .success(let info) = info else {
            Issue.record("request should be successful")
            return
        }

        #expect(info.streams == 1)
        let badInfo: Response<AccountInfo> = try await ctx.request(
            "STREAM.INFO.BAD", message: Data())
        guard case .error(let jetStreamAPIResponse) = badInfo else {
            Issue.record("should get error")
            return
        }

        #expect(ErrorCode.streamNotFound == jetStreamAPIResponse.error.errorCode)

    }
}
