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
import XCTest

#if canImport(Glibc)
    import Glibc
#endif

#if canImport(FoundationNetworking)
    import FoundationNetworking
#endif

/// One connection as the server itself reports it on `/connz`.
public struct NatsMonitoredConnection: Decodable {
    public let cid: UInt64
    public let kind: String
    public let subscriptions: Int
    /// Omitted by the server when the connection holds no subscriptions.
    public let subscriptionsList: [String]?

    enum CodingKeys: String, CodingKey {
        case cid
        case kind
        case subscriptions
        case subscriptionsList = "subscriptions_list"
    }
}

public struct NatsMonitoringError: Error, CustomStringConvertible {
    public let description: String
}

public class NatsServer {
    public var port: Int? { return natsServerPort }
    public var clientURL: String {
        let scheme = tlsEnabled ? "tls://" : "nats://"
        if let natsServerPort {
            return "\(scheme)localhost:\(natsServerPort)"
        } else {
            return ""
        }
    }

    public var clientWebsocketURL: String {
        let scheme = tlsEnabled ? "wss://" : "ws://"
        if let natsWebsocketPort {
            return "\(scheme)localhost:\(natsWebsocketPort)"
        } else {
            return ""
        }
    }

    public var monitoringURL: String {
        if let natsMonitoringPort {
            return "http://localhost:\(natsMonitoringPort)"
        } else {
            return ""
        }
    }

    private var process: Process?
    private var natsServerPort: Int?
    private var natsWebsocketPort: Int?
    private var natsMonitoringPort: Int?
    private var tlsEnabled = false
    private var pidFile: URL?

    public init() {}

    // TODO: When implementing JetStream, creating and deleting store dir should be handled in start/stop methods
    public func start(
        port: Int = -1, cfg: String? = nil, file: StaticString = #file, line: UInt = #line
    ) {
        XCTAssertNil(
            self.process, "nats-server is already running on port \(port)", file: file, line: line)
        let process = Process()
        let pipe = Pipe()

        let fileManager = FileManager.default
        pidFile = fileManager.temporaryDirectory.appendingPathComponent("nats-server.pid")

        let tempDir = FileManager.default.temporaryDirectory.appending(component: UUID().uuidString)

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "nats-server", "-p", "\(port)", "-P", pidFile!.path, "--store_dir",
            tempDir.path,
        ]
        if let cfg {
            process.arguments?.append(contentsOf: ["-c", cfg])
        }
        process.standardError = pipe
        process.standardOutput = pipe

        let outputHandle = pipe.fileHandleForReading
        let semaphore = DispatchSemaphore(value: 0)
        var lineCount = 0
        let maxLines = 100
        var serverError: String?
        var outputBuffer = Data()

        outputHandle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            guard data.count > 0 else { return }
            outputBuffer.append(data)

            guard let output = String(data: outputBuffer, encoding: .utf8) else { return }

            let lines = output.split(separator: "\n", omittingEmptySubsequences: false)
            let completedLines = lines.dropLast()

            for lineSequence in completedLines {
                let line = String(lineSequence)
                lineCount += 1

                let errorLine = self.extracErrorMessage(from: line)

                if let port = self.extractPort(from: line, for: "client connections") {
                    self.natsServerPort = port
                }

                if let port = self.extractPort(from: line, for: "websocket clients") {
                    self.natsWebsocketPort = port
                }

                if let port = self.extractMonitoringPort(from: line) {
                    self.natsMonitoringPort = port
                }

                let ready = line.contains("Server is ready")

                if !self.tlsEnabled && self.isTLS(from: line) {
                    self.tlsEnabled = true
                }

                if ready || errorLine != nil || lineCount >= maxLines {
                    serverError = errorLine
                    semaphore.signal()
                    outputHandle.readabilityHandler = { handle in
                        if handle.availableData.isEmpty {
                            handle.readabilityHandler = nil
                        }
                    }
                    return
                }
            }

            if output.hasSuffix("\n") {
                outputBuffer.removeAll()
            } else {
                if let lastLine = lines.last, let incompleteLine = lastLine.data(using: .utf8) {
                    outputBuffer = incompleteLine
                }
            }
        }

        XCTAssertNoThrow(
            try process.run(), "error starting nats-server on port \(port)", file: file, line: line)

        let result = semaphore.wait(timeout: .now() + .seconds(10))

        XCTAssertFalse(
            result == .timedOut, "timeout waiting for server to be ready", file: file, line: line)
        XCTAssertNil(
            serverError, "error starting nats-server: \(serverError!)", file: file, line: line)

        self.process = process
    }

    /// The connections the server itself reports, with their subscription detail — the other
    /// end of any assertion about a subscription's lifetime. Requires a configuration with a
    /// monitoring port (`http_port: -1`).
    public func monitoredConnections() throws -> [NatsMonitoredConnection] {
        guard !monitoringURL.isEmpty, let url = URL(string: "\(monitoringURL)/connz?subs=1")
        else {
            throw NatsMonitoringError(
                description: "the server was started without a monitoring port")
        }
        return try JSONDecoder().decode(Connz.self, from: try Data(contentsOf: url)).connections
    }

    /// The single client connection on the server, for a suite that connects exactly one.
    public func soleClientConnection() throws -> NatsMonitoredConnection {
        let clients = try monitoredConnections().filter { $0.kind == "Client" }
        guard clients.count == 1 else {
            throw NatsMonitoringError(
                description: "expected exactly one client connection, got \(clients.count)")
        }
        return clients[0]
    }

    /// The connection with the given `cid`, so that a client reconnecting from another suite
    /// onto the same ephemeral port cannot be mistaken for the one under test.
    public func monitoredConnection(cid: UInt64) throws -> NatsMonitoredConnection {
        let matches = try monitoredConnections().filter { $0.cid == cid }
        guard matches.count == 1 else {
            throw NatsMonitoringError(
                description: "expected connection \(cid) in /connz, got \(matches.count)")
        }
        return matches[0]
    }

    private struct Connz: Decodable {
        let connections: [NatsMonitoredConnection]
    }

    public func stop() {
        guard let process else {
            return
        }

        (process.standardOutput as? Pipe)?.fileHandleForReading.readabilityHandler = nil
        kill(process.processIdentifier, SIGKILL)
        let deadline = Date().addingTimeInterval(5)
        while process.isRunning && Date() < deadline {
            Thread.sleep(forTimeInterval: 0.005)
        }
        self.process = nil
        natsServerPort = port
        natsMonitoringPort = nil
        tlsEnabled = false
    }

    public func sendSignal(_ signal: Signal, file: StaticString = #file, line: UInt = #line) {
        let process = Process()

        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["nats-server", "--signal", "\(signal.rawValue)=\(self.pidFile!.path)"]

        XCTAssertNoThrow(
            try process.run(), "error setting signal", file: file, line: line)
        self.process = nil
    }

    private func extractPort(from string: String, for phrase: String) -> Int? {
        // Listening for websocket clients on
        // Listening for client connections on
        return extractPort(from: string, matching: "Listening for \(phrase) on .*?:(\\d+)$")
    }

    private func extractMonitoringPort(from string: String) -> Int? {
        // Starting http monitor on 0.0.0.0:8222
        return extractPort(from: string, matching: "Starting http monitor on .*?:(\\d+)$")
    }

    private func extractPort(from string: String, matching pattern: String) -> Int? {
        let regex = try! NSRegularExpression(pattern: pattern)
        let nsrange = NSRange(string.startIndex..<string.endIndex, in: string)

        if let match = regex.firstMatch(in: string, options: [], range: nsrange) {
            let portRange = match.range(at: 1)
            if let swiftRange = Range(portRange, in: string) {
                let portString = String(string[swiftRange])
                return Int(portString)
            }
        }

        return nil
    }

    private func extracErrorMessage(from logLine: String) -> String? {
        if logLine.contains("nats-server: No such file or directory") {
            return "nats-server not found - make sure nats-server can be found in PATH"
        }
        guard let range = logLine.range(of: "[FTL]") else {
            return nil
        }

        let messageStartIndex = range.upperBound
        let message = logLine[messageStartIndex...]

        return String(message).trimmingCharacters(in: .whitespaces)
    }

    private func isTLS(from logLine: String) -> Bool {
        return logLine.contains("TLS required for client connections")
            || logLine.contains("websocket clients on wss://")
    }

    deinit {
        stop()
    }

    public enum Signal: String {
        case lameDuckMode = "ldm"
        case reload = "reload"
    }
}
