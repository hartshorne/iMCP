import Logging
import MCP
import Network
import ServiceLifecycle
import SystemPackage

import struct Foundation.Data
import class Foundation.RunLoop

let log: Logger = {
    var logger = Logger(label: "me.mattt.iMCP.server") { StreamLogHandler.standardError(label: $0) }
    #if DEBUG
        logger.logLevel = .debug
    #else
        logger.logLevel = .warning
    #endif
    return logger
}()

// Network setup
let serviceType = "_mcp._tcp"
let serviceDomain = "local."
let parameters = NWParameters.tcp
parameters.acceptLocalOnly = true
parameters.includePeerToPeer = false

if let tcpOptions = parameters.defaultProtocolStack.internetProtocol as? NWProtocolIP.Options {
    tcpOptions.version = .v4
}

private func normalizeBonjourDomain(_ domain: String) -> String {
    var normalizedDomain = domain.lowercased()
    if normalizedDomain.hasSuffix(".") {
        normalizedDomain.removeLast()
    }
    return normalizedDomain
}

private func localMCPServiceName(from endpoint: NWEndpoint) -> String? {
    guard case let .service(name: name, type: type, domain: domain, interface: _) = endpoint,
        type == serviceType,
        normalizeBonjourDomain(domain) == normalizeBonjourDomain(serviceDomain)
    else {
        return nil
    }

    return name
}

private func isLikelyIMCPService(_ endpoint: NWEndpoint) -> Bool {
    guard let serviceName = localMCPServiceName(from: endpoint) else {
        return false
    }

    return serviceName.lowercased().contains("imcp")
}

actor ConnectionState {
    private var hasResumed = false

    func checkAndSetResumed() -> Bool {
        if !hasResumed {
            hasResumed = true
            return true
        }
        return false
    }
}

/// An actor that provides a configurable proxy between standard I/O and network connections
actor StdioProxy {
    // Connection configuration
    private let endpoint: NWEndpoint
    private let parameters: NWParameters
    private let stdinBufferSize: Int
    private let networkBufferSize: Int

    // Connection state
    private var connection: NWConnection?
    private var isRunning = false

    // Message buffering for proper JSON-RPC message boundaries
    private var networkToStdoutBuffer = Data()

    /// Creates a new StdioProxy with the specified network configuration
    /// - Parameters:
    ///   - endpoint: The network endpoint to connect to
    ///   - parameters: Network connection parameters
    ///   - stdinBufferSize: Buffer size for reading from stdin (default: 4096)
    ///   - networkBufferSize: Buffer size for reading from network (default: 4096)
    init(
        endpoint: NWEndpoint,
        parameters: NWParameters = .tcp,
        stdinBufferSize: Int = 10 * 1024 * 1024,
        networkBufferSize: Int = 10 * 1024 * 1024
    ) {
        self.endpoint = endpoint
        self.parameters = parameters
        self.stdinBufferSize = stdinBufferSize
        self.networkBufferSize = networkBufferSize
    }

    /// Starts the proxy
    func start() async throws {
        guard !isRunning else { return }
        isRunning = true

        // Create the connection
        let connection = NWConnection(to: endpoint, using: parameters)
        self.connection = connection

        // Start the connection
        connection.start(queue: .main)

        // Set up state monitoring for the entire lifetime of the connection
        connection.stateUpdateHandler = { state in
            Task { [weak self] in
                await self?.handleConnectionState(state, continuation: nil, connectionState: nil)
            }
        }

        // Wait for the connection to become ready
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<Void, Swift.Error>) in
            let connectionState = ConnectionState()
            connection.stateUpdateHandler = { state in
                Task { [weak self] in
                    await self?.handleConnectionState(
                        state,
                        continuation: continuation,
                        connectionState: connectionState
                    )
                }
            }
        }

        // Create a structured concurrency task group for handling I/O
        try await withThrowingTaskGroup(of: Void.self) { group in
            // Add task for handling stdin to network
            group.addTask { [stdinBufferSize] in
                do {
                    try await self.handleStdinToNetwork(bufferSize: stdinBufferSize)
                } catch {
                    log.error("Stdin handler failed: \(error)")
                    throw error
                }
            }

            // Add task for handling network to stdout
            group.addTask { [networkBufferSize] in
                do {
                    try await self.handleNetworkToStdout(bufferSize: networkBufferSize)
                } catch {
                    log.error("Network handler failed: \(error)")
                    throw error
                }
            }

            // Wait for any task to complete (or fail)
            try await group.next()
            log.debug("A task completed, cancelling remaining tasks")

            // If we get here, one of the tasks completed or failed
            // Cancel all remaining tasks
            group.cancelAll()

            // Stop the proxy
            await self.stop()
        }
    }

    /// Stops the proxy and cleans up resources
    func stop() async {
        isRunning = false
        connection?.cancel()
        connection = nil
    }

    /// Handles connection state changes
    private func handleConnectionState(
        _ state: NWConnection.State,
        continuation: CheckedContinuation<Void, Swift.Error>?,
        connectionState: ConnectionState?
    ) async {
        switch state {
        case .ready:
            log.debug("Connection established to \(endpoint)")
            if await shouldResume(connectionState: connectionState) {
                continuation?.resume()
            }
        case .failed(let error):
            log.debug("Connection failed: \(error)")
            if let continuation = continuation,
                await shouldResume(connectionState: connectionState)
            {
                continuation.resume(throwing: error)
            }
            await stop()
        case .cancelled:
            log.debug("Connection cancelled")
            if let continuation = continuation,
                await shouldResume(connectionState: connectionState)
            {
                continuation.resume(throwing: CancellationError())
            }
            await stop()
        case .waiting(let error):
            log.debug("Connection waiting: \(error)")
        case .preparing:
            log.debug("Connection preparing...")
        case .setup:
            log.debug("Connection setup...")
        @unknown default:
            log.debug("Unknown connection state")
        }
    }

    private func shouldResume(connectionState: ConnectionState?) async -> Bool {
        if let connectionState = connectionState {
            return await connectionState.checkAndSetResumed()
        }
        return true
    }

    private func setNonBlocking(fileDescriptor: FileDescriptor) throws {
        let flags = fcntl(fileDescriptor.rawValue, F_GETFL)
        guard flags >= 0 else {
            throw MCPError.transportError(Errno.badFileDescriptor)
        }
        let result = fcntl(fileDescriptor.rawValue, F_SETFL, flags | O_NONBLOCK)
        guard result >= 0 else {
            throw MCPError.transportError(Errno.badFileDescriptor)
        }
    }

    /// Handles forwarding data from stdin to the network
    private func handleStdinToNetwork(bufferSize: Int) async throws {
        let stdin = FileDescriptor.standardInput
        try setNonBlocking(fileDescriptor: stdin)

        var buffer = [UInt8](repeating: 0, count: bufferSize)
        var pendingData = Data()

        while true {
            // Check connection state at the beginning of each loop iteration
            guard isRunning, let connection = self.connection else {
                log.debug("Connection no longer active, stopping stdin handler")
                throw StdioProxyError.connectionClosed
            }

            // Also check connection state
            if connection.state != .ready && connection.state != .preparing {
                log.debug(
                    "Connection state changed to \(connection.state), stopping stdin handler"
                )
                throw StdioProxyError.connectionClosed
            }

            do {
                // Read data from stdin using SystemPackage approach
                let bytesRead = try buffer.withUnsafeMutableBufferPointer { pointer in
                    try stdin.read(into: UnsafeMutableRawBufferPointer(pointer))
                }

                if bytesRead == 0 {
                    // EOF reached
                    log.debug("EOF reached on stdin, stopping stdin handler")
                    break
                }

                if bytesRead > 0 {
                    // Append the read bytes to pending data
                    pendingData.append(contentsOf: buffer[0 ..< bytesRead])

                    // Check if the data is only whitespace
                    let isOnlyWhitespace = pendingData.allSatisfy {
                        let char = Character(UnicodeScalar($0))
                        return char.isWhitespace || char.isNewline
                    }

                    // Only send if we have non-whitespace content
                    if !isOnlyWhitespace && !pendingData.isEmpty {
                        // Send data to the network connection
                        try await withCheckedThrowingContinuation {
                            (continuation: CheckedContinuation<Void, Swift.Error>) in
                            connection.send(
                                content: pendingData,
                                completion: .contentProcessed { error in
                                    if let error = error {
                                        continuation.resume(throwing: error)
                                    } else {
                                        continuation.resume()
                                    }
                                }
                            )
                        }

                        log.debug("Sent \(pendingData.count) bytes to network")
                    } else if isOnlyWhitespace && !pendingData.isEmpty {
                        log.trace(
                            "Skipping send of \(pendingData.count) whitespace-only bytes"
                        )
                    }

                    // Clear pending data after processing
                    pendingData.removeAll(keepingCapacity: true)
                }
            } catch {
                if let posixError = error as? Errno, posixError == .wouldBlock {
                    try await Task.sleep(for: .milliseconds(10))  // Keep the sleep to yield CPU
                    continue
                }

                log.error("Error in stdin handler: \(error)")
                throw error
            }
        }

        log.debug("Stdin handler task completed")
    }

    /// Handles forwarding data from the network to stdout
    private func handleNetworkToStdout(bufferSize: Int) async throws {
        let stdout = FileDescriptor.standardOutput
        var consecutiveEmptyReads = 0
        let maxConsecutiveEmptyReads = 100  // After this many consecutive empty reads, we'll check connection state

        while true {
            // Check connection state at the beginning of each loop iteration
            guard isRunning, let connection = self.connection else {
                log.debug("Connection no longer active, stopping network handler")
                throw StdioProxyError.connectionClosed
            }

            // Also check connection state
            if connection.state != .ready && connection.state != .preparing {
                log.debug(
                    "Connection state changed to \(connection.state), stopping network handler"
                )
                throw StdioProxyError.connectionClosed
            }

            do {
                // Check connection state periodically if we're getting consecutive empty reads
                if consecutiveEmptyReads > 0
                    && consecutiveEmptyReads % maxConsecutiveEmptyReads == 0
                {
                    // If we've had too many empty reads, consider it a timeout
                    if consecutiveEmptyReads > maxConsecutiveEmptyReads * 10 {
                        log.warning(
                            "Network read timed out after \(consecutiveEmptyReads) consecutive empty reads"
                        )
                        throw StdioProxyError.networkTimeout
                    }
                }

                // Receive data from the network connection
                let data = try await withCheckedThrowingContinuation {
                    (continuation: CheckedContinuation<Data, Swift.Error>) in
                    connection.receive(minimumIncompleteLength: 1, maximumLength: bufferSize) {
                        data,
                        _,
                        isComplete,
                        error in
                        if let error = error {
                            continuation.resume(throwing: error)
                            return
                        }

                        if let data = data {
                            continuation.resume(returning: data)
                        } else if isComplete {
                            log.debug("Network connection complete")
                            continuation.resume(throwing: StdioProxyError.connectionClosed)
                        } else {
                            continuation.resume(returning: Data())
                        }
                    }
                }

                var processedData = data

                // Check for and filter out heartbeat messages using MCP.NetworkTransport.Heartbeat
                // Assuming MCP module and NetworkTransport.Heartbeat are available
                if NetworkTransport.Heartbeat.isHeartbeat(processedData) {
                    log.debug(
                        "Heartbeat signature detected in received network data using MCP definition."
                    )

                    // Try to parse a full heartbeat. MCP.NetworkTransport.Heartbeat.from(data:) checks for minimum length internally.
                    if let heartbeat = NetworkTransport.Heartbeat.from(data: processedData) {
                        let heartbeatLength = heartbeat.rawValue.count  // This should typically be 12
                        log.debug(
                            "Full MCP heartbeat message (\(heartbeatLength) bytes) received from network, skipping output."
                        )
                        // Remove the full heartbeat from the data
                        processedData = processedData.dropFirst(heartbeatLength)
                    } else {
                        // MCP.NetworkTransport.Heartbeat.isHeartbeat was true, but .from(data:) failed.
                        // This means we have the magic bytes but not the full message (e.g., data length < 12 but >= 4).
                        let expectedHeartbeatLength = MCP.NetworkTransport.Heartbeat().rawValue
                            .count  // Get expected length (12)
                        log.debug(
                            "Partial MCP heartbeat message (<\(expectedHeartbeatLength) bytes) received, discarding this chunk to prevent garbled output."
                        )
                        processedData = Data()  // Discard the chunk
                    }
                }

                if processedData.isEmpty {
                    // No data available (or entire chunk was a heartbeat), yield to other tasks
                    // If original data was not empty, but processedData is, it means it was a heartbeat.
                    if !data.isEmpty {  // Original data was not empty, so this was a heartbeat
                        consecutiveEmptyReads = 0  // Reset counter as we did receive something (a heartbeat)
                    } else {
                        consecutiveEmptyReads += 1
                    }
                    try await Task.sleep(for: .milliseconds(10))
                    continue
                } else {
                    // Reset counter when we get actual data (not just a heartbeat)
                    consecutiveEmptyReads = 0
                    log.debug(
                        "Received \(processedData.count) bytes of application data from network"
                    )
                }

                // Add data to buffer for message assembly
                networkToStdoutBuffer.append(processedData)

                // Process complete messages (delimited by newlines)
                while let newlineIndex = networkToStdoutBuffer.firstIndex(of: UInt8(ascii: "\n")) {
                    let messageData = networkToStdoutBuffer[..<newlineIndex]
                    var messageWithNewline = Data(messageData)
                    messageWithNewline.append(UInt8(ascii: "\n"))

                    // Remove processed message from buffer
                    networkToStdoutBuffer = networkToStdoutBuffer[(newlineIndex + 1)...]

                    // Write complete message to stdout
                    var remainingDataToWrite = messageWithNewline
                    while !remainingDataToWrite.isEmpty {
                        let bytesWritten: Int = try remainingDataToWrite.withUnsafeBytes { buffer in
                            try stdout.write(UnsafeRawBufferPointer(buffer))
                        }

                        if bytesWritten < remainingDataToWrite.count {
                            log.debug(
                                "Partial write: \(bytesWritten) of \(remainingDataToWrite.count) bytes"
                            )
                            // Remove the bytes that were written
                            remainingDataToWrite = remainingDataToWrite.dropFirst(bytesWritten)
                        } else {
                            // All bytes were written
                            remainingDataToWrite.removeAll()
                        }

                        // If we still have data to write, give a small delay to allow the system to process
                        if !remainingDataToWrite.isEmpty {
                            try await Task.sleep(for: .milliseconds(1))
                        }
                    }
                }
            } catch let error as NWError where error.errorCode == 96 {
                // Handle "No message available on STREAM" error
                log.debug("Network read yielded no data, waiting...")
                consecutiveEmptyReads += 1
                try await Task.sleep(for: .milliseconds(100))
            } catch {
                // Check if the connection was cancelled or closed
                if let nwError = error as? NWError,
                    nwError.errorCode == 57  // Socket is not connected
                        || nwError.errorCode == 54  // Connection reset by peer
                {
                    log.debug("Connection closed by peer: \(error)")
                    throw StdioProxyError.connectionClosed
                }

                if error is StdioProxyError {
                    throw error
                }

                log.error("Error in network handler: \(error)")
                throw error
            }
        }
    }
}

// Define custom errors for the StdioProxy
enum StdioProxyError: Swift.Error {
    case networkTimeout
    case connectionClosed
}

enum MCPServiceTermination: Swift.Error {
    case clientDisconnected
}

// Create MCPService class to manage lifecycle
actor MCPService: Service {
    private var browser: NWBrowser?
    private var currentProxy: StdioProxy?

    func run() async throws {
        while true {
            do {
                log.info("Starting Bonjour service discovery for \(serviceType) on \(serviceDomain)")

                let browser = NWBrowser(
                    for: .bonjour(type: serviceType, domain: serviceDomain),
                    using: parameters
                )
                self.browser = browser

                // Find and connect to iMCP app with improved reliability
                let endpoint: NWEndpoint = try await withCheckedThrowingContinuation {
                    continuation in
                    let connectionState = ConnectionState()

                    // Set up a timeout task to ensure we don't wait forever
                    let timeoutTask = Task {
                        // Allow 30 seconds to find the service
                        try await Task.sleep(for: .seconds(30))

                        // If we haven't found a service by now, resume with an error
                        if await connectionState.checkAndSetResumed() {
                            log.error("Bonjour service discovery timed out after 30 seconds")
                            continuation.resume(
                                throwing: MCPError.internalError("Service discovery timeout")
                            )
                        }
                    }

                    // Convert async handlers to sync handlers
                    browser.stateUpdateHandler = { state in
                        Task {
                            switch state {
                            case .failed(let error):
                                log.error("Browser failed: \(error)")
                                if await connectionState.checkAndSetResumed() {
                                    timeoutTask.cancel()
                                    browser.cancel()
                                    continuation.resume(throwing: error)
                                }
                            case .ready:
                                log.info("Browser is ready and searching for services")
                            case .waiting(let error):
                                log.warning("Browser is waiting: \(error)")
                            default:
                                log.debug("Browser state changed: \(state)")
                            }
                        }
                    }

                    browser.browseResultsChangedHandler = { results, changes in
                        Task {
                            log.debug(
                                "Found \(results.count) Bonjour services (changes: \(changes.count))"
                            )

                            // Log all discovered services for debugging
                            for (index, result) in results.enumerated() {
                                log.debug("Service \(index + 1): \(result.endpoint)")
                            }

                            let localMCPServices = results.filter {
                                localMCPServiceName(from: $0.endpoint) != nil
                            }

                            if localMCPServices.isEmpty {
                                return
                            }

                            let selectedService =
                                localMCPServices.first(where: { isLikelyIMCPService($0.endpoint) })
                                ?? (localMCPServices.count == 1 ? localMCPServices.first : nil)

                            guard let selectedService else {
                                log.warning(
                                    "Found \(localMCPServices.count) local MCP services, but none matched iMCP by name. Waiting for iMCP service."
                                )
                                return
                            }

                            if let serviceName = localMCPServiceName(from: selectedService.endpoint) {
                                log.info(
                                    "Selected Bonjour service '\(serviceName)' at \(selectedService.endpoint)"
                                )
                            } else {
                                log.info("Selected endpoint: \(selectedService.endpoint)")
                            }

                            if await connectionState.checkAndSetResumed() {
                                timeoutTask.cancel()
                                browser.cancel()
                                continuation.resume(returning: selectedService.endpoint)
                            }
                        }
                    }

                    Task {
                        log.info(
                            "Starting Bonjour browser to discover MCP services on \(serviceDomain)..."
                        )
                    }
                    browser.start(queue: .main)
                }

                log.info("Creating connection to endpoint...")

                // Create the proxy
                let proxy = StdioProxy(
                    endpoint: endpoint,
                    parameters: parameters,
                    stdinBufferSize: 10 * 1024 * 1024,  // 10MB for large responses
                    networkBufferSize: 10 * 1024 * 1024  // 10MB for large responses
                )
                self.currentProxy = proxy

                do {
                    try await proxy.start()
                } catch let error as StdioProxyError {
                    switch error {
                    // Removed stdinTimeout case as it's no longer thrown
                    // case .stdinTimeout:
                    //     log.info("Stdin timed out, will reconnect...")
                    //     try await Task.sleep(for: .seconds(1))
                    //     continue
                    case .networkTimeout:
                        log.info("Network timed out, will reconnect...")
                        try await Task.sleep(for: .seconds(1))
                        continue
                    case .connectionClosed:
                        log.info("Connection closed by client or app. Exiting...")
                        throw MCPServiceTermination.clientDisconnected
                    }
                } catch let error as NWError where error.errorCode == 54 || error.errorCode == 57 {
                    // Handle connection reset by peer (54) or socket not connected (57)
                    log.info("Network connection terminated (\(error)). Exiting...")
                    throw MCPServiceTermination.clientDisconnected
                } catch {
                    // Rethrow other errors to be handled by the outer catch block
                    throw error
                }
            } catch MCPServiceTermination.clientDisconnected {
                throw MCPServiceTermination.clientDisconnected
            } catch {
                // Handle all other errors with retry
                log.error("Connection error: \(error)")
                log.info("Will retry connection in 5 seconds...")
                try await Task.sleep(for: .seconds(5))
            }
        }
    }

    func shutdown() async throws {
        browser?.cancel()
        if let proxy = currentProxy {
            await proxy.stop()
        }
    }
}

// Update the ServiceLifecycle initialization
let lifecycle = ServiceGroup(
    configuration: .init(
        services: [MCPService()],
        logger: log
    )
)

do {
    try await lifecycle.run()
} catch MCPServiceTermination.clientDisconnected {
    log.info("Client disconnected, shutting down iMCP server process")
}
