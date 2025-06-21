import Foundation
import Network
import Logging
import MCPError

// MARK: - StreamableHTTPServerTransport Implementation

/// A custom implementation of MCP's Streamable HTTP transport for server-side use.
/// This transport implements the 2025-03-26 MCP specification for Streamable HTTP,
/// designed to be stateless and compatible with clients like langchain-mcp-adapters.
///
/// Key features:
/// - Stateless request handling (no session persistence)
/// - Proper CORS support
/// - Origin validation
/// - SSE streaming for long-running operations
/// - Compliance with MCP 2025-03-26 specification
actor StreamableHTTPServerTransport: Transport {
    public nonisolated let logger: Logger
    
    private let port: UInt16
    private let host: String
    private var listener: NWListener?
    private var isRunning = false
    private var messageHandler: ((Data) async throws -> Data)?
    
    // Configuration
    private let allowedOrigins: Set<String>
    private let maxRequestSize: Int
    
    public init(
        port: UInt16 = 8080,
        host: String = "0.0.0.0",
        allowedOrigins: Set<String> = ["*"],
        maxRequestSize: Int = 1024 * 1024, // 1MB
        logger: Logger? = nil
    ) {
        self.port = port
        self.host = host
        self.allowedOrigins = allowedOrigins
        self.maxRequestSize = maxRequestSize
        self.logger = logger ?? Logger(label: "mcp.transport.streamable-http")
    }
    
    // Set the message handler - called by the MCP Server
    func setMessageHandler(_ handler: @escaping (Data) async throws -> Data) {
        self.messageHandler = handler
    }
    
    public func connect() async throws {
        guard !isRunning else {
            logger.warning("Transport is already running")
            return
        }
        
        let parameters = NWParameters.tcp
        parameters.allowLocalEndpointReuse = true
        
        listener = try NWListener(using: parameters, on: NWEndpoint.Port(integerLiteral: port))
        
        listener?.newConnectionHandler = { [weak self] connection in
            Task {
                await self?.handleConnection(connection)
            }
        }
        
        listener?.start(queue: .global(qos: .userInitiated))
        isRunning = true
        
        logger.info("StreamableHTTP server listening on \(host):\(port)")
    }
    
    public func disconnect() async {
        guard isRunning else { return }
        
        listener?.cancel()
        listener = nil
        isRunning = false
        
        logger.info("StreamableHTTP server stopped")
    }
    
    public func send(_ data: Data) async throws {
        // In the stateless model, sending is handled per-request
        // This method is not used in the server transport pattern
        throw MCPError.internalError("Send not supported in server transport")
    }
    
    public func receive() -> AsyncThrowingStream<Data, any Swift.Error> {
        // Server transport doesn't use a continuous receive stream.
        // Each request is handled independently, so we immediately finish with an error.
        return AsyncThrowingStream<Data, any Swift.Error> { continuation in
            continuation.finish(throwing: MCPError.internalError("Receive not supported in server transport"))
        }
    }
}

// MARK: - HTTP Request Handling

extension StreamableHTTPServerTransport {
    
    private func handleConnection(_ connection: NWConnection) async {
        connection.start(queue: .global(qos: .userInitiated))
        
        // Wait for connection to be ready
        for await state in await connection.stateUpdateStream {
        switch state {
        case .ready:
                await processConnection(connection)
                return
        case .failed(let error):
                logger.error("Connection failed: \(error)")
                return
        case .cancelled:
                return
        default:
                continue
            }
        }
    }
    
    private func processConnection(_ connection: NWConnection) async {
        do {
            // Read HTTP request
            let requestData = try await readHTTPRequest(from: connection)
            let request = try parseHTTPRequest(requestData)
            
            // Handle the request based on method and path
            let response = await handleHTTPRequest(request)
            
            // Send response
            try await sendHTTPResponse(response, to: connection)
            
        } catch {
            logger.error("Error processing connection: \(error)")
            
            // Send error response
            let errorResponse = HTTPResponse(
                statusCode: 500,
                headers: ["Content-Type": "text/plain"],
                body: "Internal Server Error".data(using: .utf8) ?? Data()
            )
            
            try? await sendHTTPResponse(errorResponse, to: connection)
        }
        
        connection.cancel()
    }
    
    private func readHTTPRequest(from connection: NWConnection) async throws -> Data {
        return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Data, Swift.Error>) in
            var requestData = Data()
            var headersComplete = false
        var contentLength = 0
            
            func readMoreData() {
                connection.receive(minimumIncompleteLength: 1, maximumLength: 8192) { data, _, isComplete, error in
                    if let error = error {
                        continuation.resume(throwing: error)
            return
        }

                    if let data = data {
                        requestData.append(data)
                        
                        if !headersComplete {
                            // Look for end of headers
                            if let headersEnd = requestData.range(of: "\r\n\r\n".data(using: .utf8)!) {
                                headersComplete = true
                                let headersData = requestData[..<headersEnd.upperBound]
                                
                                // Parse Content-Length
                                if let headersString = String(data: headersData, encoding: .utf8) {
                                    contentLength = self.extractContentLength(from: headersString)
                                }
                                
                                // Check if we have the complete body
                                let bodyStart = headersEnd.upperBound
                                let currentBodyLength = requestData.count - bodyStart
                                
                                if currentBodyLength >= contentLength {
                                    continuation.resume(returning: requestData)
                return
            }
        }
                        } else {
                            // Headers complete, check if body is complete
                            let headerEndRange = requestData.range(of: "\r\n\r\n".data(using: .utf8)!)!
                            let bodyStart = headerEndRange.upperBound
                            let currentBodyLength = requestData.count - bodyStart
        
                            if currentBodyLength >= contentLength {
                                continuation.resume(returning: requestData)
            return
                            }
                        }
        }
        
                    if isComplete && !headersComplete {
                        continuation.resume(returning: requestData)
            return
        }
        
                    if !isComplete {
                        readMoreData()
                    }
                }
            }
            
            readMoreData()
        }
    }
    
    nonisolated private func extractContentLength(from headers: String) -> Int {
        let lines = headers.components(separatedBy: "\r\n")
        for line in lines {
            if line.lowercased().hasPrefix("content-length:") {
                let parts = line.components(separatedBy: ":")
                if parts.count >= 2 {
                    let lengthString = parts[1].trimmingCharacters(in: .whitespaces)
                    return Int(lengthString) ?? 0
                }
            }
        }
        return 0
    }
    
    private func parseHTTPRequest(_ data: Data) throws -> HTTPRequest {
        guard let requestString = String(data: data, encoding: .utf8) else {
            throw MCPError.invalidRequest("Invalid UTF-8 encoding")
        }
        
        let components = requestString.components(separatedBy: "\r\n\r\n")
        guard components.count >= 2 else {
            throw MCPError.invalidRequest("Invalid HTTP request format")
        }
        
        let headerSection = components[0]
        let bodyData = String(components[1...].joined(separator: "\r\n\r\n")).data(using: .utf8) ?? Data()
        
        let headerLines = headerSection.components(separatedBy: "\r\n")
        guard let firstLine = headerLines.first else {
            throw MCPError.invalidRequest("Missing request line")
        }
        
        let requestLineParts = firstLine.components(separatedBy: " ")
        guard requestLineParts.count >= 3 else {
            throw MCPError.invalidRequest("Invalid request line")
        }
        
        let method = requestLineParts[0]
        let path = requestLineParts[1]
        
        // Parse headers
        var headers: [String: String] = [:]
        for line in headerLines.dropFirst() {
            let parts = line.components(separatedBy: ":")
            if parts.count >= 2 {
                let key = parts[0].trimmingCharacters(in: .whitespaces)
                let value = parts[1...].joined(separator: ":").trimmingCharacters(in: .whitespaces)
                headers[key.lowercased()] = value
            }
        }
        
        return HTTPRequest(method: method, path: path, headers: headers, body: bodyData)
    }
    
    private func handleHTTPRequest(_ request: HTTPRequest) async -> HTTPResponse {
        // Handle CORS preflight
        if request.method == "OPTIONS" {
            return handleCORSPreflight(request)
        }
        
        // Validate origin
        if !isOriginAllowed(request.headers["origin"]) {
            return HTTPResponse(
                statusCode: 403,
                headers: ["Content-Type": "text/plain"],
                body: "Forbidden: Invalid origin".data(using: .utf8) ?? Data()
            )
        }
        
        // Route based on path and method
        switch (request.method, request.path) {
        case ("GET", _):
            return await handleGETRequest(request)
        case ("POST", _):
            return await handlePOSTRequest(request)
        default:
            return HTTPResponse(
                statusCode: 405,
                headers: [
                    "Allow": "GET, POST, OPTIONS",
                    "Content-Type": "text/plain"
                ],
                body: "Method Not Allowed".data(using: .utf8) ?? Data()
            )
        }
    }
    
    private func handleCORSPreflight(_ request: HTTPRequest) -> HTTPResponse {
        var headers: [String: String] = [:]
        
        if let origin = request.headers["origin"], isOriginAllowed(origin) {
            headers["Access-Control-Allow-Origin"] = origin
        } else if allowedOrigins.contains("*") {
            headers["Access-Control-Allow-Origin"] = "*"
        }
        
        headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        headers["Access-Control-Allow-Headers"] = "Content-Type, Accept, Mcp-Version, Authorization"
        headers["Access-Control-Max-Age"] = "86400"
        
        return HTTPResponse(statusCode: 204, headers: headers, body: Data())
    }
    
    private func handleGETRequest(_ request: HTTPRequest) async -> HTTPResponse {
        // Check Accept header for SSE support
        let acceptHeader = request.headers["accept"] ?? ""
        let supportsSSE = acceptHeader.contains("text/event-stream")
        
        if supportsSSE {
            // Return SSE stream for long-running operations
            return createSSEResponse()
        } else {
            // Return basic server info
            let info = [
                "name": "MCP StreamableHTTP Server",
                "version": "1.0.0",
                "transport": "streamable-http",
                "specification": "2025-03-26"
            ]
            
            guard let jsonData = try? JSONSerialization.data(withJSONObject: info) else {
                return HTTPResponse(
                    statusCode: 500,
                    headers: ["Content-Type": "text/plain"],
                    body: "Internal Server Error".data(using: .utf8) ?? Data()
                )
            }
            
            var headers = ["Content-Type": "application/json"]
            addCORSHeaders(&headers, origin: request.headers["origin"])
            
            return HTTPResponse(statusCode: 200, headers: headers, body: jsonData)
        }
    }
    
    private func handlePOSTRequest(_ request: HTTPRequest) async -> HTTPResponse {
        // Validate Content-Type
        let contentType = request.headers["content-type"] ?? ""
        guard contentType.contains("application/json") else {
            return HTTPResponse(
                statusCode: 400,
                headers: ["Content-Type": "text/plain"],
                body: "Bad Request: Content-Type must be application/json".data(using: .utf8) ?? Data()
            )
        }
        
        // Validate request size
        guard request.body.count <= maxRequestSize else {
            return HTTPResponse(
                statusCode: 413,
                headers: ["Content-Type": "text/plain"],
                body: "Request Entity Too Large".data(using: .utf8) ?? Data()
            )
        }
        
        guard let handler = messageHandler else {
            return HTTPResponse(
                statusCode: 500,
                headers: ["Content-Type": "text/plain"],
                body: "Server not properly configured".data(using: .utf8) ?? Data()
            )
        }
        
        do {
            // Process MCP message
            let responseData = try await handler(request.body)
            
            var headers = ["Content-Type": "application/json"]
            addCORSHeaders(&headers, origin: request.headers["origin"])
            
            return HTTPResponse(statusCode: 200, headers: headers, body: responseData)
            
        } catch {
            logger.error("Error processing MCP message: \(error)")
            
            let errorResponse = [
                "error": [
                    "code": -32603,
                    "message": "Internal error",
                    "data": error.localizedDescription
                ]
            ]
            
            guard let errorData = try? JSONSerialization.data(withJSONObject: errorResponse) else {
                return HTTPResponse(
                    statusCode: 500,
                    headers: ["Content-Type": "text/plain"],
                    body: "Internal Server Error".data(using: .utf8) ?? Data()
                )
            }
            
            var headers = ["Content-Type": "application/json"]
            addCORSHeaders(&headers, origin: request.headers["origin"])
            
            return HTTPResponse(statusCode: 500, headers: headers, body: errorData)
        }
    }
    
    private func createSSEResponse() -> HTTPResponse {
        // For now, return a simple SSE response
        // In a full implementation, this would handle streaming responses
        let sseData = "data: {\"type\":\"connected\"}\n\n".data(using: .utf8) ?? Data()
        
        let headers = [
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": allowedOrigins.contains("*") ? "*" : allowedOrigins.first ?? ""
        ]
        
        return HTTPResponse(statusCode: 200, headers: headers, body: sseData)
    }
    
    private func isOriginAllowed(_ origin: String?) -> Bool {
        guard let origin = origin else { return true } // Allow requests without origin header
        return allowedOrigins.contains("*") || allowedOrigins.contains(origin)
    }
    
    private func addCORSHeaders(_ headers: inout [String: String], origin: String?) {
        if let origin = origin, isOriginAllowed(origin) {
            headers["Access-Control-Allow-Origin"] = origin
        } else if allowedOrigins.contains("*") {
            headers["Access-Control-Allow-Origin"] = "*"
        }
        
        headers["Access-Control-Allow-Credentials"] = "true"
        headers["Vary"] = "Origin"
    }
    
    private func sendHTTPResponse(_ response: HTTPResponse, to connection: NWConnection) async throws {
        let statusLine = "HTTP/1.1 \(response.statusCode) \(httpStatusText(response.statusCode))\r\n"
        var responseString = statusLine
        
        // Add headers
        for (key, value) in response.headers {
            responseString += "\(key): \(value)\r\n"
        }
        
        // Add Content-Length if not present
        if response.headers["content-length"] == nil {
            responseString += "Content-Length: \(response.body.count)\r\n"
        }
        
        responseString += "\r\n"
        
        // Combine headers and body
        var responseData = responseString.data(using: .utf8) ?? Data()
        responseData.append(response.body)
        
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Swift.Error>) in
            connection.send(content: responseData, completion: .contentProcessed { error in
                if let error = error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            })
        }
    }
    
    private func httpStatusText(_ code: Int) -> String {
        switch code {
        case 200: return "OK"
        case 204: return "No Content"
        case 400: return "Bad Request"
        case 403: return "Forbidden"
        case 404: return "Not Found"
        case 405: return "Method Not Allowed"
        case 413: return "Request Entity Too Large"
        case 500: return "Internal Server Error"
        default: return "Unknown"
        }
    }
}

// MARK: - Supporting Types

struct HTTPRequest {
    let method: String
    let path: String
    let headers: [String: String]
    let body: Data
}

struct HTTPResponse {
    let statusCode: Int
    let headers: [String: String]
    let body: Data
}

// MARK: - NWConnection Extensions

extension NWConnection {
    var stateUpdateStream: AsyncStream<NWConnection.State> {
        AsyncStream { continuation in
            self.stateUpdateHandler = { state in
                continuation.yield(state)
                if case .cancelled = state {
                    continuation.finish()
                }
            }
        }
    }
}