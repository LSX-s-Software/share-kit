import Foundation
import NIOTransportServices
import WebSocketKit
import Atomics
import OSLog

public enum ShareClientError: Error, LocalizedError {
    case alreadyShutdown
    
    public var errorDescription: String? {
        switch self {
        case .alreadyShutdown: "Client already shutdown."
        }
    }
}

public final class ShareClient {
    public enum EventLoopGroupProvider {
        case shared(EventLoopGroup)
        case createNew
    }

    public struct Configuration {
        public var reconnect: Bool

        public init(reconnect: Bool = true) {
            self.reconnect = reconnect
        }
    }

    private static let logger = Logger(subsystem: "ShareKit", category: "ShareClient")
    private let eventLoopGroupProvider: EventLoopGroupProvider
    private let eventLoopGroup: EventLoopGroup
    private let configuration: Configuration
    private let isShutdown = ManagedAtomic<Bool>(false)

    public init(eventLoopGroupProvider: EventLoopGroupProvider, configuration: Configuration = .init()) {
        self.eventLoopGroupProvider = eventLoopGroupProvider
        switch self.eventLoopGroupProvider {
        case .shared(let group):
            self.eventLoopGroup = group
        case .createNew:
            self.eventLoopGroup = NIOTSEventLoopGroup(loopCount: 2)
        }
        self.configuration = configuration
    }

    public func connect(
        _ url: String,
        connection: ShareConnection? = nil,
        onConnect: @escaping (ShareConnection) -> Void
    ) {
        let eventLoop = connection?.eventLoop ?? eventLoopGroup.next()
        let wsFuture = WebSocket.connect(to: url, on: eventLoopGroup) { socket in
            if let existingConnection = connection {
                existingConnection.webSocket = socket
            } else {
                let connection = ShareConnection(socket: socket, on: eventLoop)
                socket.onClose.whenComplete { _ in
                    Self.logger.info("WebSocket closed.")
                    guard self.configuration.reconnect else {
                        return
                    }
                    Self.logger.info("Reconnecting...")
                    eventLoop.execute {
                        connection.disconnect()
                        self.connect(url, connection: connection, onConnect: onConnect)
                    }
                }
                onConnect(connection)
            }
        }
        wsFuture.whenFailure { error in
            Self.logger.warning("WebSocket connect failed: \(error)")
            guard self.configuration.reconnect else {
                return
            }
            Self.logger.info("Retrying in 1 second...")
            eventLoop.scheduleTask(in: .seconds(1)) {
                connection?.disconnect()
                self.connect(url, connection: connection, onConnect: onConnect)
            }
        }
    }

    public func syncShutdown() throws {
        switch self.eventLoopGroupProvider {
        case .shared:
            return
        case .createNew:
            if self.isShutdown.compareExchange(expected: false, desired: true, ordering: .sequentiallyConsistent).exchanged {
                try self.eventLoopGroup.syncShutdownGracefully()
            } else {
                throw ShareClientError.alreadyShutdown
            }
        }
    }

    deinit {
        switch self.eventLoopGroupProvider {
        case .shared:
            return
        case .createNew:
            assert(self.isShutdown.load(ordering: .relaxed), "ShareClient not shutdown before deinit.")
        }
    }
}
