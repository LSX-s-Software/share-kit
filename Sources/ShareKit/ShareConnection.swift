import Foundation
import WebSocketKit
import OSLog
import Atomics

final public class ShareConnection {
    private static let logger = Logger(subsystem: "ShareKit", category: "ShareConnection")

    enum ShareConnectionError: Swift.Error, LocalizedError {
        case encodeMessage
        case unsupportedType
        case documentEntityType
        case unkownQueryID
        case unknownDocument
        public var errorDescription: String? {
            return "\(self)"
        }
    }

    public private(set) var clientID: String?

    let eventLoop: EventLoop
    var webSocket: WebSocket {
        didSet {
            initiateSocket()
        }
    }

    private(set) var defaultTransformer: OperationalTransformer.Type
    private var documentStore = AsyncDict<DocumentID, OperationalTransformDocument>()
    private var opSequence = ManagedAtomic<UInt>(1)

    private var queryCollectionStore = [UInt: OperationalTransformQuery]()
    private var querySequence = ManagedAtomic<UInt>(1)

    init(socket: WebSocket, on eventLoop: EventLoop) {
        self.webSocket = socket
        self.eventLoop = eventLoop
        self.defaultTransformer = JSON0Transformer.self
        initiateSocket()
    }

    public func create<Entity>(_ data: Entity, in collection: String) async throws -> ShareDocument<Entity> where Entity: Codable {
        let key = UUID().uuidString
        let document: ShareDocument<Entity> = try getDocument(key, in: collection)
        try await document.create(data)
        return document
    }

    public func getDocument<Entity>(_ key: String, in collection: String) throws -> ShareDocument<Entity> where Entity: Codable {
        let documentID = DocumentID(key, in: collection)
        let document: ShareDocument<Entity>
        if documentStore.get(key: documentID) != nil {
            guard let storedDocument = documentStore.get(key: documentID) as? ShareDocument<Entity> else {
                throw ShareConnectionError.documentEntityType
            }
            document = storedDocument
        } else {
            document = ShareDocument<Entity>(documentID, connection: self)
        }
        documentStore.set(key: documentID, value: document)
        return document
    }

    public func subscribe<Entity>(document: String, in collection: String) async throws -> ShareDocument<Entity> where Entity: Codable {
        let document: ShareDocument<Entity> = try getDocument(document, in: collection)
        try await document.subscribe()
        return document
    }

    public func subscribe<Entity>(query: AnyCodable, in collection: String) throws -> ShareQueryCollection<Entity> where Entity: Codable {
        let collection: ShareQueryCollection<Entity> = ShareQueryCollection(query, in: collection, connection: self)
        let sequence = querySequence.loadThenWrappingIncrement(ordering: .sequentiallyConsistent)
        collection.subscribe(sequence)
        queryCollectionStore[sequence] = collection
        return collection
    }

    func disconnect() {
        documentStore.forEach { _, document in
            Task {
                try? await document.pause()
            }
        }
    }

    func send<Message>(message: Message) -> EventLoopFuture<Void> where Message: Encodable {
        let promise = eventLoop.makePromise(of: Void.self)
        eventLoop.execute {
            let sendMessage: Message
            if var operationMessage = message as? OperationMessage {
                operationMessage.sequence = self.opSequence.loadThenWrappingIncrement(ordering: .sequentiallyConsistent)
                sendMessage = operationMessage as! Message
            } else {
                sendMessage = message
            }
            guard let data = try? JSONEncoder().encode(sendMessage),
                  let messageString = String(data: data, encoding: .utf8) else {
                promise.fail(ShareConnectionError.encodeMessage)
                return
            }
            ShareConnection.logger.debug("Sent: \(messageString)")
            self.webSocket.send(messageString, promise: promise)
        }
        return promise.futureResult
    }
}

private extension ShareConnection {
    func initiateSocket() {
        webSocket.onText(handleSocketText)
        let message = HandshakeMessage(clientID: self.clientID)
        send(message: message).whenFailure { _ in
            let _ = self.webSocket.close()
        }
    }

    func handleSocketText(_ socket: WebSocket, _ text: String) async {
        guard let data = text.data(using: .utf8),
              let message = try? JSONDecoder().decode(GenericMessage.self, from: data) else {
            ShareConnection.logger.warning("Socket received invalid message: \(text)")
            return
        }
        ShareConnection.logger.debug("Received \(text)")
        if let error = message.error {
            ShareConnection.logger.warning("Socket received error: \(error)")
            await handleErrorMessage(message, data: data)
            return
        }
        do {
            try await handleMessage(message.action, data: data)
        } catch {
            ShareConnection.logger.warning("Message handling error: \(error)")
        }
    }

    func handleMessage(_ action: MessageAction, data: Data) async throws {
        switch action {
        case .handshake:
            try handleHandshakeMessage(data)
        case .subscribe:
            try await handleSubscribeMessage(data)
        case .querySubscribe:
            try await handleQuerySubscribeMessage(data)
        case .query:
            try await handleQueryMessage(data)
        case .operation:
            try await handleOperationMessage(data)
        }
    }

    func handleHandshakeMessage(_ data: Data) throws {
        let message = try JSONDecoder().decode(HandshakeMessage.self, from: data)
        clientID = message.clientID
        if let defaultType = message.type {
            guard let transformer = OperationalTransformTypes[defaultType] else {
                throw ShareConnectionError.unsupportedType
            }
            self.defaultTransformer = transformer
        }
    }

    func handleSubscribeMessage(_ data: Data) async throws {
        let message = try JSONDecoder().decode(SubscribeMessage.self, from: data)
        let documentID = DocumentID(message.document, in: message.collection)
        guard let document = documentStore.get(key: documentID) else {
            throw ShareConnectionError.unknownDocument
        }
        if let versionedData = message.data {
            if versionedData.type == nil && versionedData.data == nil {
                try await document.setNotCreated()
            } else {
                try await document.put(versionedData.data, version: versionedData.version, type: message.type)
            }
        } else {
            // TODO ack empty subscribe resp
//            try document.ack()
        }
    }

    func handleQuerySubscribeMessage(_ data: Data) async throws {
        let message = try JSONDecoder().decode(QuerySubscribeMessage.self, from: data)
        guard let collection = queryCollectionStore[message.queryID] else {
            throw ShareConnectionError.unkownQueryID
        }
        if let versionedData = message.data {
            try await collection.put(versionedData)
        } else {
            // TODO ack empty subscribe resp
//            try document.ack()
        }
    }

    func handleQueryMessage(_ data: Data) async throws {
        let message = try JSONDecoder().decode(QueryMessage.self, from: data)
        guard let collection = queryCollectionStore[message.queryID] else {
            throw ShareConnectionError.unkownQueryID
        }
        try await collection.sync(message.diff)
    }

    func handleOperationMessage(_ data: Data) async throws {
        let message = try JSONDecoder().decode(OperationMessage.self, from: data)
        let documentID = DocumentID(message.document, in: message.collection)
        guard let document = documentStore.get(key: documentID) else {
            return
        }
        if message.source == clientID {
            try await document.ack(version: message.version, sequence: message.sequence)
        } else {
            guard let operationData = message.data else {
                throw OperationalTransformError.missingOperationData
            }
            try await document.sync(operationData, version: message.version)
        }
    }

    func handleErrorMessage(_ message: GenericMessage, data: Data) async {
        guard let error = message.error, let code = ShareDBError(rawValue: error.code) else {
            ShareConnection.logger.warning("Unknown error message: \(message.error?.message ?? "nil")")
            return
        }
        // TODO: rollback if action = op
        do {
            let message = try JSONDecoder().decode(OperationMessage.self, from: data)
            let documentID = DocumentID(message.document, in: message.collection)
            guard let document = documentStore.get(key: documentID) else {
                throw ShareConnectionError.unknownDocument
            }
            switch code {
            case .docAlreadyCreated, .docWasDeleted:
                if code == .docAlreadyCreated {
                    try await document.resume()
                } else {
                    try await document.sync(.delete(isDeleted: true), version: message.version)
                }
            case .docTypeNotRecognized:
                guard case let .create(type, _) = message.data else { break }
                ShareConnection.logger.warning("Document type \"\(type.rawValue)\" not recognized. Please register this type on the server.")
                try await document.sync(.delete(isDeleted: true), version: message.version)
            default:
                ShareConnection.logger.error("Unhandled error: \(error)")
            }
        } catch {
            ShareConnection.logger.warning("Error handling error: \(error)")
        }
    }
}
