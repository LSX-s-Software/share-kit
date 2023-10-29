import Foundation
import NIOCore
import Combine
import OSLog

public struct DocumentID: Hashable {
    let key: String
    let collection: String

    public init(_ key: String, in collection: String) {
        self.key = key
        self.collection = collection
    }
}

public actor ShareDocument<Entity>: Identifiable where Entity: Codable {
    private let logger = Logger(subsystem: "ShareKit", category: "ShareDocument")

    enum ShareDocumentError: Error {
        case transformType
        case documentState
        case stateEvent
        case decodeDocumentData
        case operationalTransformType
        case applyTransform
        case operationVersion
        case operationAck
        case alreadySubscribed
    }

    public let id: DocumentID

    public private(set) var value: CurrentValueSubject<Entity?, Never>
    public private(set) var version: UInt?
    private var data: AnyCodable?

    private(set) var state: State
    public var notCreated: Bool {
        return state == .notCreated
    }

    var documentTransformer: OperationalTransformer.Type?
    var transformer: OperationalTransformer.Type {
        return documentTransformer ?? connection.defaultTransformer
    }

    var inflightOperation: OperationData?
    var queuedOperations = CircularBuffer<OperationData>()

    let connection: ShareConnection

    init(_ documentID: DocumentID, connection: ShareConnection) {
        self.id = documentID
        self.connection = connection
        self.state = .blank
        self.value = CurrentValueSubject(nil)
    }

    func setInflightOperation(_ operation: OperationData?) {
        inflightOperation = operation
    }

    func appendOperation(_ operation: OperationData) {
        queuedOperations.append(operation)
    }

    func prependOperation(_ operation: OperationData) {
        queuedOperations.prepend(operation)
    }

    public func create(_ entity: Entity, type: OperationalTransformType? = nil) async throws {
        if !notCreated {
            throw ShareDocumentError.documentState
        }
        let jsonData = try JSONEncoder().encode(entity)
        let json = try AnyCodable(data: jsonData)
        try await put(json, version: 0, type: type)
        try await send(.create(type: type ?? connection.defaultTransformer.type, data: json))
    }

    public func delete() async throws {
        try trigger(event: .delete)
        try await self.send(.delete(isDeleted: true))
    }

    public func subscribe() async throws {
        return try await withCheckedThrowingContinuation { continuation in
            guard state == .blank else {
                continuation.resume(throwing: ShareDocumentError.alreadySubscribed)
                return
            }
            let msg = SubscribeMessage(collection: id.collection, document: id.key, version: version)
            connection.send(message: msg).whenComplete { result in
                Task {
                    do {
                        switch result {
                        case .success:
                            try await self.trigger(event: .fetch)
                            continuation.resume()
                        case .failure(let error):
                            try await self.trigger(event: .fail)
                            continuation.resume(throwing: error)
                        }
                    } catch {
                        continuation.resume(throwing: error)
                    }
                }
            }
        }
    }

    public func change(onChange: (JSON0Proxy) throws -> Void) async throws {
        guard let data = data else {
            return
        }
        let transaction = Transaction()
        let proxy = JSON0Proxy(path: [], data: data, transaction: transaction)
        try onChange(proxy)

        guard !transaction.operations.isEmpty else {
            return
        }
        try apply(operations: transaction.operations)
        try await send(.update(operations: transaction.operations))
    }
}

extension ShareDocument {
    enum State: Equatable {
        case blank
        case paused
        case pending
        case ready
        case deleted
        case fetchError
        case notCreated
    }

    enum Event {
        case fetch
        case put
        case apply
        case pause
        case resume
        case delete
        case fail
        case setNotCreated
    }

    typealias Transition = () throws -> State

    func makeTransition(for event: Event) throws -> Transition {
        switch (state, event) {
        case (.pending, .setNotCreated):
            return { .notCreated }
        case (.blank, .fetch):
            return { .pending }
        case (.blank, .put), (.pending, .put), (.ready, .put):
            return { .ready }
        case (.ready, .pause):
            return { .paused }
        case (.paused, .resume), (.ready, .resume):
            return { .ready }
        case (.paused, .apply):
            return { .paused }
        case (.ready, .apply):
            return { .ready }
        case (.ready, .delete), (.paused, .delete):
            return { .deleted }
        case (.blank, .fail), (.pending, .fail):
            return { .fetchError }
        default:
            throw ShareDocumentError.stateEvent
        }
    }

    func trigger(event: Event) throws {
        let transition = try makeTransition(for: event)
        state = try transition()
    }
}

extension ShareDocument {
    /// Apply raw JSON operation with OT transformer
    func apply(operations: [AnyCodable]) throws {
        guard let data = self.data else {
            return
        }
        try trigger(event: .apply)
        let newJSON = try transformer.apply(operations, to: data)
        try update(json: newJSON)
    }

    /// Update document JSON and cast to entity
    func update(json: AnyCodable) throws {
        let data = try JSONEncoder().encode(json)
        self.value.send(try JSONDecoder().decode(Entity.self, from: data))
        self.data = json
    }

    /// Update document version and validate version sequence
    func update(version: UInt, validateSequence: Bool) throws {
        if validateSequence, let oldVersion = self.version {
            guard version == oldVersion + 1 else {
                throw ShareDocumentError.operationVersion
            }
        }
        self.version = version
    }

    /// Send ops to server or append to ops queue
    func send(_ operation: OperationData) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            guard inflightOperation == nil, let source = connection.clientID, let version = version else {
                queuedOperations.prepend(operation)
                continuation.resume()
                return
            }
            let msg = OperationMessage(
                collection: id.collection,
                document: id.key,
                source: source,
                data: operation,
                version: version
            )
            connection.send(message: msg).whenComplete { result in
                Task {
                    switch result {
                    case .success:
                        await self.setInflightOperation(operation)
                        continuation.resume()
                    case .failure(let error):
                        // Put op group back to beginning of queue
                        await self.appendOperation(operation)
                        await self.setInflightOperation(nil)
                        continuation.resume(throwing: error)
                    }
                }
            }
        }
    }
}
