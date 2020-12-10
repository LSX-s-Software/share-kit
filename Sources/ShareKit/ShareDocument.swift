import Foundation
import Combine
import SwiftyJSON

public struct DocumentID: Hashable {
    let key: String
    let collection: String

    public init(_ key: String, in collection: String) {
        self.key = key
        self.collection = collection
    }
}

final public class ShareDocument<Entity>: Identifiable where Entity: Codable {
    enum ShareDocumentError: Error {
        case transformType
        case documentState
        case stateEvent
        case decodeDocumentData
        case operationalTransformType
        case applyTransform
        case subscription
        case operationVersion
        case operationAck
    }

    public let id: DocumentID

    public private(set) var data: CurrentValueSubject<Entity?, Never>
    public private(set) var version: UInt?
    public private(set) var json = JSON()

    private(set) var state: State

    var documentTransformer: OperationalTransformer.Type?
    var transformer: OperationalTransformer.Type {
        return documentTransformer ?? connection.defaultTransformer
    }

    var inflightOperation: OperationData?
    var queuedOperations: [OperationData] = []

    let connection: ShareConnection

    init(_ documentID: DocumentID, connection: ShareConnection) {
        self.id = documentID
        self.connection = connection
        self.state = .blank
        self.data = CurrentValueSubject(nil)
    }

    public func create(_ data: Entity, type: OperationalTransformType? = nil) throws {
        let jsonData = try JSONEncoder().encode(data)
        let json = JSON(jsonData)
        try put(json, version: 0, type: type)
        send(.create(type: type ?? connection.defaultTransformer.type, data: json))
    }

    public func delete() {
        try? trigger(event: .delete)
        self.send(.delete(isDeleted: true))
    }

    public func subscribe() {
        guard state == .blank else {
            print("Document subscribe canceled: \(state)")
            return
        }
        let msg = SubscribeMessage(collection: id.collection, document: id.key, version: version)
        connection.send(message: msg).whenComplete { result in
            switch result {
            case .success:
                try? self.trigger(event: .fetch)
            case .failure:
                try? self.trigger(event: .fail)
            }
        }
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
    }

    enum Event {
        case fetch
        case put
        case apply
        case pause
        case resume
        case delete
        case fail
    }

    typealias Transition = () throws -> State

    func makeTransition(for event: Event) throws -> Transition {
        switch (state, event) {
        case (.blank, .fetch):
            return { .pending }
        case (.blank, .put), (.pending, .put):
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
    // Apply raw JSON operation with OT transformer
    func apply(operations: [JSON]) throws {
        try trigger(event: .apply)
        let newJSON = try transformer.apply(operations, to: self.json)
        try update(json: newJSON)
    }

    // Update document JSON and cast to entity
    func update(json: JSON) throws {
        let jsonData = try json.rawData()
        self.data.send(try JSONDecoder().decode(Entity.self, from: jsonData))
        self.json = json
    }

    // Update document version and validate version sequence
    func update(version: UInt, validateSequence: Bool) throws {
        if validateSequence, let oldVersion = self.version {
            guard version == oldVersion + 1 else {
                throw ShareDocumentError.operationVersion
            }
        }
        self.version = version
    }

    // Send ops to server or append to ops queue
    func send(_ operation: OperationData) {
        guard inflightOperation == nil, let source = connection.clientID, let version = version else {
            if let queueItem = queuedOperations.first,
               case .update(let queueOps) = queueItem,
               case .update(let currentOps) = operation {
                // Merge with last op group at end of queue
                var newOps = queueOps
                for operation in currentOps {
                    newOps = transformer.append(operation, to: newOps)
                }
                self.queuedOperations[0] = .update(operations: newOps)
            } else {
                // Enqueue op group
                self.queuedOperations.insert(operation, at: 0)
            }
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
            switch result {
            case .success:
                self.inflightOperation = operation
            case .failure:
                // Put op group back to beginning of queue
                self.queuedOperations.append(operation)
                self.inflightOperation = nil
            }
        }
    }
}
