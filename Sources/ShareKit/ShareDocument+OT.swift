import Foundation

protocol OperationalTransformDocument {
    func pause() async throws
    func resume() async throws
    func put(_ data: AnyCodable?, version: UInt, type: OperationalTransformType?) async throws
    func sync(_ data: OperationData, version: UInt) async throws
    func ack(version: UInt, sequence: UInt) async throws
    func rollback(_ data: OperationData?, version: UInt) async throws
    func setNotCreated() async throws
}

extension ShareDocument: OperationalTransformDocument {
    /// Shift inflightOps into queuedOps for re-send
    func pause() throws {
        try trigger(event: .pause)
        if let inflight = inflightOperation {
            queuedOperations.append(inflight)
            inflightOperation = nil
        }
    }

    func resume() async throws {
        try trigger(event: .resume)
        guard let group = queuedOperations.popLast() else {
            return
        }
        try await send(group)
    }

    /// Replace document data
    func put(_ data: AnyCodable?, version: UInt, type: OperationalTransformType?) async throws {
        if let type = type {
            guard let transformer = OperationalTransformTypes[type] else {
                throw ShareDocumentError.operationalTransformType
            }
            documentTransformer = transformer
        }

        if let json = data {
            try trigger(event: .put)
            try update(json: json)
        } else {
            try trigger(event: .delete)
        }

        try update(version: version, validateSequence: false)
        try await resume()
    }

    /// Sync with remote ops from server
    func sync(_ data: OperationData, version: UInt) async throws {
        switch data {
        case .create(let type, let document):
            try await put(document, version: version, type: type)
        case .update(let ops):
            try update(version: version + 1, validateSequence: true)
            try apply(operations: ops)
        case .delete:
            try trigger(event: .delete)
        }
    }

    /// Verify server ack for inflight message
    func ack(version: UInt, sequence: UInt) async throws {
        guard inflightOperation != nil else {
            throw ShareDocumentError.operationAck
        }
        try update(version: version + 1, validateSequence: true)
        inflightOperation = nil
        try await resume()
    }

    /// Rejected message from server
    func rollback(_ data: OperationData?, version: UInt) {
        guard let data = data else { return }
//        self.version = min(version, self.version)
        print("rollback \(data)")
//      ops.forEach(apply)
    }

    func setNotCreated() throws {
        try trigger(event: .setNotCreated)
        inflightOperation = nil
    }
}
