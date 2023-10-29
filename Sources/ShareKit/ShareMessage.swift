import Foundation

public enum OperationalTransformType: String, Codable {
    case JSON0 = "http://sharejs.org/types/JSONv0"
    case TEXT0 = "http://sharejs.org/types/textv0"
}

public enum OperationalTransformSubtype: String, Codable {
    case TEXT0 = "text0"
}

struct GenericMessage: Decodable {
    let action: MessageAction
    let error: Error?

    struct Error: Decodable, CustomStringConvertible {
        let code: String
        let message: String

        var description: String {
            "\(message) (\(code))"
        }
    }

    enum CodingKeys: String, CodingKey {
        case action = "a"
        case error
    }
}

struct HandshakeMessage: Codable {
    var action = MessageAction.handshake
    var clientID: String?
    var protocolMajor: UInt?
    var protocolMinor: UInt?
    var type: OperationalTransformType?

    enum CodingKeys: String, CodingKey {
        case action = "a"
        case clientID = "id"
        case protocolMajor = "protocol"
        case protocolMinor = "protocolMinor"
        case type
    }
}

struct SubscribeMessage: Codable {
    var action = MessageAction.subscribe
    var collection: String
    var document: String
    var version: UInt?
    var type: OperationalTransformType?
    var data: VersionedData?

    enum CodingKeys: String, CodingKey {
        case action = "a"
        case collection = "c"
        case document = "d"
        case version = "v"
        case type
        case data
    }
}

struct QuerySubscribeMessage: Codable {
    var action = MessageAction.querySubscribe
    var queryID: UInt
    var query: AnyCodable?
    var collection: String?
    var data: [VersionedDocumentData]?

    enum CodingKeys: String, CodingKey {
        case action = "a"
        case collection = "c"
        case queryID = "id"
        case query = "q"
        case data
    }
}

struct VersionedDocumentData: Codable {
    var document: String
    var version: UInt
    var data: AnyCodable?
    var type: OperationalTransformType?

    enum CodingKeys: String, CodingKey {
        case document = "d"
        case version = "v"
        case data
        case type
    }
}

struct VersionedData: Codable {
    var data: AnyCodable?
    var version: UInt
    var type: OperationalTransformType?

    enum CodingKeys: String, CodingKey {
        case data
        case version = "v"
    }
}

struct QueryMessage: Codable {
    var action = MessageAction.query
    var queryID: UInt
    var diff: [ArrayChange]

    enum CodingKeys: String, CodingKey {
        case action = "a"
        case queryID = "id"
        case diff
    }
}

enum ArrayChange: Codable {
    case move(from: Int, to: Int, howMany: Int)
    case insert(index: Int, values: [VersionedDocumentData])
    case remove(index: Int, howMany: Int)

    enum ArrayChangeType: String, Codable {
        case move, insert, remove
    }

    enum CodingKeys: String, CodingKey {
        case type
        case from
        case to
        case index
        case howMany
        case values
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let type = try values.decode(ArrayChangeType.self, forKey: .type)
        switch type {
        case .move:
            let from = try values.decode(Int.self, forKey: .from)
            let to = try values.decode(Int.self, forKey: .to)
            let howMany = try values.decode(Int.self, forKey: .howMany)
            self = .move(from: from, to: to, howMany: howMany)
        case .insert:
            let index = try values.decode(Int.self, forKey: .index)
            let newValues = try values.decode([VersionedDocumentData].self, forKey: .values)
            self = .insert(index: index, values: newValues)
        case .remove:
            let index = try values.decode(Int.self, forKey: .index)
            let howMany = try values.decode(Int.self, forKey: .howMany)
            self = .remove(index: index, howMany: howMany)
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .move(let from, let to, let howMany):
            try container.encode(ArrayChangeType.move, forKey: .type)
            try container.encode(from, forKey: .from)
            try container.encode(to, forKey: .to)
            try container.encode(howMany, forKey: .howMany)
        case .insert(let index, let values):
            try container.encode(ArrayChangeType.insert, forKey: .type)
            try container.encode(index, forKey: .index)
            try container.encode(values, forKey: .values)
        case .remove(let index, let howMany):
            try container.encode(ArrayChangeType.remove, forKey: .type)
            try container.encode(index, forKey: .index)
            try container.encode(howMany, forKey: .howMany)
        }
    }
}

struct OperationMessage: Codable {
    struct CreateData: Codable {
        var type: OperationalTransformType
        var data: AnyCodable
    }

    var action = MessageAction.operation
    var collection: String
    var document: String
    var source: String
    var data: OperationData?
    var version: UInt
    var sequence: UInt

    enum CodingKeys: String, CodingKey {
        case action = "a"
        case collection = "c"
        case document = "d"
        case source = "src"
        case sequence = "seq"
        case version = "v"
        case create
        case operations = "op"
        case delete = "del"
    }

    init(collection: String, document: String, source: String, data: OperationData, version: UInt) {
        self.collection = collection
        self.document = document
        self.source = source
        self.data = data
        self.version = version
        self.sequence = 0
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        action = try values.decode(MessageAction.self, forKey: .action)
        collection = try values.decode(String.self, forKey: .collection)
        document = try values.decode(String.self, forKey: .document)
        source = try values.decode(String.self, forKey: .source)
        version = try values.decode(UInt.self, forKey: .version)
        sequence = try values.decode(UInt.self, forKey: .sequence)

        if let updateData = try? values.decode([AnyCodable].self, forKey: .operations) {
            data = .update(operations: updateData)
        } else if let createData = try? values.decode(CreateData.self, forKey: .create) {
            data = .create(type: createData.type, data: createData.data)
        } else if let deleteData = try? values.decode(Bool.self, forKey: .delete) {
            data = .delete(isDeleted: deleteData)
        } else {
            data = nil
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(action, forKey: .action)
        try container.encode(collection, forKey: .collection)
        try container.encode(document, forKey: .document)
        try container.encode(source, forKey: .source)
        try container.encode(version, forKey: .version)
        try container.encode(sequence, forKey: .sequence)

        switch data {
        case .create(let type, let data)?:
            let createData = CreateData(type: type, data: data)
            try container.encode(createData, forKey: .create)
        case .update(let operations)?:
            try container.encode(operations, forKey: .operations)
        case .delete(let isDeleted)?:
            try container.encode(isDeleted, forKey: .delete)
        case nil:
            break
        }
    }
}

enum OperationData {
    case create(type: OperationalTransformType, data: AnyCodable)
    case update(operations: [AnyCodable])
    case delete(isDeleted: Bool)
}

enum OperationKey {
    static let path = "p"
    static let subtype = "t"
    static let operation = "o"
    static let numberAdd = "na"
    static let objectInsert = "oi"
    static let objectDelete = "od"
    static let listInsert = "li"
    static let listDelete = "ld"
    static let stringInsert = "si"
    static let stringDelete = "sd"
    static let insert = "i"
    static let delete = "d"
}

enum MessageAction: String, Codable {
    case handshake = "hs"
    case subscribe = "s"
    case query = "q"
    case querySubscribe = "qs"
    case operation = "op"
}

/// ShareDBError
enum ShareDBError: String, Codable {
    /// `ERR_OP_SUBMIT_REJECTED`
    ///
    /// The op submitted by the client has been rejected by the server for a non-critical reason.
    ///
    /// When the client receives this code, it will attempt to roll back the rejected op, leaving the client in a usable
    /// state.
    ///
    /// This error might be used as part of standard control flow. For example, consumers may define a middleware that
    /// validates document structure, and rejects operations that do not conform to this schema using this error code to reset the client to a valid state.
    case opSubmitRejected = "ERR_OP_SUBMIT_REJECTED"
    /// `ERR_PENDING_OP_REMOVED_BY_OP_SUBMIT_REJECTED`
    ///
    /// This may happen if server rejected op with `ERR_OP_SUBMIT_REJECTED` and the type is not invertible or there are
    /// some pending ops after the create op was rejected with `ERR_OP_SUBMIT_REJECTED`
    case pendingOpRemovedByOpSubmitRejected = "ERR_PENDING_OP_REMOVED_BY_OP_SUBMIT_REJECTED"
    /// `ERR_OP_ALREADY_SUBMITTED`
    ///
    /// The same op has been received by the server twice.
    ///
    /// This is non-critical, and part of normal control flow, and is sent as an error in order to short-circuit the op
    /// processing. It is eventually swallowed by the server, and shouldn't need further handling.
    case opAlreadySubmitted = "ERR_OP_ALREADY_SUBMITTED"
    /// `ERR_SUBMIT_TRANSFORM_OPS_NOT_FOUND`
    ///
    /// The ops needed to transform the submitted op up to the current version of the snapshot could not be found.
    ///
    /// If a client on an old version of a document submits an op, that op needs to be transformed by all the ops that
    /// have been applied to the document in the meantime. If the server cannot fetch these ops from the database,
    /// then this error is returned.
    ///
    /// The most common case of this would be ops being deleted from the database.
    /// For example, let's assume we have a TTL set up on the ops in our database. Let's also say we have a client that is so out-of-date that the op corresponding to its version has been deleted by the TTL policy. If this client then attempts to submit an op, the server will not be able to find the ops required to transform the op to apply to the current version of the snapshot.
    ///
    /// Other causes of this error may be dropping the ops collection all together, or having the database corrupted in
    /// some other way.
    case submitTransformOpsNotFound = "ERR_SUBMIT_TRANSFORM_OPS_NOT_FOUND"
    /// `ERR_MAX_SUBMIT_RETRIES_EXCEEDED`
    ///
    /// The number of retries defined by the `maxSubmitRetries`
    /// %}#options) option has been exceeded by a submission.
    case maxSubmitRetriesExceeded = "ERR_MAX_SUBMIT_RETRIES_EXCEEDED"
    /// `ERR_DOC_ALREADY_CREATED`
    ///
    /// The creation request has failed, because the document was already created by another client.
    ///
    /// This can happen when two clients happen to simultaneously try to create the same document,
    /// and is potentially recoverable by simply fetching the already-created document.
    case docAlreadyCreated = "ERR_DOC_ALREADY_CREATED"
    /// `ERR_DOC_WAS_DELETED`
    ///
    /// The deletion request has failed, because the document was already deleted by another client.
    ///
    /// This can happen when two clients happen to simultaneously try to delete the same document. Given that the end
    /// result is the same, this error can potentially just be ignored.
    case docWasDeleted = "ERR_DOC_WAS_DELETED"
    /// `ERR_DOC_TYPE_NOT_RECOGNIZED`
    ///
    /// The specified document type has not been registered with ShareDB.
    ///
    /// This error can usually be remedied by remembering to register any types you need.
    case docTypeNotRecognized = "ERR_DOC_TYPE_NOT_RECOGNIZED"
    /// `ERR_DEFAULT_TYPE_MISMATCH`
    ///
    /// The default type being used by the client does not match the default type expected by the server.
    ///
    /// This will typically only happen when using a different default type to the built-in `json0` used by ShareDB by
    /// default (e.g. if using a fork). The exact same type must be used by both the client and the server, and
    /// should be registered as the default type:
    ///
    /// ```javascript
    /// var ShareDB = require('sharedb');
    /// var forkedJson0 = require('forked-json0');
    /// 
    /// // Make sure to also do this on your client
    /// ShareDB.types.defaultType = forkedJson0.type;
    /// ```
    case defaultTypeMismatch = "ERR_DEFAULT_TYPE_MISMATCH"
    /// `ERR_OP_NOT_ALLOWED_IN_PROJECTION`
    ///
    /// The submitted op is not valid when applied to the projection.
    ///
    /// This may happen if the op targets some property that is not included in the projection.
    case opNotAllowedInProjection = "ERR_OP_NOT_ALLOWED_IN_PROJECTION"
    /// `ERR_TYPE_CANNOT_BE_PROJECTED`
    ///
    /// The document's type cannot be projected. `json0` is currently the
    /// only type that supports projections.
    case typeCannotBeProjected = "ERR_TYPE_CANNOT_BE_PROJECTED"
}
