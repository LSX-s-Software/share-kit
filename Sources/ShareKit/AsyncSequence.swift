//
//  AsyncSequence.swift
//
//
//  Created by 林思行 on 2023/10/29.
//

import Foundation

extension Sequence {
    func asyncMap<T>(_ transform: (Element) async throws -> T) async rethrows -> [T] {
        var values = [T]()
        if self is Array<Any> {
            values.reserveCapacity((self as! Array<Any>).count)
        }
        for element in self {
            try await values.append(transform(element))
        }
        return values
    }

    func asyncForEach(_ operation: (Element) async throws -> Void) async rethrows {
        for element in self {
            try await operation(element)
        }
    }

    func concurrentForEach(_ operation: @escaping (Element) async -> Void) async {
        await withTaskGroup(of: Void.self) { group in
            for element in self {
                group.addTask {
                    await operation(element)
                }
            }
        }
    }
}

class AsyncDict<K: Hashable, V> {
    private var threadUnsafeDict = [K: V]()
    private let dispatchQueue = DispatchQueue(label: UUID().uuidString, attributes: .concurrent)

    func get(key: K) -> V? {
        var result: V?
        dispatchQueue.sync {
            result = threadUnsafeDict[key]
        }
        return result
    }

    func set(key: K, value: V?) {
        dispatchQueue.async(flags: .barrier) {
            self.threadUnsafeDict[key] = value
        }
    }

    func forEach(_ operation: @escaping (K, V) throws -> Void) rethrows {
        try dispatchQueue.sync {
            for item in self.threadUnsafeDict {
                try operation(item.key, item.value)
            }
        }
    }
}
