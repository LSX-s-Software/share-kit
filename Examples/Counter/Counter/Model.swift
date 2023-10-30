import SwiftUI
import Combine
import ShareKit

struct Counter: Codable {
    var numClicks = 0
}

class CounterViewModel: ObservableObject {
    @Published
    var counter = Counter()

    private var document: ShareDocument<Counter>?
    private var bag = Set<AnyCancellable>()
    private var client: ShareClient?

    init() {
        let client = ShareClient(eventLoopGroupProvider: .createNew)
        client.connect("ws://localhost:8080") { connection in
            Task {
                do {
                    let document: ShareDocument<Counter> = try await connection.subscribe(document: "counter", in: "examples")
                    if await document.notCreated {
                        try await document.create(Counter())
                    }
                    await document.value
                        .compactMap { $0 }
                        .receive(on: RunLoop.main)
                        .assign(to: \.counter, on: self)
                        .store(in: &self.bag)
                    self.document = document
                } catch {
                    print(error)
                }
            }
        }
        self.client = client
    }

    deinit {
        try? client?.syncShutdown()
    }

    func bumpCounter() {
        Task {
            do {
                try await document?.change {
                    try $0.numClicks.set(counter.numClicks + 1)
                }
            } catch {
                print(error)
            }
        }
    }
}
