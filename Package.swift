// swift-tools-version:5.3
import PackageDescription

let package = Package(
    name: "share-kit",
    platforms: [
        .macOS(.v11),
        .iOS(.v14),
    ],
    products: [
        .library(name: "ShareKit", targets: ["ShareKit"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/websocket-kit.git", from: "2.1.0"),
    ],
    targets: [
        .target(name: "ShareKit", dependencies: [
            .product(name: "WebSocketKit", package: "websocket-kit"),
        ]),
        .testTarget(name: "ShareKitTests", dependencies: [
            .target(name: "ShareKit"),
        ]),
    ]
)
