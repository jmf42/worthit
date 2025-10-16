import Foundation

/// Drop-in logger that writes every raw SSE line (including blank lines) to a file.
/// Turn on/off with `SSERecorder.isEnabled`.
final class SSERecorder {
    static var isEnabled: Bool = true   // flip to false in production if you want

    private let fileURL: URL
    private var handle: FileHandle?

    init?(sessionName: String = "stream") {
        guard Self.isEnabled else { return nil }

        do {
            let dir = try FileManager.default.url(
                for: .documentDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            let ts = ISO8601DateFormatter().string(from: Date())
                .replacingOccurrences(of: ":", with: "-")
            fileURL = dir.appendingPathComponent("sse-\(sessionName)-\(ts).log")
            FileManager.default.createFile(atPath: fileURL.path, contents: nil, attributes: nil)
            handle = try FileHandle(forWritingTo: fileURL)
            writeHeader("SSE capture started \(ts)")
            print("📄 SSERecorder logging to: \(fileURL.path)")
        } catch {
            print("SSERecorder init error: \(error)")
            return nil
        }
    }

    func write(rawLine: String) {
        let stamp = ISO8601DateFormatter().string(from: Date())
        let safe = rawLine.isEmpty ? "<BLANK>" : rawLine
        let line = "[\(stamp)] \(safe)\n"
        guard let data = line.data(using: .utf8) else { return }
        handle?.write(data)
    }

    func writeHeader(_ text: String) {
        let header = "----- \(text) -----\n"
        handle?.write(Data(header.utf8))
    }

    func close() {
        writeHeader("SSE capture ended")
        try? handle?.close()
        handle = nil
    }

    deinit { close() }
}