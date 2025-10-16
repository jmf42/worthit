// Models.swift
import Foundation
import SwiftUI // Needed for Color, Font if Theme were here, but Theme is separate

// MARK: - App Constants

struct AppConstants {
    static let appGroupID = "group.Group.com.juanma.worthmytime" // Your actual App Group ID
    static let bundleID = Bundle.main.bundleIdentifier ?? "com.juanma.worthmytime" // Your bundle ID
    static let urlScheme = "worthitai"
    static let apiBaseURLKey = "API_PROXY_BASE_URL"
}
// MARK: - Decoding Helpers
private extension KeyedDecodingContainer {
    func decodeLossyDouble(forKey key: Key) throws -> Double {
        if let num = try? decode(Double.self, forKey: key) { return num }
        if let str = try? decode(String.self, forKey: key),
           let v   = Double(str) { return v }
        // If we reach here, the key was present but not a Double or String convertible to Double.
        throw DecodingError.dataCorrupted(
            DecodingError.Context(codingPath: codingPath + [key],
                                  debugDescription: "Expected Double or numeric String for key '\(key.stringValue)' but found none or invalid format."))
    }

    func decodeIfPresentLossyDouble(forKey key: Key) throws -> Double? {
        if contains(key) { // Check if key exists first
            if let value = try? decodeNil(forKey: key), value { return nil } // Handle explicit null
            return try decodeLossyDouble(forKey: key) // If not null, try to decode it
        }
        return nil // Key not present
    }

    func decodeIfPresentLossyInt(forKey key: Key) throws -> Int? {
        if contains(key) {
            if let value = try? decodeNil(forKey: key), value { return nil }
            if let num = try? decode(Int.self, forKey: key) { return num }
            if let str = try? decode(String.self, forKey: key),
               let v = Int(str) { return v }
            if let dbl = try? decodeLossyDouble(forKey: key) { return Int(dbl) } // Try decoding as double then converting
            // If key is present but not decodable to Int or a type convertible to Int
            Logger.shared.warning("Could not decode key '\(key.stringValue)' as Int or String convertible to Int, though present.", category: .parsing, extra: ["codingPath": codingPath.map { $0.stringValue }.joined(separator: ".")])
            return nil // Or throw depending on strictness for optional fields. Returning nil is more tolerant.
        }
        return nil // Key not present
    }
}

// MARK: - Core Data Structures for Analysis
struct ContentAnalysis: Codable, Identifiable {
    var id: String { videoId ?? UUID().uuidString }
    let longSummary: String?
    let takeaways: [String]?
    let gemsOfWisdom: [String]?
    let videoId: String?
    let videoTitle: String?
    let videoDurationSeconds: Int?
    let videoThumbnailUrl: String?
    let CommentssentimentSummary: String?
    let topThemes: [CommentTheme]?
    let categorizedComments: [CategorizedCommentAI]?

    enum CodingKeys: String, CodingKey {
        case longSummary, takeaways, gemsOfWisdom
        case videoId, videoTitle, videoDurationSeconds, videoThumbnailUrl
        case CommentssentimentSummary, topThemes
        case categorizedComments
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        longSummary = try container.decodeIfPresent(String.self, forKey: .longSummary)
        // Robust decoding for takeaways
        if let arr = try? container.decodeIfPresent([String].self, forKey: .takeaways) {
            takeaways = arr
        } else if let str = try? container.decodeIfPresent(String.self, forKey: .takeaways) {
            takeaways = [str]
        } else {
            takeaways = nil
        }
        // Robust decoding for gemsOfWisdom
        if let arr = try? container.decodeIfPresent([String].self, forKey: .gemsOfWisdom) {
            gemsOfWisdom = arr
        } else if let str = try? container.decodeIfPresent(String.self, forKey: .gemsOfWisdom) {
            gemsOfWisdom = [str]
        } else {
            gemsOfWisdom = nil
        }
        videoId = try container.decodeIfPresent(String.self, forKey: .videoId)
        videoTitle = try container.decodeIfPresent(String.self, forKey: .videoTitle)
        videoDurationSeconds = try container.decodeIfPresentLossyInt(forKey: .videoDurationSeconds)
        videoThumbnailUrl = try container.decodeIfPresent(String.self, forKey: .videoThumbnailUrl)
        CommentssentimentSummary = try container.decodeIfPresent(String.self, forKey: .CommentssentimentSummary)
        topThemes = try container.decodeIfPresent([CommentTheme].self, forKey: .topThemes)
        categorizedComments = try container.decodeIfPresent([CategorizedCommentAI].self, forKey: .categorizedComments)
    }

    // Convenience initializer for SwiftUI previews & tests
    init(
        longSummary: String? = nil,
        videoTitle: String? = nil,
        takeaways: [String]? = nil
    ) {
        self.longSummary = longSummary
        self.takeaways = takeaways
        self.gemsOfWisdom = nil
        self.videoId = nil
        self.videoTitle = videoTitle
        self.videoDurationSeconds = nil
        self.videoThumbnailUrl = nil
        self.CommentssentimentSummary = nil
        self.topThemes = nil
        self.categorizedComments = nil
    }

    /// Full member‑wise initializer for situations (like `.placeholder()`) that need to set more fields.
    init(
        longSummary: String? = nil,
        takeaways: [String]? = nil,
        gemsOfWisdom: [String]? = nil,
        videoId: String? = nil,
        videoTitle: String? = nil,
        videoDurationSeconds: Int? = nil,
        videoThumbnailUrl: String? = nil,
        CommentssentimentSummary: String? = nil,
        topThemes: [CommentTheme]? = nil,
        categorizedComments: [CategorizedCommentAI]? = nil,
        
    ) {
        self.longSummary = longSummary
        self.takeaways = takeaways
        self.gemsOfWisdom = gemsOfWisdom
        self.videoId = videoId
        self.videoTitle = videoTitle
        self.videoDurationSeconds = videoDurationSeconds
        self.videoThumbnailUrl = videoThumbnailUrl
        self.CommentssentimentSummary = CommentssentimentSummary
        self.topThemes = topThemes
        self.categorizedComments = categorizedComments
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encodeIfPresent(longSummary, forKey: .longSummary)
        try container.encodeIfPresent(takeaways, forKey: .takeaways)
        try container.encodeIfPresent(gemsOfWisdom, forKey: .gemsOfWisdom)
        try container.encodeIfPresent(videoId, forKey: .videoId)
        try container.encodeIfPresent(videoTitle, forKey: .videoTitle)
        try container.encodeIfPresent(videoDurationSeconds, forKey: .videoDurationSeconds)
        try container.encodeIfPresent(videoThumbnailUrl, forKey: .videoThumbnailUrl)
        try container.encodeIfPresent(CommentssentimentSummary, forKey: .CommentssentimentSummary)
        try container.encodeIfPresent(topThemes, forKey: .topThemes)
        try container.encodeIfPresent(categorizedComments, forKey: .categorizedComments)
    }
}

// New struct for AI's categorized comment response
struct CategorizedCommentAI: Codable {
    let index: Int // Index of the comment in the input array
    let category: String // e.g., "humor", "insightful", "spam", "neutral", "controversial"
}



// (Removed KeyQuestion and SpotlightComment: not used in UI)

// MARK: - Comment-level insights
struct CommentInsights: Codable, Identifiable {
    var id: String { videoId ?? UUID().uuidString }

    // Core
    let videoId: String?
    let viewerTips: [String]?

    // Fast metrics
    let overallCommentSentimentScore: Double?
    let contentDepthScore: Double?

    // ALWAYS present after the intro call
    let suggestedQuestions: [String]

    enum CodingKeys: String, CodingKey {
        case videoId, viewerTips
        case overallCommentSentimentScore, contentDepthScore, suggestedQuestions
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        videoId                     = try c.decodeIfPresent(String.self,  forKey: .videoId)
        viewerTips                  = try c.decodeIfPresent([String].self,        forKey: .viewerTips)
        overallCommentSentimentScore = try c.decodeIfPresent(Double.self, forKey: .overallCommentSentimentScore)
        contentDepthScore            = try c.decodeIfPresent(Double.self, forKey: .contentDepthScore)
        // Ensure we always have a questions array
        suggestedQuestions = (try? c.decode([String].self, forKey: .suggestedQuestions)) ?? []
    }

    // Convenience member‑wise init for previews / tests
    init(
        videoId: String? = nil,
        viewerTips: [String]? = nil,
        overallCommentSentimentScore: Double? = nil,
        contentDepthScore: Double? = nil,
        suggestedQuestions: [String] = []
    ) {
        self.videoId = videoId
        self.viewerTips = viewerTips
        self.overallCommentSentimentScore = overallCommentSentimentScore
        self.contentDepthScore = contentDepthScore
        self.suggestedQuestions = suggestedQuestions
    }
}

/// Alias so legacy code can still refer to EssentialsCommentAnalysis
typealias EssentialsCommentAnalysis = CommentInsights

struct CommentTheme: Codable, Identifiable, Hashable {
    let id = UUID()
    let theme: String
    let sentiment: String
    let sentimentScore: Double?
    let exampleComment: String?

    enum CodingKeys: String, CodingKey { case theme, sentiment, sentimentScore, exampleComment }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        theme = try container.decode(String.self, forKey: .theme)
        sentiment = try container.decode(String.self, forKey: .sentiment)
        sentimentScore = try container.decodeIfPresentLossyDouble(forKey: .sentimentScore)
        exampleComment = try container.decodeIfPresent(String.self, forKey: .exampleComment)
    }

    // For previews/testing
    init(theme: String, sentiment: String, sentimentScore: Double?, exampleComment: String?) {
        self.theme = theme
        self.sentiment = sentiment
        self.sentimentScore = sentimentScore
        self.exampleComment = exampleComment
    }
}

struct ChatMessage: Identifiable, Equatable, Hashable, Codable {
    let id: UUID
    let content: String
    let isUser: Bool
    let timestamp: Date

    init(id: UUID = UUID(), content: String, isUser: Bool, timestamp: Date = Date()) {
        self.id = id
        self.content = content
        self.isUser = isUser
        self.timestamp = timestamp
    }
}

struct ScoreBreakdown: Identifiable {
    let id = UUID()
    let contentDepthScore: Double
    let commentSentimentScore: Double
    let hasComments: Bool // New property
    let contentDepthRaw: Double
    let commentSentimentRaw: Double
    let finalScore: Double
    let videoTitle: String
    let positiveCommentThemes: [String]
    let negativeCommentThemes: [String]
}

// MARK: - View State Enum
enum ViewState: Equatable {
    case idle
    case processing
    case showingInitialOptions
    case showingEssentials
    case showingAskAnything
    case error

    var isProcessing: Bool { self == .processing }
}

// MARK: - UserFriendlyError Struct
struct UserFriendlyError: Equatable, Identifiable {
    let id = UUID()
    let message: String
    let canRetry: Bool
    let videoIdForRetry: String?
}

struct APIError: Error, LocalizedError, Identifiable {
    let id = UUID()
    let message: String
    let underlyingError: Error?

    var errorDescription: String? { return message }

    init(message: String, underlyingError: Error? = nil) {
        self.message = message
        self.underlyingError = underlyingError
        Logger.shared.error(message, category: .networking, error: underlyingError)
    }
}

extension Error {
    var userFriendlyMessage: String {
        if let apiError = self as? APIError {
            return apiError.message
        } else if let urlError = self as? URLError {
            switch urlError.code {
            case .timedOut: return "The request timed out. Please check your connection."
            case .cannotFindHost, .cannotConnectToHost: return "Cannot connect to the server."
            case .notConnectedToInternet: return "No internet connection. Please connect and try again."
            case .networkConnectionLost: return "Network connection lost. Please try again."
            case .cancelled: return "The operation was cancelled."
            default: return "A network error occurred (\(urlError.code.rawValue)). Please try again."
            }
        } else if let decodingError = self as? DecodingError {
            var details = "Details: "
            switch decodingError {
            case .typeMismatch(let type, let context):
                details += "Type '\(type)' mismatch. Path: \(context.codingPath.map { $0.stringValue }.joined(separator: ".")), Debug: \(context.debugDescription)"
            case .valueNotFound(let type, let context):
                details += "Value '\(type)' not found. Path: \(context.codingPath.map { $0.stringValue }.joined(separator: ".")), Debug: \(context.debugDescription)"
            case .keyNotFound(let key, let context):
                details += "Key '\(key.stringValue)' not found. Path: \(context.codingPath.map { $0.stringValue }.joined(separator: ".")), Debug: \(context.debugDescription)"
            case .dataCorrupted(let context):
                details += "Data corrupted. Path: \(context.codingPath.map { $0.stringValue }.joined(separator: ".")), Debug: \(context.debugDescription)"
            @unknown default:
                details += "Unknown decoding error."
            }
            Logger.shared.error("User-facing decoding error: \(details)", category: .parsing, error: decodingError) // Log the detailed decoding error
            return "There was an issue processing the data from the server. Please ensure the format is correct." // User-facing message
        }
        return "An unexpected error occurred. Please try again."
    }
}

struct BackendTranscriptResponse: Codable {
    let video_id: String? // Make optional if backend might omit
    let text: String?     // Make optional
}

struct BackendCommentsResponse: Codable {
    let video_id: String? // Make optional
    let comments: [String]? // Make optional
}

struct OpenAIProxyRequest<T: Encodable>: Encodable {
    let model: String
    let temperature: Double
    let max_output_tokens: Int
    let instructions: String?
    let input: T? // Made input optional for the textPayloadInput case
    let text: OpenAIRequestTextPayload?
    let previous_response_id: String?
    let metadata: [String:String]?            // ← NEW

    init(model: String, temperature: Double = 0.2, max_output_tokens: Int, instructions: String? = nil, promptInput: String, previous_response_id: String? = nil, metadata: [String:String]? = nil) where T == String {
        self.model = model
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.instructions = instructions
        self.input = promptInput
        self.text = nil
        self.previous_response_id = previous_response_id
        self.metadata = metadata
    }

    // This initializer sets `input` to nil if textPayloadInput is used.
    init(model: String, temperature: Double = 0.2, max_output_tokens: Int, instructions: String? = nil, textPayloadInput: String, previous_response_id: String? = nil, metadata: [String:String]? = nil) where T == String? { // T is String?
        self.model = model
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.instructions = instructions
        self.input = nil // Input is nil when textPayloadInput is used
        self.text = OpenAIRequestTextPayload(format: .init(type: "json_object"), input: textPayloadInput)
        self.previous_response_id = previous_response_id
        self.metadata = metadata
    }
}

struct OpenAIRequestTextPayload: Encodable {
    struct Format: Encodable {
        let type: String
    }
    let format: Format
    let input: String
}

// MARK: - OpenAI Proxy DTO (To match actual CURL output from Render)
struct OpenAIProxyResponseDTO: Decodable {
    // Top-level fields observed in your CURL output
    let id: String?
    let model: String?
    let object: String? // e.g., "response"
    let created_at: Int? // Unix timestamp
    // Convenience concatenated text sometimes provided by Responses API
    let output_text: String?

    // The 'output' array is the primary container for the content
    let output: [OutputItemFromProxy]?

    // Other top-level fields from your curl output (optional, add if needed)
    let background: Bool?
    // let error: YourErrorType? // Define YourErrorType if error can be non-null and structured
    let incomplete_details: IncompleteDetails?
    let instructions: String?       // Assuming null or string
    let max_output_tokens: Int?
    // let metadata: YourMetadataType? // Define if metadata is structured and needed
    let parallel_tool_calls: Bool?
    let previous_response_id: String? // Assuming null or string
    // let reasoning: YourReasoningType? // Define if needed
    let service_tier: String?
    let status: String?
    let store: Bool?
    let temperature: Double?
    // The 'text' field at the root of your CURL output is an OBJECT, not a simple string.
    // If you need to decode it, define a struct for it. For now, we only need the nested text.
    // let text: ProxyTextObject?
    // struct ProxyTextObject: Decodable { let format: ProxyTextFormat? }
    // struct ProxyTextFormat: Decodable { let type: String? }
    // let usage: YourUsageType? // Define if needed

    // Nested struct for items within the "output" array
    struct OutputItemFromProxy: Decodable {
        let content: [OutputContentFromProxy]?
        let id: String?
        let role: String? // e.g., "assistant"
        let status: String? // e.g., "completed"
        let type: String? // e.g., "message"
    }

    // Nested struct for items within the "content" array (inside "output" items)
    struct OutputContentFromProxy: Decodable {
        // The Responses API returns either a raw string, or an object with at least a `value: String`
        struct TextObject: Decodable { let value: String? }

        let text: String?
        let type: String? // Expected to be "output_text" for the main content (but be tolerant)

        enum CodingKeys: String, CodingKey { case text, type }
        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            self.type = try container.decodeIfPresent(String.self, forKey: .type)
            // Prefer simple string
            if let s = try? container.decode(String.self, forKey: .text) {
                self.text = s
            } else if let obj = try? container.decode(TextObject.self, forKey: .text) {
                self.text = obj.value
            } else {
                self.text = nil
            }
        }
    }

    // Helper to get the primary text content which is the stringified JSON
    func getPrimaryTextContent() -> String? {
        // 1) Prefer the new top-level convenience field if present
        if let top = output_text, !top.isEmpty {
            Logger.shared.debug("OpenAIProxyResponseDTO.getPrimaryTextContent: Using top-level output_text.", category: .parsing)
            return top
        }

        // 2) Standard nested path: first output → first content with type output_text
        if let firstOutputItem = output?.first,
           let firstContentItem = firstOutputItem.content?.first(where: { ($0.type == "output_text" || $0.type == "text" || $0.type == nil) && ($0.text != nil && !$0.text!.isEmpty) }),
           let jsonString = firstContentItem.text, !jsonString.isEmpty {
            Logger.shared.debug("OpenAIProxyResponseDTO.getPrimaryTextContent: Extracted from output[0].content[output_text].", category: .parsing)
            return jsonString
        }

        Logger.shared.debug("OpenAIProxyResponseDTO.getPrimaryTextContent: Standard path missing; checking fallbacks.", category: .parsing)

        // 3) Fallback: find the first non-empty text across any content items
        if let anyText = output?.compactMap({ $0.content }).flatMap({ $0 }).compactMap({ $0.text }).first(where: { !$0.isEmpty }) {
            Logger.shared.debug("OpenAIProxyResponseDTO.getPrimaryTextContent: Using fallback text from any content item.", category: .parsing)
            return anyText
        }

        Logger.shared.error("OpenAIProxyResponseDTO.getPrimaryTextContent: No primary text content found.", category: .parsing)
        return nil
    }
}

// Flexible decode for incomplete_details which can be string or object
enum IncompleteDetails: Decodable {
    case string(String)
    case dict([String: String])

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let s = try? container.decode(String.self) {
            self = .string(s)
        } else if let d = try? container.decode([String: String].self) {
            self = .dict(d)
        } else {
            self = .dict([:])
        }
    }
}

struct QAResponse: Codable {
    let answer: String
}

// MARK: - String Extensions
extension String {
    func truncate(to length: Int, trailing: String = "...") -> String {
        if self.count > length { return String(self.prefix(length)) + trailing }
        return self
    }
    func stripTimestamps() -> String {
        let pattern = "\\[[0-9:,\\.\\s-->]+\\]|\\(\\d{2}:\\d{2}(:\\d{2})?\\s?(\\w+)?\\)|\\d{1,2}:\\d{2}(:\\d{2})?\\s*-*\\s*"
        return self.replacingOccurrences(of: pattern, with: "", options: .regularExpression).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    /// Fast, conservative cleanup for LLM context. O(n), regex-only, preserves quotes and meaningful content.
    /// Steps: strip timestamps, drop stage directions, remove CTA/link noise at head/tail, remove filler-only short lines,
    /// collapse bare URLs (unless quoted), dedupe exact/near-identical short lines, normalize whitespace, and merge fragments.
    func cleanedForLLM() -> String {
        // 0) Normalize newlines and strip timestamps first
        let text = self.replacingOccurrences(of: "\r\n", with: "\n")
                       .replacingOccurrences(of: "\r", with: "\n")
                       .stripTimestamps()

        // 1) Prepare regexes and keyword sets
        let stageDirRegex = try? NSRegularExpression(pattern: "^\\s*\\[(music|applause|laughter|inaudible|silence|intro|outro|sfx|ambient|background)\\]\\s*$", options: [.caseInsensitive])
        let fillerOnlyRegex = try? NSRegularExpression(pattern: "^\\s*(uh+|um+|erm+|mmm+|hmm+|like|you\\s+know|kinda|sorta|yeah|yep|right|ok(ay)?|alright)[\\.!?,…]*\\s*$", options: [.caseInsensitive])
        let urlRegex = try? NSRegularExpression(pattern: "https?://[^\\s)]+", options: [])

        // Be conservative: avoid generic words like "code" that appear in technical content
        let ctaKeywords: [String] = [
            "sponsor", "sponsored", "subscribe", "like", "comment", "ring the bell", "notification",
            "follow", "promo", "discount", /* removed: "code" */ "coupon", "affiliate", "patreon", "merch",
            "link in description", "newsletter", "squarespace", "nordvpn", "raid shadow legends",
            "promo code", "discount code"
        ]

        // 2) Split into lines and pre-compute head/tail bounds
        let lines = text.components(separatedBy: "\n")
        let n = max(lines.count, 1)
        let headTailCount = max(1, n / 7) // ~15% of lines

        // Helper to test regex match
        func matches(_ regex: NSRegularExpression?, _ s: String) -> Bool {
            guard let r = regex else { return false }
            let ns = s as NSString
            let range = NSRange(location: 0, length: ns.length)
            return r.firstMatch(in: s, options: [], range: range) != nil
        }

        // Helper to remove URLs if no quotes present
        func stripURLsIfNotQuoted(_ s: String) -> String {
            if s.contains("\"") { return s } // preserve links inside quotes
            guard let r = urlRegex else { return s }
            let ns = NSMutableString(string: s)
            var removed = 0
            r.enumerateMatches(in: s, options: [], range: NSRange(location: 0, length: (s as NSString).length)) { match, _, _ in
                guard let m = match else { return }
                let adj = NSRange(location: m.range.location - removed, length: m.range.length)
                ns.replaceCharacters(in: adj, with: "")
                removed += m.range.length
            }
            return String(ns)
        }

        // Helper CTA detection
        func containsCTA(_ s: String) -> Bool {
            let lower = s.lowercased()
            for k in ctaKeywords { if lower.contains(k) { return true } }
            return false
        }

        // 3) Filter + light transform pass
        var filtered: [String] = []
        filtered.reserveCapacity(lines.count)
        var lastNormalized: String? = nil
        for (idx, rawLine) in lines.enumerated() {
            var line = rawLine.trimmingCharacters(in: .whitespaces)
            if line.isEmpty { continue }

            // Stage directions like [Music]
            if matches(stageDirRegex, line) { continue }

            // CTA/link noise near head/tail: drop line with obvious CTA cues
            if idx < headTailCount || idx >= n - headTailCount {
                if containsCTA(line) { continue }
            }

            // Filler-only very short lines (outside quotes)
            if !line.contains("\"") && matches(fillerOnlyRegex, line) { continue }

            // Remove bare URLs unless quoted
            line = stripURLsIfNotQuoted(line)

            // Normalize inner whitespace (keep single spaces)
            line = line.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)

            // Deduplicate exact/near-identical short lines in a rolling fashion
            // Build a normalization that keeps only a-z, 0-9, space, and basic punctuation to avoid regex escape pitfalls
            let allowedScalars = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyz0123456789 -.,!?")
            var norm = String(line.lowercased().unicodeScalars.filter { allowedScalars.contains($0) })
            norm = norm.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
                .trimmingCharacters(in: .whitespaces)
            if let last = lastNormalized, last == norm { continue }
            lastNormalized = norm

            filtered.append(line)
        }

        // 4) Merge fragments: join consecutive lines into paragraphs if previous doesn't end with terminal punctuation
        let terminators: Set<Character> = Set([".", "!", "?", ":", ";"])
        var merged: [String] = []
        merged.reserveCapacity(filtered.count)
        for line in filtered {
            if var last = merged.popLast() {
                if let c = last.trimmingCharacters(in: .whitespacesAndNewlines).last, terminators.contains(c) {
                    merged.append(last)
                    merged.append(line)
                } else if last.count < 300 { // soft wrap
                    last += (last.isEmpty ? "" : " ") + line
                    merged.append(last)
                } else {
                    merged.append(last)
                    merged.append(line)
                }
            } else {
                merged.append(line)
            }
        }

        // 5) Final whitespace normalization and cleanup
        var result = merged.joined(separator: "\n")
        result = result.replacingOccurrences(of: "\n{3,}", with: "\n\n", options: .regularExpression)
        result = result.trimmingCharacters(in: .whitespacesAndNewlines)
        return result
    }
}

// Ensure sanitizedJSONData is here:
func sanitizedJSONData(from text: String) -> Data? {
    // 1) Strip code fences and trim
    var cleaned = text
        .replacingOccurrences(of: "^```json", with: "", options: .regularExpression)
        .replacingOccurrences(of: "^```", with: "", options: .regularExpression)
        .replacingOccurrences(of: "```$", with: "", options: .regularExpression)
        .trimmingCharacters(in: .whitespacesAndNewlines)

    // 2) Normalize smart quotes to plain quotes/safe characters to avoid invalid JSON
    cleaned = cleaned
        .replacingOccurrences(of: "\u{201C}", with: "\"") // “
        .replacingOccurrences(of: "\u{201D}", with: "\"") // ”
        .replacingOccurrences(of: "\u{2018}", with: "'")    // ‘
        .replacingOccurrences(of: "\u{2019}", with: "'")    // ’

    // 3) Extract ONLY the first complete top-level JSON object using a brace counter (ignore quote state)
    // Rationale: model sometimes emits unescaped quotes inside values; tracking string state would break.
    var braceDepth = 0
    var started = false
    var startIndex: String.Index? = nil
    var endIndex: String.Index? = nil
    for i in cleaned.indices {
        let ch = cleaned[i]
        if !started {
            if ch == "{" { started = true; braceDepth = 1; startIndex = i }
            continue
        }
        if ch == "{" { braceDepth += 1; continue }
        if ch == "}" {
            braceDepth -= 1
            if braceDepth == 0 { endIndex = i; break }
        }
    }

    guard let s = startIndex, let e = endIndex else {
        Logger.shared.warning("sanitizedJSONData: Could not isolate first JSON object. Preview: \(cleaned.prefix(200))", category: .parsing)
        return nil
    }
    var jsonString = String(cleaned[s...e])

    // 4) Remove trailing commas in objects/arrays
    let trailingCommaPattern = #",(\s*[}\]])"#
    jsonString = jsonString.replacingOccurrences(of: trailingCommaPattern, with: "$1", options: .regularExpression)

    // 5) Best-effort fix: collapse doubled quotes occurrences inside strings (e.g., ""Quote"")
    jsonString = jsonString.replacingOccurrences(of: "\"\"", with: "\"")

    // 6) Validate
    guard let data = jsonString.data(using: .utf8) else { return nil }
    if (try? JSONSerialization.jsonObject(with: data)) == nil {
        Logger.shared.warning("sanitizedJSONData: JSON still invalid after fixes. Preview: \(jsonString.prefix(200))", category: .parsing)
    }
    return data
}

// Ensure DecodingError extensions are here:
fileprivate extension DecodingError.Context {
    var codingPathString: String {
        return codingPath.map { $0.stringValue }.joined(separator: " -> ")
    }
}
fileprivate extension DecodingError {
    var codingPathString: String {
        switch self {
        case .typeMismatch(_, let context): return context.codingPathString
        case .valueNotFound(_, let context): return context.codingPathString
        case .keyNotFound(_, let context): return context.codingPathString
        case .dataCorrupted(let context): return context.codingPathString
        @unknown default: return "Unknown Path (Error has no codingPath context)"
        }
    }
}

// MARK: - ContentAnalysis Empty Placeholder
extension ContentAnalysis {
    static func placeholder() -> ContentAnalysis {
        return ContentAnalysis(
            longSummary: "",
            takeaways: [],
            gemsOfWisdom: [],
            videoId: "",
            videoTitle: "",
            videoDurationSeconds: nil,
            videoThumbnailUrl: "",
            CommentssentimentSummary: nil,
            topThemes: []
        )
    }
}
