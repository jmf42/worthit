
// APIManager.swift

import Foundation

// MARK: - Data Models


enum NetworkError: LocalizedError {
    case serverUnavailable
    case rateLimited
    case decodingFailed
    case network(Error)
    case unknownStatus(Int)

    var errorDescription: String? {
        switch self {
        case .serverUnavailable:
            return "Our servers are temporarily unavailable. Please try again later."
        case .rateLimited:
            return "Too many requests — please wait a moment and retry."
        case .decodingFailed:
            return "We couldn't read the server response."
        case .network(let err):
            return err.localizedDescription
        case .unknownStatus(let code):
            return "Unexpected server response (code \(code))."
        }
    }
}

protocol GPTServiceProtocol {
    func fetchContentAnalysis(transcript: String, videoTitle: String, comments: [String]) async throws -> ContentAnalysis
    func fetchCommentInsights(comments: [String], transcriptContext: String) async throws -> CommentInsights
    func answerQuestion(transcript: String, question: String, history: [ChatMessage]) async throws -> QAResponse
}

class APIManager: GPTServiceProtocol {
    private let session: URLSession
    private let baseURL: URL
    private var openAIModelName: String
    var lastResponseId: String?
    private var cachedCommentInsights: CommentInsights?

    init(session: URLSession = URLSession.shared) {
        let configuration = URLSessionConfiguration.default
        configuration.timeoutIntervalForRequest = 15        // seconds
        configuration.timeoutIntervalForResource = 30       // seconds
        self.session = URLSession(configuration: configuration)
        guard let baseURLString = Bundle.main.infoDictionary?[AppConstants.apiBaseURLKey] as? String,
              let url = URL(string: baseURLString) else {
            Logger.shared.critical("APIManager Fatal Error: \(AppConstants.apiBaseURLKey) not set or invalid in Info.plist", category: .networking)
            fatalError("\(AppConstants.apiBaseURLKey) not set or invalid in Info.plist")
        }
        self.baseURL = url
        // Default to GPT‑5 Nano (low thinking) model
        self.openAIModelName = "gpt-5-nano-2025-08-07"
        Logger.shared.info("APIManager initialized with base URL: \(self.baseURL.absoluteString)", category: .services)
    }

    // Generic request performer - T must be Decodable
    private func performRequest<T: Decodable>(endpoint: String, method: String = "GET", queryParams: [String: String]? = nil, body: Data? = nil, timeout: TimeInterval = 35, extraHeaders: [String: String]? = nil) async throws -> T {
        // Build an absolute or relative URL without duplicating the baseURL
        let targetURL: URL
        if endpoint.hasPrefix("http") {
            guard let abs = URL(string: endpoint) else {
                throw NetworkError.unknownStatus(-1)
            }
            targetURL = abs
        } else {
            targetURL = baseURL.appendingPathComponent(endpoint)
        }
        var components = URLComponents(url: targetURL, resolvingAgainstBaseURL: false)!
        if let queryParams = queryParams {
            components.queryItems = queryParams.map { URLQueryItem(name: $0.key, value: $0.value) }
        }

        let url = components.url ?? targetURL

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = timeout
        request.cachePolicy = .reloadIgnoringLocalCacheData

        var logExtra: [String: Any] = [:]
        if let bodyData = body {
            request.httpBody = bodyData
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            logExtra["body_size"] = bodyData.count
        }

        if let headers = extraHeaders {
            for (k, v) in headers { request.setValue(v, forHTTPHeaderField: k) }
        }

        Logger.shared.debug("Requesting \(method) \(url.absoluteString)", category: .networking, extra: logExtra)

        var attempt = 0
        let maxRetries = 1
        while true {
            do {
                let (data, response) = try await session.data(for: request)

                guard let httpResponse = response as? HTTPURLResponse else {
                    throw NetworkError.serverUnavailable
                }

                Logger.shared.debug("Response status: \(httpResponse.statusCode) for \(url.absoluteString) (endpoint: \(endpoint))", category: .networking)

                guard (200...299).contains(httpResponse.statusCode) else {
                    let errorPayload = String(data: data, encoding: .utf8) ?? "No error details"
                    Logger.shared.error("API Error: \(httpResponse.statusCode) for \(url.absoluteString) (endpoint: \(endpoint))", category: .networking, extra: ["details": errorPayload.prefix(1000)])
                    AnalyticsService.shared.logNetworkError(endpoint: endpoint, error: "HTTP \(httpResponse.statusCode)")
                    if httpResponse.statusCode == 429 { throw NetworkError.rateLimited }
                    if (try? JSONDecoder().decode(BackendErrorResponse.self, from: data)) != nil {
                        throw NetworkError.unknownStatus(httpResponse.statusCode)
                    }
                    throw NetworkError.unknownStatus(httpResponse.statusCode)
                }

                if T.self == Data.self {
                    return data as! T
                }

                do {
                    let decoder = JSONDecoder()
                    let decodedObject = try decoder.decode(T.self, from: data)
                    return decodedObject
                } catch let decodingError {
                    let dataPreview = String(data: data, encoding: .utf8)?.prefix(1000) ?? "Invalid UTF-8 data"
                    var errorDetails = "Raw data preview: \(dataPreview)"
                    // Use the general Error.userFriendlyMessage extension for consistent messaging
                    errorDetails += "\nDecoding Error Context: \(decodingError.userFriendlyMessage)"
                    Logger.shared.error("Decoding error for \(T.self) from \(endpoint) URL: \(url.absoluteString). \(errorDetails)", category: .parsing, error: decodingError)
                    throw NetworkError.decodingFailed
                }
            }
            catch {
                if (error as? URLError)?.code == .timedOut, attempt < maxRetries {
                    attempt += 1
                    try await Task.sleep(nanoseconds: UInt64(0.5 * 1_000_000_000))
                    continue
                }
                AnalyticsService.shared.logNetworkError(endpoint: endpoint, error: error.localizedDescription)
                throw NetworkError.network(error)
            }
        }
    }

    private struct BackendErrorResponse: Decodable {
        let error: String
    }

    func fetchTranscript(videoId: String) async throws -> String {
        // Build preferred language list: device language first, then popular fallbacks
        let deviceLang: String = {
            guard let id = Locale.preferredLanguages.first, !id.isEmpty else { return "en" }
            if #available(iOS 16.0, *) {
                let lang = Locale.Language(identifier: id)
                if let code = lang.languageCode?.identifier { return code }
            } else {
                // Fallback without deprecated Locale.languageCode: parse primary subtag of BCP‑47 (e.g., "en-US" → "en")
                if let primary = id.split(separator: "-").first, !primary.isEmpty {
                    return String(primary)
                }
            }
            let simple = id.prefix(2)
            return simple.isEmpty ? "en" : String(simple)
        }()
        // Popular coverage list
        let base = ["en","es","pt","hi","ar"]
        var ordered = [String]()
        func appendUnique(_ code: String) { if !ordered.contains(code) { ordered.append(code) } }
        appendUnique(deviceLang)
        for c in base { appendUnique(c) }
        // Truncate to 5
        ordered = Array(ordered.prefix(5))
        let languagesCSV = ordered.joined(separator: ",")
        // Accept-Language with q-weights
        let acceptLang = ordered.enumerated().map { idx, code in
            let q = String(format: "%.1f", 1.0 - (Double(idx) * 0.1))
            return "\(code);q=\(q)"
        }.joined(separator: ", ")

        // Primary attempt with ordered languages
        Logger.shared.info("Transcript request starting for \(videoId) with languages=\(languagesCSV)", category: .networking, extra: ["accept": acceptLang])
        do {
            let response: BackendTranscriptResponse = try await performRequest(
                endpoint: "transcript",
                queryParams: ["videoId": videoId, "languages": languagesCSV],
                timeout: 15,
                extraHeaders: [
                    "Accept-Language": acceptLang,
                    "X-Device-Languages": languagesCSV
                ]
            )
            if let text = response.text, !text.isEmpty { return text }
            throw NetworkError.decodingFailed
        } catch let NetworkError.unknownStatus(code) where code == 404 {
            Logger.shared.notice("Transcript 404 for \(videoId) with languages list. Retrying without languages...", category: .networking)
            // Fallback 1: omit languages entirely to let backend auto-detect
            if let text = try? await performRequest(
                endpoint: "transcript",
                queryParams: ["videoId": videoId],
                timeout: 15,
                extraHeaders: [:]
            ) as BackendTranscriptResponse, let t = text.text, !t.isEmpty {
                Logger.shared.info("Transcript fallback succeeded without languages for \(videoId)", category: .networking)
                return t
            }
            // Fallback 2: try a minimal single language (en)
            if let text = try? await performRequest(
                endpoint: "transcript",
                queryParams: ["videoId": videoId, "languages": "en"],
                timeout: 15
            ) as BackendTranscriptResponse, let t = text.text, !t.isEmpty {
                Logger.shared.info("Transcript fallback succeeded with languages=en for \(videoId)", category: .networking)
                return t
            }
            Logger.shared.warning("Transcript not available after fallbacks for \(videoId)", category: .networking)
            throw NetworkError.unknownStatus(404)
        } catch let NetworkError.network(inner) {
            // Unwrap inner NetworkError to detect 404 and apply the same fallbacks
            if let ne = inner as? NetworkError {
                if case .unknownStatus(let code) = ne, code == 404 {
                    Logger.shared.notice("Transcript 404 (wrapped) for \(videoId). Retrying fallbacks...", category: .networking)
                    if let text = try? await performRequest(
                        endpoint: "transcript",
                        queryParams: ["videoId": videoId],
                        timeout: 15,
                        extraHeaders: [:]
                    ) as BackendTranscriptResponse, let t = text.text, !t.isEmpty {
                        Logger.shared.info("Transcript fallback succeeded without languages for \(videoId)", category: .networking)
                        return t
                    }
                    if let text = try? await performRequest(
                        endpoint: "transcript",
                        queryParams: ["videoId": videoId, "languages": "en"],
                        timeout: 15
                    ) as BackendTranscriptResponse, let t = text.text, !t.isEmpty {
                        Logger.shared.info("Transcript fallback succeeded with languages=en for \(videoId)", category: .networking)
                        return t
                    }
                    Logger.shared.warning("Transcript not available after fallbacks for \(videoId)", category: .networking)
                }
            }
            throw NetworkError.network(inner)
        }
    }

    func fetchComments(videoId: String) async throws -> [String] {
        struct CommentsResponse: Decodable { let comments: [String]? }

        do {
            let response: CommentsResponse = try await performRequest(
                endpoint: "comments",
                queryParams: ["videoId": videoId],
                timeout: 15
            )
            return response.comments ?? []
        } catch {
            Logger.shared.warning("Comments unavailable for \(videoId): \(error)")
            AnalyticsService.shared.logCommentsNotFound(videoId: videoId)
            return [] // graceful fallback
        }
    }

    private func performOpenAIRequest<Payload: Encodable, ResponseType: Decodable>(
        payload: Payload, // This is OpenAIProxyRequest<String>
        captureResponseId: Bool = false
    ) async throws -> ResponseType {
        // Encode the payload (do not implicitly chain here; call sites decide)
        let initialBody = try JSONEncoder().encode(payload)
        var bodyDict = try JSONSerialization.jsonObject(with: initialBody) as! [String: Any]
        // Force JSON object responses via Responses API text.format only (proxy rejects top-level response_format)
        var textObj = (bodyDict["text"] as? [String: Any]) ?? [:]
        var formatObj = (textObj["format"] as? [String: Any]) ?? [:]
        formatObj["type"] = "json_object"
        textObj["format"] = formatObj
        // Reduce verbosity to minimize output tokens
        textObj["verbosity"] = "low"
        bodyDict["text"] = textObj
        // Reduce reasoning effort to lower latency and token use
        var reasoningObj = (bodyDict["reasoning"] as? [String: Any]) ?? [:]
        reasoningObj["effort"] = "minimal"
        bodyDict["reasoning"] = reasoningObj
        // Ensure responses are stored for observability and debugging
        bodyDict["store"] = true
        // Retain central temperature stripping for GPT‑5 nano to avoid API complaints
        if let modelInBody = bodyDict["model"] as? String, modelInBody.hasPrefix("gpt-5-nano") {
            bodyDict.removeValue(forKey: "temperature")
        }
        let requestBody = try JSONSerialization.data(withJSONObject: bodyDict)

        // 1. Get raw Data from the proxy endpoint.
        let rawProxyResponseData: Data = try await performRequest(
            endpoint: "openai/responses",
            method: "POST",
            body: requestBody,
            timeout: 120
        )

        let rawProxyResponseStringForLog = String(data: rawProxyResponseData, encoding: .utf8) ?? "Could not convert proxy response data to UTF8 string"
        Logger.shared.debug("Raw data from OpenAI proxy (size: \(rawProxyResponseData.count)) before DTO decoding: \(rawProxyResponseStringForLog.prefix(1500))", category: .networking)

        // 2. Decode the raw data into OpenAIProxyResponseDTO (best-effort). If it fails, fall back to raw-walk parsing later.
        let proxyDTO: OpenAIProxyResponseDTO?
        do {
            proxyDTO = try JSONDecoder().decode(OpenAIProxyResponseDTO.self, from: rawProxyResponseData)
        } catch let dtoDecodingError {
            Logger.shared.error("Failed to decode OpenAIProxyResponseDTO from proxy response.", category: .parsing, error: dtoDecodingError, extra: ["rawDataPreview": rawProxyResponseStringForLog.prefix(500)])
            // Proceed with fallback path using raw JSON below
            proxyDTO = nil
        }

        // Save the response id only when explicitly requested (Q&A chaining)
        if captureResponseId, let newId = proxyDTO?.id, !newId.isEmpty {
            lastResponseId = newId
            Logger.shared.info("Updated lastResponseId to \(newId)", category: .services)
        }

        // 3. Extract the primary JSON text content from the DTO (with GPT-5 fallback)
        var extractedText = proxyDTO?.getPrimaryTextContent()
        if extractedText == nil {
            // Fallback for GPT‑5 Responses: try root `output_text` or walk `output[].content[]`
            if let root = try? JSONSerialization.jsonObject(with: rawProxyResponseData, options: []) as? [String: Any] {
                if let ot = root["output_text"] as? String, !ot.isEmpty {
                    extractedText = ot
                    Logger.shared.info("Used fallback output_text from response.", category: .parsing)
                } else if let outputs = root["output"] as? [[String: Any]] {
                    outer: for item in outputs {
                        if let content = item["content"] as? [[String: Any]] {
                            for piece in content {
                                if let textDict = piece["text"] as? [String: Any], let value = textDict["value"] as? String, !value.isEmpty {
                                    extractedText = value; break outer
                                }
                                if let val = piece["value"] as? String, !val.isEmpty { extractedText = val; break outer }
                                if let t = piece["text"] as? String, !t.isEmpty { extractedText = t; break outer }
                            }
                        }
                        if let message = item["message"] as? [String: Any], let content = message["content"] as? [[String: Any]] {
                            for piece in content {
                                if let textDict = piece["text"] as? [String: Any], let value = textDict["value"] as? String, !value.isEmpty { extractedText = value; break outer }
                            }
                        }
                    }
                    if extractedText != nil {
                        Logger.shared.info("Used fallback by walking output[].content[].", category: .parsing)
                    }
                }
            }
        }
        // If we still couldn't extract text, and the response indicates it hit max_output_tokens, try one continuation call
        if extractedText == nil {
            var shouldContinue = false
            if let status = (try? JSONSerialization.jsonObject(with: rawProxyResponseData) as? [String: Any])?["status"] as? String, status == "incomplete" {
                shouldContinue = true
            }
            if let id = proxyDTO?.id, !id.isEmpty,
               case .dict(let dict)? = proxyDTO?.incomplete_details,
               dict["reason"] == "max_output_tokens" {
                shouldContinue = true
            }
            if shouldContinue, let prevId = proxyDTO?.id, !prevId.isEmpty {
                // Build a minimal continuation payload
                let continuationModel = proxyDTO?.model ?? openAIModelName
                var cont: [String: Any] = [
                    "model": continuationModel,
                    "previous_response_id": prevId,
                    "max_output_tokens": 2200,
                    "store": true
                ]
                // Keep JSON formatting + low verbosity + minimal reasoning
                cont["text"] = [
                    "format": ["type": "json_object"],
                    "verbosity": "low"
                ]
                cont["reasoning"] = ["effort": "minimal"]

                let contBody = try JSONSerialization.data(withJSONObject: cont)
                let contData: Data = try await performRequest(
                    endpoint: "openai/responses",
                    method: "POST",
                    body: contBody,
                    timeout: 120
                )
                // Attempt to extract text again from continuation
                if let root = try? JSONSerialization.jsonObject(with: contData) as? [String: Any] {
                    if let ot = root["output_text"] as? String, !ot.isEmpty {
                        extractedText = ot
                        Logger.shared.info("Continuation call provided output_text.", category: .parsing)
                    } else if let outputs = root["output"] as? [[String: Any]] {
                        outer2: for item in outputs {
                            if let content = item["content"] as? [[String: Any]] {
                                for piece in content {
                                    if let textDict = piece["text"] as? [String: Any], let value = textDict["value"] as? String, !value.isEmpty { extractedText = value; break outer2 }
                                    if let val = piece["value"] as? String, !val.isEmpty { extractedText = val; break outer2 }
                                    if let t = piece["text"] as? String, !t.isEmpty { extractedText = t; break outer2 }
                                }
                            }
                        }
                        if extractedText != nil { Logger.shared.info("Continuation call yielded content[].text.", category: .parsing) }
                    }
                }
            }
        }

        guard let extractedJsonText = extractedText else {
            let dtoDescription = String(describing: proxyDTO)
            Logger.shared.error("No primary JSON text content found after DTO + fallback parse. DTO: \(dtoDescription.prefix(1000))", category: .parsing)
            throw NetworkError.decodingFailed
        }

        Logger.shared.debug("Extracted JSON text from DTO (length: \(extractedJsonText.count)) before sanitization: \(extractedJsonText.prefix(1000))", category: .parsing)

        // 4. Sanitize this JSON text and convert to Data for final decoding.
        var finalJsonData = sanitizedJSONData(from: extractedJsonText)
        if finalJsonData == nil, ResponseType.self == QAResponse.self {
            // Fallback for QA: best-effort extract of the answer field from raw text
            if let ans = APIManager.extractAnswer(from: extractedJsonText),
               let data = try? JSONSerialization.data(withJSONObject: ["answer": ans]) {
                finalJsonData = data
                Logger.shared.warning("QA fallback used to build JSON from extracted text.", category: .parsing)
            }
        }
        guard let finalJsonData else {
            Logger.shared.error("Failed to sanitize extracted JSON text or convert to Data. Original extracted text preview: \(extractedJsonText.prefix(500))", category: .parsing)
            throw NetworkError.decodingFailed
        }

        let finalJsonPreview = String(data: finalJsonData, encoding: .utf8)?.prefix(1000) ?? "Non-UTF8 data after sanitization"
        Logger.shared.info("Sanitized JSON data for final decoding (length: \(finalJsonData.count)) (preview): \(finalJsonPreview)", category: .parsing)

        // 5. Decode the finalJsonData into the expected ResponseType (e.g., ContentAnalysis)
        do {
            let finalResult = try JSONDecoder().decode(ResponseType.self, from: finalJsonData)
            Logger.shared.info("Successfully decoded final AI response into \(ResponseType.self).", category: .parsing)
            return finalResult
        } catch let finalDecodingError {
            // QA secondary fallback: try to recover answer directly if decode failed
            if ResponseType.self == QAResponse.self,
               let ans = APIManager.extractAnswer(from: extractedJsonText),
               let data = try? JSONSerialization.data(withJSONObject: ["answer": ans]),
               let recovered = try? JSONDecoder().decode(ResponseType.self, from: data) {
                Logger.shared.warning("QA secondary fallback succeeded after decode error.", category: .parsing)
                return recovered
            }
            let detailedErrorDesc = finalDecodingError.userFriendlyMessage
            Logger.shared.error("Failed to decode sanitized JSON into \(ResponseType.self). Error: \(detailedErrorDesc)", category: .parsing, error: finalDecodingError, extra: ["jsonPreview": finalJsonPreview])
            throw NetworkError.decodingFailed
        }
    }

    // MARK: - Minimal helper to recover QA answers from malformed responses
    private static func extractAnswer(from text: String) -> String? {
        // Heuristic: find the first occurrence of "answer" : "..."
        let pattern = #"\"answer\"\s*:\s*\"([\s\S]*?)\""#
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return nil }
        let ns = text as NSString
        let range = NSRange(location: 0, length: ns.length)
        guard let m = regex.firstMatch(in: text, range: range), m.numberOfRanges > 1 else { return nil }
        let r = m.range(at: 1)
        guard let swiftRange = Range(r, in: text) else { return nil }
        var ans = String(text[swiftRange])
        ans = ans.replacingOccurrences(of: "\\\"", with: "\"")
        ans = ans.replacingOccurrences(of: "\\n", with: "\n")
        return ans
    }

    // GPTServiceProtocol Methods

    func fetchContentAnalysis(
        transcript: String,
        videoTitle: String,
        comments: [String]
    ) async throws -> ContentAnalysis {
        // Deprecated combined call. Delegate to split calls and merge.
        let transcriptPart = try await fetchTranscriptSummary(transcript: transcript, videoTitle: videoTitle)
        let commentsPart = try await fetchCommentsClassification(comments: comments)
        return ContentAnalysis(
            longSummary: transcriptPart.longSummary,
            takeaways: transcriptPart.takeaways,
            gemsOfWisdom: transcriptPart.gemsOfWisdom,
            videoId: nil,
            videoTitle: videoTitle,
            videoDurationSeconds: nil,
            videoThumbnailUrl: nil,
            CommentssentimentSummary: commentsPart.CommentssentimentSummary,
            topThemes: commentsPart.topThemes,
            categorizedComments: commentsPart.categorizedComments
        )
    }

    // MARK: - Split calls for higher reliability
    struct TranscriptOnlyResponse: Codable { let longSummary: String; let takeaways: [String]; let gemsOfWisdom: [String] }
    struct CommentsOnlyResponse: Codable { let CommentssentimentSummary: String; let topThemes: [CommentTheme]?; let categorizedComments: [CategorizedCommentAI] }

    func fetchTranscriptSummary(transcript: String, videoTitle: String) async throws -> TranscriptOnlyResponse {
        let cleaned = transcript.cleanedForLLM()
        let tx = cleaned.isEmpty ? transcript : cleaned
        let prompt = """
You are WorthIt, AI video summarizer.

TASK: Read the transcript and output EXACTLY ONE valid JSON object with ONLY these keys:
{"longSummary": string, "takeaways": [string,string,string], "gemsOfWisdom": [string,string]}

GLOBAL RULES (obey all):
- Output valid JSON only. No extra text.
- Use double quotes for keys/values. Inside ALL string values, convert every double quote (") to single quote ('). Use real line breaks between lines; JSON will escape them automatically (do not print the literal tokens "\\n" or "/n").
- Use ONLY transcript content. Ignore sponsors/ads/meta talk. Write in the transcript’s language. Do not mention the audience, channel, or comments.
- If the transcript is empty or < 50 words, return {"longSummary":"","takeaways":[],"gemsOfWisdom":[]}.
- Capitalize the first word of every sentence and bullet in "longSummary" and "takeaways". (Do NOT alter capitalization in "gemsOfWisdom".)

FIELD RULES:
1) "longSummary" (≤ 250 words) — provide a proper summary of the video, structure MUST be:
   - First line: "Highlights:"
   - Next exactly two lines, each starting with "• " (≤ 18 words):
     • Bullet 1 = core claim (what this is about).
     • Bullet 2 = method/result (how it’s argued, a key step, or concrete outcome). If an exact named term exists, include it; otherwise use a concrete detail (number/example).
   - Then a blank line (one empty line between bullets and narrative; do not print the characters "\\n" or "/n" literally).
   - Then 2–4 natural sentences, each on its own line (one line break between sentences; no extra blank lines). Do not write the literal characters "\\n" or "/n"; just separate lines — JSON will escape newlines. Simply write cohesive prose. Include concepts/definitions/rules EXACTLY as they appear (preserve casing/spelling) when present, all in the transcript.
   - Do not output standalone quote characters or empty quoted lines; write only meaningful content lines.

2) "takeaways": the top 3 takeaways from the video transcript, exactly 3 items, each 12–22 words. Make them applied and distinct from "Highlights" in the summary section:
   - If the transcript provides formal items (e.g., 'Rule', 'Step', 'Principle'), you may start with that label; otherwise avoid forced labels and write a clear, actionable sentence.
   - Each takeaway must include either (a) an exact term/step/rule/framework from the transcript (in single quotes), or (b) a specific condition/number/example. Avoid generic platitudes and avoid rephrasing the two bullets.

3) "gemsOfWisdom": find the wisdom in the transcript, and provide exactly 2 verbatim one-liners from it (≤ 24 words). Selection criteria (must pass all):
   - Self-contained and generalizable (principle, rule of thumb, cause→effect, contrast, or 'if/when… then…' guidance).
   - No CTAs, links, channel mentions, or time references; no rhetorical questions unless they read as a maxim.
   - Practical, useful quotes from the transcript, really try to find the gems of wisdom of it.
   - Prefer sentences containing words like always/never, because/therefore/so, key/the key, principle/rule/law, or a crisp comparison ('X beats Y').
   Keep original wording/punctuation; only convert internal double quotes to single quotes.

QUALITY CHECK (do silently before returning):
- JSON parses; exactly 3 keys; arrays are lengths [3] and [2].
- "longSummary" format passes: header → two "• " bullets → a single blank line → 2–4 sentences on separate lines; total ≤ 250 words; must not contain the literal tokens "\\n" or "/n" as visible text.
- No double quotes inside values; newlines will be escaped automatically in JSON strings.
- Takeaways are actionable, non-overlapping with Highlights, and include a term or a concrete detail.
- Gems are verbatim, principle-like lines that stand alone (after quote conversion only).
- Final self-check & repair: if any takeaway lacks a single-quoted exact term or a concrete number/example, revise it to include one; if any double quotes remain inside values, convert them to single quotes before returning.

Transcript:
\(tx.prefix(15000))
"""
        let selectedModel: String = transcript.count > 15000 ? "gpt-5-nano-2025-08-07" : openAIModelName
        let requestPayload = OpenAIProxyRequest<String>(model: selectedModel, max_output_tokens: 1200, promptInput: prompt)
        let result: TranscriptOnlyResponse = try await performOpenAIRequest(payload: requestPayload)
        return result
    }

    func fetchCommentsClassification(comments: [String]) async throws -> CommentsOnlyResponse {
        let count = comments.count
        let limit = min(50, count)
        let numbered = comments.prefix(limit)
            .enumerated()
            .map { "\($0.offset + 1). \($0.element)" }
            .joined(separator: "\n")

        let prompt = """
You are WorthIt.AI. Return EXACTLY ONE valid JSON object with comments-only fields:
{"CommentssentimentSummary": string, "topThemes": [{"theme": string, "sentiment": "Positive|Negative|Mixed|Neutral", "sentimentScore": number, "exampleComment": string}], "categorizedComments": [{"index": number, "category": "humor|insightful|controversial|spam|neutral"}]}

Rules:
- Valid JSON only (no extra prose or fences). Do not emit newline characters; replace each newline with a single space. Keep double quotes as-is unless JSON escaping requires \".
- Use COMMENTS ONLY. Do not reference transcript.
- CommentssentimentSummary must concisely capture the prevailing tone AND any notable minority signals (e.g., requests for full episodes, questions about therapy or next steps, concerns), without referencing the transcript. Keep it to 1–2 sentences and ≤ 220 characters.
- If comments_count == 0 ⇒ topThemes = [] and categorizedComments = [].
- Categorize indices 1..K where K is the number of comments listed above (up to 50). Indices must be contiguous, strictly increasing, and have exactly one entry per index.
- exampleComment: ≤80 chars and MUST be copied verbatim from one comment shown above. Do not alter capitalization, punctuation, emoji, spacing, or quotation marks. Choose a contiguous slice from a single comment. If the substring includes a double quote, escape it as \" so the JSON stays valid.
- topThemes: when there are at least two distinct themes supported by different comments, return exactly 2 or 3 items (each backed by a different comment). If you cannot find two distinct themes, return [].
- categorizedComments: exactly one entry per index in ascending order, no gaps or duplicates, using only the 5 allowed categories.
- Sentiment-score ranges: Positive [0.2,1], Negative [-1,-0.2], Neutral [-0.2,0.2], Mixed near 0. Round sentimentScore to 2 decimals.
- Categories MUST be one of: humor, insightful, controversial, spam, neutral. Never use sentiment words (e.g., positive/negative/mixed) here; if unsure, use neutral.
- Ensure topThemes.exampleComment strings come from distinct comments and are ≤80 chars (choose a contiguous substring if necessary).
- Final self-check & repair: confirm each exampleComment is an exact slice of a comment and that you produced 2 or 3 themes when possible.

Comments (1..N):
\(numbered)

comments_count: \(count)
"""
        let requestPayload = OpenAIProxyRequest<String>(model: openAIModelName, max_output_tokens: 1200, promptInput: prompt)
        let result: CommentsOnlyResponse = try await performOpenAIRequest(payload: requestPayload)
        return result
    }


    func fetchCommentInsights(comments: [String], transcriptContext: String) async throws -> CommentInsights {
        let cleanedCtx = transcriptContext.cleanedForLLM()
        let ctxForPrompt: String = {
            let pref = String((cleanedCtx.isEmpty ? transcriptContext : cleanedCtx).prefix(25000))
            return pref
        }()
        let prompt = """
You are WorthIt.AI Comment+Transcript Rater.

TASK
Return exactly ONE JSON object with these keys and nothing else (no extra keys):
{
  "overallCommentSentimentScore": float,  
  "contentDepthScore": float,              
  "suggestedQuestions": [string, string, string] 
}

STRICT OUTPUT RULES
- Valid JSON only. No markdown, no prose, no code fences, no trailing commas.
- Floats must be in [0,1], rounded to 2 decimals (e.g., 0.63).
- If comments are empty/insufficient ⇒ overallCommentSentimentScore = null.
- If transcript is empty/insufficient ⇒ contentDepthScore = null and suggestedQuestions = null.
- Language for suggestedQuestions: match the transcript language; if unclear, use English.
- Questions must be unique, concrete, engaging, derived from the transcript; max 6 words; no emojis/filler.

SCORING RUBRIC (apply consistently)
- overallCommentSentimentScore (comments ONLY):
  0.00–0.19 = overwhelmingly negative; 0.20–0.39 = mostly negative;
  0.40–0.60 = mixed/neutral baseline; 0.61–0.80 = mostly positive;
  0.81–1.00 = overwhelmingly positive.
  Ignore transcript; de-duplicate near-identical comments; down-weight spam/bots; handle sarcasm/emoji as intended. If unclear, treat as neutral.
- contentDepthScore (transcript ONLY — “will this add real value to my life?”). Score in [0,1] (2 decimals) using a weighted blend over MAIN content (ignore intros/outros/sponsors):
  • Actionability (0.30): clear how‑to steps, checklists, decision rules, examples that a viewer can apply immediately.
  • Specificity & Evidence (0.20): concrete numbers, data, case studies, named methods/frameworks, benchmarks.
  • Conceptual Depth (0.20): explains why/how; mechanisms, trade‑offs, assumptions, edge cases.
  • Transferability (0.15): principles generalize beyond the specific example; works in adjacent contexts.
  • Novelty (0.10): non‑obvious insights; corrects common misconceptions.
  • Caveats (0.05): limits, risks, failure modes, when not to use it.
  Think like a pragmatic learner optimizing for life/career impact. Prefer density over length. Penalize fluff, repetition, vague motivation, or generic product pitches without concrete guidance. Reward content that is both “doable” and “well‑reasoned”.
  Calibration anchors (not output): ~0.20 if mostly surface‑level hype with few specifics; ~0.80 if it provides clear steps, named methods, examples, and caveats.
  Guardrails: Avoid extremes (>0.90 or <0.10) unless multiple dimensions are clearly outstanding/deficient across the transcript.

SUGGESTED QUESTIONS (quality bar)
- Pick the 3 most interesting, high‑ROI learning prompts a viewer would want after reading the full transcript.
- Cover diverse topics (not 3 variants of the same thing) and target the biggest knowledge gains.
- Make them curiosity‑inducing and specific to the transcript (avoid generic “what is X?” if it’s already explained).
- Keep ≤6 words each, no emojis/filler; match transcript language.

QUALITY CHECK (silent)
Before returning, verify:
- Keys present exactly as specified.
- Floats within [0,1], 2 decimals; nulls only when data missing.
- suggestedQuestions length = 3, each ≤ 6 words, unique, derived from transcript, highest learning value.
 - contentDepthScore reflects rubric (no high score if primarily promo/fluff; no low score if clear, actionable depth is present).


===  Data  ===
Transcript (max. 25000 chars):
\(ctxForPrompt)

            Comments (first up to 25 lines):
            \(comments.prefix(25).joined(separator: "\\n"))


Return only the JSON object. Stop immediately after the final "}".
"""
        let requestPayload = OpenAIProxyRequest<String>(
            model: openAIModelName,
            max_output_tokens: 1500,
            promptInput: prompt
        )
        let result: CommentInsights = try await performOpenAIRequest(payload: requestPayload)
        self.cachedCommentInsights = result
        return result
    }

    func answerQuestion(transcript: String, question: String, history: [ChatMessage]) async throws -> QAResponse {
        let prompt: String

        if lastResponseId == nil {
            // ── FIRST QUESTION ── full context
            let historyString = history
                .map { ($0.isUser ? "User" : "AI") + ": " + $0.content }
                .joined(separator: "\n")

            prompt = """
              You are WorthIt.AI Tutor — a clear, friendly guide. Answer based on the provided transcript and chat history.
              
              Language:
              • Detect the language of the user's question and respond in that same language. Do not switch languages.
              
              Style and structure:
              • Direct questions (fact/date/quote): answer concisely, cite the transcript when possible. In this case keep the answer ≤120 words in a single paragraph
              • Open-ended/complex: write ≥30 words with this mini-structure:
              - Quick Context: one sentence of relevant transcript context answering the question
              - Main Answer: 2–4 concise bullets with practical clarity
              - Conclusion: optional one-liner wrap-up
              When info seems missing in the transcript:
              • Do not invent facts.
              • Still be helpful: (1) state what the transcript says that’s closest, (2) offer next steps or a clarifying question, (3) optionally add clearly labeled general background if it’s common knowledge.
              Example fallback inside the single answer string:
              What we know from transcript: …
              Helpful next step: …
              General context (not in transcript): …
              
            Output format requirement:
            • Valid JSON only. No markdown, no prose, no code fences.
            • Return EXACTLY one JSON object with ONLY this key and nothing else:
              { "answer": "string" }
            • Do not add any other keys (e.g., Helpful next steps, General context). If you have extras, include them inside the single answer string.
            • Escape any internal double quotes within the answer value (use \" inside JSON strings) to preserve valid JSON.
            • ; avoid long lists; ensure you emit the final closing '}'.

            Transcript (≤15k chars):
            \(transcript.prefix(15000))

            Chat History:
            \(historyString)

            User Question: \(question)

            Be warm, pragmatic, and actionable.
            """
        } else {
            // ── FOLLOW-UP ── improved structure and tone
            let condensedTranscript = transcript.prefix(5000)
            let recentHistory = history.suffix(3)
                .map { ($0.isUser ? "User" : "AI") + ": " + $0.content }
                .joined(separator: "\n")

            prompt = """
            You are WorthIt.AI Tutor — clear, practical, and helpful.

            Quick refresher:
            Transcript excerpt (≤1k chars):
            \(condensedTranscript)

            Recent chat:
            \(recentHistory)

            Guidance:
            • Direct questions (fact/date/quote): answer concisely, cite transcript when possible.
            • Open-ended/complex: write ≥30 words with mini-structure (Quick Context → Main Answer (use bullets) → optional Conclusion).
            • If details aren’t explicit in the transcript, do not invent facts. Instead, be helpful by:
              - Stating what is known from the transcript that relates to the question
              - Offering a clarifying question or next step (e.g., a timestamp/topic to search)
              - Optionally adding clearly labeled general background if commonly known

            Language:
            • Detect the language of the user's question and respond in that same language. Do not switch languages.

            Output format requirement:
            • Valid JSON only. No markdown, no prose, no code fences.
            • Return EXACTLY one JSON object with ONLY this key and nothing else:
              { "answer": "string" }
            • Do not add any other keys (e.g., Helpful next steps, General context). If you have extras, include them inside the single answer string.
            • Escape any internal double quotes within the answer value (use \" inside JSON strings) to preserve valid JSON.
            • Keep the answer ≤120 words in a single paragraph; avoid long lists; ensure you emit the final closing '}'.

            User Question: \(question)
            """
        }
        let requestPayload = OpenAIProxyRequest<String>(
            model: openAIModelName,
            max_output_tokens: 800,
            promptInput: prompt,
            previous_response_id: lastResponseId
        )
        do {
            return try await performOpenAIRequest(payload: requestPayload, captureResponseId: true)
        } catch let NetworkError.network(err as URLError) {
            // Lightweight retry for transient network errors in QA
            if err.code == .networkConnectionLost || err.code == .timedOut {
                Logger.shared.warning("QA request transient failure (\(err.code.rawValue)). Retrying once...", category: .networking)
                try await Task.sleep(nanoseconds: 300_000_000)
                return try await performOpenAIRequest(payload: requestPayload, captureResponseId: true)
            }
            throw NetworkError.network(err)
        } catch {
            throw error
        }
    }

 
    // MARK: - Pre-warm backend to avoid cold-start latency
    func preWarm() {
        // Use configured baseURL and a lightweight GET to '/'
        var req = URLRequest(url: baseURL)
        req.httpMethod = "GET"
        req.timeoutInterval = 5

        let session = self.session
        Task.detached {
            do {
                _ = try await session.data(for: req)
                Logger.shared.info("Pre-warm GET / succeeded", category: .services)
            } catch {
                Logger.shared.warning("Pre-warm GET / failed: \(error.localizedDescription)", category: .services)
            }
        }
    }

    // MARK: - Conversation reset (public)
    /// Clears GPT thread ID and cached intro data.
    func resetConversationState() {
        self.lastResponseId = nil
        self.cachedCommentInsights = nil
        Logger.shared.info("APIManager conversation state cleared.", category: .services)
    }
}
        
