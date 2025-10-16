//
//  MainViewModel.swift
//  WorthIt
//

import SwiftUI
import Combine
import OSLog // For Logger

struct TimeSavedEvent: Identifiable, Equatable {
    let id = UUID()
    let minutes: Double
    let cumulativeMinutes: Double
    let alreadyCounted: Bool
    let context: TimeSavedContext?
}

struct TimeSavedContext: Equatable {
    enum Kind: Equatable {
        case milestone
        case streak
        case weeklyBest
    }

    let kind: Kind
    let iconName: String
    let chipText: String
    let headline: String
    let detail: String
}

private struct TimeSavedLogEntry: Codable, Equatable {
    enum Source: String, Codable {
        case mainApp
        case shareExtension
    }

    let minutes: Double
    let timestamp: Date
    let videoId: String
    let source: Source
}

@MainActor
class MainViewModel: ObservableObject {

    // MARK: - Published Properties for UI State
    @Published var viewState: ViewState = .idle { // Now finds ViewState from Models.swift
        didSet {
            guard oldValue != viewState else { return }
            Logger.shared.notice(
                "ViewState changed \(oldValue) → \(viewState)",
                category: .ui,
                extra: ["source": "MainViewModel"]
            )
        }
    }
    @Published var currentScreenOverride: ViewState? = nil
    @Published var processingProgress: Double = 0.0
    @Published var userFriendlyError: UserFriendlyError? = nil // Now finds UserFriendlyError from Models.swift

    // MARK: - Published Properties for Data Results
    @Published var analysisResult: ContentAnalysis?
    @Published var worthItScore: Double?
    @Published var scoreBreakdownDetails: ScoreBreakdown?
    /// Quick metrics returned by the light‑weight GPT pass before a full comment scan
    @Published var essentialsCommentAnalysis: EssentialsCommentAnalysis?

    @Published var qaMessages: [ChatMessage] = []
    @Published var qaInputText: String = ""
    @Published var isQaLoading: Bool = false

    // New: Categorized comment lists for UI
    @Published var funnyComments: [String] = []
    @Published var insightfulComments: [String] = []
    @Published var controversialComments: [String] = []
    @Published var spamComments: [String] = []
    @Published var neutralComments: [String] = []
    private var allFetchedComments: [String] = [] // Full fetched comments
    private var llmAnalyzedComments: [String] = [] // The truncated array sent to LLM (up to 50)
    @Published var suggestedQuestions: [String] = [] // From Essentials (fast analysis)
    @Published var hasCommentsAvailableForError: Bool = false
    @Published var activePaywall: PaywallContext? = nil
    @Published var shouldOpenManageSubscriptions: Bool = false

    private(set) var currentVideoID: String?
    @Published var currentVideoTitle: String?
    @Published var currentVideoThumbnailURL: URL?

    var rawTranscript: String?
    private var transcriptForAnalysis: String?

    @Published var latestTimeSavedEvent: TimeSavedEvent? {
        didSet {
            guard latestTimeSavedEvent?.id != oldValue?.id else { return }
            if let event = latestTimeSavedEvent {
                var extras: [String: Any] = [
                    "minutes": event.minutes,
                    "cumulative_minutes": event.cumulativeMinutes,
                    "already_counted": event.alreadyCounted
                ]
                if let context = event.context {
                    extras["context_kind"] = String(describing: context.kind)
                }
                Logger.shared.notice(
                    "Time-saved event queued",
                    category: .timeSavings,
                    extra: extras
                )
            } else if oldValue != nil {
                Logger.shared.debug("Time-saved event cleared", category: .timeSavings)
            }
        }
    }
    @Published private(set) var cumulativeTimeSavedMinutes: Double = 0
    @Published private(set) var uniqueVideosSummarized: Int = 0

    private var activeAnalysisTask: Task<Void, Never>? = nil
    private var fullAnalysisTask: Task<Void, Never>? = nil
    private var minimumDisplayTimerTask: Task<Void, Never>? = nil
    // Keep UI snappy: shorter minimum loader time while avoiding flicker
    private let minimumDisplayTime: TimeInterval = 3.5

    private var processedVideoIDsThisSession = Set<String>()

    private let totalTimeSavedKey = "com.worthitai.totalTimeSavedMinutes"
    private let timeSavedVideoIDsKey = "com.worthitai.timeSavedVideoIDs"
    private let lastDisplayedTimeSavedKey = "com.worthitai.lastDisplayedTimeSavedTotal"
    private let timeSavedLogKey = "com.worthitai.timeSavedLog"
    private let speechWordsPerMinute: Double = 200.0
    private let commentClassificationLimit = 50
    private let commentInsightsLimit = 25
    private var timeSavedVideoIDs: Set<String> = []
    private var timeSavedLogEntries: [TimeSavedLogEntry] = []
    private let maxTimeSavedLogEntries = 200
    private let logRetentionDays = 120

    private var appCalendar: Calendar = {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone.current
        calendar.locale = Locale.current
        return calendar
    }()

    private let apiManager: APIManager
    private let cacheManager: CacheManager
    private let subscriptionManager: SubscriptionManager
    private let usageTracker: UsageTracker
    private let sharedDefaults: UserDefaults
    private var cancellables = Set<AnyCancellable>()
    @Published private(set) var isUserSubscribed: Bool = false
    private var usageReservations: [String: Bool] = [:]
    private var latestUsageSnapshot: UsageTracker.Snapshot?
    private var paywallLoggedImpression = false

    private var isRunningInExtension: Bool {
        Bundle.main.bundlePath.lowercased().contains(".appex")
    }

    struct PaywallContext {
        let reason: PaywallReason
        let usageSnapshot: UsageTracker.Snapshot
    }

    enum PaywallReason: String {
        case dailyLimitReached
        case manual

        var analyticsLabel: String {
            switch self {
            case .dailyLimitReached: return "daily_limit"
            case .manual: return "manual"
            }
        }
    }

    // Streaming removed: we now render a skeleton until full analysis is ready

    init(apiManager: APIManager, cacheManager: CacheManager, subscriptionManager: SubscriptionManager, usageTracker: UsageTracker) {
        self.apiManager = apiManager
        self.cacheManager = cacheManager
        self.subscriptionManager = subscriptionManager
        self.usageTracker = usageTracker
        self.sharedDefaults = UserDefaults(suiteName: AppConstants.appGroupID) ?? .standard
        self.isUserSubscribed = subscriptionManager.isSubscribed
        Logger.shared.info("MainViewModel initialized.", category: .lifecycle)
        self.cumulativeTimeSavedMinutes = sharedDefaults.double(forKey: totalTimeSavedKey)
        if let storedIDs = sharedDefaults.array(forKey: timeSavedVideoIDsKey) as? [String] {
            self.timeSavedVideoIDs = Set(storedIDs)
        }
        self.uniqueVideosSummarized = timeSavedVideoIDs.count
        self.timeSavedLogEntries = loadTimeSavedLogFromDefaults()
        pruneTimeSavedLogIfNeeded()
        persistTimeSavedLogIfNeeded()

        NotificationCenter.default.publisher(for: UserDefaults.didChangeNotification, object: sharedDefaults)
            .receive(on: RunLoop.main)
            .sink { [weak self] _ in
                self?.syncTimeSavedMetricsFromSharedDefaults(presentEventIfNeeded: false)
            }
            .store(in: &cancellables)

        subscriptionManager.$status
            .receive(on: RunLoop.main)
            .sink { [weak self] status in
                guard let self else { return }
                self.isUserSubscribed = status.isSubscribed
                if status.isSubscribed {
                    self.dismissPaywall(afterSuccessfulPurchase: true)
                    self.latestUsageSnapshot = nil
                    self.usageReservations.removeAll()
                    Task { [weak self] in
                        await self?.usageTracker.clearAll()
                    }
                }
            }
            .store(in: &cancellables)
    }

    // Limit and sanitize suggested questions to at most 3 concise, unique items
    private func normalizeSuggestedQuestions(_ questions: [String]) -> [String] {
        var seen = Set<String>()
        var result: [String] = []
        for raw in questions {
            let q = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            if q.isEmpty { continue }
            let key = q.lowercased()
            if seen.contains(key) { continue }
            seen.insert(key)
            result.append(q)
            if result.count == 3 { break }
        }
        return result
    }

    // Clip each takeaway to a maximum word count (non-destructive for shorter ones)
    

    private func releaseUsageReservation(for videoId: String, revertIfNeeded: Bool) {
        guard let wasCounted = usageReservations.removeValue(forKey: videoId) else { return }
        if revertIfNeeded && wasCounted {
            Task {
                await usageTracker.remove(videoId: videoId, dailyLimit: AppConstants.dailyFreeAnalysisLimit)
            }
        }
    }

    func handleDeepLink(_ url: URL) -> Bool {
        guard url.scheme == AppConstants.urlScheme else { return false }
        let host = (url.host ?? "").lowercased()
        let path = url.path.lowercased()
        if host == "subscribe" || path.contains("subscribe") {
            if isUserSubscribed {
                AnalyticsService.shared.logPaywallDeepLinkOpened()
                paywallManageTapped()
                shouldOpenManageSubscriptions = true
            } else {
                Task { @MainActor in
                    let snapshot = await usageTracker.snapshot(dailyLimit: AppConstants.dailyFreeAnalysisLimit)
                    presentPaywall(reason: .manual, usageSnapshot: snapshot)
                    AnalyticsService.shared.logPaywallDeepLinkOpened()
                }
            }
            return true
        }
        return false
    }

    func processSharedURL(_ url: URL) {
        Task { @MainActor in
            await self.processSharedURLInternal(url)
        }
    }

    private func processSharedURLInternal(_ url: URL) async {
        Logger.shared.info("Processing shared URL: \(url.absoluteString)", category: .lifecycle)

        guard let videoId = extractVideoID(from: url) else {
            setPublicError(message: "Invalid video link. Please share a valid video URL.", canRetry: false)
            return
        }

        Logger.shared.info("Extracted Video ID: \(videoId)", category: .lifecycle)

        var allowanceSnapshot: UsageTracker.Snapshot?
        var reservationWasCounted = false

        if !isUserSubscribed {
            await subscriptionManager.ensureEntitlementIfUnknown()
            if subscriptionManager.isSubscribed {
                isUserSubscribed = true
            }
        }

        if !isUserSubscribed {
            let allowance = await usageTracker.registerAttempt(for: videoId, dailyLimit: AppConstants.dailyFreeAnalysisLimit)
            allowanceSnapshot = allowance.snapshot
            latestUsageSnapshot = allowance.snapshot

            if allowance.allowed == false {
                AnalyticsService.shared.logPaywallLimitHit(videoId: videoId, used: allowance.snapshot.count, limit: allowance.snapshot.limit)
                presentPaywall(reason: .dailyLimitReached, usageSnapshot: allowance.snapshot)
                return
            }

            reservationWasCounted = allowance.wasCounted
        } else {
            latestUsageSnapshot = nil
        }

        resetForNewAnalysis()

        self.currentVideoID = videoId
        self.currentVideoThumbnailURL = URL(string: "https://i.ytimg.com/vi/\(videoId)/hq720.jpg")
        latestUsageSnapshot = allowanceSnapshot
        usageReservations[videoId] = isUserSubscribed ? false : reservationWasCounted

        // Log analytics events
        AnalyticsService.shared.logShareExtensionUsed(videoId: videoId)
        AnalyticsService.shared.logVideoAnalysisStarted(videoId: videoId)

        // Check for cached results (both session and disk cache)
        let cache = cacheManager

        if let cachedAnalysis = await cache.loadContentAnalysis(for: videoId) {
            Logger.shared.info("processSharedURL cache HIT for \(videoId).", category: .cache)
            
            // Populate view model from cache
            self.analysisResult = cachedAnalysis
            self.essentialsCommentAnalysis = await cache.loadCommentInsights(for: videoId)
            self.rawTranscript = await cache.loadTranscript(for: videoId)
            self.transcriptForAnalysis = self.rawTranscript?.stripTimestamps()
            self.currentVideoTitle = cachedAnalysis.videoTitle
            
            // Set thumbnail URL
            let fallback = "https://i.ytimg.com/vi/\(videoId)/hqdefault.jpg"
            let thumb = cachedAnalysis.videoThumbnailUrl
            self.currentVideoThumbnailURL = URL(string: (thumb?.isEmpty == false ? thumb! : fallback))

            // Process categorized comments for UI
            if let categorized = cachedAnalysis.categorizedComments {
                let cachedComments = await cache.loadComments(for: videoId) ?? []
                let truncated = Array(cachedComments.prefix(commentClassificationLimit))
                self.llmAnalyzedComments = truncated
                
                self.funnyComments = categorized.filter { $0.category == "humor" }.compactMap { cat in
                    let idx = cat.index - 1 // model uses 1-based indices per prompt
                    guard idx >= 0 && idx < truncated.count else { return nil }
                    return truncated[idx]
                }
                self.insightfulComments = categorized.filter { $0.category == "insightful" }.compactMap { cat in
                    let idx = cat.index - 1
                    guard idx >= 0 && idx < truncated.count else { return nil }
                    return truncated[idx]
                }
                self.controversialComments = categorized.filter { $0.category == "controversial" }.compactMap { cat in
                    let idx = cat.index - 1
                    guard idx >= 0 && idx < truncated.count else { return nil }
                    return truncated[idx]
                }
                self.spamComments = categorized.filter { $0.category == "spam" }.compactMap { cat in
                    let idx = cat.index - 1
                    guard idx >= 0 && idx < truncated.count else { return nil }
                    return truncated[idx]
                }
                self.neutralComments = categorized.filter { $0.category == "neutral" }.compactMap { cat in
                    let idx = cat.index - 1
                    guard idx >= 0 && idx < truncated.count else { return nil }
                    return truncated[idx]
                }
            }
            
            // Restore suggested questions from essentials cache only
            if let cachedEssentials = self.essentialsCommentAnalysis {
                self.suggestedQuestions = normalizeSuggestedQuestions(cachedEssentials.suggestedQuestions)
            }

            // Calculate score and update progress
            self.calculateAndSetWorthItScore()
            self.processingProgress = 1.0

            // Show options immediately without waiting
            self.viewState = .showingInitialOptions
            // Add to processed set so future loads are instant
            processedVideoIDsThisSession.insert(videoId)
            releaseUsageReservation(for: videoId, revertIfNeeded: false)
            return
        }

        // No cache hit: enter processing state
        self.viewState = .processing
        // Only start minimum display timer if we don't have cached results
        startMinimumDisplayTimer()

        activeAnalysisTask = Task(priority: .userInitiated) {
            do {
                // Step 1: Fetch transcript and comments in parallel
                async let transcriptTask = fetchTranscript(videoId: videoId)
                async let commentsTask = fetchComments(videoId: videoId)
                let transcriptOpt = try? await transcriptTask
                let comments = (try? await commentsTask) ?? []

                // --- Transcript Validation ---
                guard let transcript = transcriptOpt, !transcript.isEmpty else {
                    Logger.shared.warning("Transcript unavailable for \(videoId). Prompting user to try another video.", category: .services)
                    self.hasCommentsAvailableForError = false
                    self.allFetchedComments = []
                    setPublicError(message: "Transcript unavailable.", canRetry: true, videoIdForRetry: videoId)
                    updateProgress(1.0, message: "Transcript Unavailable")
                    return
                }
                self.rawTranscript = transcript
                self.transcriptForAnalysis = transcript.stripTimestamps()
                await cache.saveTranscript(transcript, for: videoId)
                updateProgress(0.4, message: "Transcript Ready")

                // --- Comments Handling ---
                await cache.saveComments(comments, for: videoId)
                updateProgress(0.5, message: "Comments Ready")
                self.allFetchedComments = comments // Store all fetched
                // Store the truncated array sent to LLM for index mapping
                let truncated = Array(comments.prefix(commentClassificationLimit))
                self.llmAnalyzedComments = truncated

                guard let finalTranscriptForAnalysis = self.transcriptForAnalysis else {
                    Logger.shared.critical("transcriptForAnalysis is nil despite successful fetch for \(videoId).", category: .services)
                    setPublicError(message: "Internal error preparing transcript.", canRetry: true, videoIdForRetry: videoId)
                    return
                }

                let titleForGpt = self.currentVideoTitle ?? videoId

                // --- FULL ANALYSIS (primary) ---
                // Launch full analysis (streaming). We no longer run fast analysis in parallel.
        self.fullAnalysisTask?.cancel()
        self.fullAnalysisTask = Task(priority: .userInitiated) {
            await self.performNonStreamingAnalysis(
                videoId: videoId,
                transcript: finalTranscriptForAnalysis,
                comments: comments,
                titleForGpt: titleForGpt
            )
        }

        // Streaming removed; skeleton UI will show until analysis completes.
            } catch {
                Logger.shared.error("Error in main analysis Task: \(error.localizedDescription)", category: .services, error: error)
                setPublicError(message: error.localizedDescription, canRetry: true, videoIdForRetry: videoId)
                updateProgress(1.0, message: "Analysis Failed")
            }
        }
    }

    // Proceed with comments-only insights when transcript is missing
    func reviewCommentsOnly() {
        guard let videoId = currentVideoID else { return }
        clearCurrentError()
        let existingComments = allFetchedComments
        let cache = cacheManager

        Task {
            do {
                let comments: [String]
                if existingComments.isEmpty {
                    comments = await cache.loadComments(for: videoId) ?? []
                } else {
                    comments = existingComments
                }
                let limitedComments = Array(comments.prefix(commentInsightsLimit))
                let essentials: EssentialsCommentAnalysis? = limitedComments.isEmpty ? nil : try await apiManager.fetchCommentInsights(comments: limitedComments, transcriptContext: "")

                await MainActor.run {
                    self.essentialsCommentAnalysis = essentials
                    if let essentials = essentials {
                        self.suggestedQuestions = self.normalizeSuggestedQuestions(essentials.suggestedQuestions)
                    } else {
                        self.suggestedQuestions = []
                    }
                    self.calculateAndSetWorthItScore()
                    self.hasCommentsAvailableForError = false
                    self.viewState = .showingInitialOptions
                }

                if let essentials = essentials {
                    await cache.saveCommentInsights(essentials, for: videoId)
                }
            } catch {
                Logger.shared.error("Comments-only insights failed: \(error.localizedDescription)", category: .services, error: error)
                await MainActor.run {
                    self.essentialsCommentAnalysis = nil
                    self.suggestedQuestions = []
                    self.calculateAndSetWorthItScore()
                    self.viewState = .showingInitialOptions
                }
            }
        }
    }

    /// Restore UI state from cached results for a given video ID without triggering processing.
    func restoreFromCache(videoId: String) async {
        Logger.shared.info("Restoring from cache for videoId: \(videoId)", category: .cache)
        // Reset any in-flight tasks/UI state, but keep caches
        resetForNewAnalysis()

        self.currentVideoID = videoId

        let cache = cacheManager

        guard let cachedAnalysis = await cache.loadContentAnalysis(for: videoId) else {
            Logger.shared.warning("restoreFromCache: No cached analysis found for \(videoId)", category: .cache)
            // Fall back to showing idle with basic thumbnail
            self.currentVideoThumbnailURL = URL(string: "https://i.ytimg.com/vi/\(videoId)/hq720.jpg")
            self.currentVideoTitle = videoId
            return
        }

        // Populate view model from cache
        self.analysisResult = cachedAnalysis
        self.essentialsCommentAnalysis = await cache.loadCommentInsights(for: videoId)
        self.rawTranscript = await cache.loadTranscript(for: videoId)
        self.transcriptForAnalysis = self.rawTranscript?.stripTimestamps()
        self.currentVideoTitle = cachedAnalysis.videoTitle

        // Set thumbnail URL (UI layer will try higher-res fallbacks automatically)
        let fallback = "https://i.ytimg.com/vi/\(videoId)/hqdefault.jpg"
        let thumb = cachedAnalysis.videoThumbnailUrl
        self.currentVideoThumbnailURL = URL(string: (thumb?.isEmpty == false ? thumb! : fallback))

        // Process categorized comments for UI using cached list
        let cachedComments = await cache.loadComments(for: videoId) ?? []
        let truncated = Array(cachedComments.prefix(commentClassificationLimit))
        self.llmAnalyzedComments = truncated
        if let categorized = cachedAnalysis.categorizedComments {
            self.funnyComments = categorized.filter { $0.category == "humor" }.compactMap { cat in
                let idx = cat.index - 1
                guard idx >= 0 && idx < truncated.count else { return nil }
                return truncated[idx]
            }
            self.insightfulComments = categorized.filter { $0.category == "insightful" }.compactMap { cat in
                let idx = cat.index - 1
                guard idx >= 0 && idx < truncated.count else { return nil }
                return truncated[idx]
            }
            self.controversialComments = categorized.filter { $0.category == "controversial" }.compactMap { cat in
                let idx = cat.index - 1
                guard idx >= 0 && idx < truncated.count else { return nil }
                return truncated[idx]
            }
            self.spamComments = categorized.filter { $0.category == "spam" }.compactMap { cat in
                let idx = cat.index - 1
                guard idx >= 0 && idx < truncated.count else { return nil }
                return truncated[idx]
            }
            self.neutralComments = categorized.filter { $0.category == "neutral" }.compactMap { cat in
                let idx = cat.index - 1
                guard idx >= 0 && idx < truncated.count else { return nil }
                return truncated[idx]
            }
        }

        // Restore suggested questions from essentials cache only
        if let cachedEssentials = self.essentialsCommentAnalysis {
            self.suggestedQuestions = normalizeSuggestedQuestions(cachedEssentials.suggestedQuestions)
        }

        // Calculate score and update state
        self.calculateAndSetWorthItScore()
        self.processingProgress = 1.0
        self.viewState = .showingInitialOptions
        processedVideoIDsThisSession.insert(videoId)
    }



    private func performNonStreamingAnalysis(videoId: String, transcript: String, comments: [String], titleForGpt: String) async {
        Logger.shared.info("Starting full analysis (non-stream) for video ID: \(videoId)", category: .analysis)
        processedVideoIDsThisSession.insert(videoId)
        updateProgress(0.6, message: "Analyzing")

        let classificationComments = Array(comments.prefix(commentClassificationLimit))
        let insightsComments = Array(comments.prefix(commentInsightsLimit))
        let cache = cacheManager

        do {
            var contentAnalysisData: ContentAnalysis!
            var insights: EssentialsCommentAnalysis?

            var commentClassificationFailed = false

            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    async let transcriptTask = self.apiManager.fetchTranscriptSummary(transcript: transcript, videoTitle: titleForGpt)
                    let transcriptResult = try await transcriptTask

                    var classificationResult: APIManager.CommentsOnlyResponse?
                    do {
                        classificationResult = try await self.apiManager.fetchCommentsClassification(comments: classificationComments)
                    } catch {
                        Logger.shared.warning("Comment classification unavailable for \(videoId): \(error.localizedDescription)", category: .services)
                        await MainActor.run {
                            commentClassificationFailed = true
                        }
                    }

                    let validatedThemes: [CommentTheme] = (classificationResult?.topThemes ?? []).filter { theme in
                        guard let ex = theme.exampleComment, ex.count <= 80 else { return false }
                        return classificationComments.contains { $0.contains(ex) }
                    }
                    let allowedCategories: Set<String> = ["humor","insightful","controversial","spam","neutral"]
                    let filteredCategorized = (classificationResult?.categorizedComments ?? []).filter { allowedCategories.contains($0.category) }

                    let analysis = ContentAnalysis(
                        longSummary: transcriptResult.longSummary,
                        takeaways: transcriptResult.takeaways,
                        gemsOfWisdom: transcriptResult.gemsOfWisdom,
                        videoId: videoId,
                        videoTitle: titleForGpt,
                        videoDurationSeconds: nil,
                        videoThumbnailUrl: nil,
                        CommentssentimentSummary: classificationResult?.CommentssentimentSummary,
                        topThemes: validatedThemes.isEmpty ? [] : validatedThemes,
                        categorizedComments: filteredCategorized
                    )

                    await MainActor.run {
                        contentAnalysisData = analysis
                    }
                }

                if !insightsComments.isEmpty {
                    group.addTask {
                        let local = try? await self.apiManager.fetchCommentInsights(
                            comments: insightsComments,
                            transcriptContext: transcript.truncate(to: 2000)
                        )
                        await MainActor.run {
                            self.essentialsCommentAnalysis = local
                            var sq = self.normalizeSuggestedQuestions(local?.suggestedQuestions ?? [])
                            if sq.isEmpty {
                                sq = self.generateFallbackSuggestedQuestions(from: transcript)
                            }
                            self.suggestedQuestions = sq
                            self.calculateAndSetWorthItScore()
                        }
                        insights = local
                    }
                }

                try await group.waitForAll()
            }

            // Persist
            self.analysisResult = contentAnalysisData
            let resolvedTitle = contentAnalysisData.videoTitle ?? titleForGpt
            self.currentVideoTitle = resolvedTitle
            self.currentVideoThumbnailURL = URL(string: contentAnalysisData.videoThumbnailUrl ?? "https://i.ytimg.com/vi/\(videoId)/hqdefault.jpg")
            await cache.saveContentAnalysis(contentAnalysisData, for: videoId)
            self.recordTimeSavedIfNeeded(
                for: videoId,
                transcript: self.rawTranscript ?? transcript,
                videoDurationSeconds: contentAnalysisData.videoDurationSeconds
            )

            if commentClassificationFailed {
                AnalyticsService.shared.logCommentsNotFound(videoId: videoId)
            }

            self.essentialsCommentAnalysis = insights
            if let insights = insights { await cache.saveCommentInsights(insights, for: videoId) }
            var finalSQ = normalizeSuggestedQuestions(insights?.suggestedQuestions ?? [])
            if finalSQ.isEmpty {
                finalSQ = generateFallbackSuggestedQuestions(from: transcript)
            }
            self.suggestedQuestions = finalSQ

            // Categorized comments mapping
            if let categorized = contentAnalysisData.categorizedComments {
                let truncated = classificationComments
                self.llmAnalyzedComments = truncated
                self.funnyComments = categorized.filter { $0.category == "humor" }.compactMap { cat in
                    let idx = cat.index - 1; return (idx >= 0 && idx < truncated.count) ? truncated[idx] : nil
                }
                self.insightfulComments = categorized.filter { $0.category == "insightful" }.compactMap { cat in
                    let idx = cat.index - 1; return (idx >= 0 && idx < truncated.count) ? truncated[idx] : nil
                }
                self.controversialComments = categorized.filter { $0.category == "controversial" }.compactMap { cat in
                    let idx = cat.index - 1; return (idx >= 0 && idx < truncated.count) ? truncated[idx] : nil
                }
                self.spamComments = categorized.filter { $0.category == "spam" }.compactMap { cat in
                    let idx = cat.index - 1; return (idx >= 0 && idx < truncated.count) ? truncated[idx] : nil
                }
                self.neutralComments = categorized.filter { $0.category == "neutral" }.compactMap { cat in
                    let idx = cat.index - 1; return (idx >= 0 && idx < truncated.count) ? truncated[idx] : nil
                }
            }

            // Score
            self.calculateAndSetWorthItScore()

            // Done
            self.updateProgress(1.0, message: "Analysis Complete")
            Logger.shared.info("Full analysis completed for \(videoId)", category: .analysis)
            AnalyticsService.shared.logVideoAnalysisCompleted(
                videoId: videoId,
                score: self.worthItScore ?? 0,
                hasComments: !(self.llmAnalyzedComments.isEmpty)
            )
            if self.minimumDisplayTimerTask == nil {
                if self.currentScreenOverride != .showingAskAnything &&
                   self.currentScreenOverride != .showingEssentials {
                    self.viewState = .showingInitialOptions
                }
            }
            releaseUsageReservation(for: videoId, revertIfNeeded: false)
        } catch {
            Logger.shared.error("Error during non-stream full analysis for \(videoId): \(error.localizedDescription)", category: .analysis, error: error)
            AnalyticsService.shared.logVideoAnalysisFailed(videoId: videoId, error: error.localizedDescription)
            showFriendlyError(for: error, videoId: videoId)
            updateProgress(1.0, message: "Analysis Failed")
        }
    }

    private func fetchTranscript(videoId: String) async throws -> String {
        let cache = cacheManager
        if let cachedTranscript = await cache.loadTranscript(for: videoId) {
            Logger.shared.info("Transcript cache HIT for \(videoId)", category: .cache)
            return cachedTranscript
        }
        Logger.shared.info("Fetching transcript from API for \(videoId)", category: .networking)
        let transcript = try await apiManager.fetchTranscript(videoId: videoId)
        await cache.saveTranscript(transcript, for: videoId)
        return transcript
    }

    private func fetchComments(videoId: String) async throws -> [String] {
        let cache = cacheManager
        if let cachedComments = await cache.loadComments(for: videoId) {
            Logger.shared.info("Comments cache HIT for \(videoId). Count: \(cachedComments.count). First: \(cachedComments.first ?? "none")", category: .cache)
            return cachedComments
        }
        Logger.shared.info("Fetching comments from API for \(videoId)", category: .networking)
        let comments = try await apiManager.fetchComments(videoId: videoId)
        Logger.shared.info("Fetched \(comments.count) comments from API for \(videoId). First: \(comments.first ?? "none")", category: .networking)
        await cache.saveComments(comments, for: videoId)
        return comments
    }

    private func recordTimeSavedIfNeeded(for videoId: String, transcript: String, videoDurationSeconds: Int?, source: TimeSavedLogEntry.Source = .mainApp) {
        let trimmedTranscript = transcript.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedTranscript.isEmpty else { return }
        let wordCount = trimmedTranscript.split { $0.isWhitespace || $0.isNewline }.count
        guard wordCount > 0 else { return }

        var estimatedMinutes = max(Double(wordCount) / speechWordsPerMinute, 0.1)
        if let videoDurationSeconds, videoDurationSeconds > 0 {
            let videoMinutes = max(Double(videoDurationSeconds) / 60.0, 0.1)
            estimatedMinutes = min(estimatedMinutes, max(videoMinutes * 0.95, videoMinutes - 0.5, 0.1))
        }
        let roundedMinutes = max((estimatedMinutes * 10).rounded() / 10.0, 0.1)
        let alreadyCounted = timeSavedVideoIDs.contains(videoId)

        var updatedTotal = cumulativeTimeSavedMinutes
        let previousTotal = cumulativeTimeSavedMinutes
        var context: TimeSavedContext? = nil
        let effectiveSource: TimeSavedLogEntry.Source = isRunningInExtension ? .shareExtension : source
        if !alreadyCounted {
            updatedTotal = max((updatedTotal + roundedMinutes) * 10, 0).rounded() / 10.0
            timeSavedVideoIDs.insert(videoId)
            sharedDefaults.set(Array(timeSavedVideoIDs), forKey: timeSavedVideoIDsKey)
            sharedDefaults.set(updatedTotal, forKey: totalTimeSavedKey)
            cumulativeTimeSavedMinutes = updatedTotal
            uniqueVideosSummarized = timeSavedVideoIDs.count

            let logEntry = TimeSavedLogEntry(
                minutes: roundedMinutes,
                timestamp: Date(),
                videoId: videoId,
                source: effectiveSource
            )
            context = registerTimeSavedLogEntry(logEntry, previousTotal: previousTotal, newTotal: updatedTotal)
        }

        let totalMinutes = alreadyCounted ? cumulativeTimeSavedMinutes : updatedTotal
        let event = TimeSavedEvent(
            minutes: roundedMinutes,
            cumulativeMinutes: totalMinutes,
            alreadyCounted: alreadyCounted,
            context: alreadyCounted ? nil : context
        )
        withAnimation(.spring(response: 0.35, dampingFraction: 0.85)) {
            latestTimeSavedEvent = event
        }
        updateLastDisplayedTimeSaved(to: totalMinutes)
        AnalyticsService.shared.logEvent(
            "time_saved_recorded",
            parameters: [
                "video_id": videoId,
                "minutes": roundedMinutes,
                "already_counted": alreadyCounted
            ]
        )
    }

    private func lastDisplayedTimeSavedTotal() -> Double {
        sharedDefaults.double(forKey: lastDisplayedTimeSavedKey)
    }

    private func updateLastDisplayedTimeSaved(to value: Double) {
        sharedDefaults.set(value, forKey: lastDisplayedTimeSavedKey)
    }

    func syncTimeSavedMetricsFromSharedDefaults(presentEventIfNeeded: Bool = true) {
        let storedTotal = sharedDefaults.double(forKey: totalTimeSavedKey)
        let storedIDs = Set((sharedDefaults.array(forKey: timeSavedVideoIDsKey) as? [String]) ?? [])
        let storedLog = loadTimeSavedLogFromDefaults()

        let previousTotal = cumulativeTimeSavedMinutes

        timeSavedVideoIDs = storedIDs
        cumulativeTimeSavedMinutes = storedTotal
        uniqueVideosSummarized = storedIDs.count
        timeSavedLogEntries = storedLog

        guard presentEventIfNeeded else {
            updateLastDisplayedTimeSaved(to: storedTotal)
            return
        }

        let lastDisplayed = lastDisplayedTimeSavedTotal()
        let deltaRaw = storedTotal - lastDisplayed
        let roundedDelta = (deltaRaw * 10).rounded() / 10.0

        guard roundedDelta > 0.049 else {
            if previousTotal != storedTotal {
                updateLastDisplayedTimeSaved(to: storedTotal)
            }
            return
        }

        let context = contextFromSyncedLog(deltaMinutes: roundedDelta, newTotal: storedTotal)

        let event = TimeSavedEvent(
            minutes: roundedDelta,
            cumulativeMinutes: storedTotal,
            alreadyCounted: false,
            context: context
        )

        withAnimation(.spring(response: 0.35, dampingFraction: 0.85)) {
            latestTimeSavedEvent = event
        }
        updateLastDisplayedTimeSaved(to: storedTotal)
    }

    private func appendToTimeSavedLog(_ entry: TimeSavedLogEntry) {
        timeSavedLogEntries.append(entry)
        timeSavedLogEntries.sort { $0.timestamp < $1.timestamp }
    }

    private func pruneTimeSavedLogIfNeeded() {
        let cutoff = appCalendar.date(byAdding: .day, value: -logRetentionDays, to: Date()) ?? Date()
        timeSavedLogEntries = timeSavedLogEntries.filter { $0.timestamp >= cutoff }
        if timeSavedLogEntries.count > maxTimeSavedLogEntries {
            timeSavedLogEntries = Array(timeSavedLogEntries.suffix(maxTimeSavedLogEntries))
        }
    }

    private func persistTimeSavedLogIfNeeded() {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .millisecondsSince1970
        do {
            let data = try encoder.encode(timeSavedLogEntries)
            if sharedDefaults.data(forKey: timeSavedLogKey) != data {
                sharedDefaults.set(data, forKey: timeSavedLogKey)
            }
        } catch {
            Logger.shared.error("Failed to persist time saved log: \(error.localizedDescription)", category: .services, error: error)
        }
    }

    private func loadTimeSavedLogFromDefaults() -> [TimeSavedLogEntry] {
        guard let data = sharedDefaults.data(forKey: timeSavedLogKey) else { return [] }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .millisecondsSince1970
        do {
            var entries = try decoder.decode([TimeSavedLogEntry].self, from: data)
            entries.sort { $0.timestamp < $1.timestamp }
            let cutoff = appCalendar.date(byAdding: .day, value: -logRetentionDays, to: Date()) ?? Date()
            entries = entries.filter { $0.timestamp >= cutoff }
            if entries.count > maxTimeSavedLogEntries {
                entries = Array(entries.suffix(maxTimeSavedLogEntries))
            }
            return entries
        } catch {
            Logger.shared.error("Failed to decode time saved log: \(error.localizedDescription)", category: .services, error: error)
            return []
        }
    }

    private func contextFromSyncedLog(deltaMinutes: Double, newTotal: Double) -> TimeSavedContext? {
        guard let lastEntry = timeSavedLogEntries.last else { return nil }
        let previousTotal = max(newTotal - deltaMinutes, 0)
        return determineContext(for: lastEntry, previousTotal: previousTotal, newTotal: newTotal)
    }

    private func registerTimeSavedLogEntry(_ entry: TimeSavedLogEntry, previousTotal: Double, newTotal: Double) -> TimeSavedContext? {
        appendToTimeSavedLog(entry)
        pruneTimeSavedLogIfNeeded()
        let context = determineContext(for: entry, previousTotal: previousTotal, newTotal: newTotal)
        persistTimeSavedLogIfNeeded()
        var extras: [String: Any] = [
            "minutes": entry.minutes,
            "source": entry.source.rawValue,
            "new_total": newTotal,
            "previous_total": previousTotal
        ]
        if let context {
            extras["context"] = String(describing: context.kind)
        }
        Logger.shared.info("Time-saved log entry recorded", category: .timeSavings, extra: extras)
        return context
    }

    private func determineContext(for entry: TimeSavedLogEntry, previousTotal: Double, newTotal: Double) -> TimeSavedContext? {
        if let milestone = milestoneContext(previousTotal: previousTotal, newTotal: newTotal) {
            return milestone
        }
        if let streak = streakContext(for: entry) {
            return streak
        }
        if let weekly = weeklyBestContext(for: entry) {
            return weekly
        }
        return nil
    }

    private func milestoneContext(previousTotal: Double, newTotal: Double) -> TimeSavedContext? {
        let thresholdsMinutes: [Double] = [30, 60, 120, 180, 300, 480, 720, 960, 1200, 1440, 1800, 2400, 3000]
        for threshold in thresholdsMinutes.sorted() {
            if previousTotal < threshold && newTotal >= threshold {
                let formatted = formatMilestoneMinutes(threshold)
                return TimeSavedContext(
                    kind: .milestone,
                    iconName: "flag.fill",
                    chipText: formatted,
                    headline: "🎯 Milestone unlocked",
                    detail: "Total stash hit \(formatted)"
                )
            }
        }
        return nil
    }

    private func streakContext(for entry: TimeSavedLogEntry) -> TimeSavedContext? {
        let calendar = appCalendar
        let dayStart = calendar.startOfDay(for: entry.timestamp)

        let hasEarlierSameDay = timeSavedLogEntries.contains {
            calendar.isDate($0.timestamp, inSameDayAs: entry.timestamp) &&
            $0.timestamp < entry.timestamp
        }
        guard !hasEarlierSameDay else { return nil }

        var streakCount = 0
        var cursor = dayStart
        while timeSavedLogEntries.contains(where: { calendar.isDate($0.timestamp, inSameDayAs: cursor) }) {
            streakCount += 1
            guard let previous = calendar.date(byAdding: .day, value: -1, to: cursor) else { break }
            cursor = calendar.startOfDay(for: previous)
        }

        guard streakCount >= 3 else { return nil }
        let chip = "Day \(streakCount)"
        let detail = streakCount >= 7 ? "🔥 On a \(streakCount)-day heater" : "Day \(streakCount) in a row"
        let headline = streakCount >= 7 ? "🔥 Streak on fire" : "🔥 Streak in motion"
        return TimeSavedContext(
            kind: .streak,
            iconName: "flame.fill",
            chipText: chip,
            headline: headline,
            detail: detail
        )
    }

    private func weeklyBestContext(for entry: TimeSavedLogEntry) -> TimeSavedContext? {
        let calendar = appCalendar
        let targetWeek = calendar.component(.weekOfYear, from: entry.timestamp)
        let targetYear = calendar.component(.yearForWeekOfYear, from: entry.timestamp)

        let weekEntries = timeSavedLogEntries.filter {
            calendar.component(.weekOfYear, from: $0.timestamp) == targetWeek &&
            calendar.component(.yearForWeekOfYear, from: $0.timestamp) == targetYear
        }

        let earlierSameDay = weekEntries.contains {
            calendar.isDate($0.timestamp, inSameDayAs: entry.timestamp) &&
            $0.timestamp < entry.timestamp
        }
        guard !earlierSameDay else { return nil }

        var previousBest: Double = 0
        for item in weekEntries where item.timestamp < entry.timestamp {
            previousBest = max(previousBest, item.minutes)
        }

        guard previousBest > 0, entry.minutes >= previousBest + 0.1 else { return nil }
        let chip = "Weekly best"
        let detail = "New high: \(formatMinutesShort(entry.minutes))"
        return TimeSavedContext(
            kind: .weeklyBest,
            iconName: "crown.fill",
            chipText: chip,
            headline: "🏆 Weekly best!",
            detail: detail
        )
    }

    private func formatMinutesShort(_ minutes: Double) -> String {
        if minutes >= 60 {
            let hours = minutes / 60.0
            return String(format: "%.1fh", hours)
        } else if minutes >= 10 {
            return "\(Int(minutes.rounded())) min"
        } else {
            return String(format: "%.1f min", minutes)
        }
    }

    private func formatMilestoneMinutes(_ minutes: Double) -> String {
        if minutes.truncatingRemainder(dividingBy: 60) == 0, minutes >= 60 {
            let hours = Int(minutes / 60)
            return "\(hours)h"
        }
        return formatMinutesShort(minutes)
    }

    func clearTimeSavedEvent() {
        latestTimeSavedEvent = nil
    }

    private func calculateAndSetWorthItScore() {
        let depthNormalized = essentialsCommentAnalysis?.contentDepthScore ?? 0.6 // rough transcript-only heuristic

        if let commentData = essentialsCommentAnalysis {
            // Normal calculation (with comments)
            let depthWeight = 0.60
            let commentSentimentWeight = 0.40
            // overallCommentSentimentScore is specified as [0,1] in the prompt
            let commentSentimentRaw = commentData.overallCommentSentimentScore ?? 0.0
            let commentSentimentNormalized = max(0.0, min(commentSentimentRaw, 1.0))

            var finalScoreValue = (depthNormalized * depthWeight) +
                             (commentSentimentNormalized * commentSentimentWeight)

            if depthNormalized > 0.8 && commentSentimentNormalized > 0.7 { finalScoreValue += 0.05 }
            if depthNormalized < 0.3 { finalScoreValue -= 0.05 }

            finalScoreValue = max(0.0, min(finalScoreValue, 1.0))
            let roundedScore = round(finalScoreValue * 100)

            self.worthItScore = roundedScore
            let positiveThemes = analysisResult?.topThemes?.filter { ($0.sentimentScore ?? 0) > 0.1 }.map { $0.theme } ?? []
            let negativeThemes = analysisResult?.topThemes?.filter { ($0.sentimentScore ?? 0) < -0.1 }.map { $0.theme } ?? []
            self.scoreBreakdownDetails = ScoreBreakdown(
                contentDepthScore: depthNormalized,
                commentSentimentScore: commentSentimentNormalized,
                hasComments: true, // Comments exist in this path
                contentDepthRaw: depthNormalized,
                commentSentimentRaw: commentSentimentRaw,
                finalScore: roundedScore,
                videoTitle: analysisResult?.videoTitle ?? currentVideoTitle ?? "Video",
                positiveCommentThemes: positiveThemes,
                negativeCommentThemes: negativeThemes
            )
        } else {
            // PATCH: No comments, score is 100% content depth
            let roundedScore = round(depthNormalized * 100)
            self.worthItScore = roundedScore
            self.scoreBreakdownDetails = ScoreBreakdown(
                contentDepthScore: depthNormalized,
                commentSentimentScore: 0.0,
                hasComments: !(self.llmAnalyzedComments.isEmpty), // Use truncated array for hasComments
                contentDepthRaw: depthNormalized,
                commentSentimentRaw: 0.0,
                finalScore: roundedScore,
                videoTitle: analysisResult?.videoTitle ?? currentVideoTitle ?? "Video",
                positiveCommentThemes: [],
                negativeCommentThemes: []
            )
        }

        Logger.shared.info("Worth-It Score calculated: \(self.worthItScore ?? -1). Depth: \(depthNormalized), Comment Sentiment: \(essentialsCommentAnalysis?.overallCommentSentimentScore ?? 0)", category: .services)
    }

    // MARK: - Fallbacks
    private func generateFallbackSuggestedQuestions(from transcript: String?) -> [String] {
        // Very light heuristic for Spanish vs English
        let t = (transcript ?? "").lowercased()
        let isSpanish = [" el ", " la ", " de ", " que ", " y ", " para "]
            .contains { t.contains($0) }
        if isSpanish {
            return [
                "Idea clave?",
                "Pasos prácticos?",
                "Por qué importa?"
            ]
        } else {
            return [
                "Key idea?",
                "Practical steps?",
                "Why it matters?"
            ]
        }
    }

    func requestEssentials() {
        // Allow navigating to Essentials as soon as score is shown, or when full results are ready
        guard analysisResult != nil || worthItScore != nil else {
            Logger.shared.warning("Essentials requested but no score/result yet. State: \(viewState)", category: .ui)
            return
        }
        if let videoId = currentVideoID {
            AnalyticsService.shared.logEssentialsViewed(videoId: videoId)
        }
        viewState = .showingEssentials
    }

    func requestAskAnything() {
        let cache = cacheManager
        Task { @MainActor in
            if let vid = currentVideoID,
               let cached = await cache.loadQAMessages(for: vid),
               !cached.isEmpty {
                qaMessages = cached
            } else if qaMessages.isEmpty {
                qaMessages.append(ChatMessage(content: "Hi! What would you like to know about this video?", isUser: false))
            }
            viewState = .showingAskAnything
        }
    }

    func sendQAQuestion() {
        guard !qaInputText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            Logger.shared.warning("Cannot send Q&A: input text missing.", category: .ui)
            return
        }

        let userMessage = ChatMessage(content: qaInputText.trimmingCharacters(in: .whitespacesAndNewlines), isUser: true)
        qaMessages.append(userMessage)
        let cache = cacheManager
        if let vid = self.currentVideoID {
            let snapshot = qaMessages
            Task {
                await cache.saveQAMessages(snapshot, for: vid)
            }
        }
        let currentQuestion = qaInputText
        
        // Log analytics
        if let videoId = currentVideoID {
            AnalyticsService.shared.logQAQuestionAsked(videoId: videoId, questionLength: currentQuestion.count)
        }
        
        qaInputText = ""
        isQaLoading = true
        userFriendlyError = nil
        hasCommentsAvailableForError = false

        let history = Array(qaMessages.dropLast().suffix(6))
        let transcriptForQA = (self.rawTranscript?.stripTimestamps()) ?? (self.transcriptForAnalysis ?? "")
        let videoIdForCache = currentVideoID

        Task {
            do {
                let response = try await apiManager.answerQuestion(
                    transcript: transcriptForQA,
                    question: currentQuestion,
                    history: history
                )
                await MainActor.run {
                    self.qaMessages.append(ChatMessage(content: response.answer, isUser: false))
                }
                if let vid = videoIdForCache {
                    let messages = await MainActor.run { self.qaMessages }
                    await cache.saveQAMessages(messages, for: vid)
                }
            } catch {
                Logger.shared.error("Failed to get Q&A answer: \(error.localizedDescription)", category: .services, error: error)
                let fallbackMessage = (error as? NetworkError)?.localizedDescription ?? error.localizedDescription
                let errorMessage = "Sorry, I couldn't answer that. \(fallbackMessage)"
                await MainActor.run {
                    self.qaMessages.append(ChatMessage(content: errorMessage, isUser: false))
                }
                if let vid = videoIdForCache {
                    let messages = await MainActor.run { self.qaMessages }
                    await cache.saveQAMessages(messages, for: vid)
                }
            }
            await MainActor.run { self.isQaLoading = false }
        }
    }

    func selectSuggestedQuestion(_ question: String) {
        qaInputText = question
        sendQAQuestion()
    }

    func askKeyQuestion(_ question: String) {
        // Switch to the Ask Anything screen
        viewState = .showingAskAnything
        
        // Set the question and send it immediately
        qaInputText = question
        sendQAQuestion()
    }

    func returnToInitialOptions() {
        userFriendlyError = nil
        viewState = .showingInitialOptions
    }

    // Public helper to return to the home (idle) screen for starting a new analysis
    func goToHome() {
        resetForNewAnalysis()
    }

    private func resetForNewAnalysis() {
        Logger.shared.info("Resetting MainViewModel for new analysis.", category: .lifecycle)
        activeAnalysisTask?.cancel()
        activeAnalysisTask = nil
        fullAnalysisTask?.cancel()
        fullAnalysisTask = nil
        minimumDisplayTimerTask?.cancel()
        minimumDisplayTimerTask = nil
        viewState = .idle
        processingProgress = 0.0
        userFriendlyError = nil
        analysisResult = nil
        worthItScore = nil
        scoreBreakdownDetails = nil
        essentialsCommentAnalysis = nil
        qaMessages = []
        qaInputText = ""
        isQaLoading = false
        suggestedQuestions = []
        currentVideoID = nil
        currentVideoTitle = nil
        currentVideoThumbnailURL = nil
        rawTranscript = nil
        transcriptForAnalysis = nil
        latestTimeSavedEvent = nil

        // Clear categorized comments
        funnyComments = []
        insightfulComments = []
        controversialComments = []
        spamComments = []
        neutralComments = []
        allFetchedComments = []
        llmAnalyzedComments = []
        
        // Reset GPT thread & intro‑cache so a fresh video starts brand‑new context
        apiManager.resetConversationState()

        usageReservations.removeAll()
        latestUsageSnapshot = nil
        activePaywall = nil
        paywallLoggedImpression = false
    }

    func clearCurrentError() {
        userFriendlyError = nil
        if case .error = viewState { // Check if current state is error
            viewState = analysisResult != nil ? .showingInitialOptions : .idle
        }
    }
    
    // MARK: - Cache Management
    
    func clearSessionCache() {
        processedVideoIDsThisSession.removeAll()
        Logger.shared.info("Session cache cleared.", category: .cache)
    }

    // MARK: - Paywall Presentation

    private func presentPaywall(reason: PaywallReason, usageSnapshot: UsageTracker.Snapshot) {
        activePaywall = PaywallContext(reason: reason, usageSnapshot: usageSnapshot)
        paywallLoggedImpression = false
        latestUsageSnapshot = usageSnapshot
    }

    func requestManualPaywallPresentation() {
        Task { @MainActor in
            let snapshot = await usageTracker.snapshot(dailyLimit: AppConstants.dailyFreeAnalysisLimit)
            presentPaywall(reason: .manual, usageSnapshot: snapshot)
        }
    }

    func paywallPresented(reason: PaywallReason) {
        guard paywallLoggedImpression == false else { return }
        paywallLoggedImpression = true
        let snapshot = activePaywall?.usageSnapshot ?? latestUsageSnapshot
        AnalyticsService.shared.logPaywallShown(
            trigger: reason.analyticsLabel,
            used: snapshot?.count ?? 0,
            limit: snapshot?.limit ?? AppConstants.dailyFreeAnalysisLimit
        )
    }

    func dismissPaywall() {
        dismissPaywall(afterSuccessfulPurchase: false)
    }

    private func dismissPaywall(afterSuccessfulPurchase: Bool) {
        guard activePaywall != nil else { return }
        activePaywall = nil
        paywallLoggedImpression = false
        latestUsageSnapshot = nil
        AnalyticsService.shared.logPaywallDismissed(purchased: afterSuccessfulPurchase)
    }

    func paywallPurchaseTapped(productId: String) {
        AnalyticsService.shared.logPaywallPurchaseTapped(productId: productId)
    }

    func paywallPurchaseSucceeded(productId: String) {
        AnalyticsService.shared.logPaywallPurchaseSucceeded(productId: productId)
        dismissPaywall(afterSuccessfulPurchase: true)
    }

    func paywallPurchaseCancelled(productId: String) {
        AnalyticsService.shared.logPaywallPurchaseCancelled(productId: productId)
    }

    func paywallPurchaseFailed(productId: String) {
        AnalyticsService.shared.logPaywallPurchaseFailed(productId: productId)
    }

    func paywallRestoreTapped() {
        AnalyticsService.shared.logPaywallRestoreTapped()
    }

    func paywallManageTapped() {
        AnalyticsService.shared.logPaywallManageTapped()
    }

    func consumeManageSubscriptionsDeepLinkRequest() {
        shouldOpenManageSubscriptions = false
    }

    func setPublicError(message: String, canRetry: Bool, videoIdForRetry: String? = nil) {
        let videoToRetry = videoIdForRetry ?? self.currentVideoID
        if let id = videoToRetry {
            releaseUsageReservation(for: id, revertIfNeeded: true)
        }
        self.userFriendlyError = UserFriendlyError(message: message, canRetry: canRetry, videoIdForRetry: videoToRetry)
        self.viewState = .error
        activeAnalysisTask?.cancel()
        activeAnalysisTask = nil
        fullAnalysisTask?.cancel()
        fullAnalysisTask = nil
        minimumDisplayTimerTask?.cancel()
        minimumDisplayTimerTask = nil
        AnalyticsService.shared.logError(message, context: "AnalysisError", videoId: videoToRetry)
    }

    func retryLastFailedAnalysis() {
        guard let errorInfo = userFriendlyError, errorInfo.canRetry, let videoId = errorInfo.videoIdForRetry else {
            Logger.shared.warning("Retry called but no retryable error or videoId found.", category: .ui)
            clearCurrentError()
            if viewState == .error { viewState = .idle }
            return
        }

        Logger.shared.info("Retrying analysis for video ID: \(videoId)", category: .lifecycle)
        clearCurrentError()

        let retryURLString = "https://www.youtube.com/watch?v=\(videoId)"
        if let retryURL = URL(string: retryURLString) {
            processedVideoIDsThisSession.remove(videoId)
            processSharedURL(retryURL)
        } else {
            Logger.shared.error("Failed to create retry URL for video ID: \(videoId)", category: .lifecycle)
            setPublicError(message: "Internal error: Could not retry.", canRetry: false)
        }
    }

    private func startMinimumDisplayTimer() {
        // Only enforce minimum display delay if we do not have cached results
        guard self.analysisResult == nil else {
            // Already have a cached result; no delay needed
            Logger.shared.debug("Skipping minimum display timer - cached results available", category: .ui)
            return
        }
        
        minimumDisplayTimerTask?.cancel()
        let startTime = Date()
        Logger.shared.debug("Starting minimum display timer (\(minimumDisplayTime)s)...", category: .ui)

        minimumDisplayTimerTask = Task {
            do {
                try await Task.sleep(nanoseconds: UInt64(minimumDisplayTime * 1_000_000_000))
                try Task.checkCancellation()

                await MainActor.run {
                    let elapsedTime = Date().timeIntervalSince(startTime)
                    Logger.shared.info("Minimum display timer finished (\(String(format: "%.2f", elapsedTime))s). Current state: \(self.viewState)", category: .ui)
                    self.minimumDisplayTimerTask = nil

                    // After minimum time, transition to options unless user navigated elsewhere
                    if self.viewState == .processing {
                        if self.currentScreenOverride != .showingAskAnything &&
                           self.currentScreenOverride != .showingEssentials {
                            Logger.shared.info("Minimum display time reached. Transitioning to initial options.", category: .ui)
                            self.viewState = .showingInitialOptions
                        } else {
                            Logger.shared.info("Minimum display time reached but user already navigated elsewhere. No transition.", category: .ui)
                        }
                    } else {
                        Logger.shared.info("Minimum display timer finished but analysis completed or state changed.", category: .ui)
                    }
                }
            } catch is CancellationError {
                Logger.shared.info("Minimum display timer cancelled.", category: .ui)
                await MainActor.run { self.minimumDisplayTimerTask = nil }
            } catch {
                Logger.shared.error("Error in minimum display timer: \(error.localizedDescription)", category: .ui, error: error)
                await MainActor.run { self.minimumDisplayTimerTask = nil }
            }
        }
    }

    private func updateProgress(_ value: Double, message: String? = nil) {
        let newProgress = min(max(value, 0.0), 1.0)
        self.processingProgress = newProgress
        if let msg = message {
            Logger.shared.debug("Progress: \(Int(newProgress * 100))% - \(msg)", category: .services)
        }
    }

    private func extractVideoID(from url: URL) -> String? {
        do {
            return try URLParser.extractVideoID(from: url)
        } catch let error as URLParserError { // Be specific about error type
            Logger.shared.error("URLParserError: \(error.localizedDescription)", category: .parsing, error: error)
            return nil
        } catch {
            Logger.shared.error("Unknown error parsing URL: \(error.localizedDescription)", category: .parsing, error: error)
            return nil
        }
    }

    deinit {
        activeAnalysisTask?.cancel()
        fullAnalysisTask?.cancel()
        minimumDisplayTimerTask?.cancel()
        Logger.shared.info("MainViewModel deinitialized.", category: .lifecycle)
    }

    // Helper for showing a friendly error message for a given error and videoId
    private func showFriendlyError(for error: Error, videoId: String?) {
        if let netErr = error as? NetworkError {
            setPublicError(
                message: netErr.localizedDescription,
                canRetry: {
                    if case .rateLimited = netErr {
                        return false
                    } else {
                        return true
                    }
                }(),
                videoIdForRetry: videoId
            )
        } else if (error as? CancellationError) == nil {
            setPublicError(
                message: error.localizedDescription,
                canRetry: true,
                videoIdForRetry: videoId
            )
        }
    }
}
