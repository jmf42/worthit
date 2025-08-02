//ShareExtensionEntry.swift

import UIKit
import os.log

// This class is referenced in Share/Info.plist's UIApplicationDelegateClassName
// It helps in acquiring a cross-process lock to prevent multiple share extension
// instances from performing redundant work, especially for the same video.
@objc(ShareExtensionEntry)
final class ShareExtensionEntry: NSObject, UIApplicationDelegate {

    private static let lockIDKey = "com.worthitai.share.active.lock"
    private var didWinLock = false
    private let logger = Logger.shared // Use your app's logger

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        logger.debug("ShareExtensionEntry: didFinishLaunchingWithOptions", category: .lifecycle)

        // Attempt to acquire a file-based lock.
        // If another instance of the share extension already holds this lock for this video ID (or a general one),
        // this instance will terminate early.
        if FileLock.acquire(Self.lockIDKey) {
            didWinLock = true
            logger.info("ShareExtensionEntry: Acquired share processing lock.", category: .lifecycle)
            // Proceed with normal launch of the ShareViewController (or ShareHostingController)
            return true
        } else {
            logger.warning("ShareExtensionEntry: Failed to acquire share processing lock. Another instance might be active. Terminating.", category: .lifecycle)
            // Silently terminate this instance to prevent duplicate processing.
            // The ShareViewController of this instance will not be presented.
            exit(0)
        }
    }

    func applicationWillTerminate(_ application: UIApplication) {
        logger.debug("ShareExtensionEntry: applicationWillTerminate", category: .lifecycle)
        if didWinLock {
            FileLock.release(Self.lockIDKey)
            logger.info("ShareExtensionEntry: Released share processing lock.", category: .lifecycle)
        }
    }
}

// Simple FileLock mechanism (ensure AppConstants.appGroupID is correctly set)
struct FileLock {
    private static let lockFileTTL: TimeInterval = 120 // 2 minutes for a lock to be considered stale

    private static var lockDirectory: URL {
        guard let groupURL = FileManager.default.containerURL(forSecurityApplicationGroupIdentifier: AppConstants.appGroupID) else {
            fatalError("App Group ID \(AppConstants.appGroupID) is not configured correctly.")
        }
        let locksURL = groupURL.appendingPathComponent("ProcessLocks", isDirectory: true)
        if !FileManager.default.fileExists(atPath: locksURL.path) {
            try? FileManager.default.createDirectory(at: locksURL, withIntermediateDirectories: true, attributes: nil)
        }
        return locksURL
    }

    static func url(for lockIdentifier: String) -> URL {
        // Sanitize identifier to be a valid filename
        let sanitizedId = lockIdentifier.replacingOccurrences(of: "[^a-zA-Z0-9_-]", with: "_", options: .regularExpression)
        return lockDirectory.appendingPathComponent("\(sanitizedId).lock")
    }

    @discardableResult
    static func acquire(_ lockIdentifier: String) -> Bool {
        let fileManager = FileManager.default
        let lockURL = url(for: lockIdentifier)

        // Check for and remove stale lock file
        if fileManager.fileExists(atPath: lockURL.path) {
            do {
                let attributes = try fileManager.attributesOfItem(atPath: lockURL.path)
                if let modificationDate = attributes[.modificationDate] as? Date,
                   Date().timeIntervalSince(modificationDate) > lockFileTTL {
                    Logger.shared.warning("FileLock: Stale lock file found for '\(lockIdentifier)'. Removing.", category: .lifecycle)
                    try? fileManager.removeItem(at: lockURL)
                } else {
                     Logger.shared.info("FileLock: Lock for '\(lockIdentifier)' already held (not stale).", category: .lifecycle)
                    return false // Lock is held and not stale
                }
            } catch {
                Logger.shared.error("FileLock: Error checking stale lock for '\(lockIdentifier)': \(error.localizedDescription)", category: .lifecycle)
                // Proceed to attempt creation, but this indicates a potential issue.
            }
        }
        
        // Atomically create the lock file.
        // O_EXCL ensures that the file is created only if it does not already exist.
        let fd = open(lockURL.path, O_RDWR | O_CREAT | O_EXCL, 0o600)
        if fd != -1 {
            close(fd) // Successfully created, close file descriptor
            Logger.shared.info("FileLock: Acquired lock for '\(lockIdentifier)'.", category: .lifecycle)
            return true
        } else {
            // This can happen in a race condition if another process created it after the stale check.
            Logger.shared.info("FileLock: Failed to acquire lock for '\(lockIdentifier)' (likely race or already held).", category: .lifecycle)
            return false
        }
    }

    static func release(_ lockIdentifier: String) {
        let lockURL = url(for: lockIdentifier)
        do {
            if FileManager.default.fileExists(atPath: lockURL.path) {
                try FileManager.default.removeItem(at: lockURL)
                Logger.shared.info("FileLock: Released lock for '\(lockIdentifier)'.", category: .lifecycle)
            }
        } catch {
            Logger.shared.error("FileLock: Failed to release lock for '\(lockIdentifier)': \(error.localizedDescription)", category: .lifecycle)
        }
    }
