
//ShareHostingController.swift`**


import UIKit
import SwiftUI
import UniformTypeIdentifiers // For UTType constants

// This UIViewController is the entry point for the Share Extension UI.
// It hosts the main SwiftUI view (`RootView`).
class ShareHostingController: UIViewController {
    private var mainViewModel: MainViewModel!
    private var cacheManager: CacheManager!
    private var apiManager: APIManager!
    private var appServices: AppServices! // Hold reference to keep services alive

    private var hostingController: UIHostingController<RootView>?
    private var initialURLProcessed = false


    override func viewDidLoad() {
        super.viewDidLoad()
        Logger.shared.info("ShareHostingController viewDidLoad.", category: .lifecycle)
        view.backgroundColor = UIColor(Theme.Color.darkBackground) // Use Theme

        // Initialize services specifically for this share extension instance
        // This ensures that if the main app is also running, they don't directly conflict
        // state-wise, though they share the CacheManager singleton.
        self.appServices = AppServices()
        self.mainViewModel = appServices.mainViewModel

        setupSwiftUIView()
        
        // Listen for dismissal requests from SwiftUI views
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(handleDismissNotification),
            name: .shareExtensionShouldDismiss,
            object: nil
        )
        
        // Process the shared item *after* the view is set up.
        // This ensures the MainViewModel is ready to receive the URL.
        if !initialURLProcessed {
             processSharedItem()
        }
    }
    
    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        Logger.shared.info("ShareHostingController viewDidAppear.", category: .lifecycle)
        // If for some reason URL processing was missed or needs re-triggering (e.g. lifecycle issues)
        // it could be done here, but typically viewDidLoad or a specific trigger is better.
        // Ensure that processSharedItem is only called once effectively.
        if !initialURLProcessed && mainViewModel.currentVideoID == nil {
             Logger.shared.warning("ShareHostingController: Reprocessing shared item in viewDidAppear as it wasn't processed.", category: .lifecycle)
             processSharedItem()
        }
    }

    private func setupSwiftUIView() {
        let rootView = RootView()
            .environmentObject(mainViewModel) // Provide MainViewModel
            .environmentObject(appServices.cacheManager) // Provide CacheManager if needed by RootView or its children
            .environmentObject(appServices) // Provide AppServices for any other service access

        hostingController = UIHostingController(rootView: rootView)
        guard let hc = hostingController else { return }

        addChild(hc)
        view.addSubview(hc.view)
        hc.view.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            hc.view.topAnchor.constraint(equalTo: view.topAnchor),
            hc.view.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            hc.view.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            hc.view.trailingAnchor.constraint(equalTo: view.trailingAnchor)
        ])
        hc.didMove(toParent: self)
        Logger.shared.info("SwiftUI RootView hosted.", category: .lifecycle)
    }

    private func processSharedItem() {
        initialURLProcessed = true // Mark as attempted
        Logger.shared.debug("ShareHostingController: Attempting to process shared item.", category: .lifecycle)

        guard let extensionContext = self.extensionContext,
              let inputItems = extensionContext.inputItems as? [NSExtensionItem] else {
            Logger.shared.error("Failed to get extension context or input items.", category: .lifecycle)
            mainViewModel.setError(message: "Could not read shared content.", canRetry: false)
            // Optionally call self.dismissExtension(withError: true) after a delay
            return
        }

        guard !inputItems.isEmpty else {
            Logger.shared.warning("No input items found in extension context.", category: .lifecycle)
            mainViewModel.setError(message: "No content was shared.", canRetry: false)
            return
        }
        
        // Prioritize URL type
        let urlTypeIdentifier = UTType.url.identifier
        let textTypeIdentifier = UTType.plainText.identifier

        for item in inputItems {
            if let attachments = item.attachments {
                for attachment in attachments {
                    if attachment.hasItemConformingToTypeIdentifier(urlTypeIdentifier) {
                        attachment.loadItem(forTypeIdentifier: urlTypeIdentifier, options: nil) { [weak self] (data, error) in
                            DispatchQueue.main.async {
                                guard let self = self else { return }
                                if let url = data as? URL {
                                    Logger.shared.info("Successfully loaded URL: \(url.absoluteString)", category: .lifecycle)
                                    self.mainViewModel.processSharedURL(url)
                                    return // Process first valid URL
                                } else if let error = error {
                                    Logger.shared.error("Error loading URL item: \(error.localizedDescription)", category: .lifecycle)
                                } else {
                                     Logger.shared.error("Failed to cast item to URL.", category: .lifecycle)
                                }
                            }
                        }
                        return // Process first provider that has a URL
                    }
                }
                // If no URL type found, try text type for URLs within text
                for attachment in attachments {
                     if attachment.hasItemConformingToTypeIdentifier(textTypeIdentifier) {
                        attachment.loadItem(forTypeIdentifier: textTypeIdentifier, options: nil) { [weak self] (data, error) in
                            DispatchQueue.main.async {
                                guard let self = self else { return }
                                if let text = data as? String, let url = URLParser.firstSupportedVideoURL(in: text) {
                                    Logger.shared.info("Successfully parsed URL from text: \(url.absoluteString)", category: .lifecycle)
                                    self.mainViewModel.processSharedURL(url)
                                    return // Process first valid URL found in text
                                } else if let error = error {
                                    Logger.shared.error("Error loading text item: \(error.localizedDescription)", category: .lifecycle)
                                } else if data != nil {
                                     Logger.shared.warning("Shared text did not contain a parsable video URL.", category: .lifecycle)
                                }
                            }
                        }
                        return // Process first provider that has text
                    }
                }
            }
        }
        // If loop completes and no URL processed by mainViewModel
        // (e.g., all attachments failed or were not of expected type)
        // Check if an error has already been set by one of the failed loadItems.
        // If not, set a generic error.
        if mainViewModel.currentVideoID == nil && mainViewModel.viewState != .error { // Check if already errored or processing
            Logger.shared.warning("No suitable URL or text item found after checking all attachments.", category: .lifecycle)
            mainViewModel.setError(message: "Could not find a valid video link in the shared content.", canRetry: false)
        }
    }
    
    @objc private func handleDismissNotification() {
        Logger.shared.info("Dismiss notification received. Closing extension.", category: .lifecycle)
        self.dismissExtension(withError: false)
    }

    private func dismissExtension(withError: Bool) {
        if withError {
            // Cancel any ongoing work in the extensionContext
            self.extensionContext?.cancelRequest(withError: NSError(domain: AppConstants.bundleID, code: 1, userInfo: [NSLocalizedDescriptionKey: "Share extension cancelled with error."]))
        } else {
            self.extensionContext?.completeRequest(returningItems: [], completionHandler: nil)
        }
        // Release the lock when the UI is being dismissed
        FileLock.release("com.worthitai.share.active.lock")
    }

    deinit {
        NotificationCenter.default.removeObserver(self)
        Logger.shared.info("ShareHostingController deinitialized.", category: .lifecycle)
    }
}

// Notification name for dismissal
extension Notification.Name {
    static let shareExtensionShouldDismiss = Notification.Name("ShareExtensionShouldDismiss")
}
