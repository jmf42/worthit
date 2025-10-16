import SwiftUI
import StoreKit

struct PaywallCard: View {
    @EnvironmentObject private var viewModel: MainViewModel
    @EnvironmentObject private var subscriptionManager: SubscriptionManager
    @Environment(\.openURL) private var openURL

    let context: MainViewModel.PaywallContext
    let isInExtension: Bool

    @State private var selectedPlanID: String?
    @State private var purchaseInFlight: String?
    @State private var isRestoring = false
    @State private var infoMessage: String?
    @State private var infoMessageColor: Color = Theme.Color.secondaryText
    @State private var didTriggerExtensionHandoff = false

    private var plans: [PaywallPlanOption] {
        let products = subscriptionManager.products
        let annual = products.first { $0.id == AppConstants.subscriptionProductAnnualID }
        let monthly = products.first { $0.id == AppConstants.subscriptionProductMonthlyID }

        return [
            PaywallPlanOption(
                id: AppConstants.subscriptionProductAnnualID,
                title: "Annual",
                priceText: annual?.displayPrice ?? "Loading…",
                detailText: "Best value • Billed yearly",
                badge: "Popular",
                product: annual,
                isRecommended: true
            ),
            PaywallPlanOption(
                id: AppConstants.subscriptionProductMonthlyID,
                title: "Monthly",
                priceText: monthly?.displayPrice ?? "Loading…",
                detailText: "Cancel anytime",
                badge: nil,
                product: monthly,
                isRecommended: false
            )
        ]
    }

    private var selectedPlan: PaywallPlanOption? {
        if let current = selectedPlanID, let match = plans.first(where: { $0.id == current }) {
            return match
        }
        return plans.first
    }

    private var usageProgress: Double {
        guard context.usageSnapshot.limit > 0 else { return 1 }
        return min(1, Double(context.usageSnapshot.count) / Double(context.usageSnapshot.limit))
    }

    var body: some View {
        VStack(spacing: 18) {
            header
            featureHighlights
            planSelector

            if let message = infoMessage {
                Text(message)
                    .font(Theme.Font.caption)
                    .foregroundColor(infoMessageColor)
                    .multilineTextAlignment(.center)
                    .padding(.top, 2)
            }

            primaryActions
        }
        .frame(maxWidth: 340)
        .padding(.vertical, 22)
        .padding(.horizontal, 20)
        .background(
            RoundedRectangle(cornerRadius: 24, style: .continuous)
                .fill(Theme.Color.sectionBackground.opacity(0.95))
                .overlay(
                    RoundedRectangle(cornerRadius: 24, style: .continuous)
                        .stroke(Theme.Color.accent.opacity(0.26), lineWidth: 1)
                )
                .shadow(color: Theme.Color.darkBackground.opacity(0.45), radius: 22, y: 16)
        )
        .onAppear {
            if selectedPlanID == nil, let defaultPlan = plans.first?.id {
                selectedPlanID = defaultPlan
            }
            if isInExtension {
                infoMessage = "Open the WorthIt app to finish upgrading."
                infoMessageColor = Theme.Color.secondaryText
            }
            viewModel.paywallPresented(reason: context.reason)
            Task {
                await subscriptionManager.refreshProducts()
                await subscriptionManager.refreshEntitlement()
            }
        }
        .onChange(of: subscriptionManager.products) { _ in
            if selectedPlanID == nil, let defaultPlan = plans.first?.id {
                selectedPlanID = defaultPlan
            }
        }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack(spacing: 12) {
                Image("AppLogo")
                    .resizable()
                    .scaledToFit()
                    .frame(width: 40, height: 40)
                    .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                    .shadow(color: Color.black.opacity(0.18), radius: 8, y: 6)

                VStack(alignment: .leading, spacing: 4) {
                    Text("WorthIt Premium")
                        .font(Theme.Font.title3.weight(.semibold))
                        .foregroundColor(Theme.Color.primaryText)
                    Text("Unlimited breakdowns. Zero waiting for tomorrow.")
                        .font(Theme.Font.caption)
                        .foregroundColor(Theme.Color.secondaryText)
                }

                Spacer()
            }

            usageSummary
        }
    }

    private var usageSummary: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Today’s free limit")
                    .font(Theme.Font.captionBold)
                    .foregroundColor(Theme.Color.secondaryText)

                Spacer()

                Text("\(context.usageSnapshot.count) of \(context.usageSnapshot.limit) used")
                    .font(Theme.Font.captionBold)
                    .foregroundColor(Theme.Color.primaryText)
            }

            ProgressView(value: usageProgress)
                .progressViewStyle(LinearProgressViewStyle(tint: Theme.Color.accent))

            Text("Reset tomorrow at midnight.")
                .font(Theme.Font.caption)
                .foregroundColor(Theme.Color.secondaryText)
        }
        .padding(14)
        .background(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .fill(Theme.Color.sectionBackground.opacity(0.9))
        )
    }

    private var featureHighlights: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("What you unlock")
                .font(Theme.Font.subheadlineBold)
                .foregroundColor(Theme.Color.primaryText)

            HStack(spacing: 12) {
                Image(systemName: "infinity")
                    .font(.system(size: 18, weight: .bold))
                    .foregroundColor(Theme.Color.accent)
                    .frame(width: 32, height: 32)
                    .background(Circle().fill(Theme.Color.sectionBackground.opacity(0.8)))

                VStack(alignment: .leading, spacing: 4) {
                    Text("Unlimited breakdowns")
                        .font(Theme.Font.subheadline.weight(.semibold))
                        .foregroundColor(Theme.Color.primaryText)
                    Text("Analyze every video back-to-back, no daily wait.")
                        .font(Theme.Font.caption)
                        .foregroundColor(Theme.Color.secondaryText)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        }
    }

    private var planSelector: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Pick your plan")
                .font(Theme.Font.subheadlineBold)
                .foregroundColor(Theme.Color.primaryText)

            ForEach(plans) { plan in
                Button(action: { select(plan) }) {
                    HStack(alignment: .center, spacing: 10) {
                        VStack(alignment: .leading, spacing: 6) {
                            HStack(spacing: 6) {
                                Text(plan.title)
                                    .font(Theme.Font.subheadline.weight(.semibold))
                                    .foregroundColor(Theme.Color.primaryText)

                                if let badge = plan.badge, plan.isRecommended {
                                    Text(badge.uppercased())
                                        .font(Theme.Font.captionBold)
                                        .padding(.horizontal, 6)
                                        .padding(.vertical, 2)
                                        .background(Theme.Color.accent.opacity(0.2))
                                        .foregroundColor(Theme.Color.accent)
                                        .clipShape(Capsule())
                                }
                            }

                            Text(plan.priceText)
                                .font(Theme.Font.title3.weight(.bold))
                                .foregroundColor(Theme.Color.primaryText)

                            Text(plan.detailText)
                                .font(Theme.Font.caption)
                                .foregroundColor(Theme.Color.secondaryText)
                        }

                        Spacer(minLength: 12)

                        Image(systemName: selectedPlan?.id == plan.id ? "checkmark.circle.fill" : "circle")
                            .font(.system(size: 20, weight: .semibold))
                            .foregroundColor(selectedPlan?.id == plan.id ? Theme.Color.accent : Theme.Color.secondaryText.opacity(0.5))
                    }
                    .padding(.vertical, 12)
                    .padding(.horizontal, 14)
                    .background(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .fill(Theme.Color.sectionBackground.opacity(selectedPlan?.id == plan.id ? 0.92 : 0.75))
                            .overlay(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .stroke(selectedPlan?.id == plan.id ? Theme.Color.accent.opacity(0.45) : Theme.Color.sectionBackground.opacity(0.3), lineWidth: 1)
                            )
                    )
                }
                .buttonStyle(.plain)
                .disabled(plan.product == nil)
            }
        }
    }

    private var primaryActions: some View {
        VStack(spacing: 16) {
            if isInExtension {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Finish in the WorthIt app")
                        .font(Theme.Font.subheadlineBold)
                        .foregroundColor(Theme.Color.primaryText)

                    Text("Finish the upgrade inside the WorthIt app when you're ready.")
                        .font(Theme.Font.caption)
                        .foregroundColor(Theme.Color.secondaryText)

                    Button(action: {
                        if didTriggerExtensionHandoff == false {
                            didTriggerExtensionHandoff = true
                        }
                        notifyOpenMainApp()
                    }) {
                        HStack(spacing: 12) {
                            Image(systemName: "arrow.up.forward.app")
                                .font(.system(size: 16, weight: .semibold))
                            Text("Open WorthIt app")
                                .font(Theme.Font.body.weight(.semibold))
                            Spacer()
                            Image(systemName: "chevron.right")
                                .font(.system(size: 14, weight: .bold))
                        }
                        .foregroundColor(Theme.Color.primaryText)
                        .padding(.horizontal, 16)
                        .padding(.vertical, 12)
                        .background(
                            RoundedRectangle(cornerRadius: 16, style: .continuous)
                                .fill(Theme.Color.sectionBackground.opacity(0.85))
                                .overlay(
                                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                                        .stroke(Theme.Color.accent.opacity(0.25), lineWidth: 1)
                                )
                        )
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("Open WorthIt app to subscribe")
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(16)
                .background(
                    RoundedRectangle(cornerRadius: 18, style: .continuous)
                        .fill(Theme.Color.sectionBackground.opacity(0.9))
                        .overlay(
                            RoundedRectangle(cornerRadius: 18, style: .continuous)
                                .stroke(Theme.Color.accent.opacity(0.18), lineWidth: 1)
                        )
                )
            } else {
                Button(action: attemptPurchase) {
                    ZStack {
                        RoundedRectangle(cornerRadius: 18, style: .continuous)
                            .fill(Theme.Gradient.appBluePurple)
                            .overlay(
                                RoundedRectangle(cornerRadius: 18, style: .continuous)
                                    .fill(Theme.Color.sectionBackground.opacity(0.65))
                                    .opacity(selectedPlan?.product == nil ? 1 : 0)
                            )

                        if purchaseInFlight != nil {
                            ProgressView()
                                .progressViewStyle(CircularProgressViewStyle(tint: .white))
                        } else {
                            Text("Continue with \(selectedPlan?.title ?? "plan")")
                                .font(Theme.Font.body.weight(.semibold))
                        }
                    }
                    .frame(maxWidth: .infinity, minHeight: 52)
                }
                .disabled(purchaseInFlight != nil || selectedPlan?.product == nil)
                .foregroundColor(selectedPlan?.product == nil ? Theme.Color.secondaryText : .white)
                .animation(.easeInOut(duration: 0.2), value: purchaseInFlight)
            }

            Button(action: dismissTapped) {
                Text("Maybe later")
                    .font(Theme.Font.body.weight(.semibold))
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 12)
            }
            .buttonStyle(.plain)
            .foregroundColor(Theme.Color.secondaryText)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Theme.Color.sectionBackground.opacity(0.65))
            )

            if !isInExtension {
                HStack(spacing: 12) {
                    Button(action: restorePurchases) {
                        if isRestoring {
                            ProgressView()
                                .progressViewStyle(CircularProgressViewStyle(tint: Theme.Color.accent))
                        } else {
                            Text("Restore purchases")
                                .font(Theme.Font.caption)
                                .foregroundColor(Theme.Color.secondaryText)
                        }
                    }
                    .disabled(isRestoring)

                    Spacer(minLength: 0)

                    Button(action: openManageSubscriptions) {
                        Text("Manage subscription")
                            .font(Theme.Font.caption)
                            .foregroundColor(Theme.Color.secondaryText)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
    }

    private func select(_ plan: PaywallPlanOption) {
        selectedPlanID = plan.id
        if isInExtension {
            if let product = plan.product {
                viewModel.paywallPurchaseTapped(productId: product.id)
            }
            infoMessage = "Open the WorthIt app to finish subscribing."
            infoMessageColor = Theme.Color.secondaryText

            if didTriggerExtensionHandoff == false, plan.product != nil {
                didTriggerExtensionHandoff = true
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.35) {
                    notifyOpenMainApp()
                }
            }
        } else {
            infoMessage = nil
            infoMessageColor = Theme.Color.secondaryText
        }
    }

    private func attemptPurchase() {
        guard let plan = selectedPlan else {
            infoMessage = "Select a plan to continue."
            infoMessageColor = Theme.Color.warning
            return
        }
        guard let product = plan.product else {
            infoMessage = "Pricing is still loading. Please try again in a moment."
            infoMessageColor = Theme.Color.warning
            return
        }
        guard purchaseInFlight == nil else { return }

        purchaseInFlight = plan.id
        infoMessage = nil
        infoMessageColor = Theme.Color.secondaryText
        viewModel.paywallPurchaseTapped(productId: plan.id)

        Task { @MainActor in
            let outcome = await subscriptionManager.purchase(product)
            purchaseInFlight = nil
            switch outcome {
            case .success(let productId):
                viewModel.paywallPurchaseSucceeded(productId: productId)
            case .userCancelled:
                infoMessage = "Purchase cancelled."
                infoMessageColor = Theme.Color.secondaryText
                viewModel.paywallPurchaseCancelled(productId: plan.id)
            case .pending:
                infoMessage = "Purchase pending. We'll unlock Premium once it's confirmed."
                infoMessageColor = Theme.Color.secondaryText
            case .failed:
                infoMessage = "Something went wrong. Please try again."
                infoMessageColor = Theme.Color.warning
                viewModel.paywallPurchaseFailed(productId: plan.id)
            }
        }
    }

    private func restorePurchases() {
        guard !isRestoring else { return }
        isRestoring = true
        infoMessage = nil
        infoMessageColor = Theme.Color.secondaryText
        viewModel.paywallRestoreTapped()

        Task { @MainActor in
            do {
                try await subscriptionManager.restorePurchases()
                infoMessage = "Restored purchases."
                infoMessageColor = Theme.Color.secondaryText
            } catch {
                infoMessage = "Could not restore purchases."
                infoMessageColor = Theme.Color.warning
                Logger.shared.error("Failed to restore purchases: \(error.localizedDescription)", category: .purchase, error: error)
            }
            isRestoring = false
        }
    }

    private func openManageSubscriptions() {
        viewModel.paywallManageTapped()
        openURL(subscriptionManager.manageSubscriptionsURL())
    }

    private func dismissTapped() {
        viewModel.dismissPaywall()
        if isInExtension {
            NotificationCenter.default.post(name: .shareExtensionShouldDismissGlobal, object: nil)
        }
    }

    private func notifyOpenMainApp() {
        guard isInExtension else { return }
        infoMessage = "Opening WorthIt…"
        infoMessageColor = Theme.Color.secondaryText
        didTriggerExtensionHandoff = true
        NotificationCenter.default.post(name: .shareExtensionOpenMainApp, object: nil)
    }
}

struct PaywallPlanOption: Identifiable, Equatable {
    let id: String
    let title: String
    let priceText: String
    let detailText: String
    let badge: String?
    let product: Product?
    let isRecommended: Bool
}
