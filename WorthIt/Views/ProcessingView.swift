
//
//  ProcessingView.swift
//  WorthIt
//
//  Created by Your Divine AI
//

import SwiftUI

struct ProcessingView: View {
    // This view is now self-contained and orchestrates its own 3-second animation.
    @State private var isAnimating = false
    @State private var currentStatusIndex = 0
    @State private var displayProgress: Double = 0.0
    
    private let statusMessages = [
        "Analyzing Transcript...",
        "Scanning Comments...",
        "Generating Insights...",
        "Calculating Score..."
    ]
    
    // This timer controls the status message updates, fitting them within the 3s window.
    private let timer = Timer.publish(every: 0.75, on: .main, in: .common).autoconnect()

    var body: some View {
        ZStack {
            Theme.Color.darkBackground.ignoresSafeArea()
            Theme.Gradient.subtleGlow
                .opacity(0.4)
                .ignoresSafeArea()

            VStack(spacing: 40) {
                Spacer()
                
                // The enhanced "Neural Pulse" animation
                ZStack {
                    ForEach(0..<5) { i in
                        Circle()
                            .stroke(
                                LinearGradient(
                                    gradient: Gradient(colors: [Color.blue.opacity(0.7), Color.purple.opacity(0.1)]),
                                    startPoint: .top,
                                    endPoint: .bottom
                                ),
                                lineWidth: 1.5
                            )
                            .scaleEffect(isAnimating ? 1.8 : 0.1)
                            .opacity(isAnimating ? 0 : 1)
                            .animation(
                                .easeInOut(duration: 2.5)
                                .repeatForever(autoreverses: false)
                                .delay(Double(i) * 0.4),
                                value: isAnimating
                            )
                    }
                    
                    Image("AppLogo")
                        .resizable()
                        .scaledToFit()
                        .frame(width: 72, height: 72)
                        .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                        .shadow(color: .black.opacity(0.25), radius: 6, y: 2)
                }
                .frame(width: 200, height: 200)

                // Dynamic status text, synchronized with the timer
                Text(statusMessages[currentStatusIndex])
                    .font(Theme.Font.headline)
                    .foregroundColor(Theme.Color.primaryText)
                    .transition(.opacity.combined(with: .move(edge: .bottom)))
                    .id("StatusText_\(currentStatusIndex)")

                Spacer()
                
                // The progress bar, driven by the 3-second animation
                ShimmeringProgressView(progress: displayProgress)
                    .padding(.horizontal, 40)
            }
            .padding()
        }
        .ignoresSafeArea(.keyboard) // Prevent keyboard-induced layout shifts in Share extension
        .transition(.opacity)
        .onAppear {
            isAnimating = true
            // Start the 3-second progress animation immediately
            withAnimation(.linear(duration: 3.0)) {
                displayProgress = 1.0
            }
        }
        .onReceive(timer) { _ in
            // Cycle through the 4 status messages over the 3-second duration
            if currentStatusIndex < statusMessages.count - 1 {
                withAnimation(.easeInOut(duration: 0.4)) {
                    currentStatusIndex += 1
                }
            }
        }
        .onDisappear {
            timer.upstream.connect().cancel()
        }
    }
}

// The enhanced, self-animating shimmering progress view
struct ShimmeringProgressView: View {
    let progress: Double
    @State private var shimmerPosition: CGFloat = -0.5
    
    var body: some View {
        GeometryReader { geometry in
            ZStack(alignment: .leading) {
                // Background track
                Capsule()
                    .fill(Theme.Color.sectionBackground.opacity(0.5))
                
                // Progress fill, driven by the parent view's state
                Capsule()
                    .fill(
                        LinearGradient(
                            gradient: Gradient(colors: [Color.blue, Color.purple]),
                            startPoint: .leading,
                            endPoint: .trailing
                        )
                    )
                    .frame(width: geometry.size.width * progress)
                
                // Shimmering highlight overlay
                Capsule()
                    .fill(
                        LinearGradient(
                            colors: [.white.opacity(0.6), .white.opacity(0.2), .clear],
                            startPoint: .leading,
                            endPoint: .trailing
                        )
                    )
                    .frame(width: 50)
                    .offset(x: shimmerPosition * (geometry.size.width + 50) - 50)
                    .blur(radius: 1)
            }
            .clipShape(Capsule())
            .onAppear {
                // The shimmer animation is continuous and independent
                withAnimation(.linear(duration: 1.5).repeatForever(autoreverses: false)) {
                    shimmerPosition = 1.5
                }
            }
        }
        .frame(height: 10)
    }
}

#if DEBUG
struct ProcessingView_Previews: PreviewProvider {
    static var previews: some View {
        ProcessingView()
            .preferredColorScheme(.dark)
    }
}
#endif
