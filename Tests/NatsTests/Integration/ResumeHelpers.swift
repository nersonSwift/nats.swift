// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import NIOConcurrencyHelpers

final class ResumeOnce: Sendable {
    private let done = NIOLockedValueBox(false)
    func run(_ block: () -> Void) {
        let firstCall = done.withLockedValue { done -> Bool in
            guard !done else { return false }
            done = true
            return true
        }
        if firstCall { block() }
    }
}

final class ResumeAfter: Sendable {
    private let remaining: NIOLockedValueBox<Int>
    init(_ count: Int) { remaining = NIOLockedValueBox(count) }
    func tick(_ block: () -> Void) {
        let reachedZero = remaining.withLockedValue { remaining -> Bool in
            guard remaining > 0 else { return false }
            remaining -= 1
            return remaining == 0
        }
        if reachedZero { block() }
    }
}
