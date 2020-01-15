#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <thread>

#include "runtime_assert.hpp"
#include "type_safe.hpp"

namespace pisa::v1 {

/// Represents progress of a certain operation.
struct Progress {
    /// A number between 0 and `target` to indicate the current progress.
    std::size_t count;
    /// The target value `count` reaches at completion.
    std::size_t target;
};

/// An alias of the type of callback function used in a progress status.
using CallbackFunction =
    std::function<void(Progress, std::chrono::time_point<std::chrono::steady_clock>)>;

/// This thread-safe object is responsible for keeping the current progress of an operation.
/// At a defined interval, it invokes a callback function with the current progress, and
/// the starting time of the operation (see `CallbackFunction` type alias).
///
/// In order to ensure that terminal updates are not iterfered with, there should be no
/// writing to stdin or stderr outside of the callback function between `ProgressStatus`
/// construction and either its destruction (e.g., by going out of scope) or closing it
/// explicitly by calling `close()` member function.
struct ProgressStatus {

    /// Constructs a new progress status.
    ///
    /// \tparam Callback    A callable that conforms to `CallbackFunction` signature.
    /// \tparam Duration    A duration type that will be called to pause the status thread.
    ///
    /// \param target       Usually a number of processed elements that will be incremented
    ///                     throughout an operation.
    /// \param callback     A function that prints out the current status. See
    ///                     `DefaultProgressCallback`.
    /// \param interval     A time interval between printing progress.
    template <typename Callback, typename Duration>
    explicit ProgressStatus(std::size_t target, Callback&& callback, Duration interval)
        : m_target(target), m_callback(std::forward<Callback>(callback))
    {
        m_loop = std::thread([this, interval]() {
            this->m_callback(Progress{this->m_count.load(), this->m_target}, this->m_start);
            while (this->m_count.load() < this->m_target) {
                std::this_thread::sleep_for(interval);
                this->m_callback(Progress{this->m_count.load(), this->m_target}, this->m_start);
            }
        });
    }
    ProgressStatus(ProgressStatus const&) = delete;
    ProgressStatus(ProgressStatus&&) = delete;
    ProgressStatus& operator=(ProgressStatus const&) = delete;
    ProgressStatus& operator=(ProgressStatus&&) = delete;
    ~ProgressStatus();

    /// Increments the counter by `inc`.
    void operator+=(std::size_t inc) { m_count += inc; }
    /// Increments the counter by 1.
    void operator++() { m_count += 1; }
    /// Increments the counter by 1.
    void operator++(int) { m_count += 1; }
    /// Sets the progress to 100% and terminates the progress thread.
    /// This function should be only called if it is known that the operation is finished.
    /// An example is a situation when it is one wants to print a message after finishing a task
    /// but before the progress status goes out of the current scope.
    /// However, otherwise it is better to just let it go out of scope, which will clean up
    /// automatically to enable further writing to the standard output.
    void close();

   private:
    std::size_t const m_target;
    CallbackFunction m_callback;
    std::atomic_size_t m_count = 0;
    std::chrono::time_point<std::chrono::steady_clock> m_start = std::chrono::steady_clock::now();
    std::thread m_loop;
    bool m_open = true;
};

/// This is the default callback that prints status in the following format:
/// `Building bigram index: 1% [1m 29s] [<1h 46m 49s]` or
/// `<caption><percent>% [<elapsed>] [<<estimated remaining>]`.
struct DefaultProgressCallback {
    DefaultProgressCallback() = default;
    explicit DefaultProgressCallback(std::string caption);
    DefaultProgressCallback(DefaultProgressCallback const&) = default;
    DefaultProgressCallback(DefaultProgressCallback&&) noexcept = default;
    DefaultProgressCallback& operator=(DefaultProgressCallback const&) = default;
    DefaultProgressCallback& operator=(DefaultProgressCallback&&) noexcept = default;
    ~DefaultProgressCallback() = default;
    void operator()(Progress progress, std::chrono::time_point<std::chrono::steady_clock> start);

   private:
    std::size_t m_previous = 0;
    std::string m_caption;
    std::size_t m_prev_msg_len = 0;
};

} // namespace pisa::v1
