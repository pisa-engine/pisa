#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <thread>

namespace pisa::v1 {

std::ostream &format_interval(std::ostream &out, std::chrono::seconds time);

using CallbackFunction = std::function<void(
    std::size_t, std::size_t, std::chrono::time_point<std::chrono::steady_clock>)>;

struct DefaultProgress {
    DefaultProgress() = default;
    explicit DefaultProgress(std::string caption);
    DefaultProgress(DefaultProgress const &) = default;
    DefaultProgress(DefaultProgress &&) noexcept = default;
    DefaultProgress &operator=(DefaultProgress const &) = default;
    DefaultProgress &operator=(DefaultProgress &&) noexcept = default;
    ~DefaultProgress() = default;

    void operator()(std::size_t count,
                    std::size_t goal,
                    std::chrono::time_point<std::chrono::steady_clock> start);

   private:
    std::size_t m_previous = 0;
    std::string m_caption;
};

struct ProgressStatus {
    template <typename Callback, typename Duration>
    explicit ProgressStatus(std::size_t count, Callback &&callback, Duration interval)
        : m_goal(count), m_callback(std::forward<Callback>(callback))
    {
        m_loop = std::thread([this, interval]() {
            this->m_callback(this->m_count.load(), this->m_goal, this->m_start);
            while (this->m_count.load() < this->m_goal) {
                std::this_thread::sleep_for(interval);
                this->m_callback(this->m_count.load(), this->m_goal, this->m_start);
            }
        });
    }
    ProgressStatus(ProgressStatus const &) = delete;
    ProgressStatus(ProgressStatus &&) = delete;
    ProgressStatus &operator=(ProgressStatus const &) = delete;
    ProgressStatus &operator=(ProgressStatus &&) = delete;
    ~ProgressStatus();
    void operator+=(std::size_t inc) { m_count += inc; }
    void operator++() { m_count += 1; }
    void operator++(int) { m_count += 1; }

   private:
    std::size_t const m_goal;
    std::function<void(
        std::size_t, std::size_t, std::chrono::time_point<std::chrono::steady_clock>)>
        m_callback;
    std::atomic_size_t m_count = 0;
    std::chrono::time_point<std::chrono::steady_clock> m_start = std::chrono::steady_clock::now();
    std::thread m_loop;
};

} // namespace pisa::v1
