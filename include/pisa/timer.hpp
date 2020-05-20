#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <fmt/format.h>

namespace pisa {

/// Runs `void fn()` and returns time of its execution.
///
/// # Examples
/// ```
/// auto slept_for = irk::run_with_timer<std::chrono::seconds>([]() {
///     do_stuff();
/// });
/// ```
template <class Unit>
Unit run_with_timer(std::function<void()> fn)
{
    auto start_time = std::chrono::steady_clock::now();
    fn();
    auto end_time = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<Unit>(end_time - start_time);
}

/// Runs `void fn()` and passes the execution time to `handler`.
///
/// # Examples
/// ```
/// irk::run_with_timer<std::chrono::seconds>(
///     []() { do_stuff(); },
///     [](const auto& time) { log(time); });
/// ```
template <class Unit, class Handler>
void run_with_timer(std::function<void()> fn, Handler handler)
{
    auto start_time = std::chrono::steady_clock::now();
    fn();
    auto end_time = std::chrono::steady_clock::now();
    handler(std::chrono::duration_cast<Unit>(end_time - start_time));
}

template <class T, class Unit>
struct TimedResult {
    T result;
    Unit time;
};

/// Runs a returning function `fn()` and passes the execution time to `handler`.
///
/// \returns The result of executing `fn()`
///
/// # Examples
/// ```
/// auto stuff = irk::run_with_timer<std::chrono::seconds>(
///     []() { return get_stuff(); },
///     [](const auto& time) { log(time); });
/// ```
template <class Unit, class Function, class Handler>
auto run_with_timer_ret(Function fn, Handler handler) -> decltype(fn())
{
    auto start_time = std::chrono::steady_clock::now();
    auto result = fn();
    auto end_time = std::chrono::steady_clock::now();
    handler(std::chrono::duration_cast<Unit>(end_time - start_time));
    return result;
}

/// Runs a returning function `fn()` and returns the timed result.
///
/// \returns A `TimedResult` object with the result of `fn()` and its exectuion
///          time
///
/// # Examples
/// ```
/// auto [stuff, time] = irk::run_with_timer<std::chrono::seconds>(
///     []() { return get_stuff(); });
/// ```
template <class Unit, class Function>
auto run_with_timer_ret(Function fn) -> TimedResult<decltype(fn()), Unit>
{
    auto start_time = std::chrono::steady_clock::now();
    auto result = fn();
    auto end_time = std::chrono::steady_clock::now();
    return TimedResult<decltype(fn()), Unit>{
        result, std::chrono::duration_cast<Unit>(end_time - start_time)};
}

/// Formats time as hh:mm:ss:mil
template <class Unit>
std::string format_time(Unit time)
{
    int64_t hours = std::chrono::floor<std::chrono::hours>(time).count();
    int64_t minutes = std::chrono::floor<std::chrono::minutes>(time).count();
    int64_t seconds = std::chrono::floor<std::chrono::seconds>(time).count();
    int64_t millis = std::chrono::floor<std::chrono::milliseconds>(time).count();
    millis -= seconds * 1000;
    seconds -= minutes * 60;
    minutes -= hours * 60;
    return fmt::format("{:02d}:{:02d}:{:02d}.{:02d}", hours, minutes, seconds, millis);
}

/// Static timer allows to accumulate time in any place in the codebase to be later aggregated.
/// Useful for debugging/benchmarking.
class StaticTimer {
  public:
    StaticTimer() = default;

    [[nodiscard]] static auto get(std::string const& name) -> StaticTimer*
    {
        if (StaticTimer::m_data.find(name) == StaticTimer::m_data.end()) {
            StaticTimer::m_data[name] = std::make_unique<StaticTimer>();
        }
        return StaticTimer::m_data[name].get();
    }

    void add_time(std::chrono::nanoseconds time)
    {
        m_elapsed += time;
        // m_count += 1;
    }
    void reset()
    {
        m_elapsed = std::chrono::nanoseconds(0);
        // m_count = 0;
    }

    //[[nodiscard]] auto count() const -> std::size_t { return m_count; }

    [[nodiscard]] auto nanos() const -> std::chrono::nanoseconds { return m_elapsed; }

    [[nodiscard]] auto micros() const -> std::size_t { return m_elapsed.count() / 1000; }

    //[[nodiscard]] auto avg_nanos() const -> double
    //{
    //    return static_cast<double>(m_elapsed.count()) / m_count;
    //}

    //[[nodiscard]] auto avg_micros() const -> double
    //{
    //    return static_cast<double>(micros()) / m_count;
    //}

  private:
    static std::unordered_map<std::string, std::unique_ptr<StaticTimer>> m_data;

    std::chrono::nanoseconds m_elapsed{0};
    // std::size_t m_count{0};
};

std::unordered_map<std::string, std::unique_ptr<StaticTimer>> StaticTimer::m_data = {};

}  // namespace pisa
