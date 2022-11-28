#pragma once

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <thread>

#include <spdlog/spdlog.h>

namespace pisa {

/**
 * Progress bar.
 *
 * Prints percentage of progress and elapsed time to TTY.
 */
class progress {
  public:
    /**
     * Creates a new progress bar.
     *
     * Unless `always_enable = true`, it will read the default spdlog logger's log level, and
     * printing to stderr will be disabled if the log level is higher than `info`.
     */
    progress(std::string const& name, size_t goal, bool always_enable = false);

    progress(progress const&) = delete;
    progress(progress&&) = delete;
    progress& operator=(progress const&) = delete;
    progress& operator=(progress&&) = delete;
    ~progress();

    /** Increment the progress counter by `inc`. */
    void update(std::size_t inc);

  private:
    std::string m_name;
    size_t m_count = 0;
    size_t m_goal = 0;
    size_t m_progress = 0;
    std::chrono::seconds m_elapsed{};

    std::chrono::time_point<std::chrono::steady_clock> m_start = std::chrono::steady_clock::now();

    std::mutex m_mut;
    std::condition_variable m_status;

    bool m_disabled = false;

    void print_status();
    std::ostream& format_interval(std::ostream& out, std::chrono::seconds time);
};

}  // namespace pisa
