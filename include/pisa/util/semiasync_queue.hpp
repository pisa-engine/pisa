#pragma once

#include <deque>
#include <memory>
#include <thread>

#include "spdlog/spdlog.h"
#include "util/util.hpp"

namespace pisa {

class semiasync_queue {
  public:
    explicit semiasync_queue(double work_per_thread)
        : m_expected_work(0), m_work_per_thread(work_per_thread)
    {
        m_max_threads = std::thread::hardware_concurrency();
        spdlog::info("semiasync_queue using {} worker threads", m_max_threads);
    }

    class job {
      public:
        job() = default;
        job(job const&) = default;
        job(job&&) noexcept = default;
        job& operator=(job const&) = default;
        job& operator=(job&&) noexcept = default;
        virtual ~job() = default;
        virtual void prepare() = 0;
        virtual void commit() = 0;
    };

    using job_ptr_type = std::shared_ptr<job>;

    void add_job(job_ptr_type j, double expected_work)
    {
        if (m_max_threads != 0U) {
            m_next_thread.first.push_back(j);
            m_expected_work += expected_work;
            if (m_expected_work >= m_work_per_thread) {
                spawn_next_thread();
            }
        } else {  // all in main thread
            j->prepare();
            j->commit();
            j.reset();
        }
    }

    void complete()
    {
        if (!m_next_thread.first.empty()) {
            spawn_next_thread();
        }
        while (!m_running_threads.empty()) {
            commit_thread();
        }
    }

  private:
    void spawn_next_thread()
    {
        if (m_running_threads.size() == m_max_threads) {
            commit_thread();
        }

        m_running_threads.emplace_back();
        std::swap(m_next_thread, m_running_threads.back());

        std::vector<job_ptr_type> const& cur_queue = m_running_threads.back().first;
        m_running_threads.back().second = std::thread([&]() {
            for (auto const& j: cur_queue) {
                j->prepare();
            }
        });

        m_expected_work = 0;
    }

    void commit_thread()
    {
        assert(!m_running_threads.empty());
        m_running_threads.front().second.join();
        for (auto& j: m_running_threads.front().first) {
            j->commit();
            j.reset();
        }
        m_running_threads.pop_front();
    }

    using thread_t = std::pair<std::vector<job_ptr_type>, std::thread>;
    thread_t m_next_thread;
    std::deque<thread_t> m_running_threads;

    size_t m_expected_work;
    double m_work_per_thread;
    size_t m_max_threads;
};

}  // namespace pisa
