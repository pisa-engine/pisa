#pragma once

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <thread>

namespace pisa {

class progress {

   public:
    progress(const std::string &name, size_t goal) : m_name(name) {
        if (goal == 0) {
            throw std::runtime_error("goal must be positive");
        }
        m_goal = goal;
    }
    ~progress() {
        m_status.notify_one();
        std::unique_lock<std::mutex> lock(m_mut);
        print_status();
        std::cerr << std::endl;
    }

    void update(size_t inc) {
        std::unique_lock<std::mutex> lock(m_mut);
        m_count += inc;
        print_status();
    }

   private:
    std::string m_name;
    size_t m_count = 0;
    size_t m_goal  = 0;

    std::chrono::time_point<std::chrono::steady_clock> m_start = std::chrono::steady_clock::now();

    std::mutex m_mut;
    std::condition_variable m_status;

    void print_status() {
        size_t progress = (100 * m_count) / m_goal;
        std::chrono::seconds elapsed  = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - m_start);
        std::cerr << '\r' << m_name << ": " << progress << "% [";
        format_interval(std::cerr, elapsed);
        std::cerr << "]";
    }

    std::ostream& format_interval(std::ostream& out, std::chrono::seconds time) {
        using std::chrono::hours;
        using std::chrono::minutes;
        using std::chrono::seconds;
        hours h = std::chrono::duration_cast<hours>(time);
        minutes m = std::chrono::duration_cast<minutes>(time - h);
        seconds s = std::chrono::duration_cast<seconds>(time - h - m);
        if (h.count() > 0) { out << h.count() << "h "; }
        if (m.count() > 0) { out << m.count() << "m "; }
        out << s.count() << "s";
        return out;
    }
};

}
