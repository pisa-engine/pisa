#include "pisa/util/progress.hpp"

namespace pisa {

progress::progress(std::string const& name, size_t goal, bool always_enable) : m_name(name)
{
    if (goal == 0) {
        throw std::runtime_error("Progress bar must have a positive goal but 0 given");
    }
    m_goal = goal;
    if (!always_enable && spdlog::default_logger()->level() > spdlog::level::level_enum::info) {
        m_disabled = true;
    }
}

progress::~progress()
{
    if (!m_disabled) {
        m_status.notify_one();
        std::unique_lock<std::mutex> lock(m_mut);
        print_status();
        std::cerr << std::endl;
    }
}

void progress::update(std::size_t inc)
{
    if (!m_disabled) {
        std::unique_lock<std::mutex> lock(m_mut);
        m_count += inc;
        print_status();
    }
}

void progress::print_status()
{
    size_t progress = (100 * m_count) / m_goal;
    std::chrono::seconds elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - m_start);
    if (progress != m_progress or elapsed != m_elapsed) {
        m_progress = progress;
        m_elapsed = elapsed;
        std::cerr << '\r' << m_name << ": " << progress << "% [";
        format_interval(std::cerr, elapsed);
        std::cerr << "]";
    }
}

std::ostream& progress::format_interval(std::ostream& out, std::chrono::seconds time)
{
    using std::chrono::hours;
    using std::chrono::minutes;
    using std::chrono::seconds;
    hours h = std::chrono::duration_cast<hours>(time);
    minutes m = std::chrono::duration_cast<minutes>(time - h);
    seconds s = std::chrono::duration_cast<seconds>(time - h - m);
    if (h.count() > 0) {
        out << h.count() << "h ";
    }
    if (m.count() > 0) {
        out << m.count() << "m ";
    }
    out << s.count() << "s";
    return out;
}

}  // namespace pisa
