#include "v1/progress_status.hpp"

namespace pisa::v1 {

std::ostream &format_interval(std::ostream &out, std::chrono::seconds time)
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

DefaultProgress::DefaultProgress(std::string caption) : m_caption(std::move(caption))
{
    if (not m_caption.empty()) {
        m_caption.append(": ");
    }
}

void DefaultProgress::operator()(std::size_t count,
                                 std::size_t goal,
                                 std::chrono::time_point<std::chrono::steady_clock> start)
{
    size_t progress = (100 * count) / goal;
    if (progress == m_previous) {
        return;
    }
    m_previous = progress;
    std::chrono::seconds elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start);
    std::cerr << '\r' << m_caption << progress << "% [";
    format_interval(std::cerr, elapsed);
    std::cerr << "]";
    if (progress == 100) {
        std::cerr << '\n';
    }
}

ProgressStatus::~ProgressStatus()
{
    m_count = m_goal;
    m_loop.join();
}

} // namespace pisa::v1
