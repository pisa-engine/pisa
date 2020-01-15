#include "v1/progress_status.hpp"

#include <iomanip>
#include <ios>

#include <fmt/format.h>

namespace pisa::v1 {

using std::chrono::hours;
using std::chrono::minutes;
using std::chrono::seconds;

[[nodiscard]] auto format_interval(std::chrono::seconds time) -> std::string
{
    hours h = std::chrono::duration_cast<hours>(time);
    minutes m = std::chrono::duration_cast<minutes>(time - h);
    seconds s = std::chrono::duration_cast<seconds>(time - h - m);
    std::ostringstream os;
    if (h.count() > 0) {
        os << h.count() << "h ";
    }
    if (m.count() > 0) {
        os << m.count() << "m ";
    }
    os << s.count() << "s";
    return os.str();
}

[[nodiscard]] auto estimate_remaining(seconds elapsed, Progress const& progress) -> seconds
{
    return seconds(elapsed.count() * (progress.target - progress.count) / progress.count);
}

DefaultProgressCallback::DefaultProgressCallback(std::string caption)
    : m_caption(std::move(caption))
{
    if (not m_caption.empty()) {
        m_caption.append(": ");
    }
}

void DefaultProgressCallback::operator()(Progress current_progress,
                                         std::chrono::time_point<std::chrono::steady_clock> start)
{
    std::size_t progress = (100 * current_progress.count) / current_progress.target;
    m_previous = progress;
    std::chrono::seconds elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start);
    auto msg = fmt::format(
        "{}{}\% [{}]{}", m_caption, progress, format_interval(elapsed), [&]() -> std::string {
            if (current_progress.count > 0 && progress < 100) {
                return fmt::format(" [<{}]",
                                   format_interval(estimate_remaining(elapsed, current_progress)));
            }
            return "";
        }());
    std::cerr << '\r' << std::left << std::setfill(' ') << std::setw(m_prev_msg_len) << msg;
    if (progress == 100) {
        std::cerr << '\n';
    }
    m_prev_msg_len = msg.size();
}

void ProgressStatus::close()
{
    if (m_open) {
        m_count = m_target;
        m_loop.join();
        m_open = false;
    }
}

ProgressStatus::~ProgressStatus() { close(); }

} // namespace pisa::v1
