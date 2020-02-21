#pragma once

#include <functional>
#include <string_view>
#include <utility>

#include <fmt/format.h>

namespace pisa::v1 {

/// This is similar to the `IndexRunner` and it's used for running tasks
/// that require on-the-fly scoring.
template <typename Index, typename... Scorers>
struct ScorerRunner {
    explicit ScorerRunner(Index const& index, Scorers... scorers)
        : m_index(index), m_scorers(std::move(scorers...))
    {
    }

    template <typename Fn>
    void operator()(std::string_view scorer_name, Fn fn)
    {
        auto run = [&](auto scorer) -> bool {
            if (std::hash<std::decay_t<decltype(scorer)>>{}(scorer)
                == std::hash<std::string_view>{}(scorer_name)) {
                fn(std::move(scorer));
                return true;
            }
            return false;
        };
        bool success = std::apply(
            [&](Scorers... scorers) { return (run(std::move(scorers)) || ...); }, m_scorers);
        if (not success) {
            throw std::domain_error(fmt::format("Unknown scorer: {}", scorer_name));
        }
    }

   private:
    Index const& m_index;
    std::tuple<Scorers...> m_scorers;
};

template <typename Index, typename... Scorers>
auto scorer_runner(Index const& index, Scorers... scorers)
{
    return ScorerRunner(index, std::move(scorers...));
}

} // namespace pisa::v1
