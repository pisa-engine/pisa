#pragma once

#include <CLI/CLI.hpp>

namespace pisa::cli {

namespace options {

    template <typename Settings>
    auto threads(CLI::App &app, Settings &settings)
    {
        return app.add_option("-j,--threads", settings.threads, "Thread count");
    }

    template <typename Settings>
    auto batch_size(CLI::App &app, Settings &settings)
    {
        return app.add_option("-b,--batch-size",
                              settings.batch_size,
                              "Number of documents to process at a time",
                              true);
    }

} // namespace options
} // namespace pisa::cli

#define PISA_MAIN(NAME, FUNCTION)                                                        \
    using namespace pisa;                                                                \
    int main(int argc, char const **argv)                                                \
    {                                                                                    \
        auto result = NAME##Settings::parse(argc, argv);                                 \
        if (auto settings = std::get_if<InvertSettings>(&result); settings != nullptr) { \
            FUNCTION(*settings);                                                         \
        } else {                                                                         \
            return *std::get_if<int>(&result);                                           \
        }                                                                                \
    }
