#pragma once

#include <iostream>
#include <optional>
#include <string>

#include <CLI/CLI.hpp>
#include <CLI/Option.hpp>
#include <CLI/Optional.hpp>
#include <boost/filesystem.hpp>

namespace pisa {

template <typename T>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<std::optional<T>> : std::true_type {
};

template <typename T>
std::ostream &operator<<(std::ostream &os, std::optional<T> const &value)
{
    os << value.value_or(T{});
    return os;
}

template <class T>
struct DefaultValue {
    T value;
};

struct DefaultValueKeyword {
    template <class T>
    std::optional<DefaultValue<T>> operator=(T value)
    {
        return {{value}};
    }
} default_value;

template <class T>
class Option {
   public:
    Option(CLI::Option *option) : option_(option) {}

    [[nodiscard]] auto operator*() const -> T const& { return *value_; }

    template <class... Fn>
    auto with(Fn... fn) && -> Option<T> &&
    {
        (fn(option_), ...);
        return std::move(*this);
    }

    template <class... Fn>
    auto apply(Fn... fn) -> Option<T> &
    {
        (fn(option_), ...);
        return *this;
    }

    explicit operator bool() const { return static_cast<bool>(*option_); }

    [[nodiscard]] auto opt() -> CLI::Option * { return option_; }

   private:
    CLI::Option *option_;
    std::unique_ptr<T> value_;
};

[[nodiscard]] auto flag(CLI::App &app, std::string const &name, std::string const &description) {
    auto val = std::make_unique<bool>(false);
    return Option<bool>(app.add_flag(name, description));
}

template <class T, class U = T, class = std::enable_if_t<not is_optional<T>::value>>
[[nodiscard]] auto option(CLI::App &app,
                          std::string const &name,
                          std::string const &description,
                          std::optional<DefaultValue<U>> def = std::nullopt)
{
    auto val = std::make_unique<T>();
    if (def) {
        *val = def->value;
    }
    return Option<T>(app.add_option(name, *val, description, def.has_value()));
}

template <class T, class = std::enable_if_t<is_optional<T>::value>>
[[nodiscard]] auto option(CLI::App &app, std::string const &name, std::string const &description)
{
    auto val = std::make_unique<T>();
    return Option<T>(app.add_option(name, *val, description));
}

[[nodiscard]] auto valid_basename(std::string const &basename) -> std::string
{
    boost::filesystem::path p(basename);
    auto parent = p.parent_path();
    if (not boost::filesystem::exists(parent) or not boost::filesystem::is_directory(parent)) {
        return fmt::format(
            "Basename {} invalid: path {} is not an existing directory", basename, parent.string());
    }
    return std::string();
};

void required(CLI::Option *opt) { opt->required(); }

template <typename Pred>
auto check(Pred pred)
{
    return [=](CLI::Option *opt) { opt->check(pred); };
}

template <typename T>
auto needs(Option<T> &opt)
{
    return [&](CLI::Option *o) { o->needs(opt.opt()); };
}

namespace options {

    [[nodiscard]] auto threads(CLI::App &app)
    {
        return option<std::size_t>(
            app,
            "-j,--threads",
            "Number of threads",
            default_value = static_cast<std::size_t>(std::thread::hardware_concurrency()));
    }

    [[nodiscard]] auto stemmer(CLI::App &app)
    {
        return option<std::optional<std::string>>(app, "--stemmer", "Stemmer type");
    }

    [[nodiscard]] auto debug(CLI::App &app)
    {
        return flag(app, "--debug", "Print out debug log messages");
    }

    [[nodiscard]] auto k(CLI::App &app)
    {
        return option<int>(app, "-k", "Number of top results", default_value = 1000);
    }

    template <typename U = std::size_t>
    [[nodiscard]] auto batch_size(CLI::App &app, std::optional<DefaultValue<U>> def = std::nullopt)
    {
        return option<std::size_t>(app, "-b,--batch-size", "Batch size", def);
    }

    template <typename U = std::string>
    [[nodiscard]] auto record_format(CLI::App &app,
                                     std::optional<DefaultValue<U>> def =
                                         std::make_optional(DefaultValue<std::string>{"plaintext"}))
    {
        return option<std::string>(app, "--record-fmt", "Record format", def);
    }

    [[nodiscard]] auto content_parser(CLI::App &app)
    {
        return option<std::optional<std::string>>(app, "--content-parser", "Content parser");
    }

    [[nodiscard]] auto index_type(CLI::App &app)
    {
        return option<std::string>(app, "-t,--type", "Index type");
    }

    [[nodiscard]] auto index_basename(CLI::App &app)
    {
        return option<std::string>(app, "-i,--index", "Index basename");
    }

    [[nodiscard]] auto wand_data(CLI::App &app)
    {
        return option<std::optional<std::string>>(app, "-w,--wand", "WAND data filename");
    }

    [[nodiscard]] auto wand_compressed(CLI::App &app)
    {
        return flag(app, "--compressed-wand", "Compressed WAND input file");
    }
}

} // namespace pisa
