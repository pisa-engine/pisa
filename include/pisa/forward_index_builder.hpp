#pragma once

#include <algorithm>
#include <atomic>
#include <fstream>
#include <functional>
#include <iostream>
#include <numeric>
#include <optional>
#include <thread>
#include <unordered_set>
#include <vector>

#include "boost/filesystem.hpp"
#include "pstl/algorithm"
#include "pstl/execution"
#include "spdlog/fmt/ostr.h"
#include "spdlog/spdlog.h"
#include "tbb/concurrent_queue.h"
#include "tbb/task_group.h"

#include "binary_collection.hpp"
#include "enumerate.hpp"
#include "parsing/html.hpp"
#include "warcpp/warcpp.hpp"
#include "util/util.hpp"

namespace pisa {

using process_term_function_type    = std::function<std::string(std::string &&)>;
using process_content_function_type =
    std::function<void(std::string &&, std::function<void(std::string &&)>)>;

template <class Tag, class T, T default_value = 0>
class Id {
   public:
    Id() : m_val(default_value) {}
    explicit Id(T val) : m_val(val) {}
    explicit operator T() const { return m_val; }

    [[nodiscard]] bool operator==(Id other) const { return m_val == other.m_val; }
    [[nodiscard]] bool operator!=(Id other) const { return m_val != other.m_val; }

    Id &operator++() {
        ++m_val;
        return *this;
    }

    [[nodiscard]] Id operator+(T difference) const { return Id(m_val + difference); }
    Id &operator+=(T difference) {
        m_val += difference;
        return *this;
    }

   private:
    T m_val;
};

template <class Tag, class T, T default_value>
std::ostream &operator<<(std::ostream &os, Id<Tag, T, default_value> const &id) {
    return os << static_cast<T>(id);
}

struct document_id_tag {};
using Document_Id = Id<document_id_tag, std::ptrdiff_t>;
struct term_id_tag {};
using Term_Id = Id<term_id_tag, std::ptrdiff_t>;

void parse_plaintext_content(std::string &&content, std::function<void(std::string &&)> process) {
    std::istringstream content_stream(content);
    std::string        term;
    while (content_stream >> term) {
        process(std::move(term));
    }
}

void parse_html_content(std::string &&content, std::function<void(std::string &&)> process) {
    content = parsing::html::cleantext(std::move(content));
    if (content.empty()) {
        return;
    }
    std::regex term_pattern("(\\w)+");
    auto term_it = std::sregex_iterator(content.begin(), content.end(), term_pattern);
    for (auto term_it = std::sregex_iterator(content.begin(), content.end(), term_pattern);
         term_it != std::sregex_iterator();
         ++term_it)
    {
        if (term_it->length() > 0) {
            process(term_it->str());
        }
    }
}

template <typename Record>
class Forward_Index_Builder {
   public:
    using read_record_function_type = std::function<std::optional<Record>(std::istream &)>;

    template <typename Iterator>
    static std::ostream &write_document(std::ostream &os, Iterator first, Iterator last)
    {
        std::uint32_t length = std::distance(first, last);
        os.write(reinterpret_cast<const char *>(&length), sizeof(length));
        os.write(reinterpret_cast<const char *>(&(*first)), length * sizeof(*first));
        return os;
    }

    static std::ostream &write_header(std::ostream &os, uint32_t document_count)
    {
        return write_document(os, &document_count, std::next(&document_count));
    }

    [[nodiscard]] static auto batch_file(std::string const &output_file,
                                         std::ptrdiff_t     batch_number) noexcept
        -> std::string
    {
        std::ostringstream os;
        os << output_file << ".batch." << batch_number;
        return os.str();
    }

    struct Batch_Process {
        std::ptrdiff_t               batch_number;
        std::vector<Record>          records;
        Document_Id                  first_document;
        std::string const &          output_file;
    };

    void run(Batch_Process                 bp,
             process_term_function_type    process_term,
             process_content_function_type process_content) const
    {
        auto basename = batch_file(bp.output_file, bp.batch_number);

        std::ofstream os(basename);
        std::ofstream title_os(basename + ".documents");
        std::ofstream url_os(basename + ".urls");
        std::ofstream term_os(basename + ".terms");
        write_header(os, bp.records.size());

        std::map<std::string, uint32_t> map;

        for (auto &&record : bp.records) {
            title_os << record.trecid() << '\n';
            url_os << record.url() << '\n';

            std::vector<uint32_t> term_ids;

            auto process = [&](auto &&term) {
                term = process_term(std::move(term));
                uint32_t id = 0;
                if (auto pos = map.find(term); pos != map.end()) {
                    id = pos->second;
                } else {
                    id = map.size();
                    map[term] = id;
                    term_os << term << '\n';
                }
                term_ids.push_back(id);
            };
            process_content(std::move(record.content()), process);
            write_document(os, term_ids.begin(), term_ids.end());
        }
        spdlog::info("[Batch {}] Processed documents [{}, {})",
                     bp.batch_number,
                     bp.first_document,
                     bp.first_document + bp.records.size());
    }

    void merge(std::string const &basename,
               std::ptrdiff_t     document_count,
               std::ptrdiff_t     batch_count) const
    {
        using term_map_type =
            std::map<std::string, std::unordered_map<std::ptrdiff_t, std::ptrdiff_t>>;
        std::ofstream title_os(basename + ".documents");
        std::ofstream url_os(basename + ".urls");
        std::ofstream term_os(basename + ".terms");

        spdlog::info("Merging titles");
        for (auto batch : enumerate(batch_count)) {
            std::ifstream title_is(batch_file(basename, batch) + ".documents");
            title_os << title_is.rdbuf();
        }
        spdlog::info("Merging URLs");
        for (auto batch : enumerate(batch_count)) {
            std::ifstream url_is(batch_file(basename, batch) + ".urls");
            url_os << url_is.rdbuf();
        }

        spdlog::info("Mapping terms");
        term_map_type term_map;
        std::vector<std::vector<std::ptrdiff_t>> id_mappings(batch_count);
        for (auto batch : enumerate(batch_count)) {
            std::ifstream terms_is(batch_file(basename, batch) + ".terms");
            std::string term;
            std::ptrdiff_t batch_term_id = 0;
            while (std::getline(terms_is, term)) {
                term_map[term][batch] = batch_term_id++;
            }
            id_mappings[batch].resize(batch_term_id);
        }

        spdlog::info("Mapping IDs and writing terms");
        std::ptrdiff_t term_id = 0;
        for (auto const &[term, idmap] : term_map) {
            term_os << term << '\n';
            for (auto const &[batch, batch_term_id] : idmap) {
                id_mappings[batch][batch_term_id] = term_id;
            }
            ++term_id;
        }

        spdlog::info("Remapping IDs");
        for (auto batch : enumerate(batch_count)) {
            auto &mapping = id_mappings[batch];
            writable_binary_collection coll(batch_file(basename, batch).c_str());
            for (auto doc_iter = ++coll.begin(); doc_iter != coll.end(); ++doc_iter) {
                for (auto &term_id : *doc_iter) {
                    term_id = mapping[term_id];
                }
            }
        }

        spdlog::info("Concatenating batches");
        std::ofstream os(basename);
        write_header(os, document_count);
        for (auto batch : enumerate(batch_count)) {
            std::ifstream is(batch_file(basename, batch));
            is.ignore(8);
            os << is.rdbuf();
        }

        spdlog::info("Success.");
    }

    void build(std::istream &                is,
               std::string const &           output_file,
               read_record_function_type     next_record,
               process_term_function_type    process_term,
               process_content_function_type process_content,
               std::ptrdiff_t                batch_size,
               std::size_t                   threads) const
    {
        Document_Id    first_document{0};
        std::ptrdiff_t batch_number = 0;

        std::vector<Record> record_batch;
        tbb::task_group     batch_group;
        tbb::concurrent_bounded_queue<int> queue;
        queue.set_capacity((threads - 1) * 2);
        while (true) {
            std::optional<Record> record = std::nullopt;
            try {
                if (not (record = next_record(is))) {
                    auto last_batch_size = record_batch.size();
                    Batch_Process bp{
                        batch_number, std::move(record_batch), first_document, output_file};
                    queue.push(0);
                    batch_group.run(
                        [bp = std::move(bp), process_term, this, &queue, &process_content]() {
                            run(std::move(bp), process_term, process_content);
                            int x;
                            queue.try_pop(x);
                        });
                    ++batch_number;
                    first_document += last_batch_size;
                    break;
                }
            } catch (warcpp::Warc_Format_Error &err) {
                continue;
            }
            if (not record->valid()) {
                continue;
            }
            record_batch.push_back(std::move(*record)); // AppleClang is missing value() in Optional
            if (record_batch.size() == batch_size) {
                Batch_Process bp{
                    batch_number, std::move(record_batch), first_document, output_file};
                queue.push(0);
                batch_group.run(
                    [bp = std::move(bp), process_term, this, &queue, &process_content]() {
                        run(std::move(bp), process_term, process_content);
                        int x;
                        queue.try_pop(x);
                    });
                ++batch_number;
                first_document += batch_size;
                record_batch = std::vector<Record>();
            }
        }
        batch_group.wait();
        merge(output_file, static_cast<ptrdiff_t>(first_document), batch_number);
        remove_batches(output_file, batch_number);
    }

    void remove_batches(std::string const& basename, std::ptrdiff_t batch_count) const
    {
        using boost::filesystem::path;
        using boost::filesystem::remove;
        for (auto batch : enumerate(batch_count)) {
            auto batch_basename = batch_file(basename, batch);
            remove(path{batch_basename + ".documents"});
            remove(path{batch_basename + ".terms"});
            remove(path{batch_basename + ".urls"});
            remove(path{batch_basename});
        }
    }
};

class Plaintext_Record {
   public:
    Plaintext_Record() = default;
    Plaintext_Record(std::string trecid, std::string content)
        : m_trecid(std::move(trecid)), m_content(std::move(content)) {}
    [[nodiscard]] auto content() -> std::string & { return m_content; }
    [[nodiscard]] auto content() const -> std::string const & { return m_content; }
    [[nodiscard]] auto trecid() -> std::string & { return m_trecid; }
    [[nodiscard]] auto trecid() const -> std::string const & { return m_trecid; }
    [[nodiscard]] auto url() -> std::string & { return m_url; }
    [[nodiscard]] auto url() const -> std::string const & { return m_url; }
    [[nodiscard]] auto valid() const noexcept -> bool { return true; }
    [[nodiscard]] static auto read(std::istream &is) {}

   private:
    std::string m_trecid;
    std::string m_content;
    std::string m_url;
};

} // namespace pisa

auto operator>>(std::istream &is, pisa::Plaintext_Record &record) -> std::istream &
{
    is >> record.trecid();
    std::getline(is, record.content());
    return is;
}
