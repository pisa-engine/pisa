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

#include "pstl/algorithm"
#include "pstl/execution"
#include "tbb/concurrent_queue.h"
#include "tbb/task_group.h"

#include "binary_collection.hpp"
#include "enumerate.hpp"
#include "parsing/html.hpp"
#include "parsing/warc.hpp"
#include "util/util.hpp"

namespace ds2i {

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
std::ostream &operator<<(std::ostream &os, Id<Tag, T, default_value> id) {
    return os << static_cast<T>(id);
}

struct document_id_tag {};
using Document_Id = Id<document_id_tag, std::ptrdiff_t>;
struct term_id_tag {};
using Term_Id = Id<term_id_tag, std::ptrdiff_t>;

template <typename Record>
class Forward_Index_Builder {
   public:
    using read_record_function_type  = std::function<std::optional<Record>(std::istream &)>;
    using process_term_function_type = std::function<std::string(std::string const &)>;

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

    void run(Batch_Process bp, process_term_function_type process_term) const
    {
        auto basename = batch_file(bp.output_file, bp.batch_number);

        std::ofstream os(basename);
        std::ofstream title_os(basename + ".documents");
        std::ofstream url_os(basename + ".urls");
        std::ofstream term_os(basename + ".terms");
        write_header(os, bp.records.size());

        std::map<std::string, uint32_t> map;

        for (auto const &record : bp.records) {
            title_os << record.trecid() << '\n';

            auto content = record.content();
            std::vector<uint32_t> term_ids;

            auto process = [&](auto const &term) {
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
            std::istringstream content_stream(content);
            std::string term;
            while (content_stream >> term) {
                process(term);
            }
            write_document(os, term_ids.begin(), term_ids.end());
        }
        logger() << "[Batch " << bp.batch_number << "] Processed documents [" << bp.first_document
                 << ", " << (bp.first_document + bp.records.size()) << ")\n";
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

        logger() << "Merging titles\n";
        for (auto batch : enumerate(batch_count)) {
            std::ifstream title_is(batch_file(basename, batch) + ".documents");
            title_os << title_is.rdbuf();
        }
        logger() << "Merging URLs\n";
        for (auto batch : enumerate(batch_count)) {
            std::ifstream url_is(batch_file(basename, batch) + ".urls");
            url_os << url_is.rdbuf();
        }

        logger() << "Mapping terms\n";
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

        logger() << "Mapping IDs and writing terms\n";
        std::ptrdiff_t term_id = 0;
        for (auto const &[term, idmap] : term_map) {
            term_os << term << '\n';
            for (auto const &[batch, batch_term_id] : idmap) {
                id_mappings[batch][batch_term_id] = term_id;
            }
            ++term_id;
        }

        logger() << "Remapping IDs\n";
        for (auto batch : enumerate(batch_count)) {
            auto &mapping = id_mappings[batch];
            writable_binary_collection coll(batch_file(basename, batch).c_str());
            for (auto doc_iter = ++coll.begin(); doc_iter != coll.end(); ++doc_iter) {
                for (auto &term_id : *doc_iter) {
                    term_id = mapping[term_id];
                }
            }
        }

        logger() << "Concatenating batches\n";
        std::ofstream os(basename);
        write_header(os, document_count);
        for (auto batch : enumerate(batch_count)) {
            std::ifstream is(batch_file(basename, batch));
            is.ignore(8);
            os << is.rdbuf();
        }

        logger() << "Done.\n";
    }

    void build(std::istream &             is,
               std::string const &        output_file,
               read_record_function_type  next_record,
               process_term_function_type process_term,
               std::ptrdiff_t             batch_size,
               std::size_t                threads)
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
                    batch_group.run([bp = std::move(bp), process_term, this, &queue]() {
                        run(std::move(bp), process_term);
                        int x;
                        queue.try_pop(x);
                    });
                    ++batch_number;
                    first_document += last_batch_size;
                    break;
                }
            } catch (Warc_Format_Error &err) {
                continue;
            }
            if (record->type() != "response") {
                continue;
            }
            record_batch.push_back(std::move(*record)); // AppleClang is missing value() in Optional
            if (record_batch.size() == batch_size) {
                Batch_Process bp{
                    batch_number, std::move(record_batch), first_document, output_file};
                queue.push(0);
                batch_group.run([bp = std::move(bp), process_term, this, &queue]() {
                    run(std::move(bp), process_term);
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
    }
};

std::string tolower(std::string const &term) {
    std::string lower;
    lower.reserve(term.size());
    for (char const &c : term) {
        lower.push_back(std::tolower(c));
    }
    return lower;
}


class Plaintext_Record {
   public:
    Plaintext_Record() = default;
    Plaintext_Record(std::string trecid, std::string content)
        : m_trecid(std::move(trecid)), m_content(std::move(content)) {}
    [[nodiscard]] auto content() -> std::string & { return m_content; }
    [[nodiscard]] auto content() const -> std::string const & { return m_content; }
    [[nodiscard]] auto trecid() -> std::string & { return m_trecid; }
    [[nodiscard]] auto trecid() const -> std::string const & { return m_trecid; }
    [[nodiscard]] auto type() const noexcept -> std::string { return "response"; }
    [[nodiscard]] static auto read(std::istream &is) {}

   private:
    std::string m_trecid;
    std::string m_content;
    //friend auto operator>>(std::istream &is, Plaintext_Record &record) -> std::istream &;
};

} // namespace ds2i

auto operator>>(std::istream &is, ds2i::Plaintext_Record &record) -> std::istream &
{
    is >> record.trecid();
    std::getline(is, record.content());
    return is;
}
