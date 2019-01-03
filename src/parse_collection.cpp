#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <numeric>
#include <thread>
#include <functional>
#include <unordered_set>

#include "CLI/CLI.hpp"
#include "pstl/algorithm"
#include "pstl/execution"
#include "tbb/concurrent_queue.h"
#include "tbb/task_group.h"
#include "tbb/task_scheduler_init.h"

#include "binary_collection.hpp"
#include "parsing/html.hpp"
#include "parsing/stemmer.hpp"
#include "parsing/warc.hpp"
#include "util/util.hpp"

using ds2i::logger;
using namespace ds2i;

template <typename T>
class Enumerator_Index {
   public:
    Enumerator_Index(T idx) : idx_(idx) {}

    T &               operator*() { return idx_; }
    T const &         operator*() const { return idx_; }
    Enumerator_Index &operator++() {
        ++idx_;
        return *this;
    }
    operator T() const { return idx_; }

   private:
    T idx_;
};

template <typename T>
struct Enumerator_Range {
    Enumerator_Index<T> begin_;
    Enumerator_Index<T> end_;
    [[nodiscard]] auto  begin() -> Enumerator_Index<T> { return begin_; }
    [[nodiscard]] auto  end() -> Enumerator_Index<T> { return end_; }
};

template <typename T>
[[nodiscard]] auto bound(T first) -> Enumerator_Index<T> {
    return {first};
}

template <typename T>
[[nodiscard]] auto enumerate(T last) -> Enumerator_Range<T> {
    assert(0 <= last);
    return Enumerator_Range<T>{{0}, {last}};
}

template <typename T>
[[nodiscard]] auto enumerate(T first, T last) -> Enumerator_Range<T> {
    assert(0 <= last);
    return Enumerator_Range<T>{{first}, {last}};
}

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

    static std::ostream &write_header(std::ostream &os, uint32_t document_count) {
        return write_document(os, &document_count, &document_count + sizeof(document_count));
    }

    static std::ostream &write_terms(std::ostream &os, std::map<std::string, uint32_t> map)
    {
        for (auto const &[term, hash] : map) {
            os << term << '\n';
        }
        return os;
    }

    struct Batch_Process {
        std::ptrdiff_t               batch_number;
        std::vector<Record>          records;
        Document_Id                  first_document;
        std::string const &          output_file;
    };

    [[nodiscard]] auto batch_file(std::string const &output_file, std::ptrdiff_t batch_number) const
        noexcept -> std::string
    {
        std::ostringstream os;
        os << output_file << ".batch." << batch_number;
        return os.str();
    }

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
            // TODO(michal): it gets stuck on this for some reason
            //               once fixed, uncomment; not crucial at the moment
            // url_os << record.url() << '\n';

            auto content = parsing::html::cleantext(record.content());
            std::vector<uint32_t> term_ids;

            auto process = [&](auto term_iter) {
                auto term = process_term(term_iter->str());
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
            std::regex term_pattern("(\\w+)");
            auto term_iter = std::sregex_iterator(content.begin(), content.end(), term_pattern);
            if (term_iter != std::sregex_iterator()) {
                process(term_iter);
                term_iter++;
                for (; term_iter != std::sregex_iterator(); term_iter++) {
                    process(term_iter);
                }
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
            binary_collection coll(batch_file(basename, batch).c_str());
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
                    break;
                }
            } catch (Warc_Format_Error &err) {
                continue;
            }
            if (record->type() != "response") {
                continue;
            }
            record_batch.push_back(std::move(record.value()));
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
        lower.push_back(tolower(c));
    }
    return lower;
}

int main(int argc, char **argv) {

    std::string input_basename;
    std::string output_filename;
    size_t      threads = std::thread::hardware_concurrency();
    ptrdiff_t   batch_size = 100'000;

    CLI::App app{"parse_collection - parse collection and store as forward index."};
    app.add_option("-o,--output", output_filename, "Forward index filename")->required();
    app.add_option("-j,--threads", threads, "Thread count");
    app.add_option(
        "-b,--batch-size", batch_size, "Number of documents to process in one thread", true);
    CLI11_PARSE(app, argc, argv);

    tbb::task_scheduler_init init(threads);
    logger() << "Number of threads: " << threads << '\n';

    Forward_Index_Builder<Warc_Record> builder;
    builder.build(
        std::cin,
        output_filename,
        [](std::istream &in) -> std::optional<Warc_Record> {
            Warc_Record record;
            if (read_warc_record(in, record)) {
                return record;
            }
            return std::nullopt;
        },
        [&](std::string const &term) -> std::string { return Porter2_Stemmer{}(tolower(term)); },
        batch_size,
        threads);

    return 0;
}
