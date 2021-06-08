#pragma once

#include <algorithm>
#include <cctype>
#include <fstream>
#include <functional>
#include <numeric>
#include <optional>
#include <sstream>
#include <stack>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <gsl/gsl_assert>
#include <pstl/algorithm>
#include <pstl/execution>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/zip.hpp>
#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>
#include <tbb/concurrent_queue.h>
#include <tbb/task_group.h>

#include "binary_collection.hpp"
#include "document_record.hpp"
#include "io.hpp"
#include "payload_vector.hpp"
#include "query/term_processor.hpp"
#include "tokenizer.hpp"
#include "type_safe.hpp"

namespace pisa {

using process_term_function_type = std::function<std::string(std::string)>;
using process_content_function_type =
    std::function<void(std::string&&, std::function<void(std::string&&)>)>;

class Forward_Index_Builder {
  public:
    using read_record_function_type = std::function<std::optional<Document_Record>(std::istream&)>;

    template <typename Iterator>
    static std::ostream& write_document(std::ostream& os, Iterator first, Iterator last)
    {
        std::uint32_t length = std::distance(first, last);
        os.write(reinterpret_cast<const char*>(&length), sizeof(length));
        os.write(reinterpret_cast<const char*>(&(*first)), length * sizeof(*first));
        return os;
    }

    static std::ostream& write_header(std::ostream& os, uint32_t document_count)
    {
        return write_document(os, &document_count, std::next(&document_count));
    }

    [[nodiscard]] static auto
    batch_file(std::string const& output_file, std::ptrdiff_t batch_number) noexcept -> std::string
    {
        std::ostringstream os;
        os << output_file << ".batch." << batch_number;
        return os.str();
    }

    struct Batch_Process {
        std::ptrdiff_t batch_number;
        std::vector<Document_Record> records;
        Document_Id first_document;
        std::string const& output_file;
    };

    void
    run(Batch_Process bp,
        process_term_function_type process_term,
        process_content_function_type process_content) const
    {
        spdlog::debug(
            "[Batch {}] Processing documents [{}, {})",
            bp.batch_number,
            bp.first_document,
            bp.first_document + bp.records.size());
        auto basename = batch_file(bp.output_file, bp.batch_number);

        std::ofstream os(basename);
        std::ofstream title_os(basename + ".documents");
        std::ofstream url_os(basename + ".urls");
        std::ofstream term_os(basename + ".terms");
        write_header(os, bp.records.size());

        std::map<std::string, uint32_t> map;

        for (auto&& record: bp.records) {
            title_os << record.title() << '\n';
            url_os << record.url() << '\n';

            std::vector<uint32_t> term_ids;

            auto process = [&](auto&& term) {
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
        spdlog::info(
            "[Batch {}] Processed documents [{}, {})",
            bp.batch_number,
            bp.first_document,
            bp.first_document + bp.records.size());
    }

    [[nodiscard]] static auto reverse_mapping(std::vector<std::string>&& terms)
        -> std::unordered_map<std::string, Term_Id>
    {
        std::unordered_map<std::string, Term_Id> mapping;
        Term_Id term_id{0};
        for (std::string& term: terms) {
            mapping.emplace(std::move(term), term_id);
            ++term_id;
        }
        return mapping;
    }

    [[nodiscard]] static auto collect_terms(std::string const& basename, std::ptrdiff_t batch_count)
    {
        struct Term_Span {
            size_t first;
            size_t last;
            size_t lvl;
        };
        std::stack<Term_Span> spans;
        std::vector<std::string> terms;
        auto merge_spans = [&](Term_Span lhs, Term_Span rhs) {
            Expects(lhs.last == rhs.first);
            auto first = std::next(terms.begin(), lhs.first);
            auto mid = std::next(terms.begin(), lhs.last);
            auto last = std::next(terms.begin(), rhs.last);
            std::inplace_merge(pstl::execution::par, first, mid, last);
            terms.erase(std::unique(pstl::execution::par, first, last), last);
            return Term_Span{lhs.first, terms.size(), lhs.lvl + 1};
        };
        auto push_span = [&](Term_Span s) {
            while (not spans.empty() and spans.top().lvl == s.lvl) {
                s = merge_spans(spans.top(), s);
                spans.pop();
            }
            spans.push(s);
        };

        spdlog::info("Collecting terms");
        for (auto batch: ranges::views::iota(0, batch_count)) {
            spdlog::debug("[Collecting terms] Batch {}/{}", batch, batch_count);
            auto mid = terms.size();
            std::ifstream terms_is(batch_file(basename, batch) + ".terms");
            std::string term;
            while (std::getline(terms_is, term)) {
                terms.push_back(term);
            }
            std::sort(std::next(terms.begin(), mid), terms.end());
            push_span(Term_Span{mid, terms.size(), 0U});
        }
        while (spans.size() > 1) {
            auto rhs = spans.top();
            spans.pop();
            auto lhs = spans.top();
            spans.pop();
            spans.push(merge_spans(lhs, rhs));
        }
        terms.shrink_to_fit();
        return terms;
    }

    void merge(std::string const& basename, std::ptrdiff_t document_count, std::ptrdiff_t batch_count) const
    {
        std::ofstream term_os(basename + ".terms");

        {
            spdlog::info("Merging titles");
            std::ofstream title_os(basename + ".documents");
            for (auto batch: ranges::views::iota(0, batch_count)) {
                spdlog::debug("[Merging titles] Batch {}/{}", batch, batch_count);
                std::ifstream title_is(batch_file(basename, batch) + ".documents");
                title_os << title_is.rdbuf();
            }
        }
        {
            spdlog::info("Creating document lexicon");
            std::ifstream title_is(basename + ".documents");
            encode_payload_vector(
                std::istream_iterator<io::Line>(title_is), std::istream_iterator<io::Line>())
                .to_file(basename + ".doclex");
        }
        {
            spdlog::info("Merging URLs");
            std::ofstream url_os(basename + ".urls");
            for (auto batch: ranges::views::iota(0, batch_count)) {
                spdlog::debug("[Merging URLs] Batch {}/{}", batch, batch_count);
                std::ifstream url_is(batch_file(basename, batch) + ".urls");
                url_os << url_is.rdbuf();
            }
        }

        auto terms = collect_terms(basename, batch_count);

        spdlog::info("Writing terms");
        for (auto const& term: terms) {
            term_os << term << '\n';
        }
        encode_payload_vector(terms.begin(), terms.end()).to_file(basename + ".termlex");

        spdlog::info("Mapping terms");
        auto term_mapping = reverse_mapping(std::move(terms));

        spdlog::info("Remapping IDs");
        for (auto batch: ranges::views::iota(0, batch_count)) {
            spdlog::debug("[Remapping IDs] Batch {}/{}", batch, batch_count);
            auto batch_terms = io::read_string_vector(batch_file(basename, batch) + ".terms");
            std::vector<Term_Id> mapping(batch_terms.size());
            std::transform(
                batch_terms.begin(), batch_terms.end(), mapping.begin(), [&](auto const& bterm) {
                    return term_mapping[bterm];
                });
            writable_binary_collection coll(batch_file(basename, batch).c_str());
            for (auto doc_iter = ++coll.begin(); doc_iter != coll.end(); ++doc_iter) {
                for (auto& term_id: *doc_iter) {
                    term_id = mapping[term_id].as_int();
                }
            }
        }
        term_mapping.clear();

        spdlog::info("Concatenating batches");
        std::ofstream os(basename);
        write_header(os, document_count);
        for (auto batch: ranges::views::iota(0, batch_count)) {
            spdlog::debug("[Concatenating batches] Batch {}/{}", batch, batch_count);
            std::ifstream is(batch_file(basename, batch));
            is.ignore(8);
            os << is.rdbuf();
        }

        spdlog::info("Success.");
    }

    template <typename TermProcessorConstruct>
    void build(
        std::istream& is,
        std::string const& output_file,
        read_record_function_type next_record,
        TermProcessorConstruct&& term_processor,
        process_content_function_type process_content,
        std::ptrdiff_t batch_size,
        std::size_t threads) const
    {
        if (threads < 2) {
            spdlog::error("Building forward index requires at least 2 threads");
            std::abort();
        }
        Document_Id first_document{0};
        std::ptrdiff_t batch_number = 0;

        std::vector<Document_Record> record_batch;
        tbb::task_group batch_group;
        tbb::concurrent_bounded_queue<int> queue;
        queue.set_capacity((threads - 1) * 2);
        while (true) {
            std::optional<Document_Record> record = std::nullopt;
            if (not(record = next_record(is))) {
                auto last_batch_size = record_batch.size();
                auto batch_process = [&] {
                    return Batch_Process{
                        batch_number, std::move(record_batch), first_document, output_file};
                };
                queue.push(0);
                batch_group.run([bp = batch_process(),
                                 term_processor = term_processor(),
                                 this,
                                 &queue,
                                 &process_content]() {
                    run(bp, std::move(term_processor), process_content);
                    int x;
                    queue.try_pop(x);
                });
                ++batch_number;
                first_document += last_batch_size;
                break;
            }
            spdlog::debug("Parsed document {}", record->title());
            record_batch.push_back(std::move(*record));  // AppleClang is missing value() in
                                                         // Optional
            if (record_batch.size() == batch_size) {
                auto batch_process = [&] {
                    return Batch_Process{
                        batch_number, std::move(record_batch), first_document, output_file};
                };
                queue.push(0);
                batch_group.run([bp = batch_process(),
                                 term_processor = term_processor(),
                                 this,
                                 &queue,
                                 &process_content]() {
                    run(bp, std::move(term_processor), process_content);
                    int x;
                    queue.try_pop(x);
                });
                ++batch_number;
                first_document += batch_size;
                record_batch = std::vector<Document_Record>();
            }
        }
        batch_group.wait();
        merge(output_file, first_document.as_int(), batch_number);
        remove_batches(output_file, batch_number);
    }

    void try_remove(boost::filesystem::path const& file) const
    {
        using boost::filesystem::remove;
        try {
            remove(file);
        } catch (...) {
            spdlog::warn("Unable to remove temporary batch file {}", file.c_str());
        }
    }

    void remove_batches(std::string const& basename, std::ptrdiff_t batch_count) const
    {
        using boost::filesystem::path;
        for (auto batch: ranges::views::iota(0, batch_count)) {
            auto batch_basename = batch_file(basename, batch);
            try_remove(path{batch_basename + ".documents"});
            try_remove(path{batch_basename + ".terms"});
            try_remove(path{batch_basename + ".urls"});
            try_remove(path{batch_basename});
        }
    }
};

}  // namespace pisa
