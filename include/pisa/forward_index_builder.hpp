#pragma once

#include <cctype>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>

#include <boost/filesystem.hpp>

#include "document_record.hpp"
#include "query/term_processor.hpp"
#include "type_safe.hpp"

namespace pisa {

using process_content_function_type =
    std::function<void(std::string&&, std::function<void(std::string&&)>)>;

class Forward_Index_Builder {
  public:
    using read_record_function_type = std::function<std::optional<Document_Record>(std::istream&)>;

    struct Batch_Process {
        std::ptrdiff_t batch_number;
        std::vector<Document_Record> records;
        Document_Id first_document;
        std::string const& output_file;
    };

    template <typename Iterator>
    static std::ostream& write_document(std::ostream& os, Iterator first, Iterator last)
    {
        std::uint32_t length = std::distance(first, last);
        os.write(reinterpret_cast<const char*>(&length), sizeof(length));
        os.write(reinterpret_cast<const char*>(&(*first)), length * sizeof(*first));
        return os;
    }

    /// Writes the first sequence, which contains only the number of documents.
    static std::ostream& write_header(std::ostream& os, std::uint32_t document_count);

    /// Resolves the batch file path for the given batch number.
    [[nodiscard]] static auto
    batch_file(std::string const& output_file, std::ptrdiff_t batch_number) noexcept -> std::string;

    /// Given a mapping from ID to term, constructs a mapping from term to ID.
    [[nodiscard]] static auto reverse_mapping(std::vector<std::string>&& terms)
        -> std::unordered_map<std::string, Term_Id>;

    /// Collects all unique terms from batches into a vector.
    [[nodiscard]] static auto collect_terms(std::string const& basename, std::ptrdiff_t batch_count)
        -> std::vector<std::string>;

    /// Processes a single batch.
    void
    run(Batch_Process bp,
        TermTransformer process_term,
        process_content_function_type process_content) const;

    /// Merges batches.
    void
    merge(std::string const& basename, std::ptrdiff_t document_count, std::ptrdiff_t batch_count) const;

    /// Builds a forward index.
    void build(
        std::istream& is,
        std::string const& output_file,
        read_record_function_type next_record,
        TermTransformerBuilder term_transformer_builder,
        process_content_function_type process_content,
        std::ptrdiff_t batch_size,
        std::size_t threads) const;

    /// Removes all intermediate batches.
    void remove_batches(std::string const& basename, std::ptrdiff_t batch_count) const;
};

}  // namespace pisa
