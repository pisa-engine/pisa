#pragma once

#include <cstdint>
#include <functional>

#include <gsl/span>

#include "v1/posting_format_header.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

/// Builds a "posting file" from passed values.
///
/// TODO: Probably the offsets should be part of the file along with the size.
template <typename Value>
struct PostingBuilder {
    template <typename WriterImpl>
    explicit PostingBuilder(WriterImpl writer) : m_writer(Writer<Value>(std::move(writer)))
    {
        m_offsets.push_back(0);
    }

    template <typename CharT>
    void write_header(std::basic_ostream<CharT> &os) const
    {
        std::array<std::byte, 8> header{};
        PostingFormatHeader{.version = FormatVersion::current(),
                            .type = value_type<Value>(),
                            .encoding = m_writer.encoding()}
            .write(gsl::make_span(header));
        os.write(reinterpret_cast<CharT const *>(header.data()), header.size());
    }

    template <typename ValueIterator, typename CharT>
    auto write_segment(std::basic_ostream<CharT> &os, ValueIterator first, ValueIterator last)
        -> std::basic_ostream<CharT> &
    {
        std::for_each(first, last, [&](auto &&value) { m_writer.push(value); });
        return flush_segment(os);
    }

    void accumulate(Value value) { m_writer.push(value); }

    template <typename CharT>
    auto flush_segment(std::basic_ostream<CharT> &os) -> std::basic_ostream<CharT> &
    {
        m_offsets.push_back(m_offsets.back() + m_writer.write(os));
        m_writer.reset();
        return os;
    }

    [[nodiscard]] auto offsets() const -> gsl::span<std::size_t const>
    {
        return gsl::make_span(m_offsets);
    }

    [[nodiscard]] auto offsets() -> std::vector<std::size_t> && { return std::move(m_offsets); }

   private:
    Writer<Value> m_writer;
    std::vector<std::size_t> m_offsets{};
};

} // namespace pisa::v1
