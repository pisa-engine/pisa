#pragma once

#include <cstdint>
#include <limits>
#include <tuple>

#include <tl/optional.hpp>

#include "codec/block_codecs.hpp"
#include "codec/integer_codes.hpp"
#include "global_parameters.hpp"
#include "util/compiler_attribute.hpp"
#include "v1/base_index.hpp"
#include "v1/bit_cast.hpp"
#include "v1/bit_vector.hpp"
#include "v1/cursor_traits.hpp"
#include "v1/runtime_assert.hpp"
#include "v1/types.hpp"

namespace pisa::v1 {

template <typename BitSequence>
struct BitSequenceCursor {
    using value_type = std::uint32_t;
    using sequence_enumerator_type = typename BitSequence::enumerator;

    BitSequenceCursor(std::shared_ptr<BitVector> bits, sequence_enumerator_type sequence_enumerator)
        : m_sequence_enumerator(std::move(sequence_enumerator)), m_bits(std::move(bits))
    {
        reset();
    }

    void reset()
    {
        m_position = 0;
        m_current_value = m_sequence_enumerator.move(0).second;
    }

    [[nodiscard]] constexpr auto operator*() const -> value_type
    {
        if (PISA_UNLIKELY(empty())) {
            return sentinel();
        }
        return m_current_value;
    }
    [[nodiscard]] constexpr auto value() const noexcept -> value_type { return *(*this); }

    constexpr void advance()
    {
        std::tie(m_position, m_current_value) = m_sequence_enumerator.next();
    }

    PISA_FLATTEN_FUNC void advance_to_position(std::size_t position)
    {
        std::tie(m_position, m_current_value) = m_sequence_enumerator.move(position);
    }

    constexpr void advance_to_geq(value_type value)
    {
        std::tie(m_position, m_current_value) = m_sequence_enumerator.next_geq(value);
    }

    [[nodiscard]] constexpr auto empty() const noexcept -> bool { return m_position == size(); }
    [[nodiscard]] constexpr auto position() const noexcept -> std::size_t { return m_position; }
    [[nodiscard]] constexpr auto size() const -> std::size_t
    {
        return m_sequence_enumerator.size();
    }
    [[nodiscard]] constexpr auto sentinel() const -> value_type
    {
        return m_sequence_enumerator.universe();
    }

   private:
    std::uint64_t m_position = 0;
    std::uint64_t m_current_value{};
    sequence_enumerator_type m_sequence_enumerator;
    std::shared_ptr<BitVector> m_bits;
};

template <typename BitSequence>
struct DocumentBitSequenceCursor : public BitSequenceCursor<BitSequence> {
    using value_type = std::uint32_t;
    using sequence_enumerator_type = typename BitSequence::enumerator;
    explicit DocumentBitSequenceCursor(std::shared_ptr<BitVector> bits,
                                       sequence_enumerator_type sequence_enumerator)
        : BitSequenceCursor<BitSequence>(std::move(bits), std::move(sequence_enumerator))
    {
    }
};

template <typename BitSequence>
struct PayloadBitSequenceCursor : public BitSequenceCursor<BitSequence> {
    using value_type = std::uint32_t;
    using sequence_enumerator_type = typename BitSequence::enumerator;
    explicit PayloadBitSequenceCursor(std::shared_ptr<BitVector> bits,
                                      sequence_enumerator_type sequence_enumerator)
        : BitSequenceCursor<BitSequence>(std::move(bits), std::move(sequence_enumerator))
    {
    }
};

template <typename BitSequence, typename Cursor>
struct BitSequenceReader {
    using value_type = std::uint32_t;

    [[nodiscard]] auto read(gsl::span<std::byte const> bytes) const -> Cursor
    {
        runtime_assert(bytes.size() % sizeof(BitVector::storage_type) == 0).or_throw([&]() {
            return fmt::format(
                "Attempted to read no. bytes ({}) not aligned with the storage type of size {}",
                bytes.size(),
                sizeof(typename BitVector::storage_type));
        });

        auto true_bit_length = bit_cast<typename BitVector::storage_type>(
            bytes.first(sizeof(typename BitVector::storage_type)));
        bytes = bytes.subspan(sizeof(typename BitVector::storage_type));
        auto bits = std::make_shared<BitVector>(
            gsl::span<typename BitVector::storage_type const>(
                reinterpret_cast<typename BitVector::storage_type const*>(bytes.data()),
                bytes.size() / sizeof(BitVector::storage_type)),
            true_bit_length);
        BitVector::enumerator enumerator(*bits, 0);
        std::uint64_t universe = read_gamma_nonzero(enumerator);
        std::uint64_t n = 1;
        if (universe > 1) {
            n = enumerator.take(ceil_log2(universe + 1));
        }
        return Cursor(bits,
                      typename BitSequence::enumerator(
                          *bits, enumerator.position(), universe + 1, n, global_parameters()));
    }

    void init([[maybe_unused]] BaseIndex const& index) {}
    constexpr static auto encoding() -> std::uint32_t
    {
        return EncodingId::BitSequence | encoding_traits<BitSequence>::encoding_tag::encoding();
    }
};

template <typename BitSequence>
struct DocumentBitSequenceReader
    : public BitSequenceReader<BitSequence, DocumentBitSequenceCursor<BitSequence>> {
};
template <typename BitSequence>
struct PayloadBitSequenceReader
    : public BitSequenceReader<BitSequence, PayloadBitSequenceCursor<BitSequence>> {
};

template <typename BitSequence, bool DocumentWriter>
struct BitSequenceWriter {
    using value_type = std::uint32_t;
    BitSequenceWriter() = default;
    explicit BitSequenceWriter(std::size_t num_documents) : m_num_documents(num_documents) {}
    BitSequenceWriter(BitSequenceWriter const&) = default;
    BitSequenceWriter(BitSequenceWriter&&) noexcept = default;
    BitSequenceWriter& operator=(BitSequenceWriter const&) = default;
    BitSequenceWriter& operator=(BitSequenceWriter&&) noexcept = default;
    ~BitSequenceWriter() = default;

    constexpr static auto encoding() -> std::uint32_t
    {
        return EncodingId::BitSequence | encoding_traits<BitSequence>::encoding_tag::encoding();
    }

    void init(pisa::binary_freq_collection const& collection)
    {
        m_num_documents = collection.num_docs();
    }
    void push(value_type const& posting)
    {
        m_sum += posting;
        m_postings.push_back(posting);
    }
    void push(value_type&& posting)
    {
        m_sum += posting;
        m_postings.push_back(posting);
    }

    template <typename CharT>
    [[nodiscard]] auto write(std::basic_ostream<CharT>& os) const -> std::size_t
    {
        runtime_assert(m_num_documents.has_value())
            .or_throw("Uninitialized writer. Must call `init()` before writing.");
        runtime_assert(!m_postings.empty()).or_throw("Tried to write an empty posting list");
        bit_vector_builder builder;
        auto universe = [&]() {
            if constexpr (DocumentWriter) {
                return m_num_documents.value() - 1;
            } else {
                return m_sum;
            }
        }();
        write_gamma_nonzero(builder, universe);
        if (universe > 1) {
            builder.append_bits(m_postings.size(), ceil_log2(universe + 1));
        }
        BitSequence::write(
            builder, m_postings.begin(), universe + 1, m_postings.size(), global_parameters());
        typename BitVector::storage_type true_bit_length = builder.size();
        auto data = builder.move_bits();
        os.write(reinterpret_cast<CharT const*>(&true_bit_length), sizeof(true_bit_length));
        auto memory = gsl::as_bytes(gsl::make_span(data.data(), data.size()));
        os.write(reinterpret_cast<CharT const*>(memory.data()), memory.size());
        auto bytes_written = sizeof(true_bit_length) + memory.size();
        runtime_assert(bytes_written % sizeof(typename BitVector::storage_type) == 0)
            .or_throw([&]() {
                return fmt::format(
                    "Bytes written ({}) are not aligned with the storage type of size {}",
                    bytes_written,
                    sizeof(typename BitVector::storage_type));
            });
        return bytes_written;
    }

    void reset()
    {
        m_postings.clear();
        m_sum = 0;
    }

   private:
    std::vector<value_type> m_postings{};
    value_type m_sum = 0;
    tl::optional<std::size_t> m_num_documents{};
};

template <typename BitSequence>
using DocumentBitSequenceWriter = BitSequenceWriter<BitSequence, true>;

template <typename BitSequence>
using PayloadBitSequenceWriter = BitSequenceWriter<BitSequence, false>;

template <typename BitSequence>
struct CursorTraits<DocumentBitSequenceCursor<BitSequence>> {
    using Value = std::uint32_t;
    using Writer = DocumentBitSequenceWriter<BitSequence>;
    using Reader = DocumentBitSequenceReader<BitSequence>;
};

template <typename BitSequence>
struct CursorTraits<PayloadBitSequenceCursor<BitSequence>> {
    using Value = std::uint32_t;
    using Writer = PayloadBitSequenceWriter<BitSequence>;
    using Reader = PayloadBitSequenceReader<BitSequence>;
};

} // namespace pisa::v1
