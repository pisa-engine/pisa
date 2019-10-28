#pragma once

#include <cstdint>
#include <functional>

#include <gsl/span>

#define Unreachable() std::abort();

namespace pisa::v1 {

using TermId = std::uint32_t;
using DocId = std::uint32_t;
using Frequency = std::uint32_t;
using Score = float;
using Result = std::pair<DocId, Score>;

enum EncodingId { Raw = 0xda43 };

template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};

template <class... Ts>
overloaded(Ts...)->overloaded<Ts...>;

template <typename Cursor>
struct Reader {
    using Value = std::decay_t<decltype(*std::declval<Cursor>())>;

    template <typename ReaderImpl>
    explicit constexpr Reader(ReaderImpl &&reader)
    {
        m_read = [reader = std::forward<ReaderImpl>(reader)](gsl::span<std::byte const> bytes) {
            return reader.read(bytes);
        };
    }

    [[nodiscard]] auto read(gsl::span<std::byte const> bytes) const -> Cursor
    {
        return m_read(bytes);
    }

   private:
    std::function<Cursor(gsl::span<std::byte const>)> m_read;
};

template <typename T>
struct Writer {

    template <typename W>
    explicit constexpr Writer(W writer) : m_internal_writer(std::make_unique<WriterImpl<W>>(writer))
    {
    }

    void push(T const &posting) { m_internal_writer->push(posting); }
    void push(T &&posting) { m_internal_writer->push(posting); }
    auto write(std::ostream &os) const -> std::size_t { return m_internal_writer->write(os); }
    [[nodiscard]] auto encoding() const -> std::uint32_t { return m_internal_writer->encoding(); }
    void reset() { return m_internal_writer->reset(); }

    struct WriterInterface {
        WriterInterface() = default;
        WriterInterface(WriterInterface const &) = default;
        WriterInterface(WriterInterface &&) noexcept = default;
        WriterInterface &operator=(WriterInterface const &) = default;
        WriterInterface &operator=(WriterInterface &&) noexcept = default;
        virtual ~WriterInterface() = default;
        virtual void push(T const &posting) = 0;
        virtual void push(T &&posting) = 0;
        virtual auto write(std::ostream &os) const -> std::size_t = 0;
        [[nodiscard]] virtual auto encoding() const -> std::uint32_t = 0;
        virtual void reset() = 0;
    };

    template <typename W>
    struct WriterImpl : WriterInterface {
        explicit WriterImpl(W writer) : m_writer(std::move(writer)) {}
        WriterImpl() = default;
        WriterImpl(WriterImpl const &) = default;
        WriterImpl(WriterImpl &&) noexcept = default;
        WriterImpl &operator=(WriterImpl const &) = default;
        WriterImpl &operator=(WriterImpl &&) noexcept = default;
        ~WriterImpl() = default;
        void push(T const &posting) override { m_writer.push(posting); }
        void push(T &&posting) override { m_writer.push(posting); }
        auto write(std::ostream &os) const -> std::size_t override { return m_writer.write(os); }
        [[nodiscard]] auto encoding() const -> std::uint32_t override { return W::encoding(); }
        void reset() override { return m_writer.reset(); }

       private:
        W m_writer;
    };

   private:
    std::unique_ptr<WriterInterface> m_internal_writer;
};

} // namespace pisa::v1
