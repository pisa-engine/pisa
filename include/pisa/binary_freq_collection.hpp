#pragma once

#include <stdexcept>
#include <iterator>
#include <cstdint>

#include "binary_collection.hpp"

namespace pisa {

struct BinaryFreqCollection {
   public:
    BinaryFreqCollection(char const *basename);

    struct sequence {
        BinaryCollection::const_sequence documents;
        BinaryCollection::const_sequence frequencies;
    };

    struct iterator {
       public:
        using difference_type = BinaryCollection::const_iterator::difference_type;
        using value_type = sequence;
        using pointer = sequence const *;
        using reference = sequence const &;
        using iterator_category = std::forward_iterator_tag;

        iterator() = default;
        sequence const &operator*() const;
        sequence const *operator->() const;
        iterator &operator++();
        bool operator==(iterator const &other) const;
        bool operator!=(iterator const &other) const;

       private:
        friend struct BinaryFreqCollection;

        iterator(BinaryCollection::const_iterator dit, BinaryCollection::const_iterator fit);

        BinaryCollection::const_iterator m_document_iterator;
        BinaryCollection::const_iterator m_frequency_iterator;
        sequence m_current_seqence;
    };
    iterator begin() const;
    iterator end() const;
    std::size_t size() const;
    std::uint64_t num_docs() const;

   private:
    BinaryCollection m_document_collection;
    BinaryCollection m_frequency_collection;
    uint64_t m_num_docs;
};

} // namespace pisa

namespace std {

template <>
struct iterator_traits<::pisa::BinaryFreqCollection::iterator> {
    using difference_type = std::ptrdiff_t;
    using value_type = ::pisa::BinaryFreqCollection::sequence;
    using pointer = value_type *;
    using reference = value_type &;
    using iterator_category = std::input_iterator_tag;
};

} // namespace std

namespace pisa {

inline std::size_t BinaryFreqCollection::size() const { return std::distance(begin(), end()); }
inline std::uint64_t BinaryFreqCollection::num_docs() const { return m_num_docs; }

} // namespace pisa
