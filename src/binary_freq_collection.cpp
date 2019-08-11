#include "binary_freq_collection.hpp"

namespace pisa {

BinaryFreqCollection::BinaryFreqCollection(char const *basename)
    : m_document_collection((std::string(basename) + ".docs").c_str()),
      m_frequency_collection((std::string(basename) + ".freqs").c_str())
{
    auto first_sequence = *m_document_collection.begin();
    if (first_sequence.size() != 1) {
        throw std::invalid_argument("First sequence should only contain number of documents");
    }
    m_num_docs = *first_sequence.begin();
}

BinaryFreqCollection::iterator BinaryFreqCollection::begin() const
{
    auto docs_it = m_document_collection.begin();
    return iterator(++docs_it, m_frequency_collection.begin());
}
BinaryFreqCollection::iterator BinaryFreqCollection::end() const
{
    return iterator(m_document_collection.end(), m_frequency_collection.end());
}

BinaryFreqCollection::sequence const &BinaryFreqCollection::iterator::operator*() const
{
    return m_current_seqence;
}
BinaryFreqCollection::sequence const *BinaryFreqCollection::iterator::operator->() const
{
    return &m_current_seqence;
}
BinaryFreqCollection::iterator &BinaryFreqCollection::iterator::operator++()
{
    m_current_seqence.documents = *++m_document_iterator;
    m_current_seqence.frequencies = *++m_frequency_iterator;
    return *this;
}
bool BinaryFreqCollection::iterator::operator==(BinaryFreqCollection::iterator const &other) const
{
    return m_document_iterator == other.m_document_iterator;
}
bool BinaryFreqCollection::iterator::operator!=(BinaryFreqCollection::iterator const &other) const
{
    return m_document_iterator != other.m_document_iterator;
}

BinaryFreqCollection::iterator::iterator(BinaryCollection::const_iterator dit,
                                         BinaryCollection::const_iterator fit)
    : m_document_iterator(dit), m_frequency_iterator(fit)
{
    m_current_seqence.documents = *m_document_iterator;
    m_current_seqence.frequencies = *m_frequency_iterator;
}

} // namespace pisa
