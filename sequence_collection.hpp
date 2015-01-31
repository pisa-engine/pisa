#pragma once

#include "bitvector_collection.hpp"
#include "compact_elias_fano.hpp"
#include "integer_codes.hpp"
#include "global_parameters.hpp"
#include "semiasync_queue.hpp"

namespace ds2i {

    template <typename IndexedSequence>
    class sequence_collection {
    public:
        typedef typename IndexedSequence::enumerator enumerator_type;

        sequence_collection()
        {}

        class builder {
        public:
            builder(global_parameters const& params)
                : m_queue(1 << 24)
                , m_params(params)
                , m_sequences(params)
            {}

            template <typename Iterator>
            void add_sequence(Iterator begin, uint64_t last_element, uint64_t n)
            {
                if (!n) throw std::invalid_argument("Sequence must be nonempty");

                // make_shared does not seem to work
                std::shared_ptr<sequence_adder<Iterator>>
                    ptr(new sequence_adder<Iterator>(*this, begin, last_element, n));
                m_queue.add_job(ptr, n);
            }

            void build(sequence_collection& sq)
            {
                m_queue.complete();
                sq.m_params = m_params;
                m_sequences.build(sq.m_sequences);
            }

        private:

            template <typename Iterator>
            struct sequence_adder : semiasync_queue::job {
                sequence_adder(builder& b,
                               Iterator begin,
                               uint64_t last_element,
                               uint64_t n)
                    : b(b)
                    , begin(begin)
                    , last_element(last_element)
                    , n(n)
                {}

                virtual void prepare()
                {
                    // store approximation of the universe as smallest power of two
                    // that can represent last_element
                    uint64_t universe_bits = ceil_log2(last_element);
                    write_gamma(bits, universe_bits);
                    write_gamma_nonzero(bits, n);
                    IndexedSequence::write(bits, begin,
                                           (uint64_t(1) << universe_bits) + 1, n,
                                           b.m_params);
                }

                virtual void commit()
                {
                    b.m_sequences.append(bits);
                }

                builder& b;
                Iterator begin;
                uint64_t last_element;
                uint64_t n;
                succinct::bit_vector_builder bits;
            };

            semiasync_queue m_queue;
            global_parameters m_params;
            bitvector_collection::builder m_sequences;
        };

        size_t size() const
        {
            return m_sequences.size();
        }

        enumerator_type operator[](size_t i) const
        {
            assert(i < size());
            auto it = m_sequences.get(m_params, i);
            uint64_t universe_bits = read_gamma(it);
            uint64_t n = read_gamma_nonzero(it);

            return enumerator_type(m_sequences.bits(), it.position(),
                                   (uint64_t(1) << universe_bits) + 1, n,
                                   m_params);
        }

        void swap(sequence_collection& other)
        {
            std::swap(m_params, other.m_params);
            std::swap(m_size, other.m_size);
            m_sequences.swap(other.m_sequences);
        }

        template <typename Visitor>
        void map(Visitor& visit)
        {
            visit
                (m_params, "m_params")
                (m_size, "m_size")
                (m_sequences, "m_sequences")
                ;
        }

    private:
        global_parameters m_params;
        size_t m_size;
        bitvector_collection m_sequences;
    };
}
