#pragma once

#include <succinct/bit_vector.hpp>

#include "compact_elias_fano.hpp"

namespace ds2i {

    class bitvector_collection {
    public:
        bitvector_collection()
            : m_size(0)
        {}

        class builder {
        public:
            builder(global_parameters const& params)
                : m_params(params)
            {
                m_endpoints.push_back(0);
            }

            void append(succinct::bit_vector_builder& bvb)
            {
                m_bitvectors.append(bvb);
                m_endpoints.push_back(m_bitvectors.size());
            }

            void build(bitvector_collection& sq)
            {
                sq.m_size = m_endpoints.size() - 1;
                succinct::bit_vector(&m_bitvectors).swap(sq.m_bitvectors);

                succinct::bit_vector_builder bvb;
                compact_elias_fano::write(bvb, m_endpoints.begin(),
                                          m_bitvectors.size(), sq.m_size,
                                          m_params);
                succinct::bit_vector(&bvb).swap(sq.m_endpoints);
            }

        private:
            global_parameters m_params;
            std::vector<uint64_t> m_endpoints;
            succinct::bit_vector_builder m_bitvectors;
        };

        size_t size() const
        {
            return m_size;
        }

        succinct::bit_vector const& bits() const
        {
            return m_bitvectors;
        }

        succinct::bit_vector::enumerator
        get(global_parameters const& params, size_t i) const
        {
            assert(i < size());
            compact_elias_fano::enumerator endpoints(m_endpoints, 0,
                                                     m_bitvectors.size(), m_size,
                                                     params);

            auto endpoint = endpoints.move(i).second;
            return succinct::bit_vector::enumerator(m_bitvectors, endpoint);
        }

        void swap(bitvector_collection& other)
        {
            std::swap(m_size, other.m_size);
            m_endpoints.swap(other.m_endpoints);
            m_bitvectors.swap(other.m_bitvectors);
        }

        template <typename Visitor>
        void map(Visitor& visit)
        {
            visit
                (m_size, "m_size")
                (m_endpoints, "m_endpoints")
                (m_bitvectors, "m_bitvectors")
                ;
        }

    private:
        size_t m_size;
        succinct::bit_vector m_endpoints;
        succinct::bit_vector m_bitvectors;
    };
}
