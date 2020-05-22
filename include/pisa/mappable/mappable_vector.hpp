#pragma once

#include <algorithm>
#include <vector>

#include "boost/function.hpp"
#include "boost/lambda/bind.hpp"
#include "boost/lambda/construct.hpp"
#include "boost/range.hpp"
#include "boost/utility.hpp"

#include "util/intrinsics.hpp"

namespace pisa { namespace mapper {

    namespace detail {
        class freeze_visitor;
        class map_visitor;
        class sizeof_visitor;
    }  // namespace detail

    using deleter_t = boost::function<void()>;

    template <typename T>  // T must be a POD
    class mappable_vector {
      public:
        using value_type = T;
        using iterator = const T*;
        using const_iterator = const T*;

        mappable_vector() : m_data(0), m_size(0), m_deleter() {}
        mappable_vector(mappable_vector const&) = delete;
        mappable_vector(mappable_vector&&) = delete;
        mappable_vector& operator=(mappable_vector const&) = delete;
        mappable_vector& operator=(mappable_vector&&) = delete;

        template <typename Range>
        explicit mappable_vector(Range const& from) : m_data(0), m_size(0)
        {
            size_t size = boost::size(from);
            T* data = new T[size];
            m_deleter = boost::lambda::bind(boost::lambda::delete_array(), data);

            std::copy(boost::begin(from), boost::end(from), data);
            m_data = data;
            m_size = size;
        }

        ~mappable_vector()
        {
            if (not m_deleter.empty()) {
                m_deleter();
            }
        }

        void swap(mappable_vector& other)
        {
            using std::swap;
            swap(m_data, other.m_data);
            swap(m_size, other.m_size);
            swap(m_deleter, other.m_deleter);
        }

        void clear() { mappable_vector().swap(*this); }

        void steal(std::vector<T>& vec)
        {
            clear();
            m_size = vec.size();
            if (m_size > 0) {
                auto* new_vec = new std::vector<T>;
                new_vec->swap(vec);
                m_deleter = boost::lambda::bind(boost::lambda::delete_ptr(), new_vec);
                m_data = &(*new_vec)[0];
            }
        }

        template <typename Range>
        void assign(Range const& from)
        {
            clear();
            mappable_vector(from).swap(*this);
        }

        uint64_t size() const { return m_size; }

        inline const_iterator begin() const { return m_data; }

        inline const_iterator end() const { return m_data + m_size; }

        inline T const& operator[](uint64_t i) const
        {
            assert(i < m_size);
            return m_data[i];
        }

        inline T const* data() const { return m_data; }

        inline void prefetch(size_t i) const { intrinsics::prefetch(m_data + i); }

        friend class detail::freeze_visitor;
        friend class detail::map_visitor;
        friend class detail::sizeof_visitor;

      protected:
        const T* m_data;
        uint64_t m_size;
        deleter_t m_deleter;
    };

}}  // namespace pisa::mapper
