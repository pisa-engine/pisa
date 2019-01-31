#pragma once

#include <vector>
#include <algorithm>

#include "boost/utility.hpp"
#include "boost/range.hpp"
#include "boost/function.hpp"
#include "boost/lambda/bind.hpp"
#include "boost/lambda/construct.hpp"

#include "util/intrinsics.hpp"

namespace pisa { namespace mapper {

    namespace detail {
        class freeze_visitor;
        class map_visitor;
        class sizeof_visitor;
    }

    using deleter_t = boost::function<void()>;

    template <typename T> // T must be a POD
    class mappable_vector {
       public:
        using value_type = T;
        using iterator = const T *;
        using const_iterator = const T *;

        mappable_vector() = default;
        mappable_vector(const mappable_vector &) = delete;
        mappable_vector &operator=(const mappable_vector &) = delete;

        template <typename Range>
        explicit mappable_vector(Range const &from)
        {
            size_t size = boost::size(from);
            T *data = new T[size]; // NOLINT(cppcoreguidelines-owning-memory)
            m_deleter = boost::lambda::bind(boost::lambda::delete_array(), data);

            std::copy(boost::begin(from), boost::end(from), data);
            m_data = data;
            m_size = size;
        }

        ~mappable_vector() {
            if (m_deleter) {
                m_deleter();
            }
        }

        void swap(mappable_vector& other) {
            using std::swap;
            swap(m_data, other.m_data);
            swap(m_size, other.m_size);
            swap(m_deleter, other.m_deleter);
        }

        void clear() {
            mappable_vector().swap(*this);
        }

        void steal(std::vector<T>& vec) {
            clear();
            m_size = vec.size();
            if (m_size) {
                auto new_vec = new std::vector<T>; // NOLINT(cppcoreguidelines-owning-memory)
                new_vec->swap(vec);
                m_deleter = boost::lambda::bind(boost::lambda::delete_ptr(), new_vec);
                m_data = &(*new_vec)[0];
            }
        }

        template <typename Range>
        void assign(Range const& from) {
            clear();
            mappable_vector(from).swap(*this);
        }

        uint64_t size() const {
            return m_size;
        }

        inline const_iterator begin() const {
            return m_data;
        }

        inline const_iterator end() const {
            return m_data + m_size;
        }

        inline T const& operator[](uint64_t i) const {
            assert(i < m_size);
            return *std::next(m_data, i);
        }

        inline T const* data() const {
            return m_data;
        }

        inline void prefetch(size_t i) const {
            intrinsics::prefetch(m_data + i); // NOLINT
        }

        friend class detail::freeze_visitor;
        friend class detail::map_visitor;
        friend class detail::sizeof_visitor;

       protected:
        const T *m_data{nullptr};
        uint64_t m_size{0};
        deleter_t m_deleter;
    };

}}
