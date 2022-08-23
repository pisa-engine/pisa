#pragma once

#include <fstream>
#include <iostream>

#include "mio/mmap.hpp"

#include "mappable/mappable_vector.hpp"

namespace pisa { namespace mapper {

    struct map_flags {
        enum { warmup = 1 };
    };

    struct size_node;
    using size_node_ptr = std::shared_ptr<size_node>;

    struct size_node {
        size_node() : size(0) {}

        std::string name;
        size_t size;
        std::vector<size_node_ptr> children;

        void dump(std::ostream& os = std::cerr, size_t depth = 0)
        {
            os << std::string(depth * 4, ' ') << name << ": " << size << '\n';
            for (auto&& child: children) {
                child->dump(os, depth + 1);
            }
        }
    };

    namespace detail {
        class freeze_visitor {
          public:
            freeze_visitor(std::ofstream& fout, uint64_t flags)
                : m_fout(fout), m_flags(flags), m_written(0)
            {
                // Save freezing flags
                m_fout.write(reinterpret_cast<const char*>(&m_flags), sizeof(m_flags));
                m_written += sizeof(m_flags);
            }

            freeze_visitor(freeze_visitor const&) = delete;
            freeze_visitor(freeze_visitor&&) = delete;
            freeze_visitor& operator=(freeze_visitor&&) = delete;
            freeze_visitor& operator=(freeze_visitor const&) = delete;
            ~freeze_visitor() = default;

            template <typename T>
            typename std::enable_if<!std::is_pod<T>::value, freeze_visitor&>::type
            operator()(T& val, const char* /* friendly_name */)
            {
                val.map(*this);
                return *this;
            }

            template <typename T>
            typename std::enable_if<std::is_pod<T>::value, freeze_visitor&>::type
            operator()(T& val, const char* /* friendly_name */)
            {
                m_fout.write(reinterpret_cast<const char*>(&val), sizeof(T));
                m_written += sizeof(T);
                return *this;
            }

            template <typename T>
            freeze_visitor& operator()(mappable_vector<T>& vec, const char* /* friendly_name */)
            {
                (*this)(vec.m_size, "size");

                auto n_bytes = static_cast<size_t>(vec.m_size * sizeof(T));
                m_fout.write(reinterpret_cast<const char*>(vec.m_data), long(n_bytes));
                m_written += n_bytes;

                return *this;
            }

            size_t written() const { return m_written; }

          protected:
            std::ofstream& m_fout;
            const uint64_t m_flags;
            uint64_t m_written;
        };

        class map_visitor {
          public:
            map_visitor(const char* base_address, uint64_t flags)
                : m_base(base_address), m_cur(m_base), m_flags(flags)
            {
                m_freeze_flags = *reinterpret_cast<const uint64_t*>(m_cur);
                m_cur += sizeof(m_freeze_flags);
            }

            map_visitor(map_visitor const&) = delete;
            map_visitor(map_visitor&&) = delete;
            map_visitor& operator=(map_visitor const&) = delete;
            map_visitor& operator=(map_visitor&&) = delete;
            ~map_visitor() = default;

            template <typename T>
            typename std::enable_if<!std::is_pod<T>::value, map_visitor&>::type
            operator()(T& val, const char* /* friendly_name */)
            {
                val.map(*this);
                return *this;
            }

            template <typename T>
            typename std::enable_if<std::is_pod<T>::value, map_visitor&>::type
            operator()(T& val, const char* /* friendly_name */)
            {
                val = *reinterpret_cast<const T*>(m_cur);
                m_cur += sizeof(T);
                return *this;
            }

            template <typename T>
            map_visitor& operator()(mappable_vector<T>& vec, const char* /* friendly_name */)
            {
                vec.clear();
                (*this)(vec.m_size, "size");

                vec.m_data = reinterpret_cast<const T*>(m_cur);
                size_t bytes = vec.m_size * sizeof(T);

                if (m_flags & map_flags::warmup) {
                    T foo;
                    volatile T* bar = &foo;
                    for (size_t i = 0; i < vec.m_size; ++i) {
                        *bar = vec.m_data[i];
                    }
                }

                m_cur += bytes;
                return *this;
            }

            size_t bytes_read() const { return size_t(m_cur - m_base); }

          protected:
            const char* m_base;
            const char* m_cur;
            const uint64_t m_flags;
            uint64_t m_freeze_flags;
        };

        class sizeof_visitor {
          public:
            explicit sizeof_visitor(bool with_tree = false) : m_size(0)
            {
                if (with_tree) {
                    m_cur_size_node = std::make_shared<size_node>();
                }
            }

            sizeof_visitor(sizeof_visitor const&) = delete;
            sizeof_visitor(sizeof_visitor&&) = delete;
            sizeof_visitor& operator=(sizeof_visitor const&) = delete;
            sizeof_visitor& operator=(sizeof_visitor&&) = delete;
            ~sizeof_visitor() = default;

            template <typename T>
            typename std::enable_if<!std::is_pod<T>::value, sizeof_visitor&>::type
            operator()(T& val, const char* friendly_name)
            {
                size_t checkpoint = m_size;
                size_node_ptr parent_node;
                if (m_cur_size_node) {
                    parent_node = m_cur_size_node;
                    m_cur_size_node = make_node(friendly_name);
                }

                val.map(*this);

                if (m_cur_size_node) {
                    m_cur_size_node->size = m_size - checkpoint;
                    m_cur_size_node = parent_node;
                }
                return *this;
            }

            template <typename T>
            typename std::enable_if<std::is_pod<T>::value, sizeof_visitor&>::type
            operator()(T& /* val */, const char* /* friendly_name */)
            {
                // don't track PODs in the size tree (they are constant sized)
                m_size += sizeof(T);
                return *this;
            }

            template <typename T>
            sizeof_visitor& operator()(mappable_vector<T>& vec, const char* friendly_name)
            {
                size_t checkpoint = m_size;
                (*this)(vec.m_size, "size");
                m_size += static_cast<size_t>(vec.m_size * sizeof(T));

                if (m_cur_size_node) {
                    make_node(friendly_name)->size = m_size - checkpoint;
                }

                return *this;
            }

            size_t size() const { return m_size; }

            size_node_ptr size_tree() const
            {
                assert(m_cur_size_node);
                return m_cur_size_node;
            }

          protected:
            size_node_ptr make_node(const char* name)
            {
                size_node_ptr node = std::make_shared<size_node>();
                m_cur_size_node->children.push_back(node);
                node->name = name;
                return node;
            }

            size_t m_size;
            size_node_ptr m_cur_size_node;
        };

    }  // namespace detail

    /**
     * Serializes data to an output stream.
     *
     * \tparam T  Type of the serialized value.
     *
     * \param val            Value to serialize.
     * \param fout           Output stream to write to.
     * \param flags          Map flags, see `pisa::struct::map_flags`.
     * \param friendly_name  Name used for debug printing.
     *
     * \throws std::ios_base::failure  May be thrown on write failure, depending on the
     *                                 stream configuration.
     */
    template <typename T>
    std::size_t
    freeze(T& val, std::ofstream& fout, uint64_t flags = 0, const char* friendly_name = "<TOP>")
    {
        detail::freeze_visitor freezer(fout, flags);
        freezer(val, friendly_name);
        return freezer.written();
    }

    /**
     * Serializes data to a file.
     *
     * \tparam T  Type of the serialized value.
     *
     * \param val            Value to serialize.
     * \param filename       Output file.
     * \param flags          Map flags, see `pisa::struct::map_flags`.
     * \param friendly_name  Name used for debug printing.
     *
     * \throws std::ios_base::failure  Thrown if failed to write to the file.
     */
    template <typename T>
    std::size_t
    freeze(T& val, const char* filename, uint64_t flags = 0, const char* friendly_name = "<TOP>")
    {
        std::ofstream fout(filename, std::ios::binary);
        fout.exceptions(std::ios::badbit | std::ios::failbit);
        return freeze(val, fout, flags, friendly_name);
    }

    /**
     * Deserializes data from memory.
     *
     * \tparam T  Type of the serialized value.
     *
     * \param[out] val            Value where the deserialized data will be written.
     * \param[in]  base_address   The beginning of the meomry of the serialized data.
     * \param[in]  flags          Map flags, see `pisa::struct::map_flags`.
     * \param[in]  friendly_name  Name used for debug printing.
     *
     * \throws std::ios_base::failure  May be thrown on write failure, depending on the
     *                                 stream configuration.
     */
    template <typename T>
    size_t
    map(T& val, const char* base_address, uint64_t flags = 0, const char* friendly_name = "<TOP>")
    {
        detail::map_visitor mapper(base_address, flags);
        mapper(val, friendly_name);
        return mapper.bytes_read();
    }

    /**
     * Deserializes data from memory.
     *
     * \tparam T  Type of the serialized value.
     *
     * \param[out] val            Value where the deserialized data will be written.
     * \param[in]  m              Memory-mapped file where the data is serialized.
     * \param[in]  flags          Map flags, see `pisa::struct::map_flags`.
     * \param[in]  friendly_name  Name used for debug printing.
     *
     * \throws std::ios_base::failure  Thrown if failed to write to the file.
     */
    template <typename T>
    size_t
    map(T& val, const mio::mmap_source& m, uint64_t flags = 0, const char* friendly_name = "<TOP>")
    {
        return map(val, m.data(), flags, friendly_name);
    }

    template <typename T>
    std::size_t size_of(T& val)
    {
        detail::sizeof_visitor sizer;
        sizer(val, "");
        return sizer.size();
    }

    template <typename T>
    size_node_ptr size_tree_of(T& val, const char* friendly_name = "<TOP>")
    {
        detail::sizeof_visitor sizer(true);
        sizer(val, friendly_name);
        assert(not sizer.size_tree()->children.empty());
        return sizer.size_tree()->children[0];
    }

}}  // namespace pisa::mapper
