#pragma once

template <class T>
class single_init_entry {
   public:
    single_init_entry() : m_value(), m_generation(0) {}

    const T &value() const { return m_value; }
    bool     has_value(size_t generation) const { return m_generation == generation; }
    void     set(size_t generation, const T &v) {
        m_value      = v;
        m_generation = generation;
    }

   private:
    T      m_value;
    size_t m_generation;
};


template <typename T>
struct Default {
    constexpr static T value = T();
};
template <>
struct Default<double> {
    constexpr static double value = 0.0;
};
template <>
struct Default<size_t> {
    constexpr static size_t value = 0;
};

template <class T>
class single_init_vector : public std::vector<single_init_entry<T>> {

   public:
    using std::vector<single_init_entry<T>>::vector;
    const T &operator[](size_t i) const {
        return (std::vector<single_init_entry<T>>::      operator[](i).has_value(m_generation)
                    ? std::vector<single_init_entry<T>>::operator[](i).value()
                    : m_defaultValue);
    }

    bool has_value(size_t i) const {
        return (std::vector<single_init_entry<T>>::operator[](i).has_value(m_generation));
    }

    void set(size_t i, const T &v) {
        std::vector<single_init_entry<T>>::operator[](i).set(m_generation, v);
    }

    void   clear() { m_generation += 1; }
    size_t m_generation   = 1;
    T      m_defaultValue = Default<T>::value;
};

struct degree_map_pair {
    single_init_vector<size_t> &left;
    single_init_vector<size_t> &right;
};