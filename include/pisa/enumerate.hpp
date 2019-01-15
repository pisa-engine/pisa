#pragma once

namespace pisa {

template <typename T>
class Enumerator_Index {
   public:
    Enumerator_Index(T idx) : idx_(idx) {}

    T &               operator*() { return idx_; }
    T const &         operator*() const { return idx_; }
    Enumerator_Index &operator++() {
        ++idx_;
        return *this;
    }
    operator T() const { return idx_; }
    bool operator!=(Enumerator_Index const &rhs) const { return idx_ != rhs.idx_; }

   private:
    T idx_;
};

template <typename T>
struct Enumerator_Range {
    Enumerator_Index<T> begin_;
    Enumerator_Index<T> end_;
    [[nodiscard]] auto  begin() -> Enumerator_Index<T> { return begin_; }
    [[nodiscard]] auto  end() -> Enumerator_Index<T> { return end_; }
};

template <typename T>
[[nodiscard]] auto bound(T first) -> Enumerator_Index<T> {
    return {first};
}

template <typename T>
[[nodiscard]] auto enumerate(T last) -> Enumerator_Range<T> {
    assert(static_cast<T>(0) <= last);
    return Enumerator_Range<T>{{static_cast<T>(0)}, {last}};
}

template <typename T>
[[nodiscard]] auto enumerate(T first, T last) -> Enumerator_Range<T> {
    assert(first <= last);
    return Enumerator_Range<T>{{first}, {last}};
}

} // namespace pisa
