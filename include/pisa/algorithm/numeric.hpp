#pragma once

namespace pisa {

template <typename L, typename H>
[[nodiscard]] auto between(L first, H last)
{
    return [=](auto x) { return x >= first and x < last; };
}

template <class InputIt, class T, class BinaryOp, class UnaryOp>
T transform_reduce(InputIt first, InputIt last, T init, BinaryOp binop, UnaryOp unary_op)
{
    while (first != last) {
        init = binop(std::move(init), unary_op(*first));
        ++first;
    }
    return init;
}

} // namespace pisa
