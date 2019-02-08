#pragma once

template <typename L, typename H>
[[nodiscard]] auto between(L first, H last)
{
    return [=](auto x) { return x >= first and x < last; };
}
