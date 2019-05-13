#pragma once

#include <cstdint>
#include <memory>
#include <vector>

namespace pisa {

class BlockCodec {
   public:
    template <typename T>
    explicit BlockCodec(T /* codec */) : self_(std::make_shared<Impl<T>>())
    {
    }

    [[nodiscard]] auto block_size() const -> std::size_t
    {
        return 128; // TODO
    }
    void encode(uint32_t const *in,
                uint32_t sum_of_values,
                size_t n,
                std::vector<uint8_t> &out) const
    {
        self_->encode(in, sum_of_values, n, out);
    }
    auto decode(uint8_t const *in, uint32_t *out, uint32_t sum_of_values, size_t n) const
        -> uint8_t const *
    {
        return self_->decode(in, out, sum_of_values, n);
    }

    struct Interface {
        Interface() = default;
        Interface(const Interface &) = default;
        Interface(Interface &&) noexcept = default;
        Interface &operator=(const Interface &) = default;
        Interface &operator=(Interface &&) noexcept = default;
        virtual ~Interface() = default;
        virtual void encode(uint32_t const *in,
                            uint32_t sum_of_values,
                            size_t n,
                            std::vector<uint8_t> &out) const = 0;
        virtual auto decode(uint8_t const *in,
                            uint32_t *out,
                            uint32_t sum_of_values,
                            size_t n) const -> uint8_t const * = 0;
        [[nodiscard]] virtual auto block_size() const -> std::size_t = 0;
    };

    template <class T>
    class Impl : public Interface {
       public:
        void encode(uint32_t const *in,
                    uint32_t sum_of_values,
                    size_t n,
                    std::vector<uint8_t> &out) const override
        {
            T::encode(in, sum_of_values, n, out);
        };
        uint8_t const *decode(uint8_t const *in,
                              uint32_t *out,
                              uint32_t sum_of_values,
                              size_t n) const override
        {
            return T::decode(in, out, sum_of_values, n);
        }
        [[nodiscard]] std::size_t block_size() const override { return T::block_size; }
    };

   private:
    std::shared_ptr<Interface> self_;
};

template<typename T>
[[nodiscard]] auto block_codec() -> BlockCodec {
    return BlockCodec(T{});
}

} // namespace pisa
