#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "codec/block_codec.hpp"
#include "codec/block_codecs.hpp"
#include "codec/compact_elias_fano.hpp"
#include "codec/maskedvbyte.hpp"
#include "codec/qmx.hpp"
#include "codec/simdbp.hpp"
#include "codec/simple16.hpp"
#include "codec/simple8b.hpp"
#include "codec/streamvbyte.hpp"
#include "codec/varintgb.hpp"
#include "mixed_block.hpp"

namespace pisa {

class BlockCodec {
   public:
    template <typename T>
    explicit BlockCodec(T /* codec */) : self_(std::make_shared<Impl<T>>())
    {
    }

    static auto from_type(std::string_view type) -> std::optional<BlockCodec>
    {
        if (type == "block_optpfor") {
            return std::make_optional(BlockCodec(optpfor_block{}));
        } else if (type == "block_varintg8iu") {
            return std::make_optional(BlockCodec(varint_G8IU_block{}));
        } else if (type == "block_streamvbyte") {
            return std::make_optional(BlockCodec(streamvbyte_block{}));
        } else if (type == "block_maskedvbyte") {
            return std::make_optional(BlockCodec(maskedvbyte_block{}));
        } else if (type == "block_interpolative") {
            return std::make_optional(BlockCodec(interpolative_block{}));
        } else if (type == "block_qmx") {
            return std::make_optional(BlockCodec(qmx_block{}));
        } else if (type == "block_varintgb") {
            return std::make_optional(BlockCodec(varintgb_block{}));
        } else if (type == "block_simple8b") {
            return std::make_optional(BlockCodec(simple8b_block{}));
        } else if (type == "block_simple16") {
            return std::make_optional(BlockCodec(simple16_block{}));
        } else if (type == "block_simdbp") {
            return std::make_optional(BlockCodec(simdbp_block{}));
        } else if (type == "block_mixed") {
            return std::make_optional(BlockCodec(mixed_block{}));
        } else {
            return std::nullopt;
        }
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
