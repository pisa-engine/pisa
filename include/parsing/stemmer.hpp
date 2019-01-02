#pragma once

#include <memory>
#include <string>

#include "parsing/snowball.hpp"

namespace ds2i {

class Porter2_Stemmer {
   public:
    Porter2_Stemmer() : env_(snowball::create_env()) {}
    Porter2_Stemmer(Porter2_Stemmer const & /* other */) : env_(snowball::create_env()) {}
    Porter2_Stemmer &operator=(Porter2_Stemmer const & /* other */) {
        env_ = snowball::create_env();
        return *this;
    }
    Porter2_Stemmer(Porter2_Stemmer &&) noexcept { env_ = snowball::create_env(); }
    Porter2_Stemmer &operator=(Porter2_Stemmer &&) noexcept {
        env_ = snowball::create_env();
        return *this;
    }
    ~Porter2_Stemmer() { snowball::close_env(env_); }

    [[nodiscard]] auto stem(std::string const &word) const -> std::string {
        snowball::SN_set_current(
            env_, word.size(), reinterpret_cast<unsigned char const *>(word.c_str()));
        snowball::stem(env_);
        auto length = env_->l;
        return std::string(env_->p, std::next(env_->p, length));
    }

    std::string operator()(const std::string &word) const { return stem(word); }

   private:
    snowball::SN_env *env_;
};

} // namespace ds2i
