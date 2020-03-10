#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <thread>

#include "boost/lexical_cast.hpp"

namespace pisa {

class configuration {
   public:
    static configuration const &get()
    {
        static configuration instance;
        return instance;
    }

    size_t quantization_bits;
    bool heuristic_greedy;

   private:
    configuration()
    {
        fillvar("PISA_HEURISTIC_GREEDY", heuristic_greedy, false);
        fillvar("PISA_QUANTIZTION_BITS", quantization_bits, 8);
    }

    template <typename T, typename T2>
    void fillvar(const char *envvar, T &var, T2 def)
    {
        const char *val = std::getenv(envvar);
        if (!val || !strlen(val)) {
            var = def;
        } else {
            var = boost::lexical_cast<T>(val);
        }
    }
};

} // namespace pisa
