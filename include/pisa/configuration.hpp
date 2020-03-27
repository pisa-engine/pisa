#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <thread>

#include "boost/lexical_cast.hpp"

namespace pisa {

class configuration {
  public:
    static configuration const& get()
    {
        static configuration instance;
        return instance;
    }

    size_t quantization_bits{8};
    bool heuristic_greedy{false};

  private:
    configuration()
    {
        fillvar("PISA_HEURISTIC_GREEDY", heuristic_greedy);
        fillvar("PISA_QUANTIZTION_BITS", quantization_bits);
    }

    template <typename T>
    void fillvar(const char* envvar, T& var)
    {
        const char* val = std::getenv(envvar);
        if (val && strlen(val)) {
            var = boost::lexical_cast<T>(val);
        }
    }
};

}  // namespace pisa
