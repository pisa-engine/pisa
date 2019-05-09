#include <string>

#include <KrovetzStemmer/KrovetzStemmer.hpp>
#include <Porter2/Porter2.hpp>

#include "parsing/stem.hpp"

namespace pisa {

namespace porter2 {
    [[nodiscard]] auto stem(const std::string &word) -> std::string
    {
        return stem::Porter2{}.stem(word);
    }
}
namespace krovetz {
    static stem::KrovetzStemmer kstemmer;
    [[nodiscard]] auto stem(const std::string &word) -> std::string
    {
        return kstemmer.kstem_stemmer(word);
    }
}

} // namespace pisa
