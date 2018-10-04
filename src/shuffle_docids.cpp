#include <fstream>
#include <iostream>
#include <algorithm>
#include <thread>
#include <numeric>
#include <random>

#include "succinct/mapper.hpp"

#include "binary_freq_collection.hpp"
#include "util/index_build_utils.hpp"
#include "util/util.hpp"

using ds2i::logger;

void emit(std::ostream& os, const uint32_t* vals, size_t n)
{
    os.write(reinterpret_cast<const char*>(vals), sizeof(*vals) * n);
}

void emit(std::ostream& os, uint32_t val)
{
    emit(os, &val, 1);
}

int main(int argc, const char** argv)
{

    using namespace ds2i;

    if (argc != 3 && argc != 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <collection basename> <output basename> [ordering file]"
                  << std::endl
                  << "Ordering file is of the form <current ID> <new ID>"
                  << std::endl;
        return 1;
    }

    constexpr uint32_t seed = 1729;
    std::mt19937 rng(seed);

    const std::string input_basename = argv[1];
    const std::string output_basename = argv[2];
    binary_freq_collection input(input_basename.c_str());
    size_t num_docs = input.num_docs();
    std::vector<uint32_t> new_doc_id(num_docs);
 
    if (argc == 4) {
      const std::string order_file = argv[3];
      std::ifstream in_order(order_file);
      uint32_t prev_id, new_id;
      size_t count = 0;
      while (in_order >> prev_id >> new_id) {
        new_doc_id[prev_id] = new_id;
        ++count;
      }
      if (new_doc_id.size() != count)
        throw std::invalid_argument("Invalid document order file.");
    }

    else {
      logger() << "Computing random permutation" << std::endl;
      std::iota(new_doc_id.begin(), new_doc_id.end(), uint32_t());
      std::shuffle(new_doc_id.begin(), new_doc_id.end(), rng);
    }
    {
        logger() << "Shuffling document sizes" << std::endl;
        binary_collection input_sizes((input_basename + ".sizes").c_str());
        auto sizes = *input_sizes.begin();
        if (sizes.size() != num_docs) {
            throw std::invalid_argument("Invalid sizes file");
        }

        std::vector<uint32_t> new_sizes(num_docs);
        for (size_t i = 0; i < num_docs; ++i) {
            new_sizes[new_doc_id[i]] = sizes.begin()[i];
        }

        std::ofstream output_sizes(output_basename + ".sizes");
        emit(output_sizes, sizes.size());
        emit(output_sizes, new_sizes.data(), num_docs);
    }

    logger() << "Shuffling posting lists" << std::endl;
    progress_logger plog;

    std::ofstream output_docs(output_basename + ".docs");
    std::ofstream output_freqs(output_basename + ".freqs");
    emit(output_docs, 1);
    emit(output_docs, num_docs);

    std::vector<std::pair<uint32_t, uint32_t>> pl;
    for (const auto& seq: input) {

        for (size_t i = 0; i < seq.docs.size(); ++i) {
            pl.emplace_back(new_doc_id[seq.docs.begin()[i]],
                            seq.freqs.begin()[i]);
        }

        std::sort(pl.begin(), pl.end());

        emit(output_docs, pl.size());
        emit(output_freqs, pl.size());
        for (const auto& posting: pl) {
            emit(output_docs, posting.first);
            emit(output_freqs, posting.second);
        }

        plog.done_sequence(seq.docs.size());
        pl.clear();
    }
    plog.log();
}
