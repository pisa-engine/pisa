#pragma once

#include <succinct/mapper.hpp>
#include "util.hpp"

using ds2i::logger;


template <typename InputCollection, typename Collection>
void verify_collection(InputCollection const& input, const char* filename)
{
    Collection coll;
    boost::iostreams::mapped_file_source m(filename);
    succinct::mapper::map(coll, m);

    logger() << "Checking the written data, just to be extra safe..." << std::endl;
    size_t s = 0;
    for (auto seq: input) {
        auto e = coll[s];
        if (e.size() != seq.docs.size()) {
            logger() << "sequence " << s
                     << " has wrong length! ("
                     << e.size() << " != " << seq.docs.size() << ")"
                     << std::endl;
            exit(1);
        }

        for (size_t i = 0; i < e.size(); ++i, e.next()) {
            uint64_t docid = *(seq.docs.begin() + i);
            uint64_t freq = *(seq.freqs.begin() + i);

            if (docid != e.docid()) {
                logger() << "docid in sequence " << s
                         << " differs at position " << i << "!" << std::endl;
                logger() << e.docid() << " != " << docid << std::endl;
                logger() << "sequence length: " << seq.docs.size() << std::endl;

                exit(1);
            }

            if (freq != e.freq()) {
                logger() << "freq in sequence " << s
                         << " differs at position " << i << "!" << std::endl;
                logger() << e.freq() << " != " << freq << std::endl;
                logger() << "sequence length: " << seq.docs.size() << std::endl;

                exit(1);
            }
        }

        s += 1;
    }
    logger() << "Everything is OK!" << std::endl;
}

