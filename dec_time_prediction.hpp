#pragma once

#include <array>
#include <sstream>
#include <string>

#include <boost/preprocessor/seq/enum.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/stringize.hpp>

#include "util.hpp"

#define DS2I_FEATURE_TYPES (n)(size)(sum_of_logs)(entropy)(nonzeros)(max_b)(pfor_b)(pfor_exceptions)

namespace ds2i { namespace time_prediction {

    constexpr size_t num_features = BOOST_PP_SEQ_SIZE(DS2I_FEATURE_TYPES);

    enum class feature_type {
        BOOST_PP_SEQ_ENUM(DS2I_FEATURE_TYPES), end
    };

    feature_type parse_feature_type(std::string const& name)
    {
        if (false) {
#define LOOP_BODY(R, DATA, T)                           \
            } else if (name == BOOST_PP_STRINGIZE(T)) { \
                return feature_type::T;                \
                    /**/
            BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_FEATURE_TYPES);
#undef LOOP_BODY
        } else {
            throw std::invalid_argument("Invalid feature name " + name);
        }

    }

    std::string feature_name(feature_type f)
    {
        switch (f) {
#define LOOP_BODY(R, DATA, T)                       \
            case feature_type::T: return BOOST_PP_STRINGIZE(T); \
            /**/

        BOOST_PP_SEQ_FOR_EACH(LOOP_BODY, _, DS2I_FEATURE_TYPES);
#undef LOOP_BODY
        default: throw std::invalid_argument("Invalid feature type");
        }
    }

    class feature_vector {
    public:
        feature_vector()
        {
            std::fill(m_features.begin(), m_features.end(), 0);
        }

        float& operator[](feature_type f) { return m_features[(size_t)f]; }
        float const& operator[](feature_type f) const { return m_features[(size_t)f]; }

        stats_line& dump(stats_line& sl) const
        {
            for (size_t i = 0; i < num_features; ++i) {
                feature_type ft = (feature_type)i;
                sl(feature_name(ft), (*this)[ft]);
            }
            return sl;
        }

    protected:
        std::array<float, num_features> m_features;
    };

    class predictor : public feature_vector {
    public:
        predictor()
            : m_bias(0)
        {}

        predictor(std::vector<std::pair<std::string, float>> const& values)
        {
            for (auto const& kv: values) {
                if (kv.first == "bias") {
                    bias() = kv.second;
                } else {
                    (*this)[parse_feature_type(kv.first)] = kv.second;
                }
            }
        }

        float& bias() { return m_bias; }
        float const& bias() const { return m_bias; }

        float operator()(feature_vector const& f) const
        {
            float result = bias();
            for (size_t i = 0; i < num_features; ++i) {
                feature_type ft = (feature_type)i;
                result += (*this)[ft] * f[ft];
            }
            return result;
        }

    protected:
        float m_bias;
    };

    void values_statistics(std::vector<uint32_t> values, feature_vector& f)
    {
        std::sort(values.begin(), values.end());
        f[feature_type::n] = values.size();
        if (values.empty()) return;

        uint32_t last_value = values.front();
        size_t group_begin = 0;
        double entropy = 0;
        double sum_of_logs = 0;
        double nonzeros = 0;
        uint32_t max_b = 0;

        for (size_t i = 1; i <= values.size(); ++i) {
            if (i == values.size() || values[i] != last_value) {
                size_t group_size = i - group_begin;
                entropy += group_size * log2(double(values.size()) / group_size);
                sum_of_logs += group_size * log2(double(last_value) + 1);
                if (last_value != 0) {
                    nonzeros += group_size;
                }
                uint32_t b = last_value ? succinct::broadword::msb(last_value) + 1 : 0;
                max_b = std::max(max_b, b);

                if (i < values.size()) {
                    last_value = values[i];
                    group_begin = i;
                }
            }
        }

        f[feature_type::entropy] = entropy;
        f[feature_type::sum_of_logs] = sum_of_logs;
        f[feature_type::nonzeros] = nonzeros;
        f[feature_type::max_b] = max_b;
    }

    bool read_block_stats(std::istream& is, uint32_t& list_id, std::vector<uint32_t>& block_counts)
    {
        thread_local std::string line;
        uint32_t count;
        block_counts.clear();
        if (!std::getline(is, line)) return false;

        std::istringstream iss(line);
        iss >> list_id;
        while (iss >> count) block_counts.push_back(count);

        return true;
    }


}}
