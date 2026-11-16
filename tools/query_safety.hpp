#pragma once

namespace pisa::queries {

// A zero initial threshold cannot prune a result. The summary is therefore
// safe even when the optional corrective rerun was not requested.
[[nodiscard]] constexpr bool summary_is_safe(bool corrective_rerun, bool has_thresholds) noexcept {
    return corrective_rerun || !has_thresholds;
}

}  // namespace pisa::queries
