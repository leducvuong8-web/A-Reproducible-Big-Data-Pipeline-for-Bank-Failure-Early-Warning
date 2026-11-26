"""Recalculate the development-only cost grid in §5.2."""
from decimal import Decimal as D

REVIEW_COST = D("90.54")
MEDIAN_LOSS = D("1995500")
FOLDS = 1564
CANDIDATES = (
    ("0.13434", 1465, 18098), ("0.13832", 1462, 17611),
    ("0.17272", 1436, 14151), ("0.19191", 1416, 12701),
    ("0.21110", 1398, 11498), ("0.23989", 1374, 9991), ("No alert", 0, 0),
)

def cost(row, share, positive_banks):
    _, hits, false_alerts = row
    return REVIEW_COST * false_alerts + MEDIAN_LOSS * share * (positive_banks - hits)

def optimal_share_bounds(candidate):
    """Find the avoidable-loss shares for which this cut beats every grid row."""
    _, hits, false_alerts = candidate
    lower, upper = D(0), D(1)
    for other in CANDIDATES:
        _, other_hits, other_false_alerts = other
        extra_hits = hits - other_hits
        extra_review_cost = REVIEW_COST * (false_alerts - other_false_alerts)
        if extra_hits > 0:
            lower = max(lower, extra_review_cost / (MEDIAN_LOSS * extra_hits))
        elif extra_hits < 0:
            upper = min(upper, extra_review_cost / (MEDIAN_LOSS * extra_hits))
        elif extra_review_cost > 0:
            raise ValueError(f"{candidate[0]} is dominated by {other[0]}")
    return lower, upper

def main():
    for share, cut in (
        ("0.002", "0.23989"), ("0.003", "0.21110"), ("0.005", "0.17272"),
        ("0.007", "0.13832"), ("0.010", "0.13434"),
    ):
        weight = D(share)
        best = min(CANDIDATES, key=lambda row: cost(row, weight, FOLDS))
        assert best[0] == cut
        print(f"{weight:.2%} | F1–F3 grid choice {cut} | USD {cost(best, weight, FOLDS):,.2f}")
    scenario = cost(("0.17272", 7, 522), D(".005"), 12)
    frozen = cost(("0.19191", 7, 466), D(".005"), 12)
    assert scenario == D("97149.38") and frozen == D("92079.14")
    print(f"F4 | cost-grid choice USD {scenario:,.2f} | frozen F3 cut USD {frozen:,.2f}")

    f3_cut = next(row for row in CANDIDATES if row[0] == "0.19191")
    lower, upper = optimal_share_bounds(f3_cut)
    assert f"{lower:.3%}" == "0.303%" and f"{upper:.3%}" == "0.329%"
    for share in (lower, (lower + upper) / 2, upper):
        assert all(cost(f3_cut, share, FOLDS) <= cost(row, share, FOLDS) for row in CANDIDATES)
    print(f"F1–F3 | interval in which the F3 cut minimizes the grid: {lower:.3%} to {upper:.3%}")

if __name__ == "__main__":
    main()
