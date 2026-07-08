/// Transaction cost modeling — spread estimation, market impact, fill price.
///
/// Pure stateless functions.  Mirrors coinbase/src/tcost.py in Rust.

fn bps(x: f64) -> f64 {
    x / 10000.0
}

/// Estimate bid-ask spread in basis points.
pub fn estimate_spread_bps(bid: f64, ask: f64) -> f64 {
    if bid <= 0.0 || ask <= 0.0 {
        return 0.0;
    }
    20000.0 * (ask - bid) / (ask + bid)
}

/// Estimate market impact in basis points.
pub fn impact_bps(notional_usd: f64, impact_coeff: f64) -> f64 {
    if notional_usd <= 0.0 {
        return 0.0;
    }
    impact_coeff * (notional_usd / 10000.0).max(1e-9).sqrt()
}

/// Compute the effective (expected) fill price for a trade, incorporating
/// spread, slippage, market impact, and taker fees.
pub fn effective_fill_price(
    side: &str,
    mid: f64,
    bid: f64,
    ask: f64,
    notional_usd: f64,
    taker_fee_bps: f64,
    slippage_bps: f64,
    impact_coeff: f64,
) -> f64 {
    let spr = estimate_spread_bps(bid, ask);
    let imp = impact_bps(notional_usd, impact_coeff);
    let total_bps = spr / 2.0 + slippage_bps + imp + taker_fee_bps;

    if mid <= 0.0 {
        return 0.0;
    }
    match side.to_lowercase().as_str() {
        "buy" => mid * (1.0 + bps(total_bps)),
        _ => mid * (1.0 - bps(total_bps)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spread_zero() {
        assert_eq!(estimate_spread_bps(0.0, 0.0), 0.0);
    }

    #[test]
    fn test_spread_normal() {
        let spr = estimate_spread_bps(100.0, 100.10);
        assert!((spr - 9.99).abs() < 0.1); // ~10 bps
    }

    #[test]
    fn test_impact_zero_notional() {
        assert_eq!(impact_bps(0.0, 1.5), 0.0);
    }

    #[test]
    fn test_impact_positive() {
        let imp = impact_bps(10000.0, 1.5);
        assert!((imp - 1.5).abs() < 0.01);
    }

    #[test]
    fn test_fill_price_buy() {
        let price = effective_fill_price("buy", 100.0, 99.9, 100.1, 10000.0, 8.0, 0.0, 1.5);
        assert!(price > 100.0); // buyer pays premium
    }

    #[test]
    fn test_fill_price_sell() {
        let price = effective_fill_price("sell", 100.0, 99.9, 100.1, 10000.0, 8.0, 0.0, 1.5);
        assert!(price < 100.0); // seller receives discount
    }

    #[test]
    fn test_impact_scales_with_notional() {
        let small = impact_bps(1000.0, 1.5);
        let large = impact_bps(100000.0, 1.5);
        assert!(large > small);
    }
}
