use std::collections::HashMap;
use crate::matching_engine::engine::TradingPair;
use crate::matching_engine::orderbook::Trade;
use tokio::sync::Mutex as TokioMutex;
use std::sync::Arc;

// The holding profit we update manually, since the midprice
// has to be updated more than just when a trade happens but TraderData will only get updated on trades
// Update the holding profit when sending the data to the user interface (each 250ms at the moment)
#[derive(Debug, Clone)]
pub struct TraderData {
    pub positions: HashMap<TradingPair, f64>,
    pub trading_profit: f64,
}
impl TraderData {
    pub fn new(trading_profit: f64) -> TraderData {
        TraderData{
            positions: HashMap::new(),
            trading_profit,
        }
    }
}

pub async fn update_trader_data(trading_pair: TradingPair,
                                trades: Vec<Trade>,
                                traders_data_arc: Arc<TokioMutex<HashMap<String, TraderData>>>
) {
    let mut traders_data = traders_data_arc.lock().await;
    for trade in trades {
        let taker_id = trade.trader_id_taker;
        let maker_id = trade.trader_id_maker;
        let trade_value = trade.price.to_string().parse::<f64>().unwrap_or(0.0)* trade.volume;

        // Update taker's data
        let taker_data = traders_data.entry(taker_id.clone()).or_insert_with(|| TraderData::new(0.0));
        *taker_data.positions.entry(trading_pair.clone()).or_insert(0.0) += trade.volume;
        taker_data.trading_profit -= trade_value + trade.taker_fee;

        // Update maker's data
        let maker_data = traders_data.entry(maker_id.clone()).or_insert_with(|| TraderData::new(0.0));
        *maker_data.positions.entry(trading_pair.clone()).or_insert(0.0) -= trade.volume;
        maker_data.trading_profit += trade_value - trade.maker_fee;
    }
}