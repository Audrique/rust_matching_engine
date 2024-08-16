#![allow(warnings)]
mod matching_engine;
use matching_engine::orderbook::{Order, BidOrAsk};
use matching_engine::engine::{MatchingEngine, TradingPair};
use rust_decimal_macros::dec;

// TODO: two main things
// 1) Prevent placing orders that will self trade (so cancel them immediately after when in the matching they would have matched with a self trade
//    and then disconnect the trader
// 2) Return trades that happen (if 1 trader with an order trades with multiple traders or with one trader at different price limits, consider them as two trades)
// 99) Then if this works test this with a lot of orders and then start connecting this engine to some real exchange and start keeping a local orderbook.
//     Before fully implementing this think carefully how one would convert the events into orders for the orderbook.
//     Then also implement a flag of 'shadow_order' which is a bool, which will trade as usual but does not remove liquidity from the orderbook
//     This is done so that we can keep a proper local orderbook which is similar to the exchange.
fn main() {
    let trader_id = String::from("trader_id_audrique");
    let mut engine = MatchingEngine::new();


    let b1_order = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "0".to_string());
    let s1_order = Order::new(BidOrAsk::Ask, 9.5, trader_id.clone(), "1".to_string());
    let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
    engine.add_new_market(pair.clone());

    let b2_order = Order::new(BidOrAsk::Bid, 6.5, trader_id.clone(), "2".to_string());
    let s2_order = Order::new(BidOrAsk::Ask, 8.5, trader_id.clone(), "3".to_string());
    let eth_pair = TradingPair::new("ETH".to_string(), "USDT".to_string());
    engine.add_new_market(eth_pair.clone());


    engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order).unwrap();
    engine.place_limit_order(pair, dec!(31_000.0), s1_order).unwrap();
    engine.place_limit_order(eth_pair.clone(), dec!(10_000.0), b2_order).unwrap();
    engine.place_limit_order(eth_pair, dec!(11_000.0), s2_order).unwrap();
    let open_orders = engine.open_orders(trader_id);
    println!("{:?}", open_orders);
}

