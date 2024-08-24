#![allow(dead_code)]
#![allow(unused_imports)]
mod connecting_to_exchanges;
mod matching_engine;

use std::sync::{Arc, Mutex};
use matching_engine::{engine::{MatchingEngine, TradingPair}, orderbook::{BidOrAsk, Order}};
use connecting_to_exchanges::deribit_connection::{authenticate_deribit,
                                                  establish_connection,
                                                  on_incoming_message,
                                                  read_config_file,
                                                  subscribe_to_channel};
use tokio;
use futures_util::{SinkExt, StreamExt};

// 1) Prevent placing orders that will self trade (so cancel them immediately after when in the matching they would have matched with a self trade
//    and then disconnect the trader
// 2) Return trades that happen (if 1 trader with an order trades with multiple traders or with one trader at different price limits, consider them as two trades)
// 3) use the StringCounter for local traders order_id and return it to the trader or something?
//    but then is the question, how to integrate the "-1" from the exchange orders?
// Note: changes from the websocket are saved in the local orderbook with order_id = -1 (all of them)
// All local strategy orders are saved with a unique iterator ('0', '1', ...) as order_id

#[tokio::main]
async fn main() {
    let trading_pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
    let mut engine = MatchingEngine::new();
    engine.add_new_market(trading_pair.clone());


    let url = "wss://www.deribit.com/ws/api/v2";
    let mut ws_stream = establish_connection(url).await;
    let (client_id, client_secret) = read_config_file();

    authenticate_deribit(&mut ws_stream, &client_id, &client_secret).await;
    let channel_btc_usdt = &format!("book.{}.raw", trading_pair.to_string());
    let channels = vec![channel_btc_usdt];
    subscribe_to_channel(&mut ws_stream, channels).await;

    // make the engine such that it can have
    // shared mutable references for the process_message function
    on_incoming_message(&mut ws_stream, Arc::new(Mutex::new(engine))).await;

}



