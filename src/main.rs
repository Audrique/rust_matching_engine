#![allow(warnings)]
mod connecting_to_exchanges;
mod matching_engine;

use connecting_to_exchanges::deribit_connection::{establish_connection,
                                                  read_config_file,
                                                  authenticate_deribit,
                                                  subscribe_to_channel,
                                                  on_incoming_message};
use tokio;
use futures_util::{StreamExt, SinkExt};
// TODO: two main things
// 1) Prevent placing orders that will self trade (so cancel them immediately after when in the matching they would have matched with a self trade
//    and then disconnect the trader
// 2) Return trades that happen (if 1 trader with an order trades with multiple traders or with one trader at different price limits, consider them as two trades)
// 99) Then if this works test this with a lot of orders and then start connecting this engine to some real exchange and start keeping a local orderbook.
//     Before fully implementing this think carefully how one would convert the events into orders for the orderbook.
//     Then also implement a flag of 'shadow_order' which is a bool, which will trade as usual but does not remove liquidity from the orderbook
//     This is done so that we can keep a proper local orderbook which is similar to the exchange.


#[tokio::main]
async fn main() {
    let url = "wss://www.deribit.com/ws/api/v2";
    let mut ws_stream = establish_connection(url).await;
    let (client_id, client_secret) = read_config_file();
    authenticate_deribit(&mut ws_stream, &client_id, &client_secret).await;
    let channels = vec!["book.BTC_USDT.raw"];
    subscribe_to_channel(&mut ws_stream, channels).await;
    on_incoming_message(&mut ws_stream).await;
}



