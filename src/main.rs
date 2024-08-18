#![allow(warnings)]
mod matching_engine;
mod connecting_to_exchanges;
use tokio_tungstenite::connect_async;
use tokio;
use tokio_tungstenite::tungstenite::Message;
use serde_json::json;
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
    println!("Trying to connect to: {}", url);
    let (mut ws_stream,_) = connect_async(url).await.expect("Failed to connect");
    println!("Connected to Deribit exchange");

    // Create the message as JSON
    let msg = json!({
        "method":"public/subscribe",
        "params":{
        "channels":[
        "book.BTC_USDT.none.1.agg2"
        ]
        },
        "jsonrpc":"2.0",
        "id":887
    });
    // Serialize the message to a string
    let msg_text = msg.to_string();

    // Send the message over the WebSocket connection
    ws_stream.send(Message::Text(msg_text)).await.expect("Failed to send message");

    // Handle incoming messages
    while let Some(message) = ws_stream.next().await {
        match message {
            Ok(msg) => match msg {
                Message::Text(text) => {
                    println!("Received: {}", text);
                    // You can add your logic here to handle the response
                }
                Message::Binary(bin) => {
                    println!("Received binary data: {:?}", bin);
                }
                Message::Close(_) => {
                    println!("Connection closed");
                    break;
                }
                _ => (),
            },
            Err(e) => {
                println!("Error receiving message: {}", e);
                break;
            }
        }
    }
}



