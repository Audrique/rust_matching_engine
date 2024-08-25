#![allow(dead_code)]
#![allow(unused_imports)]
mod warp_websocket;
mod connecting_to_exchanges;
mod matching_engine;

use std::collections::HashMap;
use std::convert::Infallible;
use warp_websocket::handler::TopicActionRequest;
use tokio::sync::{mpsc, RwLock};
use warp::{ws::Message, Filter, Rejection};
use crate::warp_websocket::handler::{add_topic, remove_topic};
use crate::warp_websocket::handler;



use std::sync::{Arc, Mutex};
use matching_engine::{engine::{MatchingEngine, TradingPair}, orderbook::{BidOrAsk, Order}};
use connecting_to_exchanges::deribit_connection::{authenticate_deribit,
                                                  establish_connection,
                                                  on_incoming_deribit_message,
                                                  read_config_file,
                                                  subscribe_to_channel};
use tokio;
use futures_util::{SinkExt, StreamExt};

// 1) Prevent placing orders that will self trade (so cancel them immediately after when in the matching they would have matched with a self trade
//    and then disconnect the trader
// 2) Return trades that happen (if 1 trader with an order trades with multiple traders
// or with one trader at different price limits, consider them as two trades) and return which one was taker and maker
// such that we can later add fees based on taker or maker when keeping track of profits
// 3) use the StringCounter for local traders order_id and return it to the trader or something?
//    but then is the question, how to integrate the "-1" from the exchange orders?
// 4) setup websocket for the local orderbook to send information to the frontend and clients (local trading strategies).
// Note: changes from the websocket are saved in the local orderbook with order_id = -1 (all of them)
// All local strategy orders are saved with a unique iterator ('0', '1', ...) as order_id

type Result<T> = std::result::Result<T, Rejection>;
type Clients = Arc<RwLock<HashMap<String, Client>>>;
#[derive(Debug, Clone)]
pub struct Client {
    pub user_id: usize,
    pub topics: Vec<String>,
    pub sender: Option<mpsc::UnboundedSender<std::result::Result<Message, warp::Error>>>,
}

#[tokio::main]
async fn main() {
    // let trading_pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
    // let mut engine = MatchingEngine::new();
    // engine.add_new_market(trading_pair.clone());
    //
    //
    // let url = "wss://www.deribit.com/ws/api/v2";
    // let mut ws_stream = establish_connection(url).await;
    // let (client_id, client_secret) = read_config_file();
    //
    // authenticate_deribit(&mut ws_stream, &client_id, &client_secret).await;
    // let channel_btc_usdt = &format!("book.{}.raw", trading_pair.to_string());
    // let channels = vec![channel_btc_usdt];
    // subscribe_to_channel(&mut ws_stream, channels).await;
    //
    // // make the engine such that it can have
    // // shared mutable references for the process_message function
    // // println!("test");
    // // println!("{:?}", &engine);
    // // println!("test2");
    //
    // // So the problem is that we want the engine to be called and mutated in the
    // // "on_incoming_deribit_message" but also on the "on_client_message" or something
    // // on_incoming_deribit_message needs a mut engine and on_client_message doesn't need it if we work with one central exchange
    // // it is also possible we would let them place orders inside the function to the engine (maybe this is better)
    // // but then it is harder to keep track of where the 'actual' orderbook is (maybe with the arc mutex it doesnt matter?)
    // // Probably also do an Arc Mutex in the latter async function.
    //
    // on_incoming_deribit_message(&mut ws_stream, Arc::new(Mutex::new(engine))).await;

    // ----------------------------------------------------------------------------------
    // testing the warp
    // ----------------------------------------------------------------------------------

    let clients: Clients = Arc::new(RwLock::new(HashMap::new()));

    let health_route = warp::path!("health").and_then(handler::health_handler);

    let register = warp::path("register");
    let register_routes = register
        .and(warp::post())
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(handler::register_handler)
        .or(register
            .and(warp::delete())
            .and(warp::path::param())
            .and(with_clients(clients.clone()))
            .and_then(handler::unregister_handler));

    let publish = warp::path!("publish")
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(handler::publish_handler);

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(warp::path::param())
        .and(with_clients(clients.clone()))
        .and_then(handler::ws_handler);

    let clients_for_add = clients.clone();
    let add_topic_route = warp::post()
        .and(warp::path("add_topic"))
        .and(warp::body::json::<TopicActionRequest>())
        .and(warp::any().map(move || clients_for_add.clone()))
        .and_then(add_topic);

    let clients_for_remove = clients.clone();
    let remove_topic_route = warp::delete()
        .and(warp::path("remove_topic"))
        .and(warp::body::json::<TopicActionRequest>())
        .and(warp::any().map(move || clients_for_remove.clone()))
        .and_then(remove_topic);

    let routes = health_route
        .or(register_routes)
        .or(ws_route)
        .or(publish)
        .or(add_topic_route)
        .or(remove_topic_route)
        .with(warp::cors().allow_any_origin());



    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}

fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}



