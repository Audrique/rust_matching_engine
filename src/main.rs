#![allow(dead_code)]
#![allow(unused_imports)]
mod warp_websocket;
mod connecting_to_exchanges;
mod matching_engine;

use std::collections::HashMap;
use std::convert::Infallible;
use warp_websocket::handler::TopicActionRequest;
use tokio::sync::{mpsc, oneshot, RwLock, Mutex as TokioMutex};
use warp::{ws::Message, Filter, Rejection};
use crate::warp_websocket::handler;
use std::sync::Arc;
use matching_engine::{engine::{MatchingEngine, TradingPair}, orderbook::{BidOrAsk, Order}};
use connecting_to_exchanges::deribit_connection::{authenticate_deribit,
                                                  establish_connection,
                                                  on_incoming_deribit_message,
                                                  read_config_file,
                                                  subscribe_to_channel};
use tokio;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, Rejection>;
type Clients = Arc<RwLock<HashMap<String, Client>>>;
#[derive(Debug, Clone)]
pub struct Client {
    pub user_id: usize,
    pub topics: Vec<String>,
    pub sender: Option<mpsc::UnboundedSender<std::result::Result<Message, warp::Error>>>,
}

fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}

async fn wait_for_server(rx: oneshot::Receiver<()>) {
    match rx.await {
        Ok(_) => println!("Server is ready, starting to process Deribit messages"),
        Err(_) => eprintln!("Failed to receive server ready signal"),
    }
}

#[tokio::main]
async fn main() {
    let trading_pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
    let mut engine = MatchingEngine::new();
    engine.add_new_market(trading_pair.clone());

    let engine = Arc::new(TokioMutex::new(engine));
    let clients: Clients = Arc::new(RwLock::new(HashMap::new()));

    // Create an oneshot-channel for signaling server readiness
    let (tx, rx) = oneshot::channel();

    // Spawn the Deribit connection handling
    let deribit_clients = clients.clone();
    let deribit_engine = engine.clone();
    tokio::spawn(async move {
        let url_deribit = "wss://www.deribit.com/ws/api/v2";
        let mut ws_stream = establish_connection(url_deribit).await;
        let (client_id, client_secret) = read_config_file();

        authenticate_deribit(&mut ws_stream, &client_id, &client_secret).await;
        let channel_btc_usdt = &format!("book.{}.raw", trading_pair.to_string());
        let channels = vec![channel_btc_usdt];
        subscribe_to_channel(&mut ws_stream, channels).await;
        // Wait for the server to be ready before processing messages
        wait_for_server(rx).await;
        on_incoming_deribit_message(&mut ws_stream, deribit_engine, deribit_clients).await;
    });

    // ----------------------------------------------------------------------------------
    // warp part

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
            .and_then(handler::unregister_handler)
            );

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
        .and_then(handler::add_topic);

    let clients_for_remove = clients.clone();
    let remove_topic_route = warp::delete()
        .and(warp::path("remove_topic"))
        .and(warp::body::json::<TopicActionRequest>())
        .and(warp::any().map(move || clients_for_remove.clone()))
        .and_then(handler::remove_topic);

    let routes = health_route
        .or(register_routes)
        .or(ws_route)
        .or(publish)
        .or(add_topic_route)
        .or(remove_topic_route)
        .with(warp::cors().allow_any_origin());

    // Signal that the server is about to start
    let _ = tx.send(());

    warp::serve(routes).run(([127, 0, 0, 1], 8000)).await;
}



