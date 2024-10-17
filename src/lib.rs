use std::collections::HashMap;
use futures_util::SinkExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;

// In this file we will add functions/structs/... that all traders can use

#[derive(Debug, PartialEq, Clone)]
pub enum BidOrAsk {
    Bid,
    Ask,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RegisterResponse {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct EngineData {
    pub message: String,
    pub topic: String,
    pub user_id: u32,
    pub update_counter: u32,
}

#[derive(Debug, Deserialize)]
pub struct BestPriceUpdate {
    pub best_bid_price: String,
    pub best_ask_price: String,
    pub changed_side: String,
}


#[derive(Debug)]
pub struct OpenOrders {
    pub asks: HashMap<Decimal, f64>,
    pub bids: HashMap<Decimal, f64>,
    pub open_ask_volume: f64,
    pub open_bid_volume: f64,
}
impl OpenOrders {
    pub fn new(asks: HashMap<Decimal, f64>, bids: HashMap<Decimal, f64>, open_ask_volume: f64, open_bid_volume: f64) -> OpenOrders {
        OpenOrders {
            asks,
            bids,
            open_ask_volume,
            open_bid_volume,
        }
    }

    pub fn add_order(&mut self, side: BidOrAsk, price: Decimal, volume: f64) {
        match side {
            BidOrAsk::Ask => {
                *self.asks.entry(price).or_insert(0.0) += volume;
                self.open_ask_volume += volume;
            }
            BidOrAsk::Bid => {
                *self.bids.entry(price).or_insert(0.0) += volume;
                self.open_bid_volume += volume;
            }
        }
    }

    pub fn remove_order(&mut self, side: BidOrAsk, price: Decimal, volume: f64) -> Result<(), String> {
        match side {
            BidOrAsk::Ask => {
                if let Some(existing_volume) = self.asks.get_mut(&price) {
                    *existing_volume -= volume;
                    self.open_ask_volume -= volume;
                    if *existing_volume <= 1e-8 {
                        self.asks.remove(&price);
                    }
                    Ok(())
                } else {
                    Err(format!("No ask order found at price {}", price))
                }
            }
            BidOrAsk::Bid => {
                if let Some(existing_volume) = self.bids.get_mut(&price) {
                    *existing_volume -= volume;
                    self.open_bid_volume -= volume;
                    if *existing_volume <= 1e-8 {
                        self.bids.remove(&price);
                    }
                    Ok(())
                } else {
                    Err(format!("No bid order found at price {}", price))
                }
            }
        }
    }

    pub fn amend_order(&mut self, side: BidOrAsk, old_price: Decimal, new_price: Decimal, old_volume: f64, new_volume: f64) -> Result<(), String> {
        self.remove_order(side.clone(), old_price, old_volume)?;
        self.add_order(side, new_price, new_volume);
        Ok(())
    }
}

//TODO: track the positions by processing trades with the given trader_id from the engine (publish these)!
#[derive(Debug)]
pub struct ClientData {
    pub open_orders: HashMap<String, OpenOrders>, // TradingPair --> open orders
    pub positions: HashMap<String, f64>, // TradingPair --> position
}

impl ClientData {
    pub fn new() -> Self {
        ClientData {
            open_orders: HashMap::new(),
            positions: HashMap::new(),
        }
    }

    pub fn add_order(&mut self, trading_pair: &str, side: BidOrAsk, price: Decimal, volume: f64) {
        let orders = self.open_orders.entry(trading_pair.to_string())
            .or_insert_with(|| OpenOrders::new(HashMap::new(), HashMap::new(), 0.0, 0.0));
        orders.add_order(side, price, volume);
    }

    pub fn remove_order(&mut self, trading_pair: &str, side: BidOrAsk, price: Decimal, volume: f64) -> Result<(), String> {
        if let Some(orders) = self.open_orders.get_mut(trading_pair) {
            orders.remove_order(side, price, volume)
        } else {
            Err(format!("No open orders found for trading pair: {}", trading_pair))
        }
    }

    pub fn amend_order(&mut self, trading_pair: &str, side: BidOrAsk, old_price: Decimal, new_price: Decimal, old_volume: f64, new_volume: f64) -> Result<(), String> {
        if let Some(orders) = self.open_orders.get_mut(trading_pair) {
            orders.amend_order(side, old_price, new_price, old_volume, new_volume)
        } else {
            Err(format!("No open orders found for trading pair: {}", trading_pair))
        }
    }

    pub fn get_open_orders(&self, trading_pair: &str) -> Option<&OpenOrders> {
        self.open_orders.get(trading_pair)
    }

    pub fn get_position(&self, trading_pair: &str) -> Option<&f64> {
        self.positions.get(trading_pair)
    }
}



pub async fn place_limit_order(trading_pair_base: &str,
                               trading_pair_quote: &str,
                               price: f64,
                               side: &str,
                               volume: f64,
                               trader_id: &str,
                               order_id: &str,
                               ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>
) {
    let add_limit_order_msg = json!({"action": "add_limit_order",
            "trading_pair_base": trading_pair_base,
            "trading_pair_quote": trading_pair_quote,
            "price": price,
            "side": side,
            "volume": volume,
            "trader_id": trader_id,
            "order_id": order_id
        });
    ws_stream_ref.send(Message::text(add_limit_order_msg.to_string())).await.unwrap();
    println!("Sent the order request to the engine.");
}

pub async fn request_open_orders_and_position(trading_pair_base: &str,
                               trading_pair_quote: &str,
                               trader_id: &str,
                               ws_stream_ref: &mut WebSocketStream<MaybeTlsStream<TcpStream>>
) {
    let request_open_orders_and_position_msg = json!({"action": "open_orders_and_positions",
            "trading_pair_base": trading_pair_base,
            "trading_pair_quote": trading_pair_quote,
            "trader_id": trader_id
    });
    ws_stream_ref.send(Message::text(request_open_orders_and_position_msg.to_string())).await.unwrap();
    println!("Sent the order request to the engine.");
}



