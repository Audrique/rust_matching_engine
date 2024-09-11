use std::collections::HashMap;
use super::orderbook::{Orderbook, Order, BidOrAsk, Trade};
use rust_decimal::prelude::*;


// TODO: implement the StringCounter to make order_id's inside the engine and return them when orders are placed
// (be careful with the "-1" od deribit orders)
struct StringCounter {
    count: usize,
}

impl StringCounter {
    fn new() -> Self {
        StringCounter { count: 0 }
    }
}

impl Iterator for StringCounter {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.count.to_string();
        self.count += 1;
        Some(result)
    }
}

// BTCUSD => BTC is base and USD the quote
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct TradingPair {
    base: String,
    quote: String,
}

impl TradingPair {
    pub fn new(base: String, quote: String) -> TradingPair {
        TradingPair {base, quote}
    }

    pub fn to_string(self) -> String {
        format!("{}_{}", self.base, self.quote)
    }
}


#[derive(Debug, Clone)]
pub struct MatchingEngine {
    pub orderbooks: HashMap<TradingPair, Orderbook>,
}

impl MatchingEngine {
    pub fn new() -> MatchingEngine {
        MatchingEngine {
            orderbooks: HashMap::new(),
        }
    }
    pub fn add_new_market(&mut self, pair: TradingPair) {
        self.orderbooks.insert(pair.clone(), Orderbook::new());
        println!("opening new orderbook for market {:?}", pair.to_string());
    }

    // TODO: this function needs to return as a result the trades that happened in
    pub fn place_limit_order(&mut self,
                             pair: TradingPair,
                             price: Decimal,
                             order: Order) -> Result<Vec<Trade>, String> {
        match self.orderbooks.get_mut(&pair) {
            Some(orderbook) => {
                let trades = orderbook.add_limit_order(price, order);
                // println!("Placed limit order at price level {}", price);
                Ok(trades)
            },
            None => {
                Err(format!(
                    "The orderbook for the given trading {} does not exist",
                    pair.to_string()
                ))
            }
        }
    }

    pub fn open_orders (&self, trader_id: String) -> HashMap<TradingPair, HashMap<Decimal, Vec<Order>>> {
        self.orderbooks
            .iter()
            .filter_map(|(trading_pair, orderbook)| {
                let orders = orderbook.open_orders_for_trader(trader_id.clone());
                if orders.is_empty() {
                    None
                } else {
                    Some((trading_pair.clone(), orders))
                }
            })
            .collect()
    }
    pub fn cancel_order(&mut self,
                        pair: TradingPair,
                        bid_or_ask: BidOrAsk,
                        price:Decimal,
                        order_id: String) -> Result<(), String> {
        match self.orderbooks.get_mut(&pair) {
            Some(orderbook) => {
                orderbook.cancel_order(bid_or_ask, price, order_id);
                Ok(())
            },
            None => {
                Err(format!(
                    "The orderbook for the given trading {} does not exist",
                    pair.to_string()
                ))
            }
        }
    }

    // This function is made since we do not have order_id's for the changes coming through the websocket
    // For example: 1) a "new" change at price 100 for 5 volume
    //              2) then a "change" change at price 100 for 10
    //              3) A local strategy places an order at the same level: 100 for volume 1
    //              4) many changes in between
    //              5) a "delete" for the price 100 (volume: 0.0)
    //              6) now all volume from the exchange should be deleted but not the order from the local strategy
    pub fn leave_volume_from_exchange(&mut self,
                         pair: TradingPair,
                         bid_or_ask: BidOrAsk,
                         price: Decimal, leave_volume: f64) -> Result<(), String> {
        match self.orderbooks.get_mut(&pair) {
            Some(orderbook) => {
                orderbook.leave_volume_from_exchange_orders(bid_or_ask, price, leave_volume);
                Ok(())
            },
            None => {
                Err(format!(
                    "The orderbook for the given trading {} does not exist",
                    pair.to_string()
                ))
            }
        }
    }
}
