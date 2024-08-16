use std::collections::HashMap;
use super::orderbook::{Orderbook, Order};
use rust_decimal::prelude::*;

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

pub struct MatchingEngine {
    orderbooks: HashMap<TradingPair, Orderbook>,
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

    pub fn place_limit_order(&mut self,
                             pair: TradingPair,
                             price: Decimal,
                             order: Order) -> Result<(), String> {
        match self.orderbooks.get_mut(&pair) {
            Some(orderbook) => {
                orderbook.add_limit_order(price, order);
                println!("Placed limit order at price level {}", price);
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
}
