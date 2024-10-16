use std::collections::{HashMap, BTreeMap};
use std::cmp::Ordering;
use rust_decimal::prelude::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::Serialize;

pub fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64) // the casting from u128 to u64 will work until the year 586_912
        .unwrap_or_else(|_| {
            eprintln!("System time went backwards!");
            0
        })
}
#[derive(Debug, Clone, Serialize)]
pub enum BuyOrSell {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum BidOrAsk {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize)]
pub struct Trade {
    pub trader_id_taker: String,
    pub trader_id_maker: String,
    pub volume: f64,
    pub price: Decimal,
    pub timestamp: u64,
    pub taker_fee: f64,
    pub maker_fee: f64,
    pub direction: BuyOrSell
}
impl Trade {
    pub fn new(trader_id_taker: String,
               trader_id_maker: String,
               volume: f64, price: Decimal,
               timestamp: u64,
               taker_fee: f64,
               maker_fee: f64,
               direction: BuyOrSell,
    ) -> Trade {
        Trade {
            trader_id_taker,
            trader_id_maker,
            volume,
            price,
            timestamp,
            taker_fee,
            maker_fee,
            direction
        }
    }
}

#[derive(Debug, Clone)]
pub struct Orderbook {
    pub asks: BTreeMap<Decimal, Limit>,
    pub bids: BTreeMap<Decimal, Limit>,
    pub taker_fee: f64,
    pub maker_fee: f64,

}

impl Orderbook {
    pub fn new(taker_fee: f64, maker_fee: f64) -> Orderbook {
        Orderbook {
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
            taker_fee,
            maker_fee
        }
    }


    pub fn fill_market_order(&mut self, market_order: &mut Order) -> Vec<Trade> {
        let taker_fee = self.taker_fee.clone();
        let maker_fee = self.maker_fee.clone();

        let limits = match market_order.bid_or_ask {
            BidOrAsk::Bid => {self.ask_limits()},
            BidOrAsk::Ask => {self.bid_limits()}
        };

        let mut prices_to_remove: Vec<Decimal> = Vec::new();
        let mut happened_trades: Vec<Trade> = Vec::new();

        for limit in limits {
            let trades = limit.fill_order(market_order, limit.price.clone(), taker_fee, maker_fee);
            happened_trades.extend(trades);
            if limit.orders.len() == 0 {
            prices_to_remove.push(limit.price);
            }

            if market_order.is_filled() {
                break;
            }
        }

        match market_order.bid_or_ask {
            BidOrAsk::Bid => for price in prices_to_remove {self.asks.remove(&price);}
            BidOrAsk::Ask => for price in prices_to_remove {self.bids.remove(&price);}
            }
        happened_trades
    }

    pub fn fill_limit_order(&mut self, limit_order: &mut Order, price: Decimal) -> (f64, Vec<Trade>) {
        let taker_fee = self.taker_fee.clone();
        let maker_fee = self.maker_fee.clone();

        let limits: Vec<&mut Limit> = match limit_order.bid_or_ask {
            BidOrAsk::Bid => self.asks
                .range_mut(..=price)
                .map(|(_, limit)| limit)
                .collect(),
            BidOrAsk::Ask => self.bids
                .range_mut(price..)
                .map(|(_, limit)| limit)
                .collect(),
        };

        let mut prices_to_remove = Vec::new();
        let mut happened_trades: Vec<Trade> = Vec::new();
        for limit in limits {
            let trades_at_limit = limit.fill_order(limit_order, limit.price.clone(), taker_fee, maker_fee);
            happened_trades.extend(trades_at_limit);
            if limit.orders.len() == 0 {
                prices_to_remove.push(limit.price);
            }

            if limit_order.is_filled() {
                break;
            }
        }
        match limit_order.bid_or_ask {
            BidOrAsk::Bid => for price in prices_to_remove {self.asks.remove(&price);}
            BidOrAsk::Ask => for price in prices_to_remove {self.bids.remove(&price);}
        }
        (limit_order.size, happened_trades)
    }

    pub fn ask_limits(&mut self) -> Vec<&mut Limit> {
        self.asks.values_mut().collect()
    }

    pub fn bid_limits(&mut self) -> Vec<&mut Limit> {
        self.bids.values_mut().rev().collect()
    }

    pub fn add_limit_order(&mut self, price: Decimal, mut order: Order) -> Vec<Trade> {
        match order.bid_or_ask {
            BidOrAsk::Bid => {
                match self.bids.get_mut(&price) {
                    Some(limit) => {
                        limit.add_order(order);
                        Vec::new()
                    },
                    None => {
                        let (remaining_volume, trades) = self.fill_limit_order(&mut order.clone(), price);
                        if remaining_volume > 0.0 {
                            let mut limit = Limit::new(price);
                            order.size = remaining_volume;
                            limit.add_order(order);
                            self.bids.insert(price, limit);
                        }
                        trades
                    },
                }

            }
            BidOrAsk::Ask => match self.asks.get_mut(&price) {
                Some(limit) => {
                    limit.add_order(order);
                    Vec::new()
                },
                None => {
                    let (remaining_volume, trades) = self.fill_limit_order(&mut order.clone(), price);
                    if remaining_volume > 0.0 {
                        let mut limit = Limit::new(price);
                        order.size = remaining_volume;
                        limit.add_order(order);
                        self.asks.insert(price, limit);
                    }
                    trades
                },
            }
        }
    }
    pub fn cancel_order(&mut self, bid_or_ask: BidOrAsk, price: Decimal, order_id: String) {
        let mut prices_to_remove = Vec::new();
        match bid_or_ask {
            BidOrAsk::Bid => {
                if let Some(limit_to_cancel_order) = self.bids.get_mut(&price) {
                    limit_to_cancel_order.cancel_order(order_id);
                    if limit_to_cancel_order.orders.is_empty() {
                        prices_to_remove.push(limit_to_cancel_order.price);
                    }
                }
            }
            BidOrAsk::Ask => {
                if let Some(limit_to_cancel_order) = self.asks.get_mut(&price) {
                    limit_to_cancel_order.cancel_order(order_id);
                    if limit_to_cancel_order.orders.is_empty() {
                        prices_to_remove.push(limit_to_cancel_order.price);
                    }
                }
            }
        }
        match bid_or_ask {
            BidOrAsk::Bid => for price in prices_to_remove {self.bids.remove(&price);}
            BidOrAsk::Ask => for price in prices_to_remove {self.asks.remove(&price);}
        }
    }

    pub fn leave_volume_from_exchange_orders(&mut self,
                                              bid_or_ask: BidOrAsk,
                                              price: Decimal,
                                              leave_volume: f64) {
        let mut prices_to_remove = Vec::new();
        match bid_or_ask {
            BidOrAsk::Bid => {
                if let Some(limit_to_cancel_order) = self.bids.get_mut(&price) {
                    limit_to_cancel_order.leave_volume_from_exchange_orders(leave_volume);
                    if limit_to_cancel_order.orders.is_empty() {
                        prices_to_remove.push(limit_to_cancel_order.price);
                    }
                }
            }
            BidOrAsk::Ask => {
                if let Some(limit_to_cancel_order) = self.asks.get_mut(&price) {
                    limit_to_cancel_order.leave_volume_from_exchange_orders(leave_volume);
                    if limit_to_cancel_order.orders.is_empty() {
                        prices_to_remove.push(limit_to_cancel_order.price);
                    }
                }
            }
        }
        match bid_or_ask {
            BidOrAsk::Bid => for price in prices_to_remove {self.bids.remove(&price);}
            BidOrAsk::Ask => for price in prices_to_remove {self.asks.remove(&price);}
        }
    }

    pub fn open_orders_for_trader(&self, trader_id: String) -> HashMap<Decimal, Vec<Order>> {
        self.asks
            .iter()
            .chain(self.bids.iter()) // combines the iterators of the asks and bids
            .filter_map(|(&price, limit)| {
                let orders = limit.open_orders_for_trader(trader_id.clone());
                if orders.is_empty() {
                    None
                } else {
                    Some((price, orders))
                }
            })
            .collect()
    }
}



#[derive(Debug, Clone)]
pub struct Limit {
    price: Decimal, // Not f64 because it can lead to inconsistencies when hashing
    pub orders: Vec<Order>,
}

impl PartialEq for Limit {
    fn eq(&self, other: &Self) -> bool {
        self.price == other.price
    }
}

impl PartialOrd for Limit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.price.partial_cmp(&other.price)
    }
}

impl Limit {
    pub fn new(price: Decimal) -> Limit {
        Limit{
            price,
            orders: Vec::new(),
        }
    }

    pub fn total_volume(&self) -> f64 {
        self.orders
            .iter()
            .map(|order| order.size)
            .reduce(|a, b| a + b)
            .unwrap()
    }

    pub fn open_orders_for_trader(&self, trader_id: String) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|order| order.trader_id == trader_id)
            .cloned()
            .collect()
    }

    // At this moment self trades are avoided by just ignoring each trader's
    // other trades. Change this to not letting you place the order in the first place?
    pub fn fill_order(&mut self, market_order: &mut Order, price: Decimal, taker_fee: f64, maker_fee: f64) -> Vec<Trade> {
        let mut i = 0;
        let mut happened_trades: Vec<Trade> = Vec::new();
        let direction = match market_order.bid_or_ask {
            BidOrAsk::Bid => {BuyOrSell::Buy},
            BidOrAsk::Ask => {BuyOrSell::Sell}
        };

        while i < self.orders.len() {
            let limit_order = &mut self.orders[i];

            if (market_order.trader_id != limit_order.trader_id) || limit_order.trader_id.ends_with("exchange")  {
                match market_order.size >= limit_order.size {
                    true => {
                        market_order.size -= limit_order.size;
                        happened_trades.push(
                            Trade::new(
                                market_order.trader_id.clone(),
                                limit_order.trader_id.clone(),
                                limit_order.size.clone(),
                                price.clone(),
                                unix_timestamp_now(),
                                taker_fee,
                                maker_fee,
                                direction.clone()
                            )
                        );
                        limit_order.size = 0.0;
                        self.orders.remove(i);

                        continue;
                    }
                    false => {
                        limit_order.size -= market_order.size;
                        happened_trades.push(
                            Trade::new(
                                market_order.trader_id.clone(),
                                limit_order.trader_id.clone(),
                                market_order.size.clone(),
                                price.clone(),
                                unix_timestamp_now(),
                                taker_fee,
                                maker_fee,
                                direction.clone()
                            )
                        );
                        market_order.size = 0.0;
                    }
                }
            }

            if market_order.is_filled() {
                break;
            }
            i += 1;
        }
        happened_trades
    }

    pub fn add_order(&mut self, order: Order) {
        // Make sure only one 'exchange order' can exist per price level
        if order.order_id == "-1" {
            if let Some(existing_exchange_order) = self.orders.iter_mut().find(|o| o.order_id == "-1") {
                existing_exchange_order.size = order.size;
            } else {self.orders.push(order);}
        } else {self.orders.push(order);}
    }

    //needs to return if the cancellation was a success or not
    // to cancel an order you have to give the price and the order_id.
    // However, the price is used to get the correct limit (so only in
    // the part of the orderbook for canceling).
    pub fn cancel_order(&mut self, order_id: String) {
        if let Some(index) = self.orders.iter().position(|a| a.order_id == order_id){
            self.orders.remove(index);
        }
    }

    // This makes our engine conservative since once the order_id="-1" is first in a certain limit
    // it stays first, even after changes (it always keeps time priority).
    pub fn leave_volume_from_exchange_orders(&mut self, leave_volume: f64) {
        if let Some(idx) = self.orders.iter().position(|order| order.order_id == "-1") {
            match leave_volume == 0.0 {
                true => {self.orders.remove(idx);},
                false => {self.orders[idx].size = leave_volume;}
            }
        }
    }

    pub fn check_exchange_order_in_limit(&self) -> bool {
        self.orders.iter().any(|order| order.order_id == "-1")
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct Order {
    pub size: f64,
    bid_or_ask: BidOrAsk,
    trader_id: String,
    order_id: String
}

// Add here which type of order (limit, market order)
impl Order {
    pub fn new(bid_or_ask: BidOrAsk, size: f64, trader_id: String, order_id: String) -> Order {
        Order{size, bid_or_ask, trader_id, order_id}
    }

    pub fn is_filled(&self) -> bool {
        self.size == 0.0
    }
}