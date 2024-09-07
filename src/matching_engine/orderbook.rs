use std::collections::{HashMap, BTreeMap};
use rust_decimal::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BidOrAsk {
    Bid,
    Ask,
}

#[derive(Debug, Clone)]
pub struct Orderbook {
    pub asks: BTreeMap<Decimal, Limit>,
    pub bids: BTreeMap<Decimal, Limit>,
}

impl Orderbook {
    pub fn new() -> Orderbook {
        Orderbook {
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
        }
    }


    pub fn fill_market_order(&mut self, market_order: &mut Order) { // Has to return the matches from the filling

        let limits = match market_order.bid_or_ask {
            BidOrAsk::Bid => {self.ask_limits()},
            BidOrAsk::Ask => {self.bid_limits()}
        };

        let mut prices_to_remove = Vec::new();

        for limit in limits {
            limit.fill_order(market_order);
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
    }

    pub fn ask_limits(&mut self) -> Vec<&mut Limit> {
        self.asks.values_mut().collect()
    }

    pub fn bid_limits(&mut self) -> Vec<&mut Limit> {
        self.bids.values_mut().rev().collect()
    }

    pub fn add_limit_order(&mut self, price: Decimal, order: Order) {
        match order.bid_or_ask {
            BidOrAsk::Bid => {
                match self.bids.get_mut(&price) {
                    Some(limit) => {
                        limit.add_order(order);
                    },
                    None => {
                        let mut limit = Limit::new(price);
                        limit.add_order(order);
                        self.bids.insert(price, limit);
                    },
                }

            }
            BidOrAsk::Ask => match self.asks.get_mut(&price) {
                Some(limit) => {
                    limit.add_order(order);
                },
                None => {
                    let mut limit = Limit::new(price);
                    limit.add_order(order);
                    self.asks.insert(price, limit);
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

impl Limit {
    pub fn new(price: Decimal) -> Limit {
        Limit{
            price,
            orders: Vec::new(),
        }
    }

    // this maybe change to a self.total_volume and add the volume when a new order comes
    // and subtract when volume is filled (this way it is more efficient)
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

    //TODO: At this moment self trades are avoided by just ignoring each trader's
    // other trades. This has to be change by not letting you place the order in the first place.
    pub fn fill_order(&mut self, market_order: &mut Order) {
        let mut i = 0;

        while i < self.orders.len() {
            let limit_order = &mut self.orders[i];

            if market_order.trader_id != limit_order.trader_id {
                match market_order.size >= limit_order.size {
                    true => {
                        market_order.size -= limit_order.size;
                        limit_order.size = 0.0;
                        self.orders.remove(i);
                        continue;
                    }
                    false => {
                        limit_order.size -= market_order.size;
                        market_order.size = 0.0;
                    }
                }
            }

            if market_order.is_filled() {
                break;
            }
            i += 1;
        }
    }

    pub fn add_order(&mut self, order: Order) {
        // Make sure only one 'exchange order' can exist per price level
        if order.order_id == "-1" {
            if let Some(existing_exchange_order) = self.orders.iter_mut().find(|o| o.order_id == "-1") {
                existing_exchange_order.size = order.size;
            } else {self.orders.push(order);}
        } else {self.orders.push(order);}
    }

    //TODO: needs to return if the cancellation was a success or not
    // to cancel an order you have to give the price and the order_id.
    // However, the price is used to get the correct limit (so only in
    // the part of the orderbook for canceling.
    pub fn cancel_order(&mut self, order_id: String) {
        if let Some(index) = self.orders.iter().position(|a| a.order_id == order_id){
            self.orders.remove(index);
        }
    }

    pub fn leave_volume_from_exchange_orders(&mut self, leave_volume: f64) {
        // Just put the .size of the order_id="-1" to the correct leaves_volume
        // but for this make sure we can only have one order per price level with order_id -1
        // put this extra checker in the 'place_limit_order' function of the orderbook/limit
        // self.orders.remove(i)
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
#[derive(Debug, Clone)]
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