use std::collections::HashMap;
use rust_decimal::prelude::*;

#[derive(Debug, Clone)]
pub enum BidOrAsk {
    Bid,
    Ask,
}

#[derive(Debug)]
pub struct Orderbook {
    pub asks: HashMap<Decimal, Limit>,
    pub bids: HashMap<Decimal, Limit>,
}

impl Orderbook {
    pub fn new() -> Orderbook {
        Orderbook {
            asks: HashMap::new(),
            bids: HashMap::new(),
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
        let mut limits = self.asks.values_mut().collect::<Vec<&mut Limit>>();
        limits.sort_by(|a, b| a.price.cmp(&b.price));
        limits
    }

    pub fn bid_limits(&mut self) -> Vec<&mut Limit> {
        let mut limits = self.bids.values_mut().collect::<Vec<&mut Limit>>();
        limits.sort_by(|a, b| b.price.cmp(&a.price));
        limits
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
                        self.bids.insert(price, limit); // here the price is copied since we also used it to make the limit
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
                    self.asks.insert(price, limit); // here the price is copied since we also used it to make the limit
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

    pub fn remove_volume_from_exchange_orders(&mut self, bid_or_ask: BidOrAsk, price: Decimal, volume: f64) {
        let mut prices_to_remove = Vec::new();
        match bid_or_ask {
            BidOrAsk::Bid => {
                if let Some(limit_to_cancel_order) = self.bids.get_mut(&price) {
                    limit_to_cancel_order.remove_volume_from_exchange_orders(volume);
                    if limit_to_cancel_order.orders.is_empty() {
                        prices_to_remove.push(limit_to_cancel_order.price);
                    }
                }
            }
            BidOrAsk::Ask => {
                if let Some(limit_to_cancel_order) = self.asks.get_mut(&price) {
                    limit_to_cancel_order.remove_volume_from_exchange_orders(volume);
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



#[derive(Debug)]
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
    // The orders from the websocket are ALLOWED to self trade. Make sure this is implemented in a way.
    // order_id = -1
    pub fn fill_order(&mut self, market_order: &mut Order) {
        let mut i = 0; // Index to keep track of the position in the orders vector

        while i < self.orders.len() { // a while loop instead of a for loop because we don't always increment i (see the 'true' case in the match)
            let limit_order = &mut self.orders[i];

            if market_order.trader_id != limit_order.trader_id { // Prevents self trades
                match market_order.size >= limit_order.size {
                    true => {
                        market_order.size -= limit_order.size;
                        limit_order.size = 0.0;
                        self.orders.remove(i); // Remove the order at index `i`

                        // No need to increment `i` since we just removed the current element
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

            i += 1; // Increment index
        }
    }

    pub fn add_order(&mut self, order: Order) {
        self.orders.push(order);
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

    pub fn remove_volume_from_exchange_orders(&mut self, mut volume: f64) {
    //     In here loop through all orders with order_id = '-1'
    //     Then keep removing the volume and if the volume of an order is zero just remove it
        // (and check if the limit needs to be removed) but this has to be done in the orderbook
        let mut i = 0;
        while (i < self.orders.len()) && (volume > 0.0) {
            let limit_order = &mut self.orders[i];
            if limit_order.order_id == "-1" {
                match volume >= limit_order.size {
                    true => {
                        volume -= limit_order.size;
                        limit_order.size = 0.0;
                        self.orders.remove(i); // Remove the order at index `i`
                        // No need to increment `i` since we just removed the current element
                        continue;
                    },
                    false => {
                        limit_order.size -= volume;
                        volume = 0.0;
                    }
                }
                i += 1;
            } else {
                i += 1;
                continue;
            }
        }
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