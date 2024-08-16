use std::collections::HashMap;
use rust_decimal::prelude::*;

#[derive(Debug, Clone)]
pub enum BidOrAsk {
    Bid,
    Ask,
}

#[derive(Debug)]
pub struct Orderbook {
    asks: HashMap<Decimal, Limit>,
    bids: HashMap<Decimal, Limit>,
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
            BidOrAsk::Ask => self.bid_limits()
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
    // TODO: return some stuff if successful or not
    pub fn cancel_order(&mut self, bid_or_ask: BidOrAsk, price: Decimal, order_id: String) {
        let mut prices_to_remove = Vec::new();
        match bid_or_ask {
            BidOrAsk::Bid => {
                let limit_to_cancel_order = self.bids.get_mut(&price).unwrap();
                limit_to_cancel_order.cancel_order(order_id);
                if limit_to_cancel_order.orders.len() == 0 {
                    prices_to_remove.push(limit_to_cancel_order.price);
                }
            }
            BidOrAsk::Ask => {
                let limit_to_cancel_order = self.asks.get_mut(&price).unwrap();
                limit_to_cancel_order.cancel_order(order_id);
                if limit_to_cancel_order.orders.len() == 0 {
                    prices_to_remove.push(limit_to_cancel_order.price);
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
    orders: Vec<Order>,
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
    fn total_volume(&self) -> f64 {
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
    fn fill_order(&mut self, market_order: &mut Order) {
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

    fn add_order(&mut self, order: Order) {
        self.orders.push(order);
    }

    //TODO: needs to return if the cancellation was a success or not
    // to cancel an order you have to give the price and the order_id.
    // However, the price is used to get the correct limit (so only in
    // the part of the orderbook for canceling.
    fn cancel_order(&mut self, order_id: String) {
        let index = self.orders.iter().position(|a| a.order_id == order_id).unwrap();
        self.orders.remove(index);

    }
}
#[derive(Debug, Clone)]
pub struct Order {
    size: f64,
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

#[cfg(test)]
pub mod tests {
    use rust_decimal_macros::dec;
    use super::*;


    #[test]
    fn check_open_orders() {
        let mut orderbook = Orderbook::new();

        let price = dec!(10_000.0);
        let trader_id = "trader_1".to_string();

        let buy_limit_order_a = Order::new(BidOrAsk::Bid, 100.0, trader_id.clone(), "0".to_string());
        let buy_limit_order_b = Order::new(BidOrAsk::Bid, 100.0, trader_id.clone(), "1".to_string());
        let sell_limit_order = Order::new(BidOrAsk::Ask, 100.0, "trader_2".to_string(), "2".to_string());
        orderbook.add_limit_order(dec!(10_000.0), buy_limit_order_a.clone());
        orderbook.add_limit_order(dec!(9_000.0), buy_limit_order_a);
        orderbook.add_limit_order(dec!(10_000.0), buy_limit_order_b);
        orderbook.add_limit_order(dec!(12_000.0), sell_limit_order);
        let open_trades_trader1 = orderbook.open_orders_for_trader(trader_id);
        let open_trades_trader2 = orderbook.open_orders_for_trader("trader_2".to_string());

        assert_eq!(open_trades_trader1.len(), 2);
        assert_eq!(open_trades_trader1.get(&dec!(10_000.0)).unwrap().len(), 2);
        assert_eq!(open_trades_trader1.get(&dec!(9_000.0)).unwrap().len(), 1);

        assert_eq!(open_trades_trader2.len(), 1);
        assert_eq!(open_trades_trader2.get(&dec!(12_000.0)).unwrap().len(), 1);
    }

    #[test]
    fn cancel_order_from_limit() {
        let price = dec!(10_000.0);
        let mut limit = Limit::new(price);
        let trader_id = "trader_audrique".to_string();

        let buy_limit_order_a = Order::new(BidOrAsk::Bid, 100.0, trader_id.clone(), "0".to_string());
        let buy_limit_order_b = Order::new(BidOrAsk::Bid, 100.0, trader_id, "1".to_string());
        limit.add_order(buy_limit_order_a);
        limit.add_order(buy_limit_order_b);
        limit.cancel_order("0".to_string());
        assert_eq!(limit.orders.len(), 1);
    }

    #[test]
    fn cancel_order_from_orderbook() {
        let mut orderbook = Orderbook::new();
        orderbook.add_limit_order(dec!(500), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "0".to_string()));
        orderbook.add_limit_order(dec!(100), Order::new(BidOrAsk::Ask, 10.0, "trader2".to_string(), "1".to_string()));
        assert_eq!(orderbook.asks.len(), 2);
        orderbook.cancel_order(BidOrAsk::Ask,dec!(500), "0".to_string());
        println!("{:?}", orderbook);
        assert_eq!(orderbook.asks.len(), 1);
    }

    #[test]
    fn orderbook_fill_market_order_ask() {
        let mut orderbook = Orderbook::new();
        orderbook.add_limit_order(dec!(500), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "0".to_string()));
        orderbook.add_limit_order(dec!(100), Order::new(BidOrAsk::Ask, 10.0, "trader2".to_string(), "1".to_string()));
        orderbook.add_limit_order(dec!(200), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "2".to_string()));
        orderbook.add_limit_order(dec!(300), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "3".to_string()));

        let mut market_order = Order::new(BidOrAsk::Bid, 10.0,"trader2".to_string(), "4".to_string());
        orderbook.fill_market_order(&mut market_order);

        let ask_limits = orderbook.ask_limits();
        let matched_limit = ask_limits.get(1).unwrap();//.orders.get(0).unwrap();
        assert_eq!(market_order.is_filled(), true);
        assert_eq!(ask_limits.len(), 3);
        println!("{:?}", orderbook.ask_limits());

    }

    #[test]
    fn limit_total_volume() {
        let price = dec!(10_000.0);
        let mut limit = Limit::new(price);
        let trader_id = "trader_audrique".to_string();

        let buy_limit_order_a = Order::new(BidOrAsk::Bid, 100.0, trader_id.clone(), "0".to_string());
        let buy_limit_order_b = Order::new(BidOrAsk::Bid, 100.0, trader_id, "1".to_string());
        limit.add_order(buy_limit_order_a);
        limit.add_order(buy_limit_order_b);
        assert_eq!(limit.total_volume(), 200.0)
    }

    #[test]
    fn limit_order_single_fill() {
        let price = dec!(10_000.0);
        let mut limit = Limit::new(price);
        let buy_limit_order = Order::new(BidOrAsk::Bid, 100.0, "trader_audrique".to_string(), "0".to_string());

        limit.add_order(buy_limit_order);

        let mut market_sell_order = Order::new(BidOrAsk::Ask, 99.0, "trader_2".to_string(), "1".to_string());

        limit.fill_order(&mut market_sell_order);

        assert_eq!(market_sell_order.is_filled(), true);
        assert_eq!(limit.orders.get(0).unwrap().size, 1.0);
    }

    #[test]
    fn limit_order_multi_fill() {
        let price = dec!(10_000.0);
        let mut limit = Limit::new(price);
        let buy_limit_order_a = Order::new(BidOrAsk::Bid, 100.0, "trader1".to_string(), "0".to_string());
        let buy_limit_order_b = Order::new(BidOrAsk::Bid, 100.0, "trader2".to_string(), "1".to_string());

        limit.add_order(buy_limit_order_a);
        limit.add_order(buy_limit_order_b);

        let mut market_sell_order = Order::new(BidOrAsk::Ask, 100.0, "trader1".to_string(), "2".to_string());

        limit.fill_order(&mut market_sell_order);

        assert_eq!(market_sell_order.is_filled(), true);
        assert_eq!(limit.orders.get(0).unwrap().is_filled(), false);
        assert_eq!(limit.orders.len(), 1);

        println!("{:?}", limit);
    }
}