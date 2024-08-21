use crate::matching_engine::orderbook::{BidOrAsk, Limit, Order, Orderbook};
use crate::matching_engine::engine::{TradingPair, MatchingEngine};

#[cfg(test)]
pub mod tests {
    use rust_decimal_macros::dec;
    use super::*;

    #[test]
    fn test_engine() {
        let trader_id = String::from("trader_id_audrique");
        let mut engine = MatchingEngine::new();

        let b1_order = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "0".to_string());
        let b1_order_to_cancel = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "1".to_string());
        let s1_order = Order::new(BidOrAsk::Ask, 9.5, trader_id.clone(), "2".to_string());
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone());

        let b2_order = Order::new(BidOrAsk::Bid, 6.5, trader_id.clone(), "3".to_string());
        let s2_order = Order::new(BidOrAsk::Ask, 8.5, trader_id.clone(), "4".to_string());
        let eth_pair = TradingPair::new("ETH".to_string(), "USDT".to_string());
        engine.add_new_market(eth_pair.clone());

        engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order).unwrap();
        engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order_to_cancel).unwrap();
        engine.place_limit_order(pair.clone(), dec!(31_000.0), s1_order).unwrap();
        engine.place_limit_order(eth_pair.clone(), dec!(10_000.0), b2_order).unwrap();
        engine.place_limit_order(eth_pair, dec!(11_000.0), s2_order).unwrap();

        let open_orders = engine.open_orders(trader_id.clone());
        assert_eq!(open_orders.len(), 2);
        let test = engine.orderbooks // get the orderbooks in the engine
            .get(&pair).unwrap() // get the orderbook of the pair that we are interested in
            .bids // get the bids
            .get(&dec!(30_000.0)).unwrap() // get the price level
            .orders.len(); // get the size of the orders in that price level
        assert_eq!(test, 2);
        let test2 = open_orders.get(&pair).unwrap()
            .get(&dec!(30_000.0)).unwrap().len();
        assert_eq!(test2, 2);
        // Now we cancel an order and see if everything remains correct
        engine.cancel_order(pair.clone(), BidOrAsk::Bid, dec!(30_000.0), "1".to_string()).unwrap();
        let test3 = engine.orderbooks // get the orderbooks in the engine
            .get(&pair).unwrap() // get the orderbook of the pair that we are interested in
            .bids // get the bids
            .get(&dec!(30_000.0)).unwrap() // get the price level
            .orders.len(); // get the size of the orders in that price level
        assert_eq!(test3, 1);
        let open_orders = engine.open_orders(trader_id);
        let test4 = open_orders.get(&pair).unwrap()
            .get(&dec!(30_000.0)).unwrap().len();
        assert_eq!(test4, 1);
    }
    #[test]
    fn cancel_order_that_is_not_there() {
        let trader_id = String::from("trader_id_audrique");
        let mut engine = MatchingEngine::new();
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone());
        let b1_order = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "0".to_string());
        engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order).unwrap();

        // Try canceling various orders that do not exist in the market
        // Three options: the pair does not exist, the price does not exist at the Bid (or the Ask) or the order_id does not exist
        // For the case of the wrong pair, we explicitly implemented an Err and thus the test would fail,
        // however, for the other cases we want to just ignore it and keep going.
        engine.cancel_order(pair.clone(), BidOrAsk::Bid, dec!(30_000.0), "1".to_string()).unwrap();
        engine.cancel_order(pair.clone(), BidOrAsk::Bid, dec!(31_000.0), "0".to_string()).unwrap();
        engine.cancel_order(pair.clone(), BidOrAsk::Ask, dec!(30_000.0), "0".to_string()).unwrap();
    }
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