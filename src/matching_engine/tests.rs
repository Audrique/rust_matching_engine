use crate::matching_engine::orderbook::{BidOrAsk, Limit, Order, Orderbook};
use crate::matching_engine::engine::{TradingPair, MatchingEngine};

#[cfg(test)]
pub mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use super::*;

    #[test]
    fn test_returning_of_trades() {
        let mut engine = MatchingEngine::new();
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone(), 0.0, 0.0);
        let (_, trade_1) = engine.place_limit_order(pair.clone(),
                                               dec!(30_000.0),
                                               Order::new(BidOrAsk::Bid, 1.1, "trader_1".to_string(), "1".to_string())
        ).unwrap();
        let (_, trade_2) = engine.place_limit_order(pair.clone(),
                                               dec!(30_000.0),
                                               Order::new(BidOrAsk::Bid, 1.2, "trader_2".to_string(), "1".to_string())
        ).unwrap();
        let (_, trade_3) = engine.place_limit_order(pair.clone(),
                                               dec!(31_000.0),
                                               Order::new(BidOrAsk::Bid, 1.3, "trader_3".to_string(), "1".to_string())
        ).unwrap();
        let (_, trade_4) = engine.place_limit_order(pair.clone(),
                                               dec!(29_000.0),
                                               Order::new(BidOrAsk::Ask, 2.5, "trader_4".to_string(), "1".to_string())
        ).unwrap();
        assert_eq!(trade_1.is_empty(), true);
        assert_eq!(trade_2.is_empty(), true);
        assert_eq!(trade_3.is_empty(), true);
        assert_eq!(trade_4.len(), 3);
        let ob_bid_remaining_size = engine.orderbooks.get(&pair).unwrap()
            .bids.get(&dec!(31_000.0)).unwrap()
            .orders.get(0).unwrap().size;
        assert_eq!(ob_bid_remaining_size, 1.1);
    }
    #[test]
    fn test_trades() {
        let mut engine = MatchingEngine::new();
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone(),0.0, 0.0);
        let b1_order = Order::new(BidOrAsk::Bid, 5.5, "trader_1".to_string(), "1".to_string());
        let s1_order = Order::new(BidOrAsk::Ask, 8.5, "trader_2".to_string(), "-1".to_string());
        let _trade_1 = engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order).unwrap();
        let _trade_2 = engine.place_limit_order(pair.clone(), dec!(30_000.0), s1_order).unwrap();
        let bids_test = &engine.orderbooks.get(&pair).unwrap().bids;
        let remaining_volume: f64 = engine.orderbooks.get(&pair).unwrap().asks.get(&dec!(30_000.0)).unwrap().total_volume();
        assert_eq!(bids_test.is_empty(), true);
        assert_eq!(remaining_volume, 3.0);

        let b1_order = Order::new(BidOrAsk::Bid, 5.5, "trader_1".to_string(), "1".to_string());
        engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order).unwrap();
        let asks_test = &engine.orderbooks.get(&pair).unwrap().asks;
        let remaining_volume: f64 = engine.orderbooks.get(&pair).unwrap().bids.get(&dec!(30_000.0)).unwrap().total_volume();
        assert_eq!(asks_test.is_empty(), true);
        assert_eq!(remaining_volume, 2.5);

        let b1_order = Order::new(BidOrAsk::Bid, 4.0, "trader_1".to_string(), "1".to_string());
        engine.place_limit_order(pair.clone(), dec!(29_000.0), b1_order).unwrap();
        let s1_order = Order::new(BidOrAsk::Ask, 8.5, "trader_2".to_string(), "-1".to_string());
        engine.place_limit_order(pair.clone(), dec!(30_000.0), s1_order).unwrap();
        let remaining_volume_asks: f64 = engine.orderbooks.get(&pair).unwrap().asks.get(&dec!(30_000.0)).unwrap().total_volume();
        let remaining_volume_bids: f64 = engine.orderbooks.get(&pair).unwrap().bids.get(&dec!(29_000.0)).unwrap().total_volume();
        let price_is_in_or_not = engine.orderbooks.get(&pair).unwrap().bids.contains_key(&dec!(30_000.0));
        assert_eq!(price_is_in_or_not, false);
        assert_eq!(remaining_volume_asks, 6.0);
        assert_eq!(remaining_volume_bids, 4.0);



    }
    #[test]
    fn test_best_bid_ask() {
        let trader_id = String::from("trader_id_audrique");
        let mut engine = MatchingEngine::new();
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone(), 0.0, 0.0);

        let b1_order = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "-1".to_string());
        let b2_order = Order::new(BidOrAsk::Bid, 10.5, trader_id.clone(), "-1".to_string());
        let s1_order = Order::new(BidOrAsk::Ask, 4.5, trader_id.clone(), "-1".to_string());
        let s2_order = Order::new(BidOrAsk::Ask, 11.5, trader_id.clone(), "-1".to_string());

        engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order).unwrap();
        engine.place_limit_order(pair.clone(), dec!(31_000.0), b2_order).unwrap();
        engine.place_limit_order(pair.clone(), dec!(32_000.0), s1_order).unwrap();
        engine.place_limit_order(pair.clone(), dec!(33_000.0), s2_order).unwrap();
        let (best_ask, _) = engine.orderbooks.get(&pair).unwrap().asks.first_key_value().unwrap();
        let (best_bid, _) = engine.orderbooks.get(&pair).unwrap().bids.last_key_value().unwrap();
        assert_eq!(best_ask.clone(), dec!(32_000));
        assert_eq!(best_bid.clone(), dec!(31_000));
        engine.leave_volume_from_exchange(
            pair.clone(),
            BidOrAsk::Bid,
            dec!(31_000.0),
            0.0
        ).unwrap();
        engine.leave_volume_from_exchange(
            pair.clone(),
            BidOrAsk::Ask,
            dec!(32_000.0),
            0.0
        ).unwrap();
        let (best_ask, _) = engine.orderbooks.get(&pair).unwrap().asks.first_key_value().unwrap();
        let (best_bid, _) = engine.orderbooks.get(&pair).unwrap().bids.last_key_value().unwrap();
        assert_eq!(best_ask.clone(), dec!(33_000));
        assert_eq!(best_bid.clone(), dec!(30_000));
    }

    #[test]
    fn test_engine() {
        let trader_id = String::from("trader_id_audrique");
        let mut engine = MatchingEngine::new();

        let b1_order = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "0".to_string());
        let b1_order_to_cancel = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "1".to_string());
        let s1_order = Order::new(BidOrAsk::Ask, 9.5, trader_id.clone(), "2".to_string());
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone(), 0.0, 0.0);

        let b2_order = Order::new(BidOrAsk::Bid, 6.5, trader_id.clone(), "3".to_string());
        let s2_order = Order::new(BidOrAsk::Ask, 8.5, trader_id.clone(), "4".to_string());
        let eth_pair = TradingPair::new("ETH".to_string(), "USDT".to_string());
        engine.add_new_market(eth_pair.clone(), 0.0, 0.0);

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
    fn leave_websocket_volume_from_engine() {
        let trader_id = String::from("trader_id_audrique");
        let mut engine = MatchingEngine::new();

        let b1_order = Order::new(BidOrAsk::Bid, 5.5, trader_id.clone(), "0".to_string());
        let b1_order_deribit = Order::new(BidOrAsk::Bid, 5.5, "deribit".to_string(), "-1".to_string());
        let s1_order = Order::new(BidOrAsk::Ask, 9.5, trader_id.clone(), "2".to_string());
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone(), 0.0, 0.0);

        let b2_order = Order::new(BidOrAsk::Bid, 6.5, trader_id.clone(), "3".to_string());
        let s2_order = Order::new(BidOrAsk::Ask, 8.5, trader_id.clone(), "4".to_string());
        let eth_pair = TradingPair::new("ETH".to_string(), "USDT".to_string());
        engine.add_new_market(eth_pair.clone(), 0.0, 0.0);

        engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order).unwrap();
        engine.place_limit_order(pair.clone(), dec!(30_000.0), b1_order_deribit).unwrap();
        engine.place_limit_order(pair.clone(), dec!(31_000.0), s1_order).unwrap();
        engine.place_limit_order(eth_pair.clone(), dec!(10_000.0), b2_order).unwrap();
        engine.place_limit_order(eth_pair, dec!(11_000.0), s2_order).unwrap();

        engine.leave_volume_from_exchange(pair.clone(), BidOrAsk::Bid, dec!(30_000.0), 0.0).unwrap();

        let test = &engine.orderbooks // get the orderbooks in the engine
            .get(&pair).unwrap() // get the orderbook of the pair that we are interested in
            .bids // get the bids
            .get(&dec!(30_000.0)).unwrap() // get the price level
            .orders; // get the orders in that price level
        assert_eq!(test.len(), 1);
        assert_eq!(test.get(0).unwrap().size, 5.5);


    }
    #[test]
    fn cancel_order_that_is_not_there() {
        let trader_id = String::from("trader_id_audrique");
        let mut engine = MatchingEngine::new();
        let pair = TradingPair::new("BTC".to_string(), "USDT".to_string());
        engine.add_new_market(pair.clone(), 0.0, 0.0);
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
        let mut orderbook = Orderbook::new(0.0, 0.0);
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
        let mut orderbook = Orderbook::new(0.0, 0.0);
        orderbook.add_limit_order(dec!(500), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "0".to_string()));
        orderbook.add_limit_order(dec!(100), Order::new(BidOrAsk::Ask, 10.0, "trader2".to_string(), "1".to_string()));
        assert_eq!(orderbook.asks.len(), 2);
        orderbook.cancel_order(BidOrAsk::Ask,dec!(500), "0".to_string());
        assert_eq!(orderbook.asks.len(), 1);
    }
    #[test]
    fn remove_websocket_volume_from_orderbook() {
        let mut orderbook = Orderbook::new(0.0, 0.0);
        orderbook.add_limit_order(dec!(500), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "0".to_string()));
        orderbook.add_limit_order(dec!(100), Order::new(BidOrAsk::Ask, 10.0, "trader2".to_string(), "1".to_string()));
        orderbook.add_limit_order(dec!(200), Order::new(BidOrAsk::Ask, 10.0, "deribit".to_string(), "-1".to_string()));
        orderbook.add_limit_order(dec!(100), Order::new(BidOrAsk::Ask, 10.0, "deribit".to_string(), "-1".to_string()));
        orderbook.add_limit_order(dec!(300), Order::new(BidOrAsk::Ask, 10.0, "deribit".to_string(), "-1".to_string()));

        orderbook.leave_volume_from_exchange_orders(BidOrAsk::Ask, dec!(100), 20.0);
        let ask_limits = orderbook.ask_limits();
        let limit_at_100 =  ask_limits.get(0).unwrap();
        assert_eq!(limit_at_100.orders.len(), 2);
        assert_eq!(limit_at_100.total_volume(), 30.0);

    }
    #[test]
    fn orderbook_fill_market_order_ask() {
        let mut orderbook = Orderbook::new(0.0, 0.0);
        orderbook.add_limit_order(dec!(500), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "0".to_string()));
        orderbook.add_limit_order(dec!(100), Order::new(BidOrAsk::Ask, 10.0, "trader2".to_string(), "1".to_string()));
        orderbook.add_limit_order(dec!(200), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "2".to_string()));
        orderbook.add_limit_order(dec!(300), Order::new(BidOrAsk::Ask, 10.0, "trader1".to_string(), "3".to_string()));

        let mut market_order = Order::new(BidOrAsk::Bid, 10.0,"trader2".to_string(), "4".to_string());
        orderbook.fill_market_order(&mut market_order);

        let ask_limits = orderbook.ask_limits();
        // let matched_limit = ask_limits.get(1).unwrap();//.orders.get(0).unwrap();
        assert_eq!(market_order.is_filled(), true);
        assert_eq!(ask_limits.len(), 3);
    }

    #[test]
    fn limit_total_volume() {
        let mut limit = Limit::new(dec!(10_000.0));
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

        let trades = limit.fill_order(&mut market_sell_order, price.clone(), 0.0, 0.0);
        assert_eq!(trades[0].volume, 99.0);
        assert_eq!(trades[0].trader_id_maker, String::from("trader_audrique"));
        assert_eq!(market_sell_order.is_filled(), true);
        assert_eq!(limit.orders.get(0).unwrap().size, 1.0);
    }

    #[test]
    fn leave_websocket_volume_from_limit() {
        let price = dec!(10_000.0);
        let mut limit = Limit::new(price);
        let buy_limit_order_a = Order::new(BidOrAsk::Bid, 100.0, "trader1".to_string(), "0".to_string());
        let buy_limit_order_b = Order::new(BidOrAsk::Bid, 100.0, "deribit".to_string(), "-1".to_string());
        let buy_limit_order_c = Order::new(BidOrAsk::Bid, 100.0, "deribit".to_string(), "-1".to_string());

        limit.add_order(buy_limit_order_a);
        limit.add_order(buy_limit_order_b);
        limit.add_order(buy_limit_order_c);
        limit.leave_volume_from_exchange_orders(0.0);
        assert_eq!(limit.orders.len(), 1);
        let buy_limit_order_b = Order::new(BidOrAsk::Bid, 100.0, "deribit".to_string(), "-1".to_string());
        let buy_limit_order_c = Order::new(BidOrAsk::Bid, 100.0, "deribit".to_string(), "-1".to_string());
        limit.add_order(buy_limit_order_b);
        limit.add_order(buy_limit_order_c);
        limit.leave_volume_from_exchange_orders(150.0);
        assert_eq!(limit.orders.get(1).unwrap().size, 150.0);
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

        let trades = limit.fill_order(&mut market_sell_order, price.clone(), 0.0, 0.0);

        assert_eq!(market_sell_order.is_filled(), true);
        assert_eq!(limit.orders.get(0).unwrap().is_filled(), false);
        assert_eq!(limit.orders.len(), 1);
        assert_eq!(trades[0].price, price.clone());
    }
}