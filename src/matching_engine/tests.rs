use crate::matching_engine::orderbook::{BidOrAsk, Limit, Order, Orderbook};
//test
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