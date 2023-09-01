use common::{
    get_currenttime_millis, txn::DtxCoordinator, u64_rand, CUSTOMER_TABLE, DISTRICT_TABLE,
    HISTORY_TABLE, NEWORDER_TABLE, ORDERLINE_TABLE, ORDER_TABLE, STOCK_TABLE, WAREHOUSE_TABLE,
};

use crate::tpcc_db::{
    customer_index, history_index, neworder_index, order_index, orderline_index, Customer,
    District, NewOrder, Order, Orderline, MAX_CARRIER_ID, MAX_OL_CNT, MAX_STOCK_LEVEL_THRESHOLD,
    MIN_CARRIER_ID, MIN_STOCK_LEVEL_THRESHOLD, NUM_CUSTOMER_PER_DISTRICT,
    NUM_DISTRICT_PER_WAREHOUSE,
};

async fn tx_new_order(coordinator: &mut DtxCoordinator) -> bool {
    /*
    "NEW_ORDER": {
    "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
    "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
    "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
    "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
    "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
    "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
    "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
    "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
    "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
    },
    */
    coordinator.tx_begin(false).await;
    let warehouse_id = 0;
    // get warehouse tax

    // get district need update district next oid
    //random a district id

    // get customer

    // inc

    true
}

async fn tx_payment(coordinator: &mut DtxCoordinator) -> bool {
    /*
    "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
    "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
    "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
    "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID = ? AND D_ID = ?", # h_amount, d_w_id, d_id
    "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
    "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
    "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
    "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    */
    coordinator.tx_begin(false).await;

    let d_id = u64_rand(1, NUM_DISTRICT_PER_WAREHOUSE + 1);
    let c_id = u64_rand(1, NUM_CUSTOMER_PER_DISTRICT);

    let h_amount = u64_rand(100, 500000) as f64 / 100.0;

    let warehouse_updated = coordinator.add_write_to_execute(0, WAREHOUSE_TABLE, "".to_string());
    let district_updated = coordinator.add_write_to_execute(d_id, DISTRICT_TABLE, "".to_string());
    let customer_updated = coordinator.add_write_to_execute(
        customer_index(c_id, d_id),
        CUSTOMER_TABLE,
        "".to_string(),
    );
    let history_updated =
        coordinator.add_write_to_execute(history_index(c_id, d_id), HISTORY_TABLE, "".to_string());
    let (status, customer) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    // FIXME: Currently, we use a random order_id to maintain the distributed transaction payload,
    // but need to search the largest o_id by o_w_id, o_d_id and o_c_id from the order table
    let o_id = u64_rand(1, NUM_CUSTOMER_PER_DISTRICT);
    coordinator.add_read_to_execute(order_index(o_id, d_id), ORDER_TABLE);
    let (status, order) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let order_record: Order = match serde_json::from_str(order[0].value()) {
        Ok(s) => s,
        Err(_) => Order::default(),
    };
    for ol in 1..=order_record.o_ol_cnt {}

    coordinator.tx_commit().await
}

async fn tx_delivery(coordinator: &mut DtxCoordinator) -> bool {
    /*
    "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
    "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
    "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
    "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
    "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
    "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
    "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
    */
    coordinator.tx_begin(false).await;

    let o_carrier_id = u64_rand(MIN_CARRIER_ID, MAX_CARRIER_ID);
    let current_ts = get_currenttime_millis();
    for d_id in 1..=NUM_DISTRICT_PER_WAREHOUSE {
        // FIXME: select the lowest NO_O_ID with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID) in the NEW-ORDER table
        let min_o_id = NUM_CUSTOMER_PER_DISTRICT * 7 / 10 + 1;
        let max_o_id = NUM_CUSTOMER_PER_DISTRICT;
        let o_id = u64_rand(min_o_id, max_o_id);
        coordinator.add_read_to_execute(neworder_index(o_id, d_id), NEWORDER_TABLE);
        let (status, new_order) = coordinator.tx_exe().await;
        if !status {
            coordinator.tx_abort().await;
            return false;
        }
        if !new_order.is_empty() {
            // update order
            let order_updated = coordinator.add_write_to_execute(
                order_index(o_id, d_id),
                ORDER_TABLE,
                "".to_string(),
            );
            let (status, order) = coordinator.tx_exe().await;
            if !status {
                coordinator.tx_abort().await;
                return false;
            }
            let mut order_record: Order = match serde_json::from_str(order[0].value()) {
                Ok(s) => s,
                Err(_) => Order::default(),
            };
            order_record.o_carried_id = o_carrier_id;
            order_updated.write().await.value = Some(serde_json::to_string(&order_record).unwrap());
            // delete new order
            coordinator.add_to_delete(new_order[0].clone());
            // get order line
            let mut sum_ol_amount = 0.0;
            for ol in 1..=MAX_OL_CNT {
                coordinator.add_read_to_execute(orderline_index(o_id, d_id, ol), ORDERLINE_TABLE);
            }
            let (status, orderlines) = coordinator.tx_exe().await;
            if !status {
                coordinator.tx_abort().await;
                return false;
            }
            for iter in orderlines.iter() {
                let ol_record: Orderline = match serde_json::from_str(iter.value()) {
                    Ok(s) => s,
                    Err(_) => Orderline::default(),
                };
                sum_ol_amount += ol_record.ol_amount;
            }
            // update customer
            let customer_updated = coordinator.add_write_to_execute(
                customer_index(o_id, d_id),
                CUSTOMER_TABLE,
                "".to_string(),
            );
            let (status, customer) = coordinator.tx_exe().await;
            if !status {
                coordinator.tx_abort().await;
                return false;
            }
            let mut customer_record: Customer = match serde_json::from_str(customer[0].value()) {
                Ok(s) => s,
                Err(_) => Customer::default(),
            };
            customer_record.c_balance += sum_ol_amount;
            customer_record.c_delivery_cnt += 1;
            customer_updated.write().await.value =
                Some(serde_json::to_string(&customer_record).unwrap());
        }
    }

    coordinator.tx_commit().await
}

async fn tx_order_status(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(true).await;

    /*
    "ORDER_STATUS": {
    "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
    "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
    "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
    "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
    },
    */
    let d_id = u64_rand(1, NUM_DISTRICT_PER_WAREHOUSE + 1);
    let c_id = u64_rand(1, NUM_CUSTOMER_PER_DISTRICT);
    coordinator.add_read_to_execute(customer_index(c_id, d_id), CUSTOMER_TABLE);
    let (status, results) = coordinator.tx_exe().await;
    if results.len() == 0 {
        return true;
    }
    let customer_record: Customer = match serde_json::from_str(results[0].value()) {
        Ok(s) => s,
        Err(_) => Customer::default(),
    };
    // FIXME: Currently, we use a random order_id to maintain the distributed transaction payload,
    // but need to search the largest o_id by o_w_id, o_d_id and o_c_id from the order table
    let order_id = u64_rand(1, NUM_CUSTOMER_PER_DISTRICT);
    coordinator.add_read_to_execute(order_index(order_id, d_id), ORDER_TABLE);
    let (status, results) = coordinator.tx_exe().await;
    if results.len() == 0 {
        return true;
    }
    let order_record: Order = match serde_json::from_str(results[0].value()) {
        Ok(s) => s,
        Err(_) => Order::default(),
    };
    for ol in 1..=order_record.o_ol_cnt {
        coordinator.add_read_to_execute(orderline_index(order_id, d_id, ol), ORDERLINE_TABLE);
    }
    let (status, results) = coordinator.tx_exe().await;
    coordinator.tx_commit().await
}

async fn tx_stock_level(coordinator: &mut DtxCoordinator) -> bool {
    /*
    "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
    "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
    */
    coordinator.tx_begin(true).await;

    let threshold = u64_rand(MIN_STOCK_LEVEL_THRESHOLD, MAX_STOCK_LEVEL_THRESHOLD);
    let d_id = u64_rand(1, NUM_DISTRICT_PER_WAREHOUSE + 1);

    coordinator.add_read_to_execute(d_id, DISTRICT_TABLE);
    let (status, results) = coordinator.tx_exe().await;
    if results.len() == 0 {
        return true;
    }
    let district: District = match serde_json::from_str(results[0].value()) {
        Ok(s) => s,
        Err(_) => District::default(),
    };

    //"SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)"
    let o_id = district.d_next_o_id;
    for order_id in (o_id - 20)..o_id {
        for line_number in 1..MAX_OL_CNT {
            let ol_index = orderline_index(order_id, d_id, line_number);
            coordinator.add_read_to_execute(ol_index, ORDERLINE_TABLE);
        }
    }
    let (status, results) = coordinator.tx_exe().await;
    if results.len() == 0 {
        return true;
    }
    //"SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?"
    for iter in results.iter() {
        let orderline_record: Orderline = match serde_json::from_str(iter.value()) {
            Ok(s) => s,
            Err(_) => Orderline::default(),
        };
        let item_id = orderline_record.ol_i_id;
        coordinator.add_read_to_execute(item_id, STOCK_TABLE);
    }
    let (status, results) = coordinator.tx_exe().await;
    if results.len() == 0 {
        return true;
    }
    coordinator.tx_commit().await
}
