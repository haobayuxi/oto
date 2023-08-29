use common::{u32_rand, u64_rand};
use serde::{Deserialize, Serialize};

pub static MIN_STREET: u32 = 10;
pub static MAX_STREET: u32 = 20;
pub static MIN_CITY: u32 = 10;
pub static MAX_CITY: u32 = 20;
pub static STATE: u32 = 2;
pub static ZIP: u32 = 9;
pub static MIN_NAME: u32 = 6;
pub static MAX_NAME: u32 = 10;

///////
pub static NUM_CUSTOMER_PER_DISTRICT: u32 = 3000;
pub static NUM_DISTRICT_PER_WAREHOUSE: u32 = 10;
pub static NUM_ITEM: u32 = 1000;
pub static NUM_STOCK_PER_WAREHOUSE: u32 = 100000;

#[derive(Serialize, Deserialize, Clone)]
pub struct Warehouse {
    pub w_id: u32,
    pub w_name: String,
    pub w_street1: String,
    pub w_street2: String,
    pub w_city: String,
    pub w_state: String,
    pub w_zip: String,
    pub w_tax: f32,
    pub w_ytd: f32,
}

impl Warehouse {
    pub fn new(w_id: u32) -> Self {
        Self {
            w_id,
            w_name: u32_rand(MIN_NAME, MAX_NAME).to_string(),
            w_street1: u32_rand(MIN_STREET, MAX_STREET).to_string(),
            w_street2: u32_rand(MIN_STREET, MAX_STREET).to_string(),
            w_city: u32_rand(MIN_CITY, MAX_CITY).to_string(),
            w_state: u32_rand(0, STATE).to_string(),
            w_zip: "12345678".to_string(),
            w_tax: u32_rand(0, 2000) as f32 / 10000.0,
            w_ytd: 300000.0,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct District {
    pub d_id: u32,
    pub d_w_id: u32,
    pub d_name: String,
    pub d_strict1: String,
    pub d_strict2: String,
    pub d_city: String,
    pub d_state: String,
    pub d_zip: String,
    pub d_tax: f32,
    pub d_ytd: f32,
    pub d_next_o_id: u32,
}

impl District {
    pub fn new(d_id: u32, d_w_id: u32) -> Self {
        Self {
            d_id,
            d_w_id,
            d_name: u32_rand(MIN_NAME, MAX_NAME).to_string(),
            d_strict1: u32_rand(MIN_STREET, MAX_STREET).to_string(),
            d_strict2: u32_rand(MIN_STREET, MAX_STREET).to_string(),
            d_city: u32_rand(MIN_CITY, MAX_CITY).to_string(),
            d_state: u32_rand(0, STATE).to_string(),
            d_zip: "12345678".to_string(),
            d_tax: u32_rand(0, 2000) as f32 / 10000.0,
            d_ytd: 30000.0,
            d_next_o_id: NUM_CUSTOMER_PER_DISTRICT + 1,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Customer {
    pub c_id: u32,
    pub c_d_id: u32,
    pub c_w_id: u32,
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street1: String,
    pub c_street2: String,
    pub c_city: String,
    pub c_state: String,
    pub c_zip: String,
    pub c_phone: String,
    pub c_since: String,
    pub c_credit: String,
    pub c_credit_lim: i32,
    pub c_discount: f32,
    pub c_balance: i32,
    pub c_ytd_payment: i32,
    pub c_payment_cnt: u32,
    pub c_delivery_cnt: u32,
    pub c_data: String,
}

impl Customer {
    pub fn new(c_id: u32, d_id: u32, w_id: u32) -> Self {
        Self {
            c_id,
            c_d_id: d_id,
            c_w_id: w_id,
            c_first: "firstname".to_string(),
            c_middle: "OE".to_string(),
            c_last: (),
            c_street1: (),
            c_street2: (),
            c_city: (),
            c_state: (),
            c_zip: (),
            c_phone: (),
            c_since: (),
            c_credit: if u32_rand(0, 100) <= 10 {
                "BC".to_string()
            } else {
                "GC".to_string()
            },
            c_credit_lim: (),
            c_discount: u32_rand(1, 5000) as f32 / 10000.0,
            c_balance: (),
            c_ytd_payment: (),
            c_payment_cnt: (),
            c_delivery_cnt: (),
            c_data: (),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct History {
    pub h_c_id: u32,
    pub h_c_d_id: u32,
    pub h_c_w_id: u32,
    pub h_d_id: u32,
    pub h_w_id: u32,
    pub h_date: String,
    pub h_amount: i32,
    pub h_data: String,
}

impl History {
    pub fn new() -> Self {
        Self {
            h_c_id: (),
            h_c_d_id: (),
            h_c_w_id: (),
            h_d_id: (),
            h_w_id: (),
            h_date: (),
            h_amount: (),
            h_data: (),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NewOrder {
    pub no_o_id: u32,
    pub no_d_id: u32,
    pub no_w_id: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Order {
    pub o_id: u32,
    pub o_d_id: u32,
    pub o_w_id: u32,
    pub o_c_id: u32,
    pub o_entry_d: String,
    pub o_carried_id: u32,
    pub o_ol_cnt: u32,
    pub o_all_local: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Orderline {
    pub ol_o_id: u32,
    pub ol_d_id: u32,
    pub ol_w_id: u32,
    pub ol_number: u32,
    pub ol_i_id: u32,
    pub ol_supply_w_id: u32,
    pub ol_delivery_d: String,
    pub ol_quantity: u32,
    pub ol_amount: i32,
    pub ol_dist_info: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Item {
    pub i_id: u32,
    pub i_im_id: u32,
    pub i_name: String,
    pub i_price: u32,
    pub i_data: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Stock {
    pub s_i_id: u32,
    pub s_w_id: u32,
    pub s_quantity: i32,
    pub s_dist: Vec<String>,
    pub s_ytd: u32,
    pub s_order_cnt: u32,
    pub s_remote_cnt: u32,
    pub s_data: String,
}
