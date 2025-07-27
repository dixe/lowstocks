use csv::Reader;
use serde;
use serde_with::{serde_as, Bytes};
use std::collections::HashMap;
use rkyv::{deserialize, rancor::Error, Archive, Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;


#[serde_as]
#[derive(Debug, Clone, serde::Deserialize, Default)]
struct RecordCsv {
    ticker: String,
    volume: i64,
    open: f32,
    close: f32,
    high: f32,
    low: f32,
    window_start: u64,
    transactions: i32
}



#[derive(Debug, Clone, Default, Archive, Deserialize, Serialize, PartialEq)]
// record without ticker, store ticker as hashmap key
struct Record {
    volume: i64,
    open: f32,
    close: f32,
    high: f32,
    low: f32,
    window_start: u64,
    transactions: i32
}

// straight into hashmap
fn load_data(path: &PathBuf, records: &mut HashMap<String, Vec::<Record>>) {

    // Build the CSV reader and iterate over each record.
    let mut rdr = Reader::from_path(path).unwrap();

    let mut i = 0;
    for result in rdr.deserialize() {
        let record_csv: RecordCsv = result.unwrap();

        let record = Record {
            volume : record_csv.volume,
            open : record_csv.open,
            close : record_csv.close,
            high : record_csv.high,
            low : record_csv.low,
            window_start : record_csv.window_start,
            transactions : record_csv.transactions
        };

        if !records.contains_key(&record_csv.ticker) {
            records.insert(&record_csv.ticker, vec![]);
        }

        if let Some(val) = records.get_mut(&record_csv.ticker) { val.push(record); };
    }
}


fn get_or_insert_ticker_id(connection: &Connection, ticker_name: &str) -> u64 {
    1
}

fn insert_record(connection: &Connection, ticker_id: u64, record: Record) {

}

fn main() {

    let paths = fs::read_dir("G:\\OttoData\\").unwrap();

    let connection = sqlite::open("G:\\OttoData\\records.db").unwrap();

    for path in paths {

        let mut records = HashMap::new();

        load_data(&path.unwrap().path(), &mut records);

        for k in records.keys() {
            let ticker_id = get_or_insert_ticker_id(k);

            for r in records[k] {
                insert_record(ticker_id, &r);
            }
        }
        panic!();
    }
}


// ticker u64 encoding and decoding functions
fn vec_str_to_u64(v : &Vec::<u8>) -> u64 {

    let mut ticker : u64 = 0;
    for b in v {
        ticker = ticker << 8;
        ticker = ticker + *b as u64;
    }

    ticker
}


fn copy_ticker_to_buffer(ticker : u64, buffer: &mut [char;5]) {
    buffer[4] = ((ticker & 0xFF) as u8) as char;
    buffer[3] = (((ticker >> 8) & 0xFF) as u8) as char;
    buffer[2] = ((ticker >> 16 & 0xFF) as u8) as char;
    buffer[1] = ((ticker >> 24 & 0xFF) as u8) as char;
    buffer[0] = ((ticker >> 32 & 0xFF) as u8) as char;
}





// load into vec, then into hashmap when ticker changes
fn load_data2(path: &PathBuf) -> HashMap<String, Vec::<Record>> {

    // Build the CSV reader and iterate over each record.
    let mut rdr = Reader::from_path(path).unwrap();
    let mut records = HashMap::new();
    let mut cur_vec = vec![];
    let mut cur_ticker = 0;
    let mut i = 0;
    for result in rdr.deserialize() {
        let record_csv: RecordCsv = result.unwrap();


        let record = Record {
            volume : record_csv.volume,
            open : record_csv.open,
            close : record_csv.close,
            high : record_csv.high,
            low : record_csv.low,
            window_start : record_csv.window_start,
            transactions : record_csv.transactions
        };


        if cur_ticker == 0 {
            cur_ticker = ticker;
        }

        // add cur data to hashmap and reset tmp vec
        if cur_ticker != ticker {
            records.insert(cur_ticker, cur_vec);
            cur_ticker = ticker;
            cur_vec = vec![];
        }

        cur_vec.push(record);
    }


    // add last cur ticker
    records.insert(cur_ticker, cur_vec);


    return records;
}
