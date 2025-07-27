use csv::Reader;
use serde;
use serde_with::{serde_as, Bytes};
use std::collections::HashMap;
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



#[derive(Debug, Copy, Clone, Default)]
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

        if record_csv.ticker.contains('.') || record_csv.ticker.ends_with('W') {
            continue;
        }

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
            records.insert(record_csv.ticker.clone(), vec![]);
        }

        if let Some(val) = records.get_mut(&record_csv.ticker) { val.push(record); };
    }
}


fn main() {

    let paths = fs::read_dir("G:\\OttoData\\").unwrap();

    let mut records = HashMap::new();

    let mut days = 0;
    for path in  paths {
        if days > 14 {
            break;
        }

        println!("{:?}", &path);
        load_data(&path.unwrap().path(), &mut records);


        days += 1;
    }

    println!("Loaded {:?} data", days);

    println!("Keys {:?}", records.keys().len());
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
