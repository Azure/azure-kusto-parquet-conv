use clap::{App, Arg};
use std::path::Path;
use std::error::Error;
use std::fs::File;
use parquet::file::reader::{SerializedFileReader, FileReader};
use parquet::record::{FieldType, Row, RowAccessor, ListAccessor, MapAccessor, List, Map};
use itertools::Itertools;
use serde_json::{Value, Number};

fn main() {
    let matches = App::new("pq2json")
        .version("0.1")
        .arg(Arg::with_name("output")
            .short("o")
            .long("output")
            .value_name("OUT_FILE")
            .help("Output file")
            .takes_value(true)
            .required(false))
        .arg(Arg::with_name("INPUT")
            .help("Input file to use")
            .required(true)
            .index(1))
        .arg(Arg::with_name("v")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity"))
        .get_matches();

    let input = matches.value_of("INPUT").expect("INPUT must be provided");
    let output = matches.value_of("OUT_FILE").unwrap_or("");

    match convert(input, output) {
        Ok(()) => (),
        Err(e) => {
            eprintln!("ERROR: {:?}", e);
            std::process::exit(-1);
        }
    }
}

fn convert(input: &str, output: &str) -> Result<(), Box<dyn Error>> {
    let file = File::open(&Path::new(input))?;
    let reader = SerializedFileReader::new(file)?;
    let mut iter = reader.get_row_iter(None)?;
    while let Some(record) = iter.next() {
        let value = row_to_value(&record)?;
        println!("{}", serde_json::to_string(&value)?);
    }
    Ok(())
}

macro_rules! element_to_value {
    ($ft:expr, $obj:ident, $i:ident) => {
        match $ft {
            FieldType::Null => Value::Null,
            FieldType::Bool => Value::Bool($obj.get_bool($i)?),
            FieldType::Byte => Value::Number($obj.get_byte($i)?.into()),
            FieldType::Short => Value::Number($obj.get_short($i)?.into()),
            FieldType::Int => Value::Number($obj.get_int($i)?.into()),
            FieldType::Long => Value::Number($obj.get_long($i)?.into()),
            FieldType::UByte => Value::Number($obj.get_ubyte($i)?.into()),
            FieldType::UShort => Value::Number($obj.get_ushort($i)?.into()),
            FieldType::UInt => Value::Number($obj.get_uint($i)?.into()),
            FieldType::ULong => Value::Number($obj.get_ulong($i)?.into()),
            FieldType::Float => float_to_value($obj.get_float($i)? as f64),
            FieldType::Double => float_to_value($obj.get_double($i)?),
            FieldType::Decimal => unimplemented!(),
            FieldType::Str => Value::String($obj.get_string($i)?.to_string()),
            FieldType::Bytes => unimplemented!(),
            FieldType::Date => Value::Number($obj.get_date($i)?.into()),
            FieldType::Timestamp => Value::Number($obj.get_timestamp($i)?.into()),
            FieldType::Group => row_to_value($obj.get_group($i)?)?,
            FieldType::List => list_to_value($obj.get_list($i)?)?,
            FieldType::Map => map_to_value($obj.get_map($i)?)?,
        }
    };
}

fn row_to_value(row: &Row) -> Result<Value, Box<dyn Error>> {
    let mut map = serde_json::Map::with_capacity(row.len());
    for i in 0..row.len() {
        let name = row.field_name(i);
        let field_type = row.field_type(i);
        let value = element_to_value!(field_type, row, i);
        if !value.is_null() {
            map.insert(name.to_string(), value);
        }
    }
    Ok(Value::Object(map))
}

fn list_to_value(list: &List) -> Result<Value, Box<dyn Error>> {
    let mut arr = Vec::<Value>::with_capacity(list.len());
    for i in 0..list.len() {
        let elt_ty = list.element_type(i);
        let value = element_to_value!(elt_ty, list, i);
        arr.push(value);
    }
    Ok(Value::Array(arr))
}

fn map_to_value(map: &Map) -> Result<Value, Box<dyn Error>> {
    let mut jsmap = serde_json::Map::with_capacity(map.len());
    let keys = map.get_keys();
    let values = map.get_values();
    for i in 0..map.len() {
        let key_ty = keys.element_type(i);
        let key = match key_ty {
            FieldType::Str => keys.get_string(i)?.to_string(),
            // TODO: return error here
            _ => panic!("Non-string key"),
        };

        let val_ty = values.element_type(i);
        let value = element_to_value!(val_ty, values, i);
        if !value.is_null() {
            jsmap.insert(key, value);
        }
    }
    Ok(Value::Object(jsmap))
}

fn float_to_value(f: f64) -> Value {
    Number::from_f64(f).map(|n| Value::Number(n)).unwrap_or_else(|| Value::Null)
}
