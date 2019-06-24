use clap::{App, Arg};
use num_bigint::{BigInt, Sign};
use parquet::data_type::Decimal;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{FieldType, List, ListAccessor, Map, MapAccessor, Row, RowAccessor};
use serde_json::{Number, Value};
use std::error::Error;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

fn main() {
    let matches = App::new("pq2json")
        .version("0.1")
        .arg(
            Arg::with_name("omit-nulls")
                .long("omit-nulls")
                .help("Omit bag entries with null values")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("omit-empty-bags")
                .long("omit-empty-bags")
                .help("Omit empty property bags")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("omit-empty-lists")
                .long("omit-empty-lists")
                .help("Omit empty lists")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("prune")
                .short("p")
                .long("prune")
                .help(
                    "Omit nulls, empty bags and empty lists \
                     (equivalent to a combination of the three --omit-... options)",
                )
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .value_name("OUT_FILE")
                .help("Output file")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("INPUT")
                .help("Input file to use")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    let input = matches.value_of("INPUT").expect("INPUT must be provided");
    let output = matches.value_of("OUT_FILE").unwrap_or("");
    let settings = Settings {
        omit_nulls: matches.is_present("omit-nulls") || matches.is_present("prune"),
        omit_empty_bags: matches.is_present("omit-empty-bags") || matches.is_present("prune"),
        omit_empty_lists: matches.is_present("omit-empty-lists") || matches.is_present("prune"),
    };

    match convert(&settings, input, output) {
        Ok(()) => (),
        Err(e) => {
            eprintln!("ERROR: {:?}", e);
            std::process::exit(-1);
        }
    }
}

#[derive(Debug, Clone)]
struct Settings {
    omit_nulls: bool,
    omit_empty_bags: bool,
    omit_empty_lists: bool,
}

const WRITER_BUF_CAP: usize = 256 * 1024;

fn convert(settings: &Settings, input: &str, output: &str) -> Result<(), Box<dyn Error>> {
    let file = File::open(&Path::new(input))?;
    let reader = SerializedFileReader::new(file)?;

    let mut writer = if output.is_empty() {
        Box::new(BufWriter::with_capacity(WRITER_BUF_CAP, io::stdout())) as Box<dyn Write>
    } else {
        Box::new(BufWriter::with_capacity(
            WRITER_BUF_CAP,
            File::create(&Path::new(output))?,
        )) as Box<dyn Write>
    };

    let mut iter = reader.get_row_iter(None)?;
    while let Some(record) = iter.next() {
        let value = top_level_row_to_value(settings, &record)?;
        writeln!(writer, "{}", serde_json::to_string(&value)?)?;
    }
    Ok(())
}

macro_rules! element_to_value {
    ($ft:expr, $obj:ident, $i:ident, $settings:ident) => {
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
            FieldType::Decimal => Value::String(decimal_to_string($obj.get_decimal($i)?)),
            FieldType::Str => Value::String($obj.get_string($i)?.to_string()),
            FieldType::Bytes => bytes_to_value($obj.get_bytes($i)?.data()),
            FieldType::Date => Value::Number($obj.get_date($i)?.into()),
            FieldType::Timestamp => Value::Number($obj.get_timestamp($i)?.into()),
            FieldType::Group => row_to_value($settings, $obj.get_group($i)?)?,
            FieldType::List => list_to_value($settings, $obj.get_list($i)?)?,
            FieldType::Map => map_to_value($settings, $obj.get_map($i)?)?,
        }
    };
}

fn top_level_row_to_value(settings: &Settings, row: &Row) -> Result<Value, Box<dyn Error>> {
    let value = row_to_value(settings, row)?;
    if value.is_null() {
        Ok(Value::Object(serde_json::Map::default()))
    } else {
        Ok(value)
    }
}

fn row_to_value(settings: &Settings, row: &Row) -> Result<Value, Box<dyn Error>> {
    let mut map = serde_json::Map::with_capacity(row.len());
    for i in 0..row.len() {
        let name = row.get_field_name(i);
        let field_type = row.get_field_type(i);
        let value = element_to_value!(field_type, row, i, settings);
        if !(settings.omit_nulls && value.is_null()) {
            map.insert(name.to_string(), value);
        }
    }

    if settings.omit_empty_bags && map.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(Value::Object(map))
    }
}

fn list_to_value(settings: &Settings, list: &List) -> Result<Value, Box<dyn Error>> {
    let mut arr = Vec::<Value>::with_capacity(list.len());
    for i in 0..list.len() {
        let elt_ty = list.get_element_type(i);
        let value = element_to_value!(elt_ty, list, i, settings);
        arr.push(value);
    }

    if settings.omit_empty_lists && arr.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(Value::Array(arr))
    }
}

fn map_to_value(settings: &Settings, map: &Map) -> Result<Value, Box<dyn Error>> {
    let mut jsmap = serde_json::Map::with_capacity(map.len());
    let keys = map.get_keys();
    let values = map.get_values();
    for i in 0..map.len() {
        let key_ty = keys.get_element_type(i);
        let key = match key_ty {
            FieldType::Str => keys.get_string(i)?.to_string(),
            // TODO: return error here
            _ => panic!("Non-string key"),
        };

        let val_ty = values.get_element_type(i);
        let value = element_to_value!(val_ty, values, i, settings);
        if !(settings.omit_nulls && value.is_null()) {
            jsmap.insert(key, value);
        }
    }

    if settings.omit_empty_bags && jsmap.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(Value::Object(jsmap))
    }
}

fn bytes_to_value(bytes: &[u8]) -> Value {
    let nums = bytes
        .iter()
        .map(|&b| Value::Number(b.into()))
        .collect::<Vec<_>>();
    Value::Array(nums)
}

fn float_to_value(f: f64) -> Value {
    Number::from_f64(f)
        .map(|n| Value::Number(n))
        .unwrap_or_else(|| Value::Null)
}

fn decimal_to_string(decimal: &Decimal) -> String {
    assert!(decimal.scale() >= 0 && decimal.precision() > decimal.scale());

    // Specify as signed bytes to resolve sign as part of conversion.
    let num = BigInt::from_signed_bytes_be(decimal.data());

    // Offset of the first digit in a string.
    let negative = if num.sign() == Sign::Minus { 1 } else { 0 };
    let mut num_str = num.to_string();
    let mut point = num_str.len() as i32 - decimal.scale() - negative;

    // Convert to string form without scientific notation.
    if point <= 0 {
        // Zeros need to be prepended to the unscaled value.
        while point < 0 {
            num_str.insert(negative as usize, '0');
            point += 1;
        }
        num_str.insert_str(negative as usize, "0.");
    } else {
        // No zeroes need to be prepended to the unscaled value, simply insert decimal
        // point.
        num_str.insert((point + negative) as usize, '.');
    }

    num_str
}
