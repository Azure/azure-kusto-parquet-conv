use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::settings::Settings;
use crate::TimestampRendering;
use chrono::Duration;
use csv::Terminator;
use num_bigint::{BigInt, Sign};
use parquet::data_type::{AsBytes, Decimal};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use parquet::record::{Field, List, Map, Row};
use parquet::schema::types::Type as SchemaType;
use serde_json::{Number, Value};

const WRITER_BUF_CAP: usize = 256 * 1024;

/// Writes Parquet file as text, either as JSONL (every line contains single JSON record)
/// or as CSV (where nested structures are formatted as JSON strings).
///
/// Arguments:
///
/// * `settings` - Converter settings
/// * `input_file` - Parquet file path
/// * `output_file` - Optional output file path (if not provided - output is written to STDOUT).
///
pub fn convert(
    settings: &Settings,
    input_file: &str,
    output_file: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(&Path::new(input_file))?;
    let reader = SerializedFileReader::new(file)?;

    let writer = match output_file {
        Some(output_file) => Box::new(BufWriter::with_capacity(
            WRITER_BUF_CAP,
            File::create(&Path::new(output_file))?,
        )) as Box<dyn Write>,
        None => Box::new(BufWriter::with_capacity(WRITER_BUF_CAP, io::stdout())) as Box<dyn Write>,
    };

    let mut missing_columns = std::collections::HashSet::new();
    let schema = settings
        .columns
        .as_ref()
        .map(|c| projected_schema(&reader, &c, &mut missing_columns).unwrap());

    let rows = reader.get_row_iter(schema)?;

    if settings.csv {
        top_level_rows_to_csv(&settings, rows, missing_columns, writer)
    } else {
        top_level_rows_to_json(&settings, rows, writer)
    }
}

fn projected_schema(
    reader: &SerializedFileReader<File>,
    columns: &Vec<String>,
    missing_columns: &mut std::collections::HashSet<std::string::String>,
) -> Result<SchemaType, Box<dyn Error>> {
    let file_meta = reader.metadata().file_metadata();
    let mut schema_fields = HashMap::new();
    for field in file_meta.schema().get_fields().iter() {
        schema_fields.insert(field.name().to_owned(), field);
    }

    let mut projected_fields = Vec::new();

    for c in columns.iter() {
        let res = schema_fields.get_mut(c);

        match res {
            Some(ptr) => {
                projected_fields.push(ptr.clone());
            }
            None => {
                missing_columns.insert(c.clone());
            }
        }
    }

    Ok(
        SchemaType::group_type_builder(&file_meta.schema().get_basic_info().name())
            .with_fields(&mut projected_fields)
            .build()
            .unwrap(),
    )
}

fn element_to_value(settings: &Settings, field: &Field) -> Value {
    match field {
        Field::ULong(ulong) => ulong_to_value(*ulong, settings),
        Field::Bytes(byte_array) => bytes_to_value(byte_array.as_bytes()),
        Field::Float(float) => float_to_value(*float as f64),
        Field::Double(double) => float_to_value(*double),
        Field::Decimal(decimal) => Value::String(decimal_to_string(decimal)),
        Field::Date(date) => date_to_value(*date).unwrap(),
        Field::TimestampMillis(ts) => timestamp_to_value(settings, *ts).unwrap(),
        Field::TimestampMicros(ts) => timestamp_to_value(settings, ts / 1000).unwrap(),
        Field::Group(row) => row_to_value(settings, row).unwrap(),
        Field::ListInternal(list) => list_to_value(settings, list).unwrap(),
        Field::MapInternal(map) => map_to_value(settings, map).unwrap(),
        _ => field.to_json_value(),
    }
}

fn top_level_rows_to_json(
    settings: &Settings,
    mut rows: RowIter,
    mut writer: Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    while let Some(row) = rows.next() {
        let value = row_to_value(settings, &row)?;
        let value = if value.is_null() {
            Value::Object(serde_json::Map::default())
        } else {
            value
        };
        writeln!(writer, "{}", serde_json::to_string(&value)?)?;
    }

    Ok(())
}

fn top_level_rows_to_csv(
    settings: &Settings,
    mut rows: RowIter,
    missing_columns: std::collections::HashSet<std::string::String>,
    mut writer: Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    while let Some(row) = rows.next() {
        let mut csv_writer = csv::WriterBuilder::new()
            .terminator(Terminator::Any(b'\r'))
            .from_writer(vec![]);
        let columns = settings.columns.as_ref();

        match columns {
            Some(cols) => {
                let mut row_columns_map: HashMap<&String, &Field> = HashMap::new();
                for (name, field) in row.get_column_iter() {
                    row_columns_map.insert(name, field);
                }

                // Produce empty values for columns specified by --columns argument, but missing in the file
                for col in cols {
                    let value = if missing_columns.contains(col) {
                        Value::Null
                    } else {
                        let field = row_columns_map.get(col).unwrap();
                        element_to_value(settings, field)
                    };

                    csv_writer.write_field(value_to_csv(&value))?;
                }
            }
            None => {
                // No columns specified by --columns argument
                for (_, field) in row.get_column_iter() {
                    let value = element_to_value(settings, field);
                    csv_writer.write_field(value_to_csv(&value))?;
                }
            }
        };

        csv_writer.write_record(None::<&[u8]>)?;
        writeln!(writer, "{}", String::from_utf8(csv_writer.into_inner()?)?)?;
    }
    Ok(())
}

fn value_to_csv(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Number(ref v) => {
            if v.is_f64() {
                let mut buffer = ryu::Buffer::new();
                truncate_trailing_zeros(buffer.format(v.as_f64().unwrap())).to_string()
            } else if v.is_u64() {
                format!("{}", v.as_u64().unwrap())
            } else {
                format!("{}", v.as_i64().unwrap())
            }
        }
        Value::String(ref v) => v.to_owned(),
        Value::Array(ref v) => serde_json::to_string(&v).unwrap(),
        Value::Object(ref v) => serde_json::to_string(&v).unwrap(),
    }
}

fn truncate_trailing_zeros(str: &str) -> &str {
    str.trim_end_matches('0').trim_end_matches('.')
}

fn row_to_value(settings: &Settings, row: &Row) -> Result<Value, Box<dyn Error>> {
    let mut map = serde_json::Map::with_capacity(row.len());
    for (name, field) in row.get_column_iter() {
        let value = element_to_value(settings, field);
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
    for field in list.elements().iter() {
        let value = element_to_value(settings, field);
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
    for (key_type, value_type) in map.entries() {
        let key = match key_type {
            Field::Null => String::from("null"),
            Field::Bool(_)
            | Field::Byte(_)
            | Field::Short(_)
            | Field::Int(_)
            | Field::Long(_)
            | Field::UByte(_)
            | Field::UShort(_)
            | Field::UInt(_)
            | Field::ULong(_) => element_to_value(settings, key_type).to_string(),
            Field::Str(string) => String::from(string),
            // TODO: return error here
            _ => panic!("Unsupported map key"),
        };

        let value = element_to_value(settings, value_type);
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

fn ulong_to_value(l: u64, settings: &Settings) -> Value {
    if settings.convert_types {
        Value::Number((l as i64).into())
    } else {
        Value::Number(l.into())
    }
}

const TICKS_TILL_UNIX_TIME: u64 = 621355968000000000u64;

fn timestamp_to_value(settings: &Settings, ts: u64) -> Result<Value, Box<dyn Error>> {
    let mut timestamp = ts as i64;
    if timestamp < 0 {
        timestamp = 0;
    }

    match settings.timestamp_rendering {
        TimestampRendering::Ticks => {
            let ticks = timestamp
                .checked_mul(10000)
                .and_then(|t| t.checked_add(TICKS_TILL_UNIX_TIME as i64));
            let v = ticks
                .map(|t| Value::Number(t.into()))
                .unwrap_or(Value::Null);
            Ok(v)
        }
        TimestampRendering::IsoStr => {
            let seconds = (timestamp / 1000) as i64;
            let nanos = ((timestamp % 1000) * 1000000) as u32;
            let datetime =
                if let Some(dt) = chrono::NaiveDateTime::from_timestamp_opt(seconds, nanos) {
                    dt
                } else {
                    return Ok(Value::Null);
                };
            let iso_str = datetime.format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string();
            Ok(Value::String(iso_str))
        }
        TimestampRendering::UnixMs => Ok(Value::Number(timestamp.into())),
    }
}

fn date_to_value(days_from_epoch: u32) -> Result<Value, Box<dyn Error>> {
    let date = match chrono::NaiveDate::from_ymd(1970, 1, 1)
        .checked_add_signed(Duration::days(days_from_epoch as i64))
    {
        Some(date) => date,
        None => return Ok(Value::Null),
    };
    let iso_str = date.format("%Y-%m-%d").to_string();
    Ok(Value::String(iso_str))
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
