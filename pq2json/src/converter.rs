use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use num_bigint::{BigInt, Sign};
use parquet::data_type::Decimal;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{FieldType, List, ListAccessor, Map, MapAccessor, Row, RowAccessor};
use parquet::schema::types::Type as SchemaType;
use serde_json::{Number, Value};

use crate::settings::{Settings, TimestampRendering};
use chrono::Duration;
use csv::Terminator;
use parquet::record::reader::RowIter;

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

    let schema = settings
        .columns
        .as_ref()
        .map(|c| projected_schema(&reader, &c).unwrap());

    let rows = reader.get_row_iter(schema)?;

    if settings.csv {
        top_level_rows_to_csv(&settings, rows, writer)
    } else {
        top_level_rows_to_json(&settings, rows, writer)
    }
}

fn projected_schema(
    reader: &SerializedFileReader<File>,
    columns: &Vec<String>,
) -> Result<SchemaType, Box<dyn Error>> {
    let file_meta = reader.metadata().file_metadata();
    let mut schema_fields = HashMap::new();
    for field in file_meta.schema().get_fields().iter() {
        schema_fields.insert(field.name().to_owned(), field);
    }

    let mut projected_fields = columns
        .iter()
        .map(|c| {
            schema_fields
                .get_mut(c)
                .expect(format!("column '{}' doesn't exist", c).as_str())
                .clone()
        })
        .collect();

    Ok(
        SchemaType::group_type_builder(&file_meta.schema().get_basic_info().name())
            .with_fields(&mut projected_fields)
            .build()
            .unwrap(),
    )
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
            FieldType::Date => date_to_value($obj.get_date($i)?)?,
            FieldType::TimestampMillis => {
                timestamp_to_value($settings, $obj.get_timestamp_millis($i)?)?
            }
            FieldType::TimestampMicros => timestamp_to_value(
                $settings,
                $obj.get_timestamp_micros($i).map(|ts| ts / 1000)?,
            )?,
            FieldType::Group => row_to_value($settings, $obj.get_group($i)?)?,
            FieldType::List => list_to_value($settings, $obj.get_list($i)?)?,
            FieldType::Map => map_to_value($settings, $obj.get_map($i)?)?,
        }
    };
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
    mut writer: Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    while let Some(row) = rows.next() {
        let mut csv_writer = csv::WriterBuilder::new()
            .terminator(Terminator::Any(b'\r'))
            .from_writer(vec![]);
        for i in 0..row.len() {
            let field_type = row.get_field_type(i);
            let value = element_to_value!(field_type, row, i, settings);
            csv_writer.write_field(value_to_csv(&value))?;
        }
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
                buffer.format(v.as_f64().unwrap()).to_owned()
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
            FieldType::Null => String::from("null"),
            FieldType::Bool => keys.get_bool(i)?.to_string(),
            FieldType::Byte => keys.get_byte(i)?.to_string(),
            FieldType::Short => keys.get_short(i)?.to_string(),
            FieldType::Int => keys.get_int(i)?.to_string(),
            FieldType::Long => keys.get_long(i)?.to_string(),
            FieldType::UByte => keys.get_ubyte(i)?.to_string(),
            FieldType::UShort => keys.get_ushort(i)?.to_string(),
            FieldType::UInt => keys.get_uint(i)?.to_string(),
            FieldType::ULong => keys.get_ulong(i)?.to_string(),
            FieldType::Str => keys.get_string(i)?.to_string(),
            // TODO: return error here
            _ => panic!("Unsupported map key"),
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

const TICKS_TILL_UNIX_TIME: u64 = 621355968000000000u64;

fn timestamp_to_value(settings: &Settings, ts: u64) -> Result<Value, Box<dyn Error>> {
    match settings.timestamp_rendering {
        TimestampRendering::Ticks => {
            let ticks = ts
                .checked_mul(10000)
                .and_then(|t| t.checked_add(TICKS_TILL_UNIX_TIME));
            let v = ticks
                .map(|t| Value::Number(t.into()))
                .unwrap_or(Value::Null);
            Ok(v)
        }
        TimestampRendering::IsoStr => {
            let seconds = (ts / 1000) as i64;
            let nanos = ((ts % 1000) * 1000000) as u32;
            let datetime =
                if let Some(dt) = chrono::NaiveDateTime::from_timestamp_opt(seconds, nanos) {
                    dt
                } else {
                    return Ok(Value::Null);
                };
            let iso_str = datetime.format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string();
            Ok(Value::String(iso_str))
        }
        TimestampRendering::UnixMs => Ok(Value::Number(ts.into())),
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
