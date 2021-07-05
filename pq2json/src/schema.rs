use std::error::Error;
use std::fs::File;
use std::path::Path;

use itertools::Itertools;
use parquet::basic::{LogicalType, Type as PhysicalType};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::{print_file_metadata, print_parquet_metadata};
use parquet::schema::types::Type;
use serde_json::Value;

/// Prints Parquet file schema information
///
/// Arguments:
///
/// * `input_file` - Parquet file path
///
pub fn print_schema(input_file: &str) -> Result<(), Box<dyn Error>> {
    let file = File::open(&Path::new(input_file))?;
    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    let mut output = Vec::new();
    print_parquet_metadata(&mut output, &meta);
    println!("\n\nParquet metadata");
    println!("=================================================");
    println!("{}", String::from_utf8(output)?);

    let mut output = Vec::new();
    let file_meta = reader.metadata().file_metadata();
    print_file_metadata(&mut output, &file_meta);
    println!("\n\nFile metadata");
    println!("=================================================");
    println!("{}", String::from_utf8(output)?);
    Ok(())
}

/// Prints KUSTO schema of specified Parquet file.
///
/// Arguments:
///
/// * `input_file` - Parquet file path
///
pub fn print_csl_schema(input_file: &str) -> Result<(), Box<dyn Error>> {
    // Instead of dealing with logical types translation, we just get the first
    // row, and print it's schema:
    let file = File::open(&Path::new(input_file))?;
    let reader = SerializedFileReader::new(file)?;
    let file_meta = reader.metadata().file_metadata();
    let schema_desc = file_meta.schema_descr();

    let fields = match schema_desc.root_schema() {
        &Type::GroupType { ref fields, .. } => fields
            .iter()
            .map(|field| field_csl_schema(field))
            .collect::<Vec<(&str, &str)>>(),
        _ => panic!("root schema is expected to be of group type!"),
    };

    let json_arr = Value::Array(
        fields
            .iter()
            .map(|(field_name, field_type)| {
                let mut map = serde_json::Map::with_capacity(2);
                map.insert(String::from("name"), Value::String(field_name.to_string()));
                map.insert(String::from("type"), Value::String(field_type.to_string()));
                Value::Object(map)
            })
            .collect_vec(),
    );
    println!("{}", serde_json::to_string(&json_arr)?);
    Ok(())
}

fn field_csl_schema(field_type: &Type) -> (&str, &str) {
    match field_type {
        Type::PrimitiveType {
            ref basic_info,
            physical_type,
            ..
        } => {
            let csl_type = match physical_type {
                PhysicalType::BOOLEAN => "bool",
                PhysicalType::BYTE_ARRAY => match basic_info.logical_type() {
                    LogicalType::UTF8 | LogicalType::ENUM | LogicalType::JSON => "string",
                    LogicalType::DECIMAL => "real",
                    _ => "dynamic",
                },
                PhysicalType::FIXED_LEN_BYTE_ARRAY => match basic_info.logical_type() {
                    LogicalType::DECIMAL => "real",
                    _ => "dynamic",
                },
                PhysicalType::DOUBLE | PhysicalType::FLOAT => "real",
                PhysicalType::INT32 => match basic_info.logical_type() {
                    LogicalType::DATE => "datetime",
                    LogicalType::DECIMAL => "real",
                    _ => "long",
                },
                PhysicalType::INT64 => match basic_info.logical_type() {
                    LogicalType::TIMESTAMP_MILLIS | LogicalType::TIMESTAMP_MICROS => "datetime",
                    LogicalType::DECIMAL => "real",
                    _ => "long",
                },
                PhysicalType::INT96 => "datetime",
            };
            (basic_info.name(), csl_type)
        }
        Type::GroupType { ref basic_info, .. } => (basic_info.name(), "dynamic"),
    }
}
