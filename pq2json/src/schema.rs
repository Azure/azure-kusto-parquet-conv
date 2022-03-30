use std::error::Error;
use std::fs::File;
use std::path::Path;

use itertools::Itertools;
use parquet::basic::{ConvertedType, Type as PhysicalType};
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
            let csl_type = match basic_info.converted_type() {
                ConvertedType::UTF8 => "string",
                ConvertedType::TIMESTAMP_MICROS
                | ConvertedType::TIMESTAMP_MILLIS
                | ConvertedType::DATE => "datetime",
                ConvertedType::INTERVAL => "timespan",
                ConvertedType::DECIMAL => "decimal",
                ConvertedType::LIST
                | ConvertedType::MAP
                | ConvertedType::MAP_KEY_VALUE
                | ConvertedType::JSON => "dynamic",
                ConvertedType::UINT_32 => "long",
                ConvertedType::UINT_64 => "decimal",
                _ => parquet_physical_to_csl_type(physical_type),
            };
            (basic_info.name(), csl_type)
        }
        Type::GroupType { ref basic_info, .. } => (basic_info.name(), "dynamic"),
    }
}

fn parquet_physical_to_csl_type(parquet_type: &PhysicalType) -> &str {
    match parquet_type {
        PhysicalType::BOOLEAN => "bool",
        PhysicalType::INT32 => "int",
        PhysicalType::INT64 => "long",
        PhysicalType::INT96 => "datetime",
        PhysicalType::FLOAT => "real",
        PhysicalType::DOUBLE => "real",
        PhysicalType::BYTE_ARRAY => "string",
        PhysicalType::FIXED_LEN_BYTE_ARRAY => "decimal",
    }
}

/// Prints limited row groups metadata of a specified Parquet file as JSON,
/// for each row group its size in bytes and the number of rows.
///
/// Arguments:
///
/// * `input_file` - Parquet file path
///
pub fn print_row_groups_metadata(input_file: &str) -> Result<(), Box<dyn Error>> {
    let file = File::open(&Path::new(input_file))?;
    let reader = SerializedFileReader::new(file)?;
    let row_groups = Value::Array(
        reader
            .metadata()
            .row_groups()
            .iter()
            .map(|row_group_metadata| {
                let mut map = serde_json::Map::with_capacity(2);
                map.insert(
                    String::from("numberOfRows"),
                    Value::String(row_group_metadata.num_rows().to_string()),
                );
                map.insert(
                    String::from("totalByteSize"),
                    Value::String(row_group_metadata.total_byte_size().to_string()),
                );
                Value::Object(map)
            })
            .collect_vec(),
    );

    println!("{}", serde_json::to_string(&row_groups)?);
    Ok(())
}
