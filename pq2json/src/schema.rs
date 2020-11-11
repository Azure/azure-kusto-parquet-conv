use std::error::Error;
use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::{print_file_metadata, print_parquet_metadata};

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
