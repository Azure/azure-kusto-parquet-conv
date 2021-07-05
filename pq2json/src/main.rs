use clap::{App, Arg};

use crate::settings::{Settings, TimestampRendering};

mod converter;
mod schema;
mod settings;

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
            Arg::with_name("convert-types")
                .short("r")
                .long("convert-types")
                .help("Implicit Parquet to Kusto types conversion (e.g. U64 into long)")
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
            Arg::with_name("csv")
                .long("csv")
                .help("Output root level fields in CSV format")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("timestamp")
                .short("t")
                .long("timestamp")
                .possible_values(&["isostr", "ticks", "unixms"])
                .default_value("isostr")
                .help(
                    "Timestamp rendering option. Either \
                     ticks (100ns ticks since 01-01-01), \
                     isostr (ISO8601 string) \
                     or unixms (milliseconds since Unix epoch)",
                )
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("columns")
                .short("c")
                .long("columns")
                .help("Comma separated top-level columns to select")
                .takes_value(true)
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
            Arg::with_name("schema")
                .long("schema")
                .help("Print schema")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("cslschema")
                .long("cslschema")
                .help("Print Kusto schema")
                .takes_value(false)
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
    let output = matches.value_of("OUT_FILE");

    let timestamp_rendering = match matches.value_of("timestamp").unwrap_or("ticks") {
        "ticks" => TimestampRendering::Ticks,
        "isostr" => TimestampRendering::IsoStr,
        "unixms" => TimestampRendering::UnixMs,
        _ => TimestampRendering::IsoStr,
    };

    let settings = Settings {
        omit_nulls: matches.is_present("omit-nulls") || matches.is_present("prune"),
        omit_empty_bags: matches.is_present("omit-empty-bags") || matches.is_present("prune"),
        timestamp_rendering,
        omit_empty_lists: matches.is_present("omit-empty-lists") || matches.is_present("prune"),
        convert_types: matches.is_present("convert-types"),
        columns: matches
            .value_of("columns")
            .map(|columns| columns.split(",").map(|s| s.to_string()).collect()),
        csv: matches.is_present("csv"),
    };

    let res = if matches.is_present("schema") {
        schema::print_schema(input)
    } else if matches.is_present("cslschema") {
        schema::print_csl_schema(input)
    } else {
        converter::convert(&settings, input, output)
    };

    match res {
        Ok(()) => (),
        Err(e) => {
            eprintln!("ERROR: {:?}", e);
            std::process::exit(-1);
        }
    }
}
