use std::collections::HashSet;
use std::fs::{File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use clap::{Arg, Command};
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

fn remove_duplicates(input_path: &str, output_path: &str) -> std::io::Result<()> {
    // Open file and count total lines in one pass
    let input_file = File::open(input_path)?;
    let reader = BufReader::new(&input_file);

    // Initialize spinner for line counting
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    spinner.enable_steady_tick(Duration::from_millis(100)); // Update spinner every 100ms
    spinner.set_message("Counting total lines...");

    // Count total lines
    let total_lines = reader.lines().count() as u64;
    let message = format!("Counted {} lines.", total_lines);
    spinner.finish_with_message(message);

    // Re-open file for actual processing (same file object)
    let input_file = File::open(input_path)?;
    let reader = BufReader::new(input_file);

    // Create output file and progress bar for processing
    let output_file = File::create(output_path)?;
    let mut writer = BufWriter::new(output_file);

    let mut seen_lines = HashSet::new();

    // Progress bar for file processing
    let pb = ProgressBar::new(total_lines);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    // Deduplicate lines and write them to the output file
    for line_result in reader.lines() {
        let line = line_result?;
        if seen_lines.insert(line.clone()) {
            writeln!(writer, "{}", line)?;
        }
        pb.inc(1); // Update progress bar
    }

    pb.finish_with_message("Processing complete.");
    Ok(())
}

fn main() {
    let matches = Command::new("Deduplicate Lines")
        .version("1.0")
        .author("Your Name <youremail@example.com>")
        .about("Removes duplicate lines from a file")
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .value_name("INPUT")
                .help("The input file path")
                .required(true)
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("OUTPUT")
                .help("The output file path")
                .required(true)
                .value_parser(clap::value_parser!(String)),
        )
        .get_matches();

    // Use `get_one` to get the argument values as `String`
    let input_path = matches.get_one::<String>("input").unwrap();
    let output_path = matches.get_one::<String>("output").unwrap();

    // Call the remove_duplicates function
    if let Err(e) = remove_duplicates(input_path, output_path) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    println!("Deduplication completed successfully.");
}