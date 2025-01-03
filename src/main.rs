use clap::{Parser};
use indicatif::{ProgressBar, ProgressStyle};
use tempfile::NamedTempFile;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::io;
use std::path::Path;

/// CLI arguments
#[derive(Parser)]
#[command(name = "Deduplicate Lines")]
#[command(author = "Ted Johnson <theodore.johnson@qiagen.com>")]
#[command(version = "1.0")]
#[command(about = "Removes duplicate lines from a file", long_about = None)]
struct Cli {
    /// Input file path
    #[arg(short, long, value_name = "INPUT_FILE")]
    input: String,

    /// Output file path
    #[arg(short, long, value_name = "OUTPUT_FILE")]
    output: String,
}

const CHUNK_SIZE: usize = 50_000_000; // Lines per chunk (adjust based on available memory)

/// Processes a single chunk by deduplicating and writing it to a temporary file

fn remove_duplicates_large_file(input_path: &str, output_path: &str) -> std::io::Result<()> {
    // Initialize a spinner to count lines
    let progress_bar = ProgressBar::new_spinner();
    progress_bar.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["-", "\\", "|", "/"]),
    );
    progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));
    progress_bar.set_message("Counting Lines...");
    progress_bar.tick();
    io::stdout().flush().unwrap();

    // Count total lines in the input file
    let input_file = File::open(input_path)?;
    let reader = BufReader::new(&input_file);
    let total_lines = reader.lines().count() as u64;
    progress_bar.finish_with_message(format!("Count complete. {} lines.", total_lines));
    std::mem::drop(progress_bar); // Discard the first progress bar

    // Re-open file for processing
    let input_file = File::open(input_path)?;
    let reader = BufReader::new(input_file);

    // Set up a progress bar for processing
    let progress_bar = ProgressBar::new(total_lines);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} lines ({percent}%) | {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
    );
    progress_bar.tick();
    io::stdout().flush().unwrap();

    // Create a temporary directory and initialize state variables
    let temp_dir = tempfile::tempdir()?;
    let mut temp_files = Vec::new();
    let mut chunk = Vec::with_capacity(CHUNK_SIZE);
    let mut lines_processed = 0;

    // Process the input file line by line
    for line_result in reader.lines() {
        let line = line_result?;
        chunk.push(line); // Add line to chunk if not seen before

        // Process the chunk when it reaches the specified size
        if chunk.len() >= CHUNK_SIZE {
            let temp_file = process_chunk_sequential(&chunk, temp_dir.path())?;
            temp_files.push(temp_file);
            chunk.clear(); // Clear chunk after processing
            lines_processed += CHUNK_SIZE as u64;
            progress_bar.set_position(lines_processed);
        }
    }

    // Process any remaining lines in the last chunk
    if !chunk.is_empty() {
        let temp_file = process_chunk_sequential(&chunk, temp_dir.path())?;
        temp_files.push(temp_file);
    }

    progress_bar.finish_with_message("File reading complete. Merging files...");
    std::mem::drop(progress_bar); // Discard the first progress bar
    // new progress bar for merging
    let progress_bar = ProgressBar::new_spinner();
    progress_bar.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg}")
            .unwrap()
            .tick_strings(&["-", "\\", "|", "/"]),
    );
    progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));
    progress_bar.set_message("Merging Temporary Files...");
    progress_bar.tick();
    io::stdout().flush().unwrap();

    merge_sorted_files(temp_files, output_path)?;
    progress_bar.finish_with_message("Deduplication completed successfully.");
    Ok(())
}

/// Processes a single chunk sequentially by deduplicating and writing it to a temporary file
fn process_chunk_sequential<'a>(
    chunk: &'a [String],
    temp_dir: &Path,
) -> std::io::Result<NamedTempFile> {
    // Sort and deduplicate lines within the chunk
    let mut lines = chunk.to_vec();
    lines.sort();
    lines.dedup();

    // Write deduplicated lines to a temporary file
    let temp_file = NamedTempFile::new_in(temp_dir)?;
    {
        let mut writer = std::io::BufWriter::new(temp_file.as_file());
        for line in lines {
            writeln!(writer, "{}", line)?;
        }
        writer.flush()?;
    }
    Ok(temp_file)
}

fn merge_sorted_files(temp_files: Vec<NamedTempFile>, output_path: &str) -> std::io::Result<()> {
    //K-way Merge Algorithm (a.k.a External Merge Sort)
    // Create a vector of `BufReader`s, one for each temporary file
    // These readers will allow reading lines from each file one at a time
    let mut readers = temp_files
        .into_iter()
        .map(|file| BufReader::new(File::open(file.path()).unwrap()))
        .collect::<Vec<_>>();

    // Open the output file where the deduplicated and sorted lines will be written
    let output_file = File::create(output_path)?;
    let mut writer = std::io::BufWriter::new(output_file);

    // Use a binary heap to maintain the smallest (lexicographically first) line
    // from the multiple readers. The heap is reversed (`std::cmp::Reverse`)
    // because Rust's `BinaryHeap` is a max-heap by default.
    let mut heap = std::collections::BinaryHeap::new();

    // Initialize the heap with the first line from each reader
    for (index, reader) in readers.iter_mut().enumerate() {
        let mut line = String::new();
        if reader.read_line(&mut line)? > 0 { // If a line was successfully read
            heap.push((std::cmp::Reverse(line.clone()), index)); // Push the line and reader index to the heap
        }
    }

    // Variable to track the last line written to avoid duplicates
    let mut last_line = String::new();

    // Continue processing until the heap is empty
    while let Some((std::cmp::Reverse(line), index)) = heap.pop() {
        // If the current line is different from the last line written, write it to the output
        if line != last_line {
            writeln!(writer, "{}", line)?;
            last_line = line; // Update the last line
        }

        // Attempt to read the next line from the reader that produced the current line
        let mut new_line = String::new();
        if readers[index].read_line(&mut new_line)? > 0 { // If a line was successfully read
            heap.push((std::cmp::Reverse(new_line.clone()), index)); // Push it back to the heap
        }
    }

    // Flush the writer to ensure all lines are written to the output file
    writer.flush()?;
    Ok(())
}

fn main() {
    let args = Cli::parse();


    if let Err(e) = remove_duplicates_large_file(&args.input, &args.output) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}