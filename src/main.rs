use std::thread;
use std::time::Instant;

fn map_reduce(chunked_data: Vec<&'static str>) {
    let mut children = vec![];

    // Map Phase
    println!("Thread Sums:\n-------------------------");
    for (i, data_segment) in chunked_data.iter().enumerate() {
        // Ownership Required For thread::spawn()
        let data_segment = data_segment.to_owned();
        children.push(thread::spawn(move || -> u32 {
            // Convert Char to Int and Sum
            let result = data_segment
                .chars()
                .map(|c| c.to_digit(10).expect("should be a digit"))
                .sum();

            // Return Results Per Thread
            println!("Processed segment {}, result={}", i, result);
            result
        }));
    }
    println!("-------------------------\n");

    // Reduce Phase
    // collect each thread's intermediate results into a new Vec
    let mut intermediate_sums = vec![];
    for child in children {
        let intermediate_sum = child.join().unwrap();
        intermediate_sums.push(intermediate_sum);
    }

    let final_result = intermediate_sums.iter().sum::<u32>();
    println!("Final sum result: {}", final_result);
}

// This is the `main` thread
fn main() {
    const THREAD_COUNT: usize = 1;

    // Input Data
    let data = "123456789123456789123456789123456789123456789123456789123456789123456789123456789123456789";

    let now = Instant::now();

    // Chunk Data
    let data_len = data.chars().count();
    let chunk_size = data_len / THREAD_COUNT;
    println!("\n\nChunk size is: {}", chunk_size);

    let chunk_vec: Vec<&str> = data
        .as_bytes()
        .chunks(chunk_size)
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap();

    println!("\nData Chunks\n-------------------------");
    println!("The number of chunks: {:?}", chunk_vec.len());
    for (i, chunk) in chunk_vec.iter().enumerate() {
        println!("Chunks {}: {:?}", i, chunk);
    }
    println!("-------------------------\n");

    // Execute Map Reduce On Chunks
    map_reduce(chunk_vec);

    println!("\nAdded {} digits.", data_len);
    println!("Total Runtime: {:?}", now.elapsed());
}
