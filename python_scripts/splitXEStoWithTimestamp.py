import xml.etree.ElementTree as ET
import os

# Function to manually parse the XES file using ElementTree
def parse_xes_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    return root

# Real-time writing while splitting traces into batches
def split_and_write_batches(root, n_batches, overlap_percentage, output_dir):
    traces = root.findall('{http://www.xes-standard.org/}trace')  # Find all traces
    total_traces = len(traces)
    batch_size = total_traces // n_batches
    
    for batch_idx in range(n_batches):
        file_path = f"{output_dir}/batch_{batch_idx + 1}.txt"
        
        # Open the file for writing
        with open(file_path, 'w') as f:
            start_idx = batch_idx * batch_size
            end_idx = start_idx + batch_size
            batch = traces[start_idx:end_idx]
            
            # Handle trace continuation based on overlap percentage
            if batch_idx < n_batches - 1:  # if not the last batch
                overlap_count = int(len(batch) * overlap_percentage)
                next_batch = traces[end_idx:end_idx + overlap_count]
                batch += next_batch  # continue traces in next batch
            
            # Write traces in real time
            for trace in batch:
                trace_id = trace.find('{http://www.xes-standard.org/}string[@key="concept:name"]').attrib['value']
                events = []
                for event in trace.findall('{http://www.xes-standard.org/}event'):
                    event_type = event.find('{http://www.xes-standard.org/}string[@key="concept:name"]').attrib['value']
                    delab = event.find('{http://www.xes-standard.org/}string[@key="org:resource"]')
                    if delab is not None:
                        delab = delab.attrib['value']
                    else:
                        delab = 'unknown'
                    timestamp = event.find('{http://www.xes-standard.org/}date[@key="time:timestamp"]').attrib['value']
                    events.append(f"{event_type}/{delab}/{timestamp}")
                
                # Write formatted trace to file
                f.write(f"{trace_id}::" + ",".join(events) + "\n")
        
        # Calculate and print the size of the batch file in MB
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB
        print(f"Batch {batch_idx + 1} size: {len(batch)} traces, File size: {file_size_mb:.2f} MB")

# Main Function to Call
def main():
    xes_file_path = '../xes_datasets/helpdesk.xes'  # Replace with your XES file path
    n_batches = 1  # Example: Split into 5 batches
    overlap_percentage = 0.2  # Example: 20% traces continue to next batch
    output_dir = '../ts_datasets'  # Directory to save the output files

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Parse the XES file
    root = parse_xes_file(xes_file_path)
    
    # Split and write batches to files
    split_and_write_batches(root, n_batches, overlap_percentage, output_dir)

if __name__ == "__main__":
    main()
