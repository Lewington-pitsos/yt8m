#!/bin/bash

# Create a directory for the downloads
mkdir -p yt8m-thumbs/video/

# List all files in the S3 bucket directory
aws s3 ls s3://vit-sae/yt8m-thumbs/video/ --recursive | while read -r line; do
    # Extract the file name, ignoring the leading directory path from the listing
    file=$(echo $line | awk '{print $4}')
    
    # Start timer
    start_time=$(date +%s)
    
    # Download the file
    # Note: Correct the path by not duplicating the base directory
    aws s3 cp s3://vit-sae/$file ./

    # End timer
    end_time=$(date +%s)
    
    # Calculate duration
    duration=$((end_time - start_time))
    
    echo "Downloaded $file in $duration seconds"
done
