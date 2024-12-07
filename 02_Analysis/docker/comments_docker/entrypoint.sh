#!/bin/bash

# Load the environment variables
export $(cat /opt/program/.env | xargs)

# Run the notebook with papermill
papermill comments_sentiment_analyzer.ipynb output.ipynb

# Check if the notebook ran successfully
if [ $? -eq 0 ]; then
    echo "Notebook executed successfully."
else
    echo "Notebook execution failed."
    exit 1
fi
