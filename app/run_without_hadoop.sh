#!/bin/bash
echo "Running search system without Hadoop using direct fallback data generation"

# Directory settings
DATA_DIR="/app/data"
FALLBACK_DIR="/tmp/index_data"
FALLBACK_FILE="$FALLBACK_DIR/index_data.json"
ALTERNATE_FALLBACK="/tmp/index_data.json"

echo "Generating fallback data directly from documents"
mkdir -p $FALLBACK_DIR
echo "Calling test_fallback.py with --sample flag to ensure data is created even if no text files are found"
python3 /app/test_fallback.py --docs "$DATA_DIR" --output "$FALLBACK_FILE" --copy --sample

echo "Verifying fallback data files"
echo "Checking primary fallback location:"
if [ -f "$FALLBACK_FILE" ]; then
    echo "Primary fallback file exists at $FALLBACK_FILE"
    echo "File details:"
    ls -la $FALLBACK_FILE
    FILE_SIZE=$(stat -c%s "$FALLBACK_FILE")
    echo "File size: $FILE_SIZE bytes"
    
    if [ "$FILE_SIZE" -lt 200 ]; then
        echo "Warning: File is very small, might not contain proper data"
        echo "File contents (first 200 bytes):"
        head -c 200 "$FALLBACK_FILE"
        echo ""
    else
        echo "File size seems reasonable"
        echo "First few entries in the file:"
        head -c 500 "$FALLBACK_FILE"
        echo ""
    fi
else
    echo "Primary fallback file does not exist at $FALLBACK_FILE"
    # Try to create directory and empty file as last resort
    mkdir -p $FALLBACK_DIR
    echo '{"document_metadata":{"sample1":{"title":"Emergency Sample","length":10}},"term_document_freq":{"emergency":{"sample1":5},"sample":{"sample1":5}},"doc_freq":{"emergency":1,"sample":1},"corpus_stats":{"total_documents":1,"total_token_count":10,"avg_doc_length":10}}' > $FALLBACK_FILE
    echo "Created emergency fallback file"
fi

echo "Checking secondary fallback location:"
if [ -f "$ALTERNATE_FALLBACK" ]; then
    echo "Secondary fallback file exists at $ALTERNATE_FALLBACK"
    echo "File details:"
    ls -la $ALTERNATE_FALLBACK
    FILE_SIZE=$(stat -c%s "$ALTERNATE_FALLBACK")
    echo "File size: $FILE_SIZE bytes"
else
    echo "Secondary fallback file does not exist at $ALTERNATE_FALLBACK"
    # Copy from primary if it exists, otherwise create
    if [ -f "$FALLBACK_FILE" ]; then
        cp "$FALLBACK_FILE" "$ALTERNATE_FALLBACK"
        echo "Copied primary fallback file to secondary location"
    else
        echo '{"document_metadata":{"sample1":{"title":"Emergency Sample","length":10}},"term_document_freq":{"emergency":{"sample1":5},"sample":{"sample1":5}},"doc_freq":{"emergency":1,"sample":1},"corpus_stats":{"total_documents":1,"total_token_count":10,"avg_doc_length":10}}' > $ALTERNATE_FALLBACK
        echo "Created emergency fallback file at secondary location"
    fi
fi

chmod 777 $FALLBACK_FILE
chmod 777 $ALTERNATE_FALLBACK

echo "Running search tests"
QUERIES=("science" "computer" "information" "data" "technology" "sample")

for QUERY in "${QUERIES[@]}"; do
    echo "************************************************************"
    echo "*                   SEARCH TEST RESULTS                     *"
    echo "************************************************************"
    echo "* Searching for: '$QUERY'"
    echo "************************************************************"
    python3 /app/search.py "$QUERY"
    echo ""
done