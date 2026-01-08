def validate_file_count(expected: int):
    if expected <= 0:
        raise ValueError("No files were found in S3")
