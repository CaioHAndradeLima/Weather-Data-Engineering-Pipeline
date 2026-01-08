import boto3

class S3Service:
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix
        self.client = boto3.client("s3")

    def list_csv_files(self) -> list[str]:
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.prefix
        )

        return [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".csv")
        ]

    def count_files(self) -> int:
        files = self.list_csv_files()
        return len(files)
