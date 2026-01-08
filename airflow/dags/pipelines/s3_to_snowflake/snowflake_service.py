class SnowflakeCopyBuilder:
    def __init__(self, stage: str, table: str, file_format: str, prefix: str):
        self.stage = stage
        self.table = table
        self.file_format = file_format
        self.prefix = prefix

    def copy_into_sql(self) -> str:
        return f"""
        COPY INTO {self.table}
        FROM {self.stage}/{self.prefix}
        FILE_FORMAT = (FORMAT_NAME = {self.file_format});
        """
