from typing import Iterable
import pandas as pd
from pathlib import Path
from datasets import Dataset
from tqdm import tqdm

import sys
sys.path.append("../smart-contract-downloader/script")
from contract import Contract

class DataProcessor():

    def __init__(self, dir_path: str, chunk_size: int):

        self.dir_path = dir_path
        self.chunk_size = chunk_size
        
        self.data = self._read_parquet(self.dir_path)
        self._buffer = None
        self._unique_file_names = pd.Series(dtype=str)

        files_count = sum(1 for f in Path(self.dir_path).glob("*.parquet"))
        self.pbar = tqdm(total=files_count)

    def _read_parquet(self, dir_path):
        """
        TODO: Write docstring
        """
        
        index = 0
        while True:
            file_path = Path(dir_path, "part." +
                             str(index) + ".parquet").resolve()
            if file_path.exists():
                shard = pd.read_parquet(file_path)
                index += 1
                yield shard
            else:
                return

    def _explode_files(self, df):
        """
        Extracts contracts from the source code
        """
        contracts = []
        for _, row in df.iterrows():
            c_files = Contract(**row).explode()
            c_files = [c.to_dict() for c in c_files]
            contracts.extend(c_files)
        df = pd.DataFrame(contracts)
        return df

    def _uniqify(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function takes a list of strings and returns a list of strings
        with duplicates removed.
        """
        file_names = df.apply(lambda row: row.file_path.split("/")[-1], axis=1)
        df["file_name"] = file_names

        dupes = df["file_name"].isin(self._unique_file_names)
        dupes[df['file_name'] == ''] = False
        df = df[~dupes]
        # Keeping first since all_contracts is sorted on most transactions first.
        df = df.drop_duplicates(subset=['file_name'], keep='first')

        self._unique_file_names = pd.concat(
            [self._unique_file_names, file_names], axis=0, ignore_index=True).drop_duplicates()
        return df

    def plain_text(self) -> Iterable[Dataset]:
        """
        This function takes a list of strings and returns a list of strings
        with duplicates removed.
        """
        self.pbar.set_description("Dataset \"plain_text\"")
        for batch in self.data:
            self.pbar.update(1)
            df = self._explode_files(batch)  # Potentially very large batch
            df = self._uniqify(df)
            df = df.rename(columns={'source_code': 'text'})
            df = df[['text', 'language']]
            chunk = self.chunk(df)
            if chunk is not None:
                yield chunk
            else:
                continue
        else:
            # Always serve the last batch
            while self._buffer is not None:
                chunk = self._buffer.iloc[:self.chunk_size]
                self._buffer = self._buffer.iloc[self.chunk_size:]
                if self._buffer.shape[0] == 0:
                    self._buffer = None
                yield chunk

    def all(self) -> Iterable[Dataset]:
        """
        This function takes a list of strings and returns a list of strings
        with duplicates removed.
        """
        self.pbar.set_description("Dataset \"all\"")
        for batch in self.data:
            self.pbar.update(1)
            chunk = self.chunk(batch)
            if chunk is not None:
                yield chunk
            else:
                continue
        else:
            # Always serve the last batch
            while self._buffer is not None:
                chunk = self._buffer.iloc[:self.chunk_size]
                self._buffer = self._buffer.iloc[self.chunk_size:]
                if self._buffer.shape[0] == 0:
                    self._buffer = None
                yield chunk

    def chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This function rebalances the dataset.
        """

        if self._buffer is not None:
            df = pd.concat([self._buffer, df], axis=0, ignore_index=True)

        if df.shape[0] < self.chunk_size:
            # If the batch is smaller than the chunk size,
            # we buffer it and add it to the next batch.
            self._buffer = df
            return
        else:
            # If the batch is larger than the chunk size,
            # we split it into chunks of size chunk_size.
            chunk = df.iloc[:self.chunk_size]
            self._buffer = df.iloc[self.chunk_size:]
            return chunk


if __name__ == '__main__':
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description='Process dataset.')

    parser.add_argument('-s', '--source', metavar='source', type=str, required=False,
                        default="parquet", help='Path to directory with files to process.')
    parser.add_argument('-o', '--output-dir', metavar='output_dir', type=str, required=False,
                        default="data", help='The directory where the output should be stored.')
    parser.add_argument('--chunk-size', metavar='chunk-size', type=int, required=False,
                        default=30000, help='The number of contracts to store in each data file.')
    parser.add_argument('--datasets', metavar='datasets', type=str, nargs=2, required=False,
                        default=["all", "plain_text"], help='The datasets to make.')
    parser.add_argument('--clean', metavar='clean', type=bool, required=False,
                        default=True, help='Wheter to clean existing files in output dir.')
    args = parser.parse_args()

    for dataset_name in args.datasets:
        dataset_dir = os.path.join(args.output_dir, dataset_name)
        if not os.path.exists(dataset_dir):
            os.makedirs(dataset_dir, exist_ok=True)
        if args.clean:
            for file in Path(dataset_dir).glob("*.parquet"):
                file.unlink()

        if dataset_name == "all":
            dp = DataProcessor(args.source, args.chunk_size).all()
            for index, dataset in enumerate(dp):
                contracts_ds = Dataset.from_pandas(dataset)
                path = os.path.join(args.output_dir, dataset_name, "part." + str(index) + ".parquet")
                contracts_ds.to_parquet(path)

        elif dataset_name == "plain_text":
            dp = DataProcessor(args.source, args.chunk_size).plain_text()
            for index, dataset in enumerate(dp):
                contracts_ds = Dataset.from_pandas(dataset)
                path = os.path.join(args.output_dir, dataset_name, "part." + str(index) + ".parquet")
                contracts_ds.to_parquet(path)
        else:
            raise ValueError("Unknown dataset: " + dataset)
