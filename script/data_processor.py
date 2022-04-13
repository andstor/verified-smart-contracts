from typing import Iterable
import pandas as pd
from pathlib import Path
from datasets import Dataset
from tqdm import tqdm
from contract import Contract
from merge_filter import merge_filter
import textdistance
from functools import partial


class DataProcessor():

    def __init__(self, dir_path: str, chunk_size: int):

        self.dir_path = dir_path
        self.chunk_size = chunk_size

        self.data = None
        self._buffer = None
        self._unique_file_names = pd.Series(dtype=str)

        files_count = sum(1 for f in Path(self.dir_path).glob("*.parquet"))
        self.pbar = tqdm(total=files_count, desc="Processing")

    def reset(self):
        self.data = self._read_parquet(self.dir_path)
        self._buffer = None
        self._unique_file_names = pd.Series(dtype=str)
        self.pbar.reset()

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

    def _uniqify(self, df: pd.DataFrame, grouping_column, threshold=0.9) -> pd.DataFrame:
        values = set()

        unique_values = df[grouping_column].unique()
        jaccard = textdistance.Jaccard()

        pbar = tqdm(
            total=unique_values.shape[0], desc="Filtering \"" + str(grouping_column) + "\"", leave=False)
        pbar.set_postfix(total=str(df.shape[0]))
        dupe_indexes = []
        dupes = 0
        for i, row in df.iterrows():
            if row[grouping_column] not in values:
                values.add(row[grouping_column])
                pbar.update(1)

                grouping_values_df = df[df[grouping_column]
                                        == row[grouping_column]]
                if not grouping_values_df.shape[0] > 1:
                    continue
                with tqdm(total=grouping_values_df.shape[0], desc="Textdistance \"" + row[grouping_column] + "\"", leave=False) as pbar2:
                    for j, row2 in grouping_values_df.iterrows():
                        pbar2.update(1)
                        if j == i:
                            continue
                        if row2[grouping_column] == row[grouping_column]:
                            jaccard_score = jaccard(
                                row.source_code, row2.source_code)
                            if jaccard_score > threshold:
                                dupe_indexes.append(j)
                                dupes += 1
                                pbar.set_postfix(dupes=str(round(dupes*100/df.shape[0], 2)) + "%")
            else:
                continue

        pbar.close()

        df.drop(df.index[dupe_indexes], inplace=True)
        return df

    def _uniqify_filename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deprecated
        """
        file_names = df.apply(lambda row: row.file_path.split("/")[-1], axis=1)
        df["file_name"] = file_names

        dupes = df["file_name"].isin(self._unique_file_names)
        # Keep empty filennames, as well as all the vyper files with standard contract name (Etherscan).
        # TODO: Actuually compare the source code for uniqness.
        dupes[(df['file_name'] == '') | (
            df['file_name'] == 'Vyper_contract.vy')] = False
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
        df_gen = self.all_files()
        
        def convert_plain_text(df: pd.DataFrame) -> pd.DataFrame:
            df = df.rename(columns={'source_code': 'text'})
            df = df[['text', 'language']]
            return df

        plain_text_gen = map(convert_plain_text, df_gen)  # Add tmp file_name column
        return plain_text_gen
    
    def all_files(self) -> Iterable[Dataset]:
        """
        This function takes a list of strings and returns a list of strings
        with duplicates removed.
        """
        self.reset()
        self.pbar.set_description("Dataset \"plain_text\"")

        def add_file_name(df):
            df["file_name"] = df.apply(
                lambda row: row.file_path.split("/")[-1], axis=1)
            return df

        # Split contracts into files
        files_gen = map(self._explode_files, self.data)
        files_gen = map(add_file_name, files_gen)  # Add tmp file_name column
        files_gen = map(lambda df: (self.pbar.update(1), df)
                        [-1], files_gen)  # Update progress bar

        filter_func = partial(self._uniqify, grouping_column="file_name", threshold=0.9)
        df = merge_filter(files_gen, filter_func)

        df.drop(columns='file_name', inplace=True)  # Drop tmp file_name column

        return self.chunk_gen(df)

    def all(self) -> Iterable[Dataset]:
        """
        This function takes a list of strings and returns a list of strings
        with duplicates removed.
        """
        self.reset()
        self.pbar.set_description("Dataset \"all\"")

        # Update progress bar
        contract_gen = map(lambda df: (self.pbar.update(1), df)[-1], self.data)

        filter_func = partial(self._uniqify, grouping_column="contract_name", threshold=0.9)
        df = merge_filter(contract_gen, filter_func)

        df = self._uniqify_filename(df)

        return self.chunk_gen(df)

    def chunk_gen(self, df: pd.DataFrame) -> Iterable[Dataset]:
        i = 0
        while i < df.shape[0]:
            yield df.iloc[i:i + self.chunk_size]
            i += self.chunk_size

    def raw(self) -> Iterable[Dataset]:
        """
        This function spits out the raw data in the chunk size specified.
        """
        self.reset()
        self.pbar.set_description("Dataset \"raw\"")
        for batch in self.data:
            self.pbar.update(1)
            chunk = self.chunk(batch)
            if chunk is not None:
                yield chunk
            else:
                continue
        else:
            # Always serve the "rest" batch
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
    parser.add_argument('--datasets', metavar='datasets', type=str, nargs='*', required=False,
                        default=["all", "plain_text"], help='The datasets to make. "all", "all_files", "plain_text" or "raw".')
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
        elif dataset_name == "all_files":
            dp = DataProcessor(args.source, args.chunk_size).all_files()
        elif dataset_name == "plain_text":
            dp = DataProcessor(args.source, args.chunk_size).plain_text()
        elif dataset_name == "raw":
            dp = DataProcessor(args.source, args.chunk_size).raw()
        else:
            raise ValueError("Unknown dataset: " + dataset)
            
        for index, dataset in enumerate(dp):
            contracts_ds = Dataset.from_pandas(dataset)
            path = os.path.join(
                args.output_dir, dataset_name, "part." + str(index) + ".parquet")
            contracts_ds.to_parquet(path)
