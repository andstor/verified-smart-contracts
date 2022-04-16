from multiprocessing import Pool, cpu_count
from typing import Iterable
import pandas as pd
from pathlib import Path
from datasets import Dataset
from tqdm import tqdm
from contract import Contract
from merge_filter import merge_filter
import textdistance
from functools import partial
import itertools
import glob
import logging
import numpy as np

log = logging.getLogger("data-processor")


def find_dupes(column, threshold, df):
    if df.shape[0] <= 1:
        return []

    dupe_indexes = []
    jaccard = textdistance.Jaccard()
    first_row = None
    for i, row in df.iterrows():
        if first_row is None:
            first_row = row
            continue
        if column is None or row[column] == first_row[column]:
            jaccard_score = jaccard(first_row.source_code, row.source_code)
            if jaccard_score > threshold:
                dupe_indexes.append(i)
    return dupe_indexes


class DataProcessor():

    def __init__(self, dir_path: str, chunk_size: int):

        self.dir_path = dir_path
        self.chunk_size = chunk_size

        self._buffer = None
        self._unique_file_names = pd.Series(dtype=str)

        self.dupes_count = 0
        self.count = 0

        self.pbar = None

    def reset(self):
        self._buffer = None
        self.dupes_count = 0
        self.count = 0
        self._unique_file_names = pd.Series(dtype=str)

    def _read_parquet(self, dir_path):
        """
        TODO: Write docstring
        """
        files_count = sum(1 for f in Path(dir_path).glob("*.parquet"))
        self.pbar = tqdm(desc="Processing", total=files_count)

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

    def applyParallel(self, iterable, total, func, chunksize=1):
        with Pool() as p:            
            res_list = []
            with tqdm(total=total, desc="Filtering", leave=False) as pbar:
                for res in p.imap_unordered(func=func, iterable=iterable, chunksize=chunksize):
                    pbar.update()
                    res_list.append(res)
            
        return list(itertools.chain.from_iterable(res_list))

    def _uniqify(self, df: pd.DataFrame, threshold=0.9, grouping_column=None) -> pd.DataFrame:
        
        # Drop pure duplicates.
        size = df.shape[0]
        df.drop_duplicates(subset=['source_code'], keep='first', inplace=True)
        self.dupes_count += size - df.shape[0]

        if grouping_column:
            shuffled_df = df.sample(frac=1, ignore_index=False)
            df_group = shuffled_df.groupby(grouping_column)
            iterable = [group for name, group in df_group]
            func = partial(find_dupes, grouping_column, threshold)
            match_indexes = self.applyParallel(iterable, len(iterable), func, 100)
        else:
            iterable = self.brute_force_gen(df)
            func = partial(find_dupes, None, threshold)
            match_indexes = self.applyParallel(iterable, df.shape[0], func, 1)
        
        df.drop(match_indexes, inplace=True)


        self.dupes_count += len(match_indexes)
        
        dupes=str(self.dupes_count) + "/" + str(self.count)
        dupes_percentage=str(round(self.dupes_count*100/self.count, 2)) + "%"
        self.pbar.set_postfix(
            dupes=dupes + " (" + str(dupes_percentage) + ")"
            )
        
        return df
    
    def brute_force_gen(self, df: pd.DataFrame):
        for i in range(df.shape[0]):
            yield df.iloc[i:]
    
    def _uniqify_filename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Deprecated
        """
        file_names = df.apply(lambda row: row.file_path.split("/")[-1], axis=1)
        df["file_name"] = file_names

        dupes = df["file_name"].isin(self._unique_file_names)
        # Keep empty filennames, as well as all the vyper files with standard contract name (Etherscan).

        dupes[(df['file_name'] == '') | (
            df['file_name'] == 'Vyper_contract.vy')] = False
        df = df[~dupes]
        # Keeping first since all_contracts is sorted on most transactions first.
        df = df.drop_duplicates(subset=['file_name'], keep='first')

        self._unique_file_names = pd.concat(
            [self._unique_file_names, file_names], axis=0, ignore_index=True).drop_duplicates()
        return df

    def process(self, split_files: bool, threshold: float) -> Iterable[Dataset]:
        self.reset()
        log.info("Generating dataset")
        log.info("Split files: " + str(split_files))
        log.info("Similarity threshold: " + str(threshold))
        data_gen = self._read_parquet(self.dir_path)
        
        def add_file_name(df):
            df["file_name"] = df.apply(
                lambda row: row.file_path.split("/")[-1], axis=1)
            return df

        if split_files:
            # Split contracts into files
            files_gen = map(self._explode_files, data_gen)
            data_gen = map(add_file_name, files_gen)  # Add tmp file_name column

        data_gen = map(lambda df: (self.pbar.update(1), df)[-1], data_gen)  # Update progress bar
        data_gen = map(lambda df: (setattr(self, 'count', (self.count + df.shape[0])), df)[-1], data_gen)  # Update counter

        if threshold: 
            grouping_column = "file_name" if split_files else "contract_name"
            filter_func = partial(
                self._uniqify, threshold=threshold, grouping_column=grouping_column)
            df = merge_filter(data_gen, filter_func) # Merge and filter data
            
            #df = self._uniqify(df=df, threshold=threshold, grouping_column=None) # Filter again without grouping (VERY COSTLY) ((n-1)^2)/cpus
            
            if split_files:
                df.drop(columns='file_name', inplace=True)  # Drop tmp file_name column
            
            return self.chunk_gen(df)
        else:
            return self.resample(data_gen)

    def resample(self, iterator: Iterable) -> Iterable[Dataset]:
        for batch in iterator:
            chunk = self.chunk(batch)
            if chunk is not None:
                yield chunk
            else:
                continue

            if self._buffer is not None:
                while self._buffer.shape[0] > self.chunk_size:
                    chunk = self._buffer.iloc[:self.chunk_size]
                    self._buffer = self._buffer.iloc[self.chunk_size:]
                    yield chunk

        # Always serve the "remainder" batch
        if self._buffer is not None:
            yield self._buffer

    
    def plain_text(self) -> Iterable[Dataset]:
        """
        This function takes a list of strings and returns a list of strings
        with duplicates removed.
        """
        self.reset()
        log.info("Generating \"plain_text\" dataset")

        data_gen = self._read_parquet(self.dir_path)
        data_gen = map(lambda df: (self.pbar.update(1), df)
                             [-1], data_gen)  # Update progress bar
        
        def convert_plain_text(df: pd.DataFrame) -> pd.DataFrame:
            df = df.rename(columns={'source_code': 'text'})
            df = df[['text', 'language']]
            return df

        # Add tmp file_name column
        plain_text_gen = map(convert_plain_text, data_gen)
        return plain_text_gen

    def chunk_gen(self, df: pd.DataFrame) -> Iterable[Dataset]:
        i = 0
        while i < df.shape[0]:
            yield df.iloc[i:i + self.chunk_size]
            i += self.chunk_size

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