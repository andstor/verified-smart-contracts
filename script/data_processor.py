from concurrent.futures import ProcessPoolExecutor
import enum
from multiprocessing import Pool, cpu_count
from multiprocessing.pool import ThreadPool
from random import shuffle
from re import S
from typing import Iterable
from numpy import number
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
        if row[column] == first_row[column]:
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

        self.tot_dupes = 0
        self.count = 0

        self.pbar = None

    def reset(self):
        self._buffer = None
        self.tot_dupes = 0
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

    def applyParallel(self, df_grouped, func):
        with Pool() as p:
            #ret_list = p.map(func=func, iterable=[group for name, group in df_grouped])
            iterable = [group for name, group in df_grouped]
            chunksize = 100
            res_list = []
            with tqdm(total=len(iterable), desc="Filtering", leave=False) as pbar:
                for res in p.imap_unordered(func=func, iterable=iterable, chunksize=chunksize):
                    pbar.update()
                    res_list.append(res)
        return list(itertools.chain.from_iterable(res_list))

    def _uniqify(self, df: pd.DataFrame, grouping_column, threshold=0.9) -> pd.DataFrame:
        shuffled_df = df.sample(frac=1)
        df_group = shuffled_df.groupby(grouping_column)

        func = partial(find_dupes, grouping_column, threshold)
        dupe_indexes = self.applyParallel(df_group, func)

        self.tot_dupes += len(dupe_indexes)
        self.count += df.shape[0]
        self.pbar.set_postfix(
            dupes=str(self.tot_dupes) + "/" + str(self.count),
            dupes_percentage=str(round(self.tot_dupes*100/self.count, 2)) + "%"
        )

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

    def process(self, split_files: bool, threshold: float) -> Iterable[Dataset]:
        self.reset()
        log.info("Generating dataset")
        log.info("Split files: \"" + str(split_files) + "\"")
        log.info("Similarity threshold: \"" + str(threshold) + "\"")
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

        if threshold: 
            grouping_column = "file_name" if split_files else "contract_name"
            filter_func = partial(
                self._uniqify, grouping_column=grouping_column, threshold=threshold)
            df = merge_filter(data_gen, filter_func)

            if split_files:
                df.drop(columns='file_name', inplace=True)  # Drop tmp file_name column
            
            return self.chunk_gen(df)

        else:
            for batch in data_gen:
                chunk = self.chunk(batch)
                if chunk is not None:
                    yield chunk
                else:
                    continue
            
            # Always serve the "rest" batch
            while self._buffer is not None:
                chunk = self._buffer.iloc[:self.chunk_size]
                self._buffer = self._buffer.iloc[self.chunk_size:]
                if self._buffer.shape[0] == 0:
                    self._buffer = None
                yield chunk
    
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