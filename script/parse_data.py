import pathlib
import sys
parser_dir = pathlib.Path(__file__).parent.parent / 'solidity-universal-parser' / 'parser'
sys.path.append(str(parser_dir))
from python3.SolidityLexer import SolidityLexer
from python3.SolidityParser import SolidityParser

from typing import Iterable
from antlr4 import InputStream, CommonTokenStream
from comment_visitor import CommentVisitor
import pandas as pd
from tqdm import tqdm
import itertools
from multiprocessing import Pool


def chunk_gen(df: pd.DataFrame, chunk_size: int) -> Iterable:
    i = 0
    while i < df.shape[0]:
        yield df.iloc[i:i + chunk_size]
        i += chunk_size


def applyParallel(iterable, total, func, chunksize=1):
    with Pool() as p:
        res_list = []
        with tqdm(total=total, desc="Extracting", leave=False) as pbar:
            for res in p.imap_unordered(func=func, iterable=iterable, chunksize=chunksize):
                pbar.update()
                res_list.append(res)

    return list(itertools.chain.from_iterable(res_list))


def parse_data_worker(iterrows):
    index, row = iterrows
    
    extract = []
    try:
        code = row['source_code']
        input_stream = InputStream(code)
        lexer = SolidityLexer(input_stream)
        lexer.removeErrorListeners() # remove default error listener
        stream = CommonTokenStream(lexer)
        parser = SolidityParser(stream)
        parser.removeErrorListeners() # remove default error listener
        visitor = CommentVisitor()
        tree = parser.sourceUnit()
        extract = visitor.visit(tree)
        for entry in extract:
            entry['contract_name'] = row['contract_name']
            entry['file_path'] = row['file_path']
            entry['contract_address'] = row['contract_address']
            entry['language'] = row['language']
            entry['compiler_version'] = row['compiler_version']
            entry['license_type'] = row['license_type']
            entry['swarm_source'] = row['swarm_source']

    except RecursionError as re:
        print(re)
        pass

    return extract


def parse_data(dir_path: str, chunk_size: int) -> pd.DataFrame:

    df = pd.read_parquet(dir_path)
    df = df[df['language'] == 'Solidity']
    df_gen = df.iterrows()
    data = applyParallel(df_gen, df.shape[0], parse_data_worker, chunksize=1)
    data_df = pd.DataFrame(
        data,
        columns=[
            "contract_name", "file_path", "contract_address", "language",
            "class_name", "class_code", "class_documentation", "class_documentation_type",
            "func_name", "func_code", "func_documentation", "func_documentation_type",
            "compiler_version", "license_type", "swarm_source"]
    )
    data_df = data_df.astype({
        'contract_name': "string",
        'file_path': "string",
        'contract_address': "string",
        'language': "string",
        'class_name': "string",
        'class_code': "string",
        'class_documentation': "string",
        'class_documentation_type': "string",
        'func_name': "string",
        'func_code': "string",
        'func_documentation': "string",
        'func_documentation_type': "string",
        'compiler_version': "string",
        'license_type': "string",
        'swarm_source': "string",
    })
    return chunk_gen(data_df, chunk_size)


if __name__ == '__main__':
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description='Generate dataset.')

    parser.add_argument('-s', '--source', metavar='source', type=str,
                        required=True, help='Path to directory with files to process.')
    parser.add_argument('-o', '--output', metavar='output', type=str,
                        required=True, help='The directory where the output files should be stored.')
    parser.add_argument('--chunk-size', metavar='chunk_size', type=int, required=False,
                        default=30000, help='The number of records to store in each data file. Default: 30000')
    args = parser.parse_args()

    if not os.path.exists(args.output):
        os.makedirs(args.output, exist_ok=True)

    data_gen = parse_data(args.source, args.chunk_size)
    for index, data in enumerate(data_gen):
        path = os.path.join(args.output, "part." + str(index) + ".parquet")
        data.to_parquet(path)
