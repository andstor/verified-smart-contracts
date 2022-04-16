from data_processor import DataProcessor
import logging

if __name__ == '__main__':
    import argparse
    import os

    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description='Generate dataset.')

    parser.add_argument('-s', '--source', metavar='source', type=str,
                        required=True, help='Path to directory with files to process.')
    parser.add_argument('-o', '--output', metavar='output', type=str,
                        required=True, help='The directory where the output files should be stored.')
    parser.add_argument('--chunk-size', metavar='chunk_size', type=int, required=False,
                        default=30000, help='The number of contracts to store in each data file. Default: 30000')
    parser.add_argument('--split-files', metavar='split_files', type=bool, action=argparse.BooleanOptionalAction,
                        default=False, help='The number of contracts to store in each data file.')
    parser.add_argument('--threshold', metavar='threshold', type=float, required=False,
                        default=0, help='The similarity threshold. Default: 0')
    args = parser.parse_args()

    if not os.path.exists(args.output):
        os.makedirs(args.output, exist_ok=True)

    dp = DataProcessor(args.source, args.chunk_size)
    data_gen = dp.process(args.split_files, args.threshold)
    for index, data in enumerate(data_gen):
        path = os.path.join(args.output, "part." + str(index) + ".parquet")
        data.to_parquet(path)
