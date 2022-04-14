from data_processor import DataProcessor
import logging

if __name__ == '__main__':
    import argparse
    import os

    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description='Process dataset.')

    parser.add_argument('-s', '--source', metavar='source', type=str,
                        required=True, help='Path to directory with files to process.')
    parser.add_argument('-o', '--output', metavar='output', type=str, required=False,
                        default="plain_text", help='The directory where the output files should be stored. Default: plain_text')
    parser.add_argument('--chunk-size', metavar='chunk-size', type=int, required=False,
                        default=30000, help='The number of contracts to store in each data file. Default: 30000')
    args = parser.parse_args()

    if not os.path.exists(args.output):
        os.makedirs(args.output, exist_ok=True)

    dp = DataProcessor(args.source, args.chunk_size)
    data_gen = dp.plain_text()

    for index, data in enumerate(data_gen):
        path = os.path.join(args.output, "part." + str(index) + ".parquet")
        data.to_parquet(path)
