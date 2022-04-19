# smart-contracts

> :page_facing_up: Verified Ethereum Smart Contract dataset

Smart Contracts is a dataset of real Ethereum Smart Contract, containing both Solidity and Vyper source code. It consists of every deployed Ethereum Smart Contract as of :black_joker: 1st of April 2022, whose been verified on Etherescan and has a least one transaction.
The dataset is available at 🤗 [Hugging Face](https://huggingface.co/datasets/andstor/smart_contracts).

The data format used is parquet files, each with a total of 30,000 records.

https://huggingface.co/datasets/andstor/smart_contracts/tree/main/data/raw
https://huggingface.co/datasets/andstor/smart_contracts/tree/main/data/flattened
https://huggingface.co/datasets/andstor/smart_contracts/tree/main/data/inflated

## Description

### Raw
The raw dataset contains mostly the raw data from Etherscan, downloaded with the [smart-contract-downlader](https://github.com/andstor/smart-contract-downloader) tool. It normalizes all different contract formats (JSON, multi-file, etc.) to a flattened source code structure.

```script
python script/2parquet.py -s data -o parquet
```

### Flattened
The flattened dataset contains smart contracts, where every contract contains all required library code. Each "file" is marked in the source code with a comment stating the original file path: `//File: path/to/file.sol`. These are then filtered for uniqeness by requiring a uniqeness factor of 0.5. The low uniqeness requirement is due to the often large amount of embedded library code. If a more unique dataset is required, see the [inflated](#inflated) dataset instead.

```script
python script/filter_data.py -s parquet -o data/flattened --threshold 0.5
```

### Inflated
The inflated dataset splits every contracts into its representative files. These are then filtered for uniqeness by requiring a uniqeness factor of 0.9.

```script
python script/filter_data.py -s parquet -o data/inflated --split-files --threshold 0.9
```

### Plain Text
A subset of the datasets above can be created by using the `2plain_text.py` script. This will produce a plain text dataset with the columns `text` (source code) and `language`.

```script
python script/2plain_text.py -s data/inflated -o data/inflated_plain_text
```
This will produce a plain text version of the inflated dataset, and save it to `data/inflated_plain_text`.

## Filtering
A large quantity of the Smart Contracts is/contains duplicated code. This is mostly due to frequent use of library code. Etherscan embeds the library code used in a contract in the source code. To mitigate this, some filtering is applied in order to produce dataset with mostly unique contract source code. This filtering is done by calculating the string distance between the surce code. Due to the large amount of contracts (~2 million), the comparison is only done in groups by `contract_name` for the flattened dataset, and by `file_name` for the inflated dataset.

The string comparison algorithm used is the [Jaccard index](https://en.wikipedia.org/wiki/Jaccard_index).

## Metrics
TODO

## License

Copyright © [André Storhaug](https://github.com/andstor)

This repository, is licensed under the [MIT License](https://github.com/andstor/verified-smart-contracts/blob/main/LICENSE).

All contracts in the dataset are publicly available, obtained by using [Etherscan APIs](https://etherscan.io/apis), and subject to their own original licenses.
