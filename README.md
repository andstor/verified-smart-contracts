# smart-contracts
:page_facing_up: Verified Ethereum Smart Contract dataset



raw:
```script
python script/filter_data.py -s parquet -o data/raw --split-files False --threshold 0
```

raw_files
```script
python script/filter_data.py -s parquet -o data/raw_files --split-files True --threshold 0
```

all
```script
python script/filter_data.py -s parquet -o data/all --split-files False --threshold 0.5
```

all_files
```script
python script/filter_data.py -s parquet -o data/all_files --split-files True --threshold 0.9
```

all_files:plain_text
```script
python script/2plain_text.py -s data/all_files -o data/plain_text
```