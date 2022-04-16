# smart-contracts
:page_facing_up: Verified Ethereum Smart Contract dataset



raw:
```script
python script/filter_data.py -s parquet -o data/raw --threshold 0
```

raw_files
```script
python script/filter_data.py -s parquet -o data/raw_files --split-files --threshold 0
```

all
```script
python script/filter_data.py -s parquet -o data/all --threshold 0.5
```

all_files 
```script
python script/filter_data.py -s parquet -o data/all_files --split-files --threshold 0.9
```

all_plain_text
```script
python script/2plain_text.py -s data/all -o data/all_plain_text
```

all_files_plain_text
```script
python script/2plain_text.py -s data/all_files -o data/all_files_plain_text
```