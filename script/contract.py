
import pandas as pd

from contract_file_handler import ContractFileHandler
from utils import str2bool


class Contract():

    labels = {
        'contract_name',
        'file_path',
        'contract_address',
        'language',
        'source_code',
        'abi',
        'compiler_version',
        'optimization_used',
        'runs',
        'constructor_arguments',
        'evm_version',
        'library',
        'license_type',
        'proxy',
        'implementation',
        'swarm_source'
    }

    etherscan_col_map = {
        'ContractName': 'contract_name',
        'SourceCode': 'source_code',
        'ABI': 'abi',
        'CompilerVersion': 'compiler_version',
        'OptimizationUsed': 'optimization_used',
        'Runs': 'runs',
        'ConstructorArguments': 'constructor_arguments',
        'EVMVersion': 'evm_version',
        'Library': 'library',
        'LicenseType': 'license_type',
        'Proxy': 'proxy',
        'Implementation': 'implementation',
        'SwarmSource': 'swarm_source',
    }

    def __init__(
        self,
        contract_name: str = None,
        file_path: str = None,
        contract_address: str = None,
        language: str = None,
        source_code: str = None,
        abi: str = None,
        compiler_version: str = None,
        optimization_used: bool = None,
        runs: int = None,
        constructor_arguments: str = None,
        evm_version: str = None,
        library: str = None,
        license_type: str = None,
        proxy: int = None,
        implementation: str = None,
        swarm_source: str = None
    ):
        # Initialize all fields to None
        self._data = {
            'contract_name': contract_name,
            'file_path': file_path,
            'contract_address': contract_address,
            'language': language,
            'source_code': source_code,
            'abi': abi,
            'compiler_version': compiler_version,
            'optimization_used': str2bool(optimization_used),
            'runs': int(runs) if runs else None,
            'constructor_arguments': constructor_arguments,
            'evm_version': evm_version,
            'library': library,
            'license_type': license_type,
            'proxy': str2bool(proxy),
            'implementation': implementation,
            'swarm_source': swarm_source,
        }

        self.code_format = None
        self.files = []
        if self._data["source_code"]:
            self._process_source_code()

        # Try to infer the contract file name from the contract name
        if len(self.files) == 1:
            # TODO: Assess if this is necessary
            self._data["file_path"] = self._data["file_path"] or self._data["contract_name"] + \
                self.get_file_extension()

    def __str__(self):
        return self._data["contract_name"]

    def __repr__(self):
        # pd.Series(self.__dict__).to_string()
        return self._data["contract_name"] + " - " + str(self._data["file_path"])

    def __getitem__(self, key):
        return self._data[key]

    @classmethod
    def from_dict(cls, dict: dict):
        df = {}
        for k, v in dict.items():
            if k in cls.labels:
                df[k] = v
            else:
                raise AttributeError("Unknown attribute: %s" % k)

        return cls(**df)

    @classmethod
    def from_etherscan_dict(cls, address: str, dict: dict):
        df = {}
        for k, v in dict.items():
            if k in cls.etherscan_col_map:
                df[cls.etherscan_col_map[k]] = v
            else:
                raise AttributeError("Unknown Etherscan attribute: %s" % k)

        return cls(contract_address=address, **df)

    def _process_source_code(self):
        # Check for Solidity Standard Json-Input format
        if "vyper" in self._data["compiler_version"]:
            self.code_format = "Text"
            self._data["language"] = "Vyper"

        elif self._data["source_code"][:2] == "{{":
            # Fix JSON by removing extranous curly brace
            self._data["source_code"] = self._data["source_code"][1:-1]
            self.code_format = "JSON"
            self._data["language"] = "Solidity"

        elif self._data["source_code"][:1] == "{":
            self.code_format = "Multi"
            self._data["language"] = "Solidity"

        else:
            self.code_format = "Text"
            self._data["language"] = "Solidity"

        handler = ContractFileHandler(
            self.code_format, self.get_file_extension())
        self.files = handler.extract(self._data["source_code"])

    def get_file_extension(self):
        if self._data["language"] == "Solidity":
            return ".sol"
        elif self._data["language"] == "Vyper":
            return ".vy"
        else:
            return ""

    def explode(self):
        """
        Extracts contracts from the source code
        """
        contracts = []
        for file in self.files:
            data = self._data.copy()
            data["source_code"] = file['source_code']
            data["file_path"] = file['file_path']
            contract = Contract(**data)
            contracts.append(contract)
        return contracts

    def normalize(self):
        """
        Produces a single contract source code from all files in the contract.
        """
        if self.code_format != "Text":
            source_code = []
            for file in self.files:
                if file['file_path']:
                    source_code.append(
                        "// File: " + file['file_path'] + "\n\n\n")
                    source_code.append(file['source_code'])
                else:
                    source_code.insert(0, file['source_code'])
            self._data["source_code"] = "\n".join(source_code)
            self.code_format = "Text"

        # Reset filename
        self._data["file_path"] = ""

        return self

    def to_dict(self, labels=None):
        data = self._data.copy()
        if labels:
            data = {key: value for key, value in data.items() if key in labels}
        return data
