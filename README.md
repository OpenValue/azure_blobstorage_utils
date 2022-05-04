Azure Blob Storage Utils
========================
A Python toolbox for Azure Blob Storage.

Documentation
-------------
Documentation is available [here](https://azure-blob-storage-utils.readthedocs.io/en/latest/)

Installation
------------
```bash
pip install git+https://github.com/OpenValue/azure_blobstorage_utils.git
```

With extensions (for extended usage) :

``` bash
pip install -e git+https://github.com/OpenValue/azure_blobstorage_utils.git#egg=azure-blobstorage-utils[extended]
```

Build
-----
```bash
git clone https://github.com/OpenValue/azure_blobstorage_utils.git
cd azure-blobstorage-utils
python setup.py bdist_wheel
```

Development
-----------
Install Anaconda:

See https://www.anaconda.com/distribution/#download-section

Edit `environment.yml` file and specify needed libraries

Install Anaconda local environment as below:

```bash
./install-conda-environment.sh
```

Activate Anaconda local environment as below:

```bash
conda activate ${PWD}/.conda
```