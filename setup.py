import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="azure_blobstorage_utils",
    version="0.1.1",
    author="François Valadier",
    author_email="francois.valadier@gmail.com",
    description="Azure Blob Storage utils",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OpenValue/azure_blobstorage_utils.git",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where='src'),
    install_requires=["azure-storage-blob"],
    extras_require={
        "extended": ["pandas", "openpyxl", "opencv-python", "pyarrow"]
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License"],
)
