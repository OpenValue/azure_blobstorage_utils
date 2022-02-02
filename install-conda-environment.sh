#!/usr/bin/env bash
echo "Removing old environment..."
rm -Rf .conda
rm -Rf .ipynb_checkpoints
echo "Removing old environment...OK"

echo "Installing new environment..."
conda env create -f environment.yml -p .conda
echo "Installing new environment...OK"


echo "Installing Jupyter kernel..."
# Create jupyter kernel with same name from environment.yml file
eval "$(conda shell.bash hook)"
conda config --append envs_dirs ${PWD}
conda activate ${PWD}/.conda

ipython kernel install --name $(head -1 environment.yml | cut -d' ' -f2)
echo "Installing Jupyter kernel...OK"
