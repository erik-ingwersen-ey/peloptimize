# Notebooks

This directory contains a collection of Jupyter notebooks that demonstrate
how to run the different parts of the pipeline.

## Folder Contents


This directory contains the following Jupyter Notebooks:

|         **Notebook**         | **Description**                                          |   **Author**   |
|:----------------------------:|:---------------------------------------------------------|:--------------:|
|  01. Preprocessamento.ipynb  | Prepare sensor data to pass to each ML <br/>model.       | Erik Ingwersen |
| 02. Modelos Preditivos.ipynb | Train ML model for each step of the pelletizing process. | Erik Ingwersen |

## Running the Notebooks

To run the notebooks, first open up a new terminal,
navigate to the project root directory, and run the following command:

```bash
jupyter notebook
```

**Or:**

```bash
jupyter jupyrter lab
```

### Installing the Project Package and Dependencies

If you didn't install the project package and dependencies yet, execute the
following command, prior to running the notebooks:

```bash
pip install -e .
```

The `-e` flag installs the project package in editable mode, so that any changes
you make to the source code will be reflected in the package.
