# Dunky

A Jupyter Kernel for DuckDB with Unity Catalog.

## Description

Dunky is a Jupyter kernel that allows you to run DuckDB queries with Unity Catalog integration directly from your Jupyter notebooks.

## Features
- Run DuckDB queries in Jupyter notebooks
- Unity Catalog integration
- No need to use magics
- Nice output formatting
- No need to load uc_catalog, delta and manage secrets 

## Installation

To install Dunky, you can use the following commands:

```sh
pip install dunky
```

## Configure Unity Catalog
You can set the following environment variables to configure Unity Catalog:

- UC_ENDPOINT: The endpoint of the Unity Catalog server.
- UC_TOKEN: The token to authenticate with the Unity Catalog server. 
- UC_AWS_REGION: The AWS region to use for the Unity Catalog server.

These settings default to localhost:8080/api/2.1/unity-catalog, not-used, and eu-west-1 respectively.

## Usage
After installing, you can start using the Dunky kernel in your Jupyter notebooks. 
Select the "Dunky" kernel from the kernel selection menu.

You can directly query DuckDB tables and use Unity Catalog features in your notebooks. 
You don't need to set up a connection or manage credentials, as Dunky handles all of that for you.

Start with attaching your database using the ATTACH DATABASE command. e.g., 

```bash
ATTACH DATABASE 'unity' AS unity (TYPE UC_CATALOG);
```
After attaching, just start writing your queries and enjoy the power of DuckDB with Unity Catalog integration!


### Remarks
This extension works well together with the junity extension