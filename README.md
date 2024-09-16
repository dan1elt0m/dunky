# Dunky

A Jupyter Kernel for DuckDB with Unity Catalog.

## Description

Dunky is a Jupyter kernel that allows you to run DuckDB queries with Unity Catalog integration directly from your Jupyter notebooks.

## Installation

To install Dunky, you can use the following commands:

```sh
pip install dunky
```

## Configure Unity Catalog
You can set the following environment variables to configure Unity Catalog:

UC_ENDPOINT: The endpoint of the Unity Catalog server.
UC_TOKEN: The token to authenticate with the Unity Catalog server. 
UC_AWS_REGION: The AWS region to use for the Unity Catalog server.

settings default to localhost:8080/api/2.1/unity-catalog, not-used, and eu-west-1 respectively.

## Usage
After installing, you can start using the Dunky kernel in your Jupyter notebooks. Select the "DuckDB" kernel from the kernel selection menu.

