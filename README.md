[![Github Actions Status](https://github.com/dan1elt0m/dunky/workflows/test/badge.svg)](https://github.com/dan1elt0m/dunky/actions/workflows/test.yml)

# Dunky

A Jupyter Kernel for DuckDB with Unity Catalog.

![Dunky Demo](./docs/demo.gif)

### Description


Dunky is a Jupyter kernel that allows you to run DuckDB queries with Unity Catalog integration directly from your Jupyter notebooks.

I created this extension because existing solutions such as `jupysql` require you to use magics, load uc_catalog, delta, and manage secrets
and don't work well with duckdb's uc_catalog extension.

### Features
- Run DuckDB queries in Jupyter notebooks
- Unity Catalog integration
- No need to use magics
- Nice output formatting
- No need to load uc_catalog, delta and manage secrets 
- CREATE EXTERNAL TABLE [table_name] LOCATION [location] OPTIONS [options] to create a Unity Catalog delta table 

### Installation

To install Dunky, you can use the following commands:

```sh
pip install dunky
```

### Configure Unity Catalog
You can set the following environment variables to configure Unity Catalog:

- `UC_ENDPOINT`: The endpoint of the Unity Catalog server.
- `UC_TOKEN`: The token to authenticate with the Unity Catalog server. 
- `UC_AWS_REGION`: The AWS region to use for the Unity Catalog server.

These settings default to localhost:8080/api/2.1/unity-catalog, not-used, and eu-west-1 respectively.

### Usage
After installing, you can start using the Dunky kernel in your Jupyter notebooks. 
Select the "Dunky" kernel from the kernel selection menu.

You can directly query DuckDB tables and use Unity Catalog features in your notebooks. 
You don't need to set up a connection or manage credentials, as Dunky handles all of that for you.

Start with attaching your database using the ATTACH DATABASE command. e.g., 
```bash
ATTACH DATABASE 'unity' AS unity (TYPE UC_CATALOG);
```

After attaching, just start writing your queries and enjoy the power of DuckDB with Unity Catalog integration!


### S3 Integration
Dunky supports AWS S3 integration with Unity Catalog.
- prerequisite: 
  - Make sure the unity catalog has S3 bucket authentication configured 
- Writing to S3: in the CREATE EXTERNAL TABLE set location to `s3://your-bucket-name`

ps. Dunky might also work with gcp and azure, but have not tested this. depends on whether unity and duckdb uc_catalog
support it. I've seen some people confirming that unity catalog and duckdb can work with Azure and gcp. 


### Example docker
In the `docker` folder, you can find an example of how to run JupyterLab with Dunky and Unity Catalog in Docker containers.
To run the example, execute:

```bash
cd docker
docker compose up --build -d
```
token/password = dunky

If not already selected, you can find Dunky kernel in the kernel list.

### Remarks
- This kernel is still in development and may have some bugs.
- This extension works well together with the [junity](https://github.com/dan1elt0m/junity) extension.


### Issues?
If you encounter any issues, please open an issue on the [GitHub repository](https://github.com/dan1elt0m/dunky/issues).