# Automatic NRG Systems files data upload to SQL Server

This project decrypt `rld` files, downloaded from NRG Systems Loggers, and uploads its data to a SQL Server 2017 database, in an automated way. It uses [nrgpy](https://pypi.org/project/nrgpy/) to decrypt the `rld` files and obtain plain text files. After that, it reads each resultant text file searching for its data to finally upload it on database tables.

It's set to use a SQL Server 2017 database, but if you want to use a different database you will need to change the environment variables and connection settings.

![Texto alternativo](Global_Asset_Lineage.svg)

This is a [Dagster](https://dagster.io/) project that defines 3 group of assets:
    <ol>
        <li><p>**RLD files search:** dedicated to list encrypted files from Symphonie's raw directory and compare it with a record of uploaded files to SQL Server, stored in a **DuckDB** local database table. At the end of this asset group, all files that have not passed yet through the pipeline will be copied into "RLD_INPUT" project's directory. Symphonie's raw directory is set on a environment variable.</li>
        <li>**RLD to Text conversion:** it will use a **nrgpy** local converter to decrypt all files in "RLD_INPUT" and put the resultant text files in "TXT_OUTPUT" project's directory. The decrypt proccess could fail sometimes, so I set `CONV_ATTEMP` environment variable to define a maximun number of attempts for this process.</li>
        <li>**Data upload to database:** dedicated to obtain the data of each text file (decrypted `rld` files) and then upload to the database. It will create a table for this, if it doesn't exist, taking care of column match. Table name is created using "Site number" parameter from the resultant text files. Record of uploaded files is kept in the same **DuckDB** table referred above.</li>
    </ol>

In addition to the extracted sensor measurements from the decrypted `rld` files, sensors and loggers parameters will be extracted and uploaded too into two tables named "NRG_Loggers" and "NRG_Sensors".

I also configured a Dagster schedule `nrg_mssql_schedule` to run automatically the pipeline at 00:00 every day, however you can modify it in the `rld_to_mssql/__init__.py` file.

**IMPORTANT**: make sure that the database username and password you set in the `.env` file can read the database *INFORMATION_SCHEMA* and has grants to *CREATE* and *ALTER* database tables.

## Requirements

This application was developed using **Python** 3.11.5 so be sure to have this version or above and **pip** to install dependeces. You will need also:

### SymphoniePro Desktop Aplication

Is required to install [SymphoniePro Desktop Aplication](https://www.nrgsystems.com/support/product-support/software/symphoniepro-desktop-application/) as this project uses **nrgpy**, wich interacts with this software. However, you can use [NGR Cloud](https://www.nrgsystems.com/apps/cloud/detail/nrg-cloud/) too but the project will need some changes on the **convert_rld_to_txt** asset. 

If for some reason, you want to use this project on a Linux based system you will have to use NRG Cloud Services, because SymphoniePro is only implemented for Windows systems.

### SQL Server ODBC

You will need also [SQL Server ODBC](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16) as its required by **sqlalchemy** and **pymssql**.

If you want to use a different relational database, like Postgres or MySQL, you will have to change bits of code in the `db_manager.py` file and declare needed packages on the ``setup.py`` file. 

I provide a short `docker-compose.yml` file wich creates a SQL Server 2017 instance for local development.

## Getting started

First clone this repository code:

```bash
git clone https://github.com/darespinoza/rld_to_mssql.git
```

It's recommended to use a Python virtual environment. To create one use:

```bash
python -m venv venv
```

Activate your new virtual environment (Windows):

```bash
.\venv\Scripts\activate
```

Install Dagster's framework:

```bash
pip install dagster
```

Navigate into project directory and create Dagster's home directory. After that, set the `DAGSTER_HOME` environment variable on the `.env` file with the absolute path of the directory you just created.

```bash
cd rld_to_mssql
mkdir dagster_data
```

Install dependences:

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Development

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project). 

You can start writing assets in rld_to_mssql/assets.py. The assets are automatically loaded into the Dagster code location as you define them.

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `rld_to_mssql_tests` directory and you can run tests using `pytest`:

```bash
pytest rld_to_mssql_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.