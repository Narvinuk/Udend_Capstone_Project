# Udend_Capstone_Project

<b>Scope of Works :</b>

The purpose of this project is to demonstrate various skills associated with data engineering projects. In particular, developing ETL pipelines using Airflow, constructing data warehouses through Redshift databases and S3 data storage as well as defining efficient data models e.g. star schema. As an example I will perform a deep dive into US immigration, primarily focusing on the type of visas being issued and the profiles associated. The scope of this project is limited to the data sources listed below with data being aggregated across numerous dimensions such as visatype, gender, port_of_entry, nationality and month.

<b>Data Description & Sources: </b> 


<b>I94 Immigration Data: </b> This data comes from the US National Tourism and Trade Office found here. Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).

<b> U.S. City Demographic Data: </b> This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 

<b> Airport Code Table: </b>  This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia). 

After extracting various immigration codes from the I94_SAS_Labels_Descriptions.SAS file, I was able to define a star schema by extracting the immigration fact table and various dimension tables as shown below:

![Image description](https://github.com/Narvinuk/Udend_Capstone_Project/blob/master/Capture_DM.JPG)

### Data Storage

Data was stored in S3 buckets in  PARQUET files. The immigration dataset extends to several million rows and thus this dataset was converted to PARQUET files to allow for easy data manipulation and processing through Dask and the ability to write to Redshift.

### ETL Pipeline

Defining the data model and creating the star schema involves various steps, made significantly easier through the use of Airflow. The process of extracting files from S3 buckets, transforming the data and then writing  PARQUET files to Redshift is accomplished through various tasks highlighted below in the ETL Dag graph. These steps include: - Extracting data from PARQQUET files on S3  to Redshift staing tables, followed by Dim loads from Redshift DB tables and then load to Fact and data quality checks

![Image description](https://github.com/Narvinuk/Udend_Capstone_Project/blob/master/capstone_af.jpg)

### Conclusion
Overall this project was a small undertaking to demonstrate the steps involved in developing a data warehouse that is easily scalable. Skills include:

Creating a Redshift Cluster, IAM Roles, Security groups.
* Developing an ETL Pipeline that copies data from S3 buckets into staging tables to be processed into a star schema
* Developing a star schema with optimization to specific queries required by the data analytics team.
* Using Airflow to automate ETL pipelines using Airflow, Python, Amazon Redshift.
* Writing custom operators to perform tasks such as staging data, filling the data warehouse, and validation through data quality checks.
* Transforming data from various sources into a star schema optimized for the analytics team's use cases.







