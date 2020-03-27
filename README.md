# Udend_Capstone_Project

<b>Scope of Works :</b>

The purpose of this project is to demonstrate various skills associated with data engineering projects. In particular, developing ETL pipelines using Airflow, constructing data warehouses through Redshift databases and S3 data storage as well as defining efficient data models e.g. star schema. As an example I will perform a deep dive into US immigration, primarily focusing on the type of visas being issued and the profiles associated. The scope of this project is limited to the data sources listed below with data being aggregated across numerous dimensions such as visatype, gender, port_of_entry, nationality and month.

<b>Data Description & Sources: </b> 


<b>I94 Immigration Data: </b> This data comes from the US National Tourism and Trade Office found here. Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).

<b> U.S. City Demographic Data: </b> This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 

<b> Airport Code Table: </b>  This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia). 

After extracting various immigration codes from the I94_SAS_Labels_Descriptions.SAS file, I was able to define a star schema by extracting the immigration fact table and various dimension tables as shown below:

![Image description](link-to-image)



