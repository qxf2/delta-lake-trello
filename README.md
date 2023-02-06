## Delta Lake implementation for Qxf2's Trello data

### Background
Delta lake is an open-source storage framework that brings reliability to data lakes by ensuring ACID transactions, scalable metadata handling and much more. It thereby enables building a Lakehouse architecure on top of existing data lakes. The Lakehouse follows a multi-layered approach to store data called medallion architecture. 
* Bronze layer - Contains raw state of the data. Can be combination of streaming and batch transactions
* Silver layer - Represents a cleaned and conformed version of data that can be used for downstream analytics
* Gold layer - highly refined and aggregated data serving specific use-case

### Data pipeline using Delta Lake tables
We have implemented the medallion architecture by using Delta Lake tables to store and analyse our Trello data. 
* Bronze Delta Lake tables (Ingestion) - The raw data of all the cards of Trello boards and the Trello board members is ingested into these tables
* Silver Delta Lake tables (Refinement) - The data from the Bronze tables is refined to pick required columns, join with the members lookup delta table and add board info 
* Gold Delta Lake tables (Aggregation) - Built based on specific use case. Eg: One of the tables consist of all the cards that are in doing list have no activity for specified number of days

### Storage and workloads
Currently the Delta Lake tables reside on local Linux system (an EC2 instance). Cron jobs are scheduled to load the data into the Delta Lake tables.
The ingestion and refinement run daily. And the aggregation on demand

### Tests



### Enchancements
- Improvise the data fetch for bronze delta tables
- Move the delta tables to data lake - AWS S3
- Visualization for the Gold Delta lake tables data
