<Data Pipeline to Deliver i94 Visitor Data for Exploratory Analysis>

   * [Description:](#description)

This is the Capstone project for the Udacity data engineering degree.  The purpose is to utilise skills and tools presented during the program.

The primary dataset is the i94 visitor data set published by for the year 2016.

The backbone of the project is a data pipeline implemented in Apache Airflow and executing jobs on Amazon EMR to produce transformed but non-aggregated i94 visitor data in parquet format on Amazon S3.

Additional steps include extraction and transformation of SAS files, web scraping of flight departure and arrival airports plus the very straightforward manual upload of files to S3.

The project produces parquet files of i94 visitor data which can be joined with flight number, airport and city data to give the potential for exploratory data analysis, geolocation analysis and potential for exploring the potential of graph databases.






The project includes:

- Step 1 - Deployed on Udacity System
  - Extract i94 data from SAS files
  - Transform nulls and nans
  - Write to chunked csv files on S3

- Step 2 - Deployed on Local Docker Airflow
  - Extract i94 data from S3 csv files  
  - Transform fields by merging lookups



The target purpose for the project data pipeline is to deliver a data set which is suitable for Data Scientists and Analysts at the United States National Travel and Tourism Office (NTTO) to carry out exploratory data analysis with the aim of providing additional data products to State Tourism Offices and businesses which may benefit from new insights.  The dataset is also intended as a dataset for exploring the potential for several machine learning algorithms which are suited to target data platform.


# Description:
A description of your project follows. A good description is clear, short, and to the point. Describe the importance of your project, and what it does.

Table of Contents: Optionally, include a table of contents in order to allow other people to quickly navigate especially long or detailed READMEs.

Installation: Installation is the next section in an effective README. Tell other users how to install your project locally. Optionally, include a gif to make the process even more clear for other people.

Usage: The next section is usage, in which you instruct other people on how to use your project after they’ve installed it. This would also be a good place to include screenshots of your project in action.

Contributing: Larger projects often have sections on contributing to their project, in which contribution instructions are outlined. Sometimes, this is a separate file. If you have specific contribution preferences, explain them so that other developers know how to best contribute to your work. To learn more about how to help others contribute, check out the guide for setting guidelines for repository contributors.

Credits: Include a section for credits in order to highlight and link to the authors of your project.

License: Finally, include a section for the license of your project. For more information on choosing a license, check out GitHub’s licensing guide!

Your README should contain only the necessary information for developers to get started using and contributing to your project. Longer documentation is best suited for wikis, outlined below.
