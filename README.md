#YouTube-Data-ExtractionStreamlit
Problem Statement
The task is to build a Streamlit app that permits users to analyze data from multiple YouTube channels. Users can input a YouTube channel ID to access data like channel information, video details, and user engagement. The app should facilitate storing the data in a MongoDB database and allow users to collect data from up to 10 different channels. Additionally, it should offer the capability to migrate selected channel data from the data lake to a SQL database for further analysis. The app should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.

Technology Stack Used
Python
Pandas
MySQL
Google Client Library
Approach
Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.
Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python.
Stored the retrieved data and converted that into Dataframe using Pandas library. Used to_sql method to store the dataframe into Sql database .
Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input.
The retrieved data is displayed within the Streamlit application.
