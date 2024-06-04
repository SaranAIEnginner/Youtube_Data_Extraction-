import pandas as pd
import plotly.express as px
import streamlit as st
import mysql.connector as sql
from PIL import Image
import sqlite3
import streamlit_option_menu as som
from googleapiclient.discovery import build

class DataExtraction:
    channel_id=[]
    questions=['Click the question that you would like to query',
            '1. What are the names of all the videos and their corresponding channels?',
            '2. Which channels have the most number of videos, and how many videos do they have?',
            '3. What are the top 10 most viewed videos and their respective channels?',
            '4. How many comments were made on each video, and what are their corresponding video names?',
            '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
            '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
            '7. What is the total number of views for each channel, and what are their corresponding channel names?',
            '8. What are the names of all the channels that have published videos in the year 2022?',
            '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
            '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
    
    def __init__(self):
        self.api_key = "AIzaSyByrwEr_hen1D5IvW3vcpEMX7b8gAwL3oE"
        self.youtube = build('youtube','v3',developerKey=self.api_key)
        self.video_ids = []
        self.ch_data = []
        self.video_stats = []
        self.Comment_data=[]
        self.selected=object
        self.ch_name=""
        #database connection
        self.connection = sqlite3.connect('Saran_Proj.db')

     #Side bar menu specifications
    def sidebarMenu(self):
        with st.sidebar:
            self.selected =som.option_menu(None, ["Extract and Store","Questions"], 
            icons=["youtube","card-text"],
            default_index=0,
            orientation="vertical",
            styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
            "--hover-color": "#C80101"},
            "icon": {"font-size": "30px"},
            "container" : {"max-width": "6000px"},
            "nav-link-selected": {"background-color": "#FF0000"}})  
             
    #Extract channel details using channel id            
    def get_channel_details(self,channel_id):
        
        response = self.youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id)
        print("response",response)
        for i in range(len(response['items'])):
            data = dict(Channel_id = channel_id[i],
                        Channel_name = response['items'][i]['snippet']['title'],
                        Subscribers = response['items'][i]['statistics']['subscriberCount'],
                        Views = response['items'][i]['statistics']['viewCount'],
                        Total_videos = response['items'][i]['statistics']['videoCount'],
                        Description = response['items'][i]['snippet']['description'],
                        Country = response['items'][i]['snippet'].get('country')
                        )
            self.ch_data.append(data)
            return self.ch_data,self.ch_name 
    
     # Method to get viodeo id using channel id
    def get_channel_video_id(self,channel_id):
        
        res =self.youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
        playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None
    
        while True:
            res = self.youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
            for i in range(len(res['items'])):
                self.video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token = res.get('nextPageToken')
        
            if next_page_token is None:
                break
        return self.video_ids  
    
    # get video details using video id 
    def get_video_details(self,video_ids):
            
        for i in range(0, len(video_ids), 50):
            response = self.youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(video_ids[i:i+50])).execute()
            for video in response['items']:
                video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                ViewCount = video['statistics']['viewCount'],
                                LikeCount = video['statistics'].get('likeCount'),
                                Comments_Count = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
                self.video_stats.append(video_details)
        return self.video_stats
    
    #to get comments details
    def get_comment_details(self,video_ids):
        
        try:
            for video_id in video_ids:
                request=self.youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
                )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                self.Comment_data.append(data)
                
        except:
            pass
        return self.Comment_data
    
     #Extract and store data to db        
    def extrtactPage(self):
        if self.selected == "Extract and Store":
            st.markdown("#    ")
            st.write("### Enter YouTube Channel_ID below :")
            channel_id= st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
            print("channel id",channel_id)
            if st.button("Submit"):
                try:
                    dataEx.get_channel_details(DataExtraction.channel_id) 
                    dataEx.get_channel_video_id(DataExtraction.channel_id)
                    dataEx.get_video_details(dataEx.video_ids)
                    dataEx.get_comment_details(dataEx.video_ids)
                    dataEx.store_channel_data()
                
                    st.success("Transformation to MySQL Successful!!!")
                except:
                    st.error("Channel details already transformed!!")               
#===================================================================================================================

    #convert List of Dictionary data into pandas data frame and store dataframe into database
    def covert_PdData_StoreSql(self,channels_data, channel_names):
       
        s=pd.DataFrame(channels_data)
        dataframe=pd.DataFrame.from_dict(s)
        print("dataframe",dataframe)
        column_name = 'Channel_name'
        value_to_find = self.ch_name
        result = self.connection.execute((f"SELECT * FROM Channels_Details WHERE {column_name} = :value"), {'value': value_to_find})
        rows = result.fetchall()
        if len(rows)!=0:
            # Print the rows found
            print("Query Results:")
            for row in rows:
                if self.ch_name not in row:
                    dataframe.to_sql(channel_names, self.connection, if_exists="append", index=False)
                else:
                    print("Channel data is already present")
        else:
            dataframe.to_sql(channel_names, self.connection, if_exists="append", index=False)
            tables = self.connection.execute((f"SELECT * FROM Channels_Details WHERE {column_name} = :value"), {'value': value_to_find})
            st.table(tables)
                         
    def select_tables_fromDB(self,query): 
        tables = pd.read_sql(query, self.connection)
        print("The inserted values are : ",tables)

    def store_channel_data(self):
        #To store channel data
        dataEx.covert_PdData_StoreSql(dataEx.ch_data, "Channels_Details")
        dataEx.select_tables_fromDB("""
            SELECT * from Channels_Details
            """)
        
        #To store channel Video data
    def store_video_data(self):
        #To store video data
        dataEx.covert_PdData_StoreSql(dataEx.video_stats,"Video_Details")
        dataEx.select_tables_fromDB("""
            SELECT * from Video_Details
            """)
        
    #To store Video comment data  
    def store_comment_data(self):
        
        dataEx.covert_PdData_StoreSql(dataEx.Comment_data,"Comments_Details")
        dataEx.select_tables_fromDB("""
            SELECT * from Comments_Details
            """)
     #question page   
    def questionPage(self):
        
            cursor=self.connection.cursor()
            if self.selected == "Questions":    
                st.write("## :orange[Select any question to get Insights]")
                DataExtraction.questions = st.selectbox('Questions',DataExtraction.questions)         
            
            if DataExtraction.questions == '1. What are the names of all the videos and their corresponding channels?':
                cursor.execute("""SELECT title AS Video_Title, Channel_name AS Channel_Name FROM Video_Details ORDER BY Channel_name""")
                df = pd.DataFrame(cursor.fetchall())
                print(df)
                st.write(df)
            
            elif DataExtraction.questions == '2. Which channels have the most number of videos, and how many videos do they have?':
                cursor.execute("""SELECT channel_name 
                AS Channel_Name, Total_videos AS Total_Videos
                                    FROM Channels_Details
                                    ORDER BY Total_videos DESC""")
                df = pd.DataFrame(cursor.fetchall())
                st.write("### :green[Number of videos in each channel :]")
                st.write(df)
                
                
                
            elif DataExtraction.questions == '3. What are the top 10 most viewed videos and their respective channels?':
                cursor.execute("""SELECT Channel_Name AS Channel_Name, Title AS Video_Title, ViewCount AS Views 
                                    FROM Video_Details
                                    ORDER BY ViewCount DESC
                                    LIMIT 10""")
                df = pd.DataFrame(cursor.fetchall())
                st.write("### :green[Top 10 most viewed videos :]")
                st.write(df)
                
            elif DataExtraction.questions == '4. How many comments were made on each video, and what are their corresponding video names?':
                cursor.execute("""SELECT a.Video_id AS Video_id, a.Title AS Video_Title, b.Comments_Count
                                    FROM Video_Details AS a
                                    LEFT JOIN (SELECT Video_id,COUNT(Comment_Id) AS Comments_Count
                                    FROM Comments_Details GROUP BY Video_id) AS b
                                    ON a.Video_id = b.Video_id
                                    ORDER BY b.Comments_Count DESC""")
                df = pd.DataFrame(cursor.fetchall())
                st.write("### :green[Comments on Videos and corresponding Title :]")
                st.write(df)
                
            elif DataExtraction.questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                cursor.execute("""SELECT Channel_name AS Channel_Name,Title AS Title,LikeCount AS Likes_Count 
                                    FROM Video_Details
                                    ORDER BY LikeCount DESC
                                    LIMIT 10""")
                df = pd.DataFrame(cursor.fetchall())
                st.write(df)
                st.write("### :green[Top 10 most liked videos :]")
                
            elif DataExtraction.questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                cursor.execute("""SELECT Title AS Title, LikeCount AS Likes_Count
                                    FROM Video_Details
                                    ORDER BY LikeCount DESC""")
                df = pd.DataFrame(cursor.fetchall())
                st.write(df)
                
            elif DataExtraction.questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                cursor.execute("""SELECT Channel_name AS Channel_Name, total_Views AS Views
                                    FROM Channels_Details
                                    ORDER BY total_Views DESC""")
                df = pd.DataFrame(cursor.fetchall())
                st.write(df)
                st.write("### :green[Channels vs Views :]")
                
            elif DataExtraction.questions == '8. What are the names of all the channels that have published videos in the year 2022?':
                cursor.execute("""SELECT Channel_name AS Channel_Name
                                    FROM Video_Details
                                    WHERE Published_date LIKE '2022%'
                                    GROUP BY Channel_name
                                    ORDER BY Channel_name""")
                df = pd.DataFrame(cursor.fetchall())
                st.write(df)
                
            elif DataExtraction.questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                cursor.execute("""select Channel_name as channelname,AVG(Duration) as averageduration from Video_Details group by Channel_name""")
                df = pd.DataFrame(cursor.fetchall())
                st.write("### :green[Average video duration for channels :]")
                st.write(df)
                               
            elif DataExtraction.questions== '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                cursor.execute("""SELECT Channel_name AS Channel_Name,Video_id AS Video_ID,Comments_Count AS Comments
                                    FROM Video_Details
                                    ORDER BY Comments_Count DESC
                                    LIMIT 10""")
                df = pd.DataFrame(cursor.fetchall())
                st.write("### :green[Videos with most comments :]")
                st.write(df)
                

#Object Creation
dataEx=DataExtraction() 
dataEx.sidebarMenu()
dataEx.extrtactPage()
dataEx.questionPage()
    



    