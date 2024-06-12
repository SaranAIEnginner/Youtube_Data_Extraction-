import pandas as pd
import sqlite3
from googleapiclient.discovery import build
import streamlit as st
import streamlit_option_menu as som
import isodate

class DataExtraction:

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
        self.api_key = "AIzaSyA8yPtH3HkF_FVFCGpJCDw7RaIM5iO9KUs"
        self.youtube = build('youtube','v3',developerKey=self.api_key)
        self.video_ids = []
        self.ch_data = []
        self.video_stats = []
        self.Comment_data=[]
        self.selected=object
        self.Ch_name=''
        self.Cmt_id=''
        
        #database connection
        self.connection = sqlite3.connect('SaranCapProjects.db')

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
                                               id= channel_id).execute()
        print("response",type(response))
        for i in range(len(response['items'])):
            data = dict(Channel_id = channel_id[i],
                        Channel_name = response['items'][i]['snippet']['title'],
                        Subscribers = response['items'][i]['statistics']['subscriberCount'],
                        Views = response['items'][i]['statistics']['viewCount'],
                        Total_videos = response['items'][i]['statistics']['videoCount'],
                        Description = response['items'][i]['snippet']['description'],
                        Country = response['items'][i]['snippet'].get('country')
                        )
            self.Ch_name=data['Channel_name']
            self.ch_data.append(data)
            return self.ch_data,self.Ch_name
    
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
                duration = isodate.parse_duration(video_details['Duration'])
                # Get the total seconds
                total_seconds = int(duration.total_seconds()) 
                video_details['Duration']=total_seconds
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

                self.Cmt_id=data['Comment_Id']    
                self.Comment_data.append(data)
                
        except:
            pass
        return self.Comment_data,self.Cmt_id
    
    #convert List of Dictionary data into pandas data frame and store dataframe into database
    def covert_PdData_StoreSql(self,channels_data, table_names):
        dataframedata=pd.DataFrame(channels_data)
        dataframe=pd.DataFrame.from_dict(dataframedata)
        print("dataframe",dataframe)
        column_name = 'Channel_name'
        colName="Comment_Id"
        value_to_find =self.Ch_name
        value=self.Cmt_id 
        cursor = self.connection.cursor()
        result = cursor.execute(f"""SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table_names}';""")
        count = result.fetchone()[0]

        print("Table presence",count)
        if count==0:
            dataframe.to_sql(table_names, self.connection, if_exists="append", index=False)
            # tables = self.connection.execute((f"SELECT * FROM Channels_Details WHERE {column_name} = :value"), {'value': value_to_find})
            # st.table(tables)
            st.success("Transformation to Databse Successfully")
            print("store data-new table and values")
        else:
            if table_names=='Channels_Details':
                query1 = f"SELECT * FROM Channels_Details WHERE {column_name} = :value"
                chtable = pd.read_sql(query1, self.connection, params={'value': value_to_find})
                row1=chtable['Channel_name']
                print("row1",row1)            
                # rows=table['Channel_name']
                print("Length of the query",chtable) 
                if len(row1)==0:
                    dataframe.to_sql(table_names, self.connection, if_exists="append", index=False)
                    st.success("channel data Stored successfully")
                    print("channel data Stored successfully")  
                else:
                    st.error("Channel data already stored")
            
            elif table_names=='Video_Details':
                query2 = f"SELECT * FROM {table_names} WHERE {column_name} = :value"
                vidtable = pd.read_sql(query2, self.connection, params={'value': value_to_find})
                row2=vidtable['Channel_name']
                print("row2",row2)  

                if len(row2)==0:
                    dataframe.to_sql(table_names, self.connection, if_exists="append", index=False)
                    st.success("Video data Stored successfully")
                else:
                    st.error("Channel data already stored")    

            elif table_names=='Comments_Details':
                query3 = f"SELECT * FROM {table_names} WHERE {colName} = :value"
                cmttable = pd.read_sql(query3, self.connection, params={'value': value})
                row3=cmttable['Comment_Id']
                print("row3",row3)
                if len(row3)==0:
                    dataframe.to_sql(table_names, self.connection, if_exists="append", index=False)
                    st.success("Comment data Stored successfully")
                else:
                    st.error("Channel data already stored")         

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

    #Extract and store data to db        
    def extrtactPage(self):
        if self.selected == "Extract and Store":
            st.markdown("#    ")
            st.write("### Enter YouTube Channel_ID below :")
            channel_id= st.text_input("Hint : Goto channel's home page.Right click.View page source.Find channel_id").split(',')
            print("channel id",channel_id)
            if st.button("Submit"): 
                                   
                    dataEx.get_channel_details(channel_id) 
                    dataEx.get_channel_video_id(channel_id)
                    dataEx.get_video_details(dataEx.video_ids)
                    dataEx.get_comment_details(dataEx.video_ids)
                    dataEx.store_channel_data()
                    dataEx.store_video_data()
                    dataEx.store_comment_data()
                                             
#question page   
    def questionPage(self):
        
            cursor=self.connection.cursor()
            if self.selected == "Questions":    
                st.write("## :orange[Select any question to get Insights]")
                DataExtraction.questions = st.selectbox('Questions',DataExtraction.questions)         
            
            if DataExtraction.questions == '1. What are the names of all the videos and their corresponding channels?':
                table=pd.read_sql("""SELECT title AS Video_Title, Channel_name AS Channel_Name FROM Video_Details ORDER BY Channel_name""",self.connection)
                print(table)
                st.write(table)
            
            elif DataExtraction.questions == '2. Which channels have the most number of videos, and how many videos do they have?':
                table=pd.read_sql("""SELECT channel_name,Total_videos
                                    FROM Channels_Details
                                    ORDER BY CAST(Total_videos AS INT) DESC""",self.connection)
                st.write("### :green[Number of videos in each channel :]")
                st.write(table)
                       
            elif DataExtraction.questions == '3. What are the top 10 most viewed videos and their respective channels?':
                table=pd.read_sql("""SELECT Channel_Name, Title AS Video_Title, ViewCount AS Views 
                                    FROM Video_Details
                                    ORDER BY CAST(ViewCount AS INT) DESC
                                    LIMIT 10""",self.connection)
                st.write("### :green[Top 10 most viewed videos :]")
                st.write(table)
                
            elif DataExtraction.questions == '4. How many comments were made on each video, and what are their corresponding video names?':
                table=pd.read_sql("""SELECT a.Video_id AS Video_id, a.Title AS Video_Title, b.Comments_Count
                                    FROM Video_Details AS a
                                    LEFT JOIN (SELECT Video_id,COUNT(Comment_Id) AS Comments_Count
                                    FROM Comments_Details GROUP BY Video_id) AS b
                                    ON a.Video_id = b.Video_id
                                    ORDER BY CAST(b.Comments_Count AS INT) DESC""",self.connection)
                st.write("### :green[Comments on Videos and corresponding Title :]")
                st.write(table)
                
            elif DataExtraction.questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                table=pd.read_sql("""SELECT Channel_name AS Channel_Name,Title AS Title,LikeCount AS Likes_Count 
                                    FROM Video_Details
                                    ORDER BY CAST(LikeCount AS INT) DESC
                                    LIMIT 10""",self.connection)
                st.write("### :green[Top 10 most liked videos :]")
                st.write(table)
                               
            elif DataExtraction.questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
                table=pd.read_sql("""SELECT Title AS Title, LikeCount AS Likes_Count
                                    FROM Video_Details
                                    ORDER BY CAST(LikeCount AS INT) DESC""",self.connection)
                st.write(table)
                
            elif DataExtraction.questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                table=pd.read_sql("""SELECT Channel_name, Views
                                    FROM Channels_Details
                                    ORDER BY CAST(Views AS INT) DESC""",self.connection)
                st.write("### :green[Channels Views for each Channel :]")
                st.write(table)
                
            elif DataExtraction.questions == '8. What are the names of all the channels that have published videos in the year 2022?':
                table=pd.read_sql("""SELECT Channel_name
                                    FROM Video_Details
                                    WHERE Published_date LIKE '2022%'
                                    GROUP BY Channel_name
                                    ORDER BY Channel_name""",self.connection)
                st.write("### :green[Name of the Channels that published videos in 2022]")
                st.write(table)
                
            elif DataExtraction.questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                table=pd.read_sql("""select Channel_name as channelname,AVG(Duration) as Average_Duration_in_Seconds from Video_Details group by Channel_name ORDER BY Duration DESC""",self.connection)
                st.write("### :green[Average video duration for channels :]")
                st.write(table)
                               
            elif DataExtraction.questions== '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                table=pd.read_sql("""SELECT Channel_name AS Channel_Name,Video_id AS Video_ID,Comments_Count AS Comments
                                    FROM Video_Details
                                    ORDER BY CAST(Comments_Count AS INT) DESC
                                    LIMIT 10""",self.connection)
                st.write("### :green[Videos with most comments :]")
                st.write(table)
                 
dataEx=DataExtraction() 
dataEx.sidebarMenu()
dataEx.extrtactPage()
dataEx.questionPage()