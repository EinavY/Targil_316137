#!/usr/bin/env python
# coding: utf-8

#    #        Part 2                         |                       Twitter Data Preparation 

# In[ ]:





# In[ ]:





# We will make use of the following libraries: **pandas, numpy, json, datetime, matplotlib**

# In[71]:


import datetime
import pandas as pd
import numpy as np
import json
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
TWEETS_FILE_PATH = r'C:\Einav\tweets.csv'
USERS_FILE_PATH = r'C:\Einav\users.csv'
tweets_data = pd.read_csv(TWEETS_FILE_PATH, encoding = "ISO-8859-1")
users_data = pd.read_csv(USERS_FILE_PATH, encoding = "ISO-8859-1")


# ## Data Cleaning

# After reading the data in Python, we would like to get an idea about the data structure (null values, length etc.).

# ### Data cleaning of "tweets_data"

# Let's take a look at tweets:

# In[13]:


tweets_data.head()


# In[14]:


tweets_data.info()


# Rename created at column to 'created_at':

# In[15]:


tweets_data = tweets_data.rename(columns={'created at':'created_at'})


# There are instances where there is no date specified ("-") - we filter these out:

# In[16]:


tweets_data = tweets_data[tweets_data.created_at != '-'].reset_index(drop=True)


# Next, we want to make sure that we keep only ID's that are in the users data.<br>We inner join with user_id column in users data to get rid of irrelevant IDs:

# In[17]:


users_user_IDs = users_data[['user_id']] #df only with user_id

tweets_data = users_user_IDs.merge(tweets_data, on='user_id', how='inner')


# In[18]:


tweets_data['user_id'].value_counts()


# Now we see that there are 541 unique IDs in tweets (less than in users).<br>
# For matching both of the data frames, we will need to do another inner join, between the user ids from tweets and the entire users dataframe. This we will do in the User_data cleaning section. 

# Problem with data: a user cannot tweet two tweets at the **same time**. <br> So, the tweets data's unique key is hereby defined by user_id + date & time ('**created_at**'). <br>
# (Users data unique key is **user_id**).<br>
# 

# Dropping duplicate rows, by our defined key:

# In[19]:


tweets_data = tweets_data.drop_duplicates(subset=['user_id', 'created_at'], keep='first').reset_index(drop=True)


# The data structure with the key:

# In[20]:


tweets_data.set_index(['user_id', 'created_at'])


# ~ 40k rows dropped from tweets after cleaning.

# ###  Data Cleaning of "Users_data"

# In[21]:


users_data.info()


# Dropping duplicate rows by key (user_id):

# In[22]:


users_data = users_data.drop_duplicates(subset = 'user_id',keep='first').reset_index(drop=True)


# In[23]:


len(users_data)


#  0 rows dropped.

# Merging (inner) user_ids from tweets data with users dataframe for matching number of user_ids:

# In[24]:


tweets_user_IDs = tweets_data[['user_id']].drop_duplicates().reset_index(drop=True)
users_data = tweets_user_IDs.merge(users_data,on='user_id', how='inner')


# In[25]:


users_data.info()


# ~50 IDs filtered ==> now tweets data's user IDs are the same as the users data.

# ### Question 1
# 
# **1.1:** For counting the number of hashtags, we applied the **loads** function from json library that basically translates the data from String to a the actual object (here: a list of dictionaries). 
# After that, we applied len() for counting the number of dictionaries in each list.

# In[26]:


tweets_data["hashtag_count"] = tweets_data["hashtags"].apply(json.loads).apply(len)
tweets_data.head()


# **1.2:** Here and in the following sections we utilize **lambda** expressions, and NumPy - **np.where**.
# <br>
# Below: return 0 if the value is NA.

# In[27]:


tweets_data["shared_geo_location"] = np.where(pd.isna(tweets_data["geo"]),0,1)
tweets_data['shared_geo_location'].value_counts()


# **1.3:** Check if the source contains the words "Twitter Web Client". If so - return PC. If not - it's a mobile phone.

# In[28]:


tweets_data["device"] = tweets_data["source"].apply(lambda x: 'PC' if 'Twitter Web Client' in x else 'Mobile')


# **1.4:** How many words in each tweet - executed by **splitting** the text into words and then applying **len()**.

# In[29]:


tweets_data["word_count"] = tweets_data["text"].apply(lambda x: len(x.split()))


# ### Question 2
# Lets take another look at our users data:

# In[30]:


users_data.head()


# **2.1:** How many words in each user's description - same as **1.4** (return 0 if the value is NA).

# In[31]:


users_data["Num_words_in_desc"] = users_data["description"].apply(lambda x: 0 if pd.isna(x) else len(x.split()))


# **2.2:** A twitter user is considered a celebrity if they have more than 100k followers. 
# 

# In[32]:


users_data['Is_celeb'] = np.where(users_data['followers_count']>=100000,'Yes','No')

#test the code:
users_data[users_data['Is_celeb']=='Yes']


# 157 celebs.

# **2.3:** In this section, our thought process goes as follows:
# <br>
# <br>
# We use **groupby** for grouping all the tweets for each user id, then count them.
# 
# 

# In[33]:


id_groups = tweets_data.groupby('user_id') #GroupBy object

users_data['Collected_tweets'] = users_data.apply(lambda x: id_groups.get_group(x['user_id']).count()['tweet_id'],axis=1)
users_data[['user_id','Collected_tweets']]


# **2.4:** We will divide the 'Collected_tweets' column by 'statuses_count' column to get the percentages of tweets collected.<br><br>
# Also, we check if their are invalid results (more collected tweets than status count); if invalid, replace value with **NaN**.

# In[34]:


users_data['Collected_tweets_percent'] = users_data.apply(lambda x: np.nan if x['Collected_tweets']>x['statuses_count'] else x['Collected_tweets']/x['statuses_count'], axis=1)
users_data[['user_id','statuses_count','Collected_tweets','Collected_tweets_percent']]


# How many invalid occurences?

# In[35]:


users_data.Collected_tweets_percent.isna().sum()


# This is the invalid user: 

# In[36]:


users_data[users_data['Collected_tweets_percent'].isna()]


# ### Question 3<br>
# For creating year and month columns we convert them to datetime format using datetime library.

# In[37]:


tweets_data['created_at'][3] # date format


# In[38]:


tweets_monthly_summary = tweets_data    
tweets_monthly_summary[['user_id','tweet_id','created_at']]


# Use datetime library to convert format:<br>
# 

# In[39]:


def format_date(date_to_format):
     pd.to_datetime(date_to_format, format = '%a %b %d %H:%M:%S %z %Y')
    
new_created_at = tweets_monthly_summary.apply(lambda x: pd.to_datetime(x['created_at'], format = '%a %b %d %H:%M:%S %z %Y') ,axis=1)
tweets_monthly_summary = tweets_monthly_summary.assign(created_at=new_created_at) #replace 'created_at' column with our new formatted column
tweets_monthly_summary[['user_id','tweet_id','created_at']]


# Year and month columns:

# In[40]:


tweets_monthly_summary['year'] = pd.DatetimeIndex(tweets_monthly_summary['created_at']).year
tweets_monthly_summary['month'] = pd.DatetimeIndex(tweets_monthly_summary['created_at']).month


# In[41]:


tweets_monthly_summary.head()


# New column 'is_mobile': 1 for Mobile and 0 for Pc.

# In[42]:


tweets_monthly_summary['is_mobile'] = tweets_monthly_summary['device'].apply(lambda x: 1 if x == 'Mobile' else 0)


# Here, in order to get monthly data, we grouped by id, year and month as follows:

# In[43]:


grouped = tweets_monthly_summary.groupby(['user_id','year','month'])
#groupby object


# Applying **agg** to the groupby object. The function gets: <br>1. column name <br>2. function to be executed. 

# **Tweet_count**, **Hashtag_count**, **Retweet_count**, **Location_sharing_count**, **Quote_count** (given by aggregation on GroupBy Object):

# In[44]:


Grpd = grouped.agg({'tweet_id': 'count','hashtag_count':sum,'is_mobile':sum,'retweet_count':sum, 'shared_geo_location':sum, 'is_quote_status':sum})


# **Percent_mobile**:

# In[45]:


Grpd['Percent_mobile'] = Grpd['is_mobile']/Grpd['tweet_id'] 


# Before the last part (Percent_tweets) - reset_index for keeping year and month columns. <br>
# After that, merge (inner) with statuses_count for calculating **Percent_tweets**:

# In[46]:


Grpd = Grpd.reset_index()

Grpd = Grpd.merge(users_data[['user_id','statuses_count']], on='user_id', how='inner')
Grpd['Percent_tweets'] = Grpd['tweet_id']/Grpd['statuses_count']


# In[47]:


rename_as_required = Grpd.rename({'user_id':'User_id','year':'Year','month':'Month','tweet_id':'Tweet_count', 'hashtag_count': 'Hashtag_count','retweet_count':'Retweet_count','shared_geo_location':'Location_sharing_count', 'is_quote_status':'Quote_count'}, axis='columns')

tweets_monthly_summary = rename_as_required[['User_id','Year','Month','Tweet_count','Hashtag_count','Percent_mobile','Retweet_count','Location_sharing_count','Quote_count','Percent_tweets']]
tweets_monthly_summary.set_index(['User_id','Year','Month'])


# ## Data Visualization

# ### Question 4

# In[48]:


users_data['Collected_tweets']


# Histogram that shows the amount of users for each number of **collected tweets**:

# In[49]:


users_data['Collected_tweets'].hist(bins=20,alpha=0.7)
plt.xlabel('Number of Collected tweets')
plt.ylabel("Amount of Users")
plt.title('Users for number of Tweets')


# In[50]:


users_data[users_data['Collected_tweets'] < 20].count()['user_id']


# We can see that more than half of the users don't pass 20 tweets. Lets "Zoom in": Divide into two groups and visualize:

# In[51]:


Below_35 = users_data[users_data['Collected_tweets'] < 35]
Below_35['Collected_tweets'].hist(bins=20,alpha=0.7)
plt.xlabel('Number of Collected tweets')
plt.ylabel("Amount of Users")
plt.title('Users for number of Tweets - < 35')


# In[52]:


Above_20 = users_data[users_data['Collected_tweets'] > 20]
Above_20['Collected_tweets'].hist(bins=20,alpha=0.7)
plt.xlabel('Number of Collected tweets')
plt.ylabel("Amount of Users")
plt.title('Users for number of Tweets - > 20')


# ## Q5

# Histogram that shows the amount of users for each number of **followers**:

# In[53]:


users_data[users_data['followers_count'] > 3000000]


# In[54]:


users_data['followers_count'].hist(bins=15,alpha=0.7)
plt.xlabel('Followers Count (in Millions)')
plt.ylabel("Amount of Users")
plt.title('Users for number of Followers')


# The histogram above shows that most of the users are not celebs.

# In[55]:


users_data[users_data['followers_count'] < 150].count()['user_id']


# Half of the users have less than 150 followers! Lets zoom in.

# In[56]:


Below_150 = users_data[users_data['followers_count'] < 150]
Below_150['followers_count'].hist(bins=20,alpha=0.7)
plt.xlabel('Followers Count')
plt.ylabel("Amount of Users")
plt.title('Users for number of Followers - < 150')


# Zoom out: we see that most of the population (users) have below than 250,000 followers.

# In[57]:


users_data[users_data['followers_count'] < 250000].count()['user_id']


# In[58]:


Above_250000 = users_data[users_data['followers_count'] > 250000]
Above_250000['followers_count'].hist(bins=20,alpha=0.7)
plt.xlabel('Followers Count (in millions)')
plt.ylabel("Amount of Users")
plt.title('Users for number of Followers - > 250000')


# ## Q6

# Barchart - shows all the celebs:

# In[59]:


df_Q6 =users_data[['name','user_id','followers_count']]
df_Q6 = df_Q6.sort_values(by=['followers_count'],ascending=False)[0:10] # sorting and picking top 10
df_Q6.plot.bar(x = 'name' ,y ='followers_count' )
plt.xlabel("Celebritys")
plt.ylabel("Amount of Followers in millions")
plt.title('Celebs number of followers - Top 10:')


# ## Q7

# In[60]:


df_Q7 =users_data[['statuses_count','followers_count']]
df_Q7 = df_Q7.sort_values(by=['statuses_count'],ascending=False)


# In[61]:


df_Q7.plot(kind='scatter', x='statuses_count', y='followers_count')
plt.xlabel("Total Tweets")
plt.ylabel("Followers")
plt.title('Relation between Followers and Tweets:')


# In[62]:


df_Q7[df_Q7['statuses_count'] < 200].count()['statuses_count']


# In[63]:


dfQ7p2 = df_Q7[df_Q7['statuses_count'] < 100000]
dfQ7p2.plot(kind='scatter', x='statuses_count', y='followers_count')
plt.xlabel("Total Tweets")
plt.ylabel("Followers")
plt.title('Relation between Followers and Tweets:')


# In[64]:


top_10 = users_data[['name','user_id','followers_count','statuses_count']]
top_10.sort_values(by=['followers_count'],ascending=False)[0:10]


# In[65]:


worst_10 = users_data[['name','user_id','followers_count','statuses_count']]
worst_10.sort_values(by=['followers_count'],ascending=True)[0:10]


# We can see that tweeting a lot does not necessarily give you a lot of followers (billie eillish,  Mohammad Shahanshah).<br>
# There could be other factors that contribute to having a lot of followers, but serious research needs to be done in order to be more certain.

# ## Q8

# In[66]:


df_Q8 =users_data[['statuses_count','friends_count']]
df_Q8 = df_Q8.sort_values(by=['statuses_count'],ascending=False)
df_Q8.plot(kind='scatter', x='statuses_count', y='friends_count')
plt.xlabel("Total Tweets")
plt.ylabel("Friends")
plt.title('Relation between Friends and Tweets:')


# In[67]:


df_Q8[df_Q8['friends_count'] < 100000].count()
df_Q8[df_Q8['friends_count'] < 100000].plot(kind='scatter', x='statuses_count', y='friends_count')
plt.xlabel("Total Tweets")
plt.ylabel("Friends")
plt.title('Relation between Friends and Tweets:')


# We can see from here that tweeting a lot does not necessarily bring you a lot of friends...

# Export to csv:

# In[69]:


LOCAL_DIRECTORY_PATH = r'C:\DataScienceIntro\tweets_monthly_summary.csv'
tweets_monthly_summary.to_csv (LOCAL_DIRECTORY_PATH, index = False, header=True , encoding = 'utf-8-sig') #Exporting the DF to a folder in our PC

