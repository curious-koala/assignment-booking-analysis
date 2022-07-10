# Commercial Booking Analysis

I have written my entire code using Jupyter Notebook.

My first part deals with all the user based inputs that we require for the code to execute the final results. 
It takes the booking data path and then checks for all the json files avaliable and creates a list for the same. 
Again it takes manual input for the airport data and checks for all the dat file and creates a list of such files.
The block then takes startdate and enddate as input from user and then simply converts it into datetime date format. 

My 2nd code block deals with batch processing of the JSON files. It takes the json files one by one from the list and 
fixes the format issues first to create a valid file and the loads the Json file using json parser functions. Now we have 
a json file which can be used for flatening.

My 3rd block basically normalize the json file to create a dataframe. Now since the JSON is somplex and nested to many levels,
this functions helps in removing the unwanted json objects and then we can work with the ones needed for analysis.

My 4th code block is the key to flatten nested json data. It takes the normalized df as input, identifies the columns into list and dict
types and then subsequently uses the normalize and explode functions for flattening the dict and list type objects respectively.
Since it starts with creating a list of all such objects, we can iterate it and then use recusrsion to reapply the same steps on the 
next level of json objects. This is a generic solution to flatten the JSON object and can be used in to flatten any JSON object. The 
output for this function is a flattened dataframe. In the next steps I have just removed unwanted columns and renamed the columns we need for analysis. 

My next block is another function which reads the dat files from the input directory and then passes them one by one in a loop.
The loop has been initialized by creating the first file into a dataframe and then subsequently the next files are concatenated. Columns have been assigned as per information provided in the airports data.

My next code block basicallu merges the two dataframes into one combined dataframe by using the IATA codes of the airports. I then filter the data for KL flights departing from Netherlands with passengers whose booking status is confirmed. 

I then process the dates by changing them to datatime datatype first and then applying the tz_convert fucntion to change the times according to the Tz_Database timezone as was requested in the assignment. 
Here I have used 2 things in order to get to apply the respective Tz_Database_Timezone. I have used the swifter library which allows me to parallelize the process to apply this fucntion on all datapoints in the time columns.
Swifter chooses the best way to implement the apply possible for the function by either vectorizing your function or by using Dask in the backend to parallelize the function. 
In our case since the data is small I am directly using swifter function. I have left teh code to use partitions as well in order to parallelize the process when need be. 

Next I have used just created weekday, month and other columns required for the analysis. I also filtered the data based on the user input for startdate and enddate. Since I need country in my output, I have merged the required columns from both the processed df and the airport data df to create a new df.

Since I couldn't find any data for weather/season in the Json or the airport file, I chose to use the data available on the internet to get the season data. I chose to provide hardcoded values for the season data insted creating a request based api to get the data as it would have been unnecessarily complex. The resson is simply that I needed season for NL only. This is why I used season data from source : https://seasonsyear.com/Netherlands and then cerated a dictionary object to store the info. I used this dict object to create the season column for my analysis. 

In the last step, I grouped the data against Country, Weekday, Season and the got total count of all the passengers against them. I sorted the data based on the decresing values of passenger count against season and weekday.
  