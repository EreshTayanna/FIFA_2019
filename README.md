# FIFA_2019

Prerequisites:
==============
--> Java 8
--> IntelliJIdea
--> SBT plugin need to be configured in intelliJIdea

Below are the steps to execute the this project :
=================================================
1. Clone the project from git and import it into intelliJIdea and build the project
2. FIFAMain is the driver program which you need to run and before running you need to pass below parameters
    a. args(0) as your running mode
    b. args(1) as your input file location either from HDFS or Local
3. Once you run the driver code, kindly notice console for the expected output

Approach:
=========
Step-1 : Loaded data into spark dataframes and selected required columns and persisted into memory for faster performance
Step-2 : 
         1. Filter out the data which includes only within 30 years of age with left footed players and found most number by doing descending order

         2. Take the 4 best midfielders, 4 best defensive players and 2 best strikers from each team and get overall rating
            by doing descending order and take highest rated team

         3. Add up the value for each team and do group by of team and order by descending order and get the most one
         
         4. We have to do group by of position and take average of value and get highest value by sorting
         
         5. Take the average of each attribute and iterate goal keepers attribute against it and take the top 4 attributes
         
         6. Take the average of each attribute and iterate strikers attribute against it and take the top 5 attributes
         
        Finally unpersist the data to cleanup memory(Not required in production and this will be taken care by GC in LRU fashion)
Step-3 : Writing a data to postgres database using write spark API's with respected modes and couldn't acheive writing data into postgres on docker due to short of time
