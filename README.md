# DoDoDFS-AnEmulatedDistributedFileSystem
The code for the DSCI-551 project

## Project Title: DODO DFS - Distributed File System with Map-Reduce Paradigm

### Project Background: 
The project aims to help students learn about mutual funds and investment. It involves creating a search application using existing data on mutual funds' past performance to assist users in making informed investment decisions.

### Implementation Details:
#### Data Cleaning and Augmentation: 
Utilized a Kaggle dataset on mutual fund performance and enhanced it using public APIs to fill in missing data and gather additional information.

#### Storing Dataset: 
Developed an Emulated Distributed File System (EDFS) where users can upload datasets using the PUT command. Files are partitioned by rows for efficient processing.

#### Emulating DFS: 
The EDFS follows a 3-cluster system and emulates the NameNode on Firebase to manage configurations, directory structures, and file-cluster mappings.

#### Task 1 - HDFS Commands:
Implemented various HDFS commands including help, displayconfig, updateconfig, mkdir, ls, cat, rm, put, getPartitionLocations, and readPartition. These commands allow users to interact with and manage their files within the EDFS.

#### Task 2 - Map-Reduce Paradigm:
Two additional functions, query-mode() and spec-ops(), were implemented:
- query-mode() enables users to query CSV-based datasets within the EDFS, providing insights into how map-reduce works.
- spec-ops() offers five interesting insights from the mutual funds dataset, such as top equity-based funds and highest-returning funds.

#### Task 3 - UI for the DFS:
Developed a user-friendly terminal-based UI that provides information about the EDFS, its capabilities, and available commands. The UI includes error handling for a smooth user experience.

#### Learning Experiences:
The project provided valuable learning experiences in distributed systems, the map-reduce paradigm, data storage technologies (MongoDB and Firebase), and UI building. It enabled the understanding of complex concepts and practical application.

### How to run this?
Make the following changes to be able to run this code for yourself!
- Create an environment with the requirements file provided.
- Edit the "launch-dodfs.bat" file by changing the environment path and the path to the main.py file.
- You should be good to go!

### Links:
Video: https://www.youtube.com/watch?v=XKMGEDI8xNg  
For more details, please refer to the provided links to the videos!






Regenerate
