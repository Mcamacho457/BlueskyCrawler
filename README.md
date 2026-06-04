# BlueskyCrawler
A Bluesky post crawler which finds relevant conspiracy theories and paranormal encounters to a user's query as defined.

## Quick Set-Up
### Step 1:
The original clean data used to build the index is provided in the repository by the directory named `bluesky_clean_data`. Build the index using this data by the following command:
#### For Mac:
```bash
./index.sh
```

#### For Windows:
```bash
.\index.bat
```

### Step 2:
The index is now stored in the directory named `bluesky_index`. Navigate to the web app (`web_app`) directory by the following command:
```bash
cd web_app/
```
Now run the app (`app.py`) and open the application in your web browser:
```
python3 app.py
```

Now you can freely execute any queries you like and view relevant Bluesky posts!

## Set-Up from Scratch (crawling and building index)
### Step 1, Run the crawler:
The commands below run the four crawlers in the order:
1. conspiracy_theories_crawl.py
2. paranormal_theories_crawl.py
3. ufo_theories_crawl.py
4. Strange_Earth_crawl.py

#### For Mac:
```bash
./crawler.sh seen_posts.txt 100 30 bluesky_conspiracy_data
```

#### For Windows:
```bash
.\crawler.bat seen_posts.txt 100 30 bluesky_conspiracy_data
```
Now the post data is stored in the directories named `bluesky_conspiracy_data`, `bluesky_paranormal_data`, `bluesky_ufo_data`, and `bluesky_strange_earth_data`, respectively.

### Step 2, clean the data:
Now the data must be cleaned as the original metadata that was crawled contains a substantial amount of miscellaneous information that is not useful for indexing. Run the following command your terminal.
```
python3 clean_data.py
```
Now the clean data is stored in the directory named `bluesky_clean_data`.
### Step 3, build the index:
Build the index using the clean data by the following command:
#### For Mac:
```bash
./index.sh
```

#### For Windows:
```bash
.\index.bat
```

### Step 4:
The index is now stored in the directory named `bluesky_index`. Navigate to the web app (`web_app`) directory by the following command:
```bash
cd web_app/
```
Now run the app (`app.py`) and open the application in your web browser:
```
python3 app.py
```

Now you can freely execute any queries you like and view relevant Bluesky posts!