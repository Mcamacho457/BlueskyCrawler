# BlueskyCrawler
A Bluesky post crawler which finds relevant conspiracy theories and paranormal encounters to a user's query as defined.

## Quick Set-Up
### Step 1:
The original clean data used to build the index is provided in the repository by the directory named `bluesky_clean_data`. To build the index using this data by the following command:
```bash
./index.sh
```

### Step 2:
The index is now stored in the directory named `bluesky_index`. Next, navigate to the web app (`web_app`) directory by the following command:
```bash
cd web_app/
```
Now run the app (`app.py`) and open the application in your web browser:
```
python3 app.py
```

Now you can freely execute any queries you like and view relevant Bluesky posts!