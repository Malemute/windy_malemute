# Tide Prophet
Theese scripts give mesurements and predictions of tide water level from the web service by NOAA.

# Quickstart

The script requires Python 3.7 or later version installed. Then use pip (or pip3, if there is a conflict with Python2) to install dependencies:
```
pip install -r requirements.txt
```

Examples of script launches on Linux, Python 3.7

predictions for 6 days from the current time with the 6 minutes interval for all the 295 active tide stations:

```bash

$ python tide_predictions.py <incoming url> # possibly requires call of python3 executive instead of just python

```

meaurements for 1 hour before the current time with the 6 minutes interval for all the 295 active tide stations:

```bash

$ python water_levels.py <incoming url> # possibly requires call of python3 executive instead of just python

```


If the script is called with no parameters, a user can input the link from the console

# Output Example

```
Test water level station request
                     predicted_wl
date_time                        
2022-01-28 20:00:00         3.504
2022-01-28 21:00:00         3.423
```

# Project Goals

The code is written for educational purposes.
