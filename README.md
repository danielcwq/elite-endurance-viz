This is an effort to map out and visualise elite endurance athletes' race (and training) performances. 

Previously, this was done in the lead up to the Paris Olympics. Please check out OLY24 Pred and README-OLY.md for more details.

### The Problem

Strava is a great tool for posting workouts and seeing workouts of what other people have done. However, what if we want to keep track of elite athletes only, and the workouts that they have completed? 

Strava doesn’t offer a manual way for this — sports fanatics would have to find their favourite elite by searching through the search bar manually, and then scroll to see his or her workout. 

Elite endurance visualisation aims to present all elite athletes’ workouts in a synchronised dashboard, allowing amateurs to learn from the best: by seeing the whole approach that elites take, beyond individual “miracle” workouts. 

This repo consists of Jupyter notebooks and CSV files. You can access the webapp [here](https://elite-endurance-viz.vercel.app/). Suggestions are welcome!

### The Pipeline

1. Getting Elites 
    1. Right now, my focus is on *distance running*. The way that we can do this for distance runners is to utilise IAAF scorings to determine what kind of effort is considered “elite”. For the purposes of collecting more data rather than lesser data, we will be setting the threshold at an arbitrary 1100 IAAF points. 
        1. Further given that there were no major track races from end OLY to now, we should be focusing on road races. Will not update the list of athletes from 800m to 10k, but will combine the code into scrape_athletes_master. 
        2. In addition, the shorter the distance gets, the more important form training plays a role (as compared to longer distances where mileage is king). So we steer away from any sprint events from 100-400m. 
    2. Previously, when I’d obtained the results from IAAF, they had a cutoff of late Aug ‘24. As a result, we would want to make sure to constantly update our list, or have some way to constantly check back on new results. New cutoff would be 29 Oct ‘24, without repeated athletes. 
        1. In the production code, need to ensure that there are constant checks with IAAF, preferably every week. (to do for VM) 
        2. Indoor performances are rated higher than outdoor performances while the raw running capabilities 
2. Getting Elites’ Training Data.
    1. If there is no public data available for a majority of athletes, then we would have to expand that boundary from 1100 IAAF points to lower, perhaps 1000 IAAF points. 
    2. The main means through which we will get data from is from publicly available workouts through Strava. 
        1. Once the list of athletes have been retrieved in (1), we need to check how many athletes actually have (a) accounts on strava, (b) public accounts that we can get the workout information from and (c) as a post-processing step acknowledging that not all of these athletes post regularly. 
3. Grabbing athletes’ workouts (what we’ve all been waiting for)
    1. Grab athlete ID 
    2. Process JSON to extract workout metadata and ID 
    3. Loop into each workout ID to extract lap data, HR (if available), power (if available) and other high-granularity variables
