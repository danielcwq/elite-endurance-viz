This is an effort to map out and visualise elite endurance athletes' race (and training) performances in the lead up to the Paris Olympics.

# [Simple] ()

- Exploratory Data Analysis of all male endurance athletes that have achieved a score of >= [1100 IAAF points] (https://worldathletics.org/news/news/scoring-tables-2022).
- 1100 IAAF points has been arbitrarily defined but times at this level is a good separator between elite / pro performances.
- 1100 IAAF points in an outdoor setting equates to:
  | | | | | | | | | |
  |---|---|---|---|---|---|---|---|---|
  |800m|1500m|Mile|2000m|3000m SC|5000m|10000m|HM|M|
  |1.47.46|3.40.52|3.57.97|5.03.42|8.35.15|13.30.73|28.21.12|62.11|2.13.42|
- Only the highest scoring time (both indoors and outdoors) are included. Do note that for the same distance / time ran, indoor performances are rated higher than outdoor performances.
- 3 CSV files are attached, namely:
  - combined_disciplines_elite, which combines each athlete's BEST race time in 800m/ 1500m/ 3000m SC/ 5000m/ 10000m/ Half Marathon / Marathon.
  - duplicated_sorted_by_name, which contains any instance of an athlete racing across multiple distances
  - aggregated_sorted_by_highest_average, which sorts in accordance to the average of each athlete's BEST performance across multiple disciplines. Yomif Kejelcha ranks the highest in the coveted 5000m/10000m double.
- simple_viz.jpg is a representation of countries with the highest distribution of elite athletes who have achieved race performances of >= 1100 points. Kenya ranks the highest with 422 counts, followed by Japan (336) and USA (252). Not many surprises here.

## Next steps:

- Proceed to complex, which aggregates ALL performances of an athlete >= 1100 points. Separate into indoor / outdoor season using regex, and predict form leading up to Olympics.
- After completing complex, map out elite athlete's training history using Strava (for athletes that post all workouts there). Can aggregate data from other sources like YouTube (TRACK: All-Access, SweatElite, etc.)
