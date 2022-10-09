# [RECRUIT STAGE](https://www.cloudskillsboost.google/games/3139/labs/19212)
___
### Task 1: Import the Data to BigQuery
Activate Cloud Shell:
![42bf1200-852e-446e-9bea-c43dcc6118cd](https://user-images.githubusercontent.com/39781072/194774555-33ff1813-461d-47ef-9c26-44c7a051897f.jpg)
Paste the shell command to create the dataset and tables:
```sh
bq mk drl
PROJECT_ID=$(gcloud config get-value project)
gsutil ls gs://spls/gsp394/tables | grep csv$ | while read file; do tname=`echo $file|cut -d"/" -f6| cut -d. -f1 `; bq load --source_format=CSV --autodetect $PROJECT_ID:drl.$tname $file ; done
```
___
### Task 2: Events in a Certain City
```sql
SELECT
  name
FROM
  drl.events
WHERE
  city = '[CITY]'
```
___
### Task 3: Event Pilot Names
```sql
SELECT
  pilots.name as name,
  event_pilots.id as event_pilots_id
FROM
  drl.event_pilots AS event_pilots
    LEFT OUTER JOIN
  drl.pilots AS pilots
    ON event_pilots.pilot_id = pilots.id;
```
___
### Task 4: Pilots Who Flew in an Event
```sql
SELECT
  pilots.name as pilot_name,
  events.name as event_name
FROM
  drl.event_pilots as event_pilots
    LEFT OUTER JOIN
  drl.pilots AS pilots
    ON event_pilots.pilot_id = pilots.id
    RIGHT OUTER JOIN
  drl.events AS events
    ON events.id = event_pilots.event_id
WHERE
  events.name = '[EVENT]'
```
___
### Task 5: Average Time of Rank 1 Round Finish
```sql
SELECT
  timestamp_seconds(
    CAST(
      AVG(
        UNIX_SECONDS(
          PARSE_TIMESTAMP(
            '%H:%M.%S', round_standings.minimum_time
          )
        )
      ) AS INT64
    )
  ) AS avg
FROM
  drl.round_standings as round_standings
WHERE
  round_standings.rank = 1
```
___
### Task 6: Clean and Combine Time Trial Data
```sql
CREATE TABLE
  drl.time_trial_cleaned AS (
    SELECT
      time_trial_group_pilot_times.id AS time_trial_group_pilot_times_id,
      time_trial_group_pilots.id AS time_trial_group_pilot_id,
      time_trial_groups.id AS time_trial_group_id,
      time_trial_groups.round_id AS round_id,
      CASE
        WHEN time_trial_group_pilot_times.time_adjusted IS NOT NULL THEN time_trial_group_pilot_times.time_adjusted
        WHEN time_trial_groups.racestack_scoring = 1 THEN time_trial_group_pilot_times.racestack_time
        ELSE time_trial_group_pilot_times.time
      END AS time
    FROM
      drl.time_trial_group_pilot_times AS time_trial_group_pilot_times
        LEFT OUTER JOIN
      drl.time_trial_group_pilots AS time_trial_group_pilots
        ON time_trial_group_pilot_times.time_trial_group_pilot_id = time_trial_group_pilots.id
        LEFT OUTER JOIN
      drl.time_trial_groups AS time_trial_groups
        ON time_trial_group_pilots.time_trial_group_id = time_trial_groups.id
  );
```
___
### Task 7: Fastest Time Trial at Event
```sql
SELECT
  MIN(time_trial_cleaned.time) AS fastest_time
FROM
  drl.time_trial_cleaned AS time_trial_cleaned
    LEFT OUTER JOIN
  drl.rounds AS rounds
    ON time_trial_cleaned.round_id = rounds.id
    LEFT OUTER JOIN
  drl.events AS events
    ON rounds.event_id = events.id
WHERE
  rounds.name = 'Time Trials' AND
  events.name = '[EVENT]'
```
___
### Task 8: Pilot Heat Statistics
```sql
SELECT
  pilots.name AS pilot_name,
  heat_standings.heat_id AS heat_id,
  heat_standings.minimum_time AS minimum_time,
  heat_standings.points AS points
FROM
  drl.heat_standings AS heat_standings
    INNER JOIN
  drl.event_pilots AS event_pilots
    ON heat_standings.event_pilot_id = event_pilots.id
    INNER JOIN
  drl.pilots AS pilots
    ON event_pilots.pilot_id = pilots.id
WHERE
  pilots.name = '[PILOT]' AND
  points IS NOT NULL AND
  NOT (minimum_time = 'DNF')
```
___
### Task 9: Pilot Running Average Heat Time
```sql
SELECT
  pilots.name AS pilot_name,
  heat_standings.heat_id AS heat_id,
  heat_standings.minimum_time AS minimum_time,
  heat_standings.points AS points,
  time(
    timestamp_seconds(
      CAST(
        AVG(
          UNIX_SECONDS(
            PARSE_TIMESTAMP(
              '%H:%M.%S', heat_standings.minimum_time
            )
          )
        ) OVER(
          ORDER BY heat_standings.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS INT64
      )
    )
  ) AS running_avg
FROM
  drl.heat_standings AS heat_standings
    INNER JOIN
  drl.event_pilots AS event_pilots
    ON heat_standings.event_pilot_id = event_pilots.id
    INNER JOIN
  drl.pilots AS pilots
    ON event_pilots.pilot_id = pilots.id
WHERE
  pilots.name = '[PILOT]' AND
  points IS NOT NULL AND
  NOT (minimum_time = 'DNF')
```
___
### Task 10: Pilot Time Improvements
```sql
SELECT
  pilots.name AS pilot_name,
  heat_standings.heat_id AS heat_id,
  heat_standings.minimum_time AS minimum_time,
  heat_standings.points AS points,
  time(
    timestamp_seconds(
      CAST(
        AVG(
          UNIX_SECONDS(
            PARSE_TIMESTAMP(
              '%H:%M.%S', heat_standings.minimum_time
            )
          )
        ) OVER(
          ORDER BY heat_standings.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS INT64
      )
    )
  ) AS running_avg,
  TIME_DIFF(
    PARSE_TIME(
      '%H:%M.%S', minimum_time
    ), time(
      timestamp_seconds(
        CAST(
          AVG(
            UNIX_SECONDS(
              PARSE_TIMESTAMP(
                '%H:%M.%S', heat_standings.minimum_time
              )
            )
          ) OVER(
            ORDER BY heat_standings.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS INT64
        )
      )
    ), SECOND
  ) AS time_diff_from_avg
FROM
  drl.heat_standings AS heat_standings
    INNER JOIN
  drl.event_pilots AS event_pilots
    ON heat_standings.event_pilot_id = event_pilots.id
    INNER JOIN
  drl.pilots AS pilots
    ON event_pilots.pilot_id = pilots.id
WHERE
  pilots.name = '[PILOT]' AND
  points IS NOT NULL AND
  NOT (minimum_time = 'DNF')
```
___
### Task 11: Visualize Pilot Heat Statistics

Open [Data Studio](https://datastudio.google.com/) and log in with the given account:

Open a Blank project and add Data using BigQuery:

![ff92adb8-15d4-4450-bcdd-00a1aa164554](https://user-images.githubusercontent.com/39781072/194774310-5f505162-749e-4093-9ded-f17f3ade3a42.jpg)

Select CUSTOM QUERY > your project:

![9f1244f5-11c3-45b7-9e55-948ebd595c3b](https://user-images.githubusercontent.com/39781072/194774297-262e478e-690c-43fa-a29a-e445e7eeb244.jpg)

Paste the following SQL query and select Add:

```sql
SELECT
  pilots.name AS pilot_name,
  heat_standings.heat_id AS heat_id,
  heat_standings.minimum_time AS minimum_time,
  heat_standings.points AS points,
  time(
    timestamp_seconds(
      CAST(
        AVG(
          UNIX_SECONDS(
            PARSE_TIMESTAMP(
              '%H:%M.%S', minimum_time
            )
          )
        ) OVER(
          ORDER BY heat_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS INT64
      )
    )
  ) AS running_avg,
  TIME_DIFF(
    PARSE_TIME(
      '%H:%M.%S', minimum_time
    ), time(
      timestamp_seconds(
        CAST(
          AVG(
            UNIX_SECONDS(
              PARSE_TIMESTAMP(
                '%H:%M.%S', minimum_time
              )
            )
          ) OVER(
            ORDER BY heat_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS INT64
        )
      )
    ), SECOND
  ) AS time_diff_from_avg
FROM
  drl.heat_standings AS heat_standings 
    LEFT JOIN
  drl.event_pilots AS event_pilots
    ON event_pilots.id = event_pilots.id
    LEFT JOIN
  drl.pilots AS pilots
    ON pilots.id = event_pilots.pilot_id
WHERE
  points != 0 AND
  points IS NOT NULL AND
  minimum_time != 'DNF' AND
  minimum_time != ''
```

Add a Line chart:

![421723c5-3811-46f1-888a-25e87e085e51](https://user-images.githubusercontent.com/39781072/194774438-0ac63c83-3ae8-4147-9487-bd229efc2bf6.jpg)

And a Drop-down list control:

![908c5a22-b4e2-45ef-9e87-9b768b2d0b9f](https://user-images.githubusercontent.com/39781072/194774441-601a9388-3b80-47a9-885f-895c93704c20.jpg)

Select heat_id as Dimension and points as metric (with Running delta as Running calculation)

___
