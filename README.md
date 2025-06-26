# NBA Clutchness Analyzer

This project is a Java + Apache Spark application that analyzes NBA play-by-play data to calculate player "Clutchness" during high-stakes game moments using an expected Win Probability Added (eWPA) model.

## Overview

This program:
- Parses play-by-play CSVs.
- Filters for clutch-time events: last 5 minutes of the 4th quarter or any overtime, with score margin ≤ 6.
- Classifies each event (e.g., "Made 3-Point Shot", "Turnover").
- Assigns eWPA values to each event, increasing weight for late-game or clutch-moment plays.
- Aggregates clutchness scores per player.
- Adjusts scores based on game importance (Regular Season, Playoffs, Finals).

## How It Works

### Event Classification
Events are classified using descriptive strings (`HOMEDESCRIPTION`, `VISITORDESCRIPTION`) and event codes. Examples:
- **Made 3-Point Shot (Clutch Margin)**: +0.100 eWPA
- **Missed Free Throw (Last 10 Seconds)**: -0.030 eWPA
- **STEAL**: +0.021 eWPA
- **BLOCK (Last 10 Seconds)**: +0.022 eWPA

### Clutchness Filtering
Only keeps events where:
- `PERIOD >= 4`
- `SECONDS_REMAINING <= 300` (last 5 minutes)
- `SCOREMARGIN` is between -6 and +6

### Score Adjustment
- Regular Season: x1
- Playoffs: x1.5
- Finals: x2

### Aggregation
Each event's eWPA is assigned to relevant players:
- Scorer (PLAYER1), Assister (PLAYER2), Blocker (PLAYER3)
- Exploded into individual rows per player for aggregation

### Output
Prints top 10 players by:
- Game-by-game adjusted clutchness score
- Overall clutchness score across all games

## Example Use

To run with a CSV:

```bash
spark-submit --class org.csu.cs435.NBABigData target/NBABigData-1.0.jar /path/to/playbyplay.csv
```
Output will show the top clutch performers, and optionally logs clutchness events for a specific player (default is John Stockton).

## Project Structure
`main():` Sets up Spark session, loads data, processes, calculates scores.

`preprocessData():` Filters events by time/score and fills missing margins.

`classifyEvent():` Tags each event with one or more human-readable labels.

`getEwpaValue():` Maps label to a numeric impact value.

`calculateClutchness():` Applies eWPA and adjusts for context.

`assignEwpaToPlayers():` Allocates impact values to player columns.

### Input Schema Expectations
| Column Name	| Description |
| ----------- | ----------- |
| GAME_ID	    | Unique game ID |
| EVENTMSGTYPE	 | Main event type code |
| EVENTMSGACTIONTYPE	 | Subtype code |
| HOMEDESCRIPTION	 | Description of event for home team |
| VISITORDESCRIPTION	 | Description of event for away team |
| PLAYER1_NAME	 | Primary player involved |
| PLAYER2_NAME	 | Secondary player (e.g. assister) |
| PLAYER3_NAME	 | Tertiary player (e.g. blocker) |
| PCTIMESTRING	 | Game clock at time of event (MM:SS) |
| PERIOD	 | Period number (4 for 4Q, 5+ for OT) |
| SCOREMARGIN	 | Current point differential |

## Configuration
To target a different player in debug output, change the playerName variable in calculateClutchness().

## Limitations
Blocks and rebounds are approximated due to data granularity.

Some edge cases (e.g., simultaneous player involvement) may underreport.

Uses forward-fill for missing SCOREMARGIN values.

## License & Credits
Developed as part of the course CS 435 - Big Data at Colorado State University.

No official affiliation with the NBA or any data providers.
