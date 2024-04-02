# New York City Airbnb Listings

## Data Source
[Inside Airbnb](http://data.insideairbnb.com/united-states/ny/new-york-city/2020-10-05/data/listings.csv.gz)

## Data Cleaning and Integrating Steps
- Remove rows with missing column (this doesn't include column with null value)
- Fix rows with extra new lines in the column which results in one record distributed in several rows.

## Challenges
- Finding rows with extra new lines is tedious unless coming up with a meticulous regular expression.
- Not sure if listings with 0 price should be cleaned or not (was it for sale at a certain time?).
- Not sure if listings with unreasonably high minimum nights (like 999 days) shoud be cleaned or not.