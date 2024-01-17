# Top_Spotify_Songs
#### The CSV file uploaded to this GitHub repository was collected on January 3, 2024. The dataset was obtained from Kaggle (www.kaggle.com) and originally compiled by the user 'asaniczka'. I sincerely appreciate 'asaniczka' for making this data available.

```sql
--Table creation query
drop table if exists spotify;
create table spotify(
spotify_id varchar(100),
	song_title text,
	artists text,
	daily_rank smallint,
	daily_movement smallint,
	weekly_movement smallint,
	country varchar(2),
	snapshot_date date,
	popularity smallint,
	explicity varchar(5) check(explicity in ('TRUE', 'FALSE')) ,
	duration_ms bigint,
	album_name text,
	album_released_date date,
	danceability numeric(5,3),
	energy numeric(5,3),
	key_num smallint,
	loudness numeric(5,3),
	mode_type bool,
	speechiness numeric(5,3),
	acousticness numeric(10,5),
	instrumentalness numeric(5,3),
	liveness numeric(5,3),
	valence numeric(5,3),
	tempo numeric(10,5),
	time_signature smallint
) ;  
-- \copied 284202 rows in psql and the data was from 03 jan 2024 .


-- spotify_id column  
    select count(*) as cnt
    from spotify 
    where spotify_id is null ; -- no null
  
     
    select spotify_id, 
           count(spotify_id) as count_num
    from spotify
    group by 1
    order by 2 desc --5587 distinct value 
	 
 -- song_title column
    select spotify_id,
           song_title ,
           artists ,
           album_name
    from spotify 
    where song_title is null
     ; --We will delete 22 rows where 'song_title' has null values, along with corresponding null values in 'artists' and 'album_name'. 

    delete from spotify
    where song_title is null ;    -- now we have 284185 rows remaining
	  
     
-- Artists column
    select *
    from spotify
    where artists is null -- no null
  
    select count(distinct artists) as num_distinct_artists
    from spotify -- 3782 distinct artist 
  
-- Country column 
    select count(*) as num_rows
    from spotify 
    where country  is null ;
-- For the 3856 entries with null values, we will fill them with 'global' since the data corresponds to the global category.
  
    update spotify
    set country = 'GL'
    where country is null ;
 
 -- We are creating a new column called 'country_name' and populating it with the full names of countries from another dataset named 'country_code'.
  
    alter table spotify
    add column country_name text
  
    update spotify
    set country_name = cc.country
    from country_code cc
    where spotify.country = cc.alpha_2;
 
 -- Update the 'country_name' to 'Global' where the 'country' is 'GL'."
    update spotify
    set country_name = 'Global'
    where country = 'GL'

-- album_name column 
    select * 
    from spotify 
    where album_name is null -- 155 rows ,which we will delete 

    delete from spotify
    where album_name is null ; -- remaining 284030 rows .


-- Add a new column named 'duration_min', and drop both the 'duration_ms' and 'country' columns.

    alter table spotify
    add column duration_min numeric ;

    update spotify 
    set duration_min = round((duration_ms::numeric / (1000 * 60)),2) 
	  
    alter table spotify
    drop duration_ms,
    drop country ;

-- Add a new column called 'continent' and fill it with the respective continent names based on the existing country names. 
    alter table spotify
    add column continent varchar(50) ;
	 
	update spotify
        set continent = case
	                when country_name in ('United States', 'Mexico' , 'Canada') then 'North America'
	                when country_name in ('Costa Rica', 'Dominican Republic', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama', 'El Salvador' ) then 'Central America'
	                when country_name in ('Argentina', 'Bolivia', 'Brazil', 'Colombia', 'Chile', 'Ecuador', 'Paraguay',
                         'Peru', 'Uruguay', 'Venezuela') then 'South America'
	                when country_name in ('Austria', 'Belgium', 'Bulgaria', 'Belarus', 'Switzerland', 'Czechia', 'Germany', 'Denmark', 'Estonia', 'Spain',
                         'Finland', 'France', 'United Kingdom', 'Greece', 'Hungary', 'Ireland', 'Iceland', 'Italy', 'Lithuania', 'Luxembourg','Latvia', 
						 'Norway', 'Netherlands', 'Poland', 'Portugal', 'Romania', 'Sweden', 'Slovakia', 'Ukraine', 'Czech Republic') then 'Europe'
	                when country_name in ('United Arab Emirates', 'Hong Kong', 'Israel', 'India', 'Indonesia', 'Japan', 'Kazakhstan', 
                         'Malaysia', 'Philippines', 'Pakistan', 'Saudi Arabia', 'Singapore', 'Thailand', 'Turkey', 'Taiwan', 
                         'Viet Nam', 'South Korea') then 'Asia'
                 	when country_name in ('Morocco', 'Nigeria', 'South Africa', 'Egypt') then 'Africa' 
                	when country_name in ('Australia', 'New Zealand') then 'Australia'
	                when country_name = 'Global' then 'Global' end;

-- Create a column called 'num_of_artists' and populate it with the count of artists for each song.
    alter table spotify
    add column num_of_artists smallint ;

    update spotify
    set num_of_artists =  array_length(string_to_array(artists, ', '), 1)
   

-- The number of distinct songs.
    select count(distinct song_title) as distinct_songs
    from spotify --5299 songs

-- The number of distinct artists we have. 
    with cte as(select unnest(string_to_array(artists, ', ')) as artist from spotify)
         select count(distinct artist)
         from cte  --4351 singers

-- The number of distinct countries.
    select count(distinct country_name) as countries
    from spotify  --73 countries

-- Explicity count
    select explicity,
           count(distinct song_title)
    from spotify
    group by 1
	-- We have a total of 5299 distinct songs, but the explicit count mentioned above adds up to 5335 songs in total
	--The following query retrieves 36 songs where both 'TRUE' and 'FALSE' values are explicitly present at the same time
    SELECT song_title,
           ARRAY_AGG(DISTINCT explicity) AS distinct_explicity_values
    FROM spotify
    GROUP BY song_title
    HAVING COUNT(DISTINCT explicity) > 1	
	
-- mode_type count 
    select mode_type,
           count(mode_type)
    from spotify
    group by 1

-- Top 25 artists with the most featured distinct songs
    with cte as (
         select distinct song_title,
                artists,
                trim(both ' ' from unnest(string_to_array(artists, ','))) as artist_name
         from spotify  )
         select artist_name, count(artist_name) as num_of_songs
         from cte
         group by artist_name
         order by count(artist_name) desc
         limit 25
   
-- Value count by continent
    select continent , count(*) as num
    from spotify
    group by 1

-- Oldest songs which appeared in the top for each country 
    with cte as (
    select song_title,
           artists ,
           country_name ,
           daily_rank ,
           snapshot_date,
           row_number() over(partition by country_name, song_title order by daily_rank asc) as top_X
    from spotify
    where album_released_date in (select min(album_released_date) from spotify) 
	    )
	   select song_title,
	   artists,
	   country_name,
	   daily_rank,
	   snapshot_date
	   from cte 
	   where top_X = '1'
     order by daily_rank asc
   

-- Longest song 
    select song_title,
           artists,
           daily_rank ,
           duration_min,
           album_released_date
    from spotify
    where duration_min in (select max(duration_min) from spotify)

-- The number of distinct countries in which songs appeared in the chart.
    select song_title ,
           count(distinct country_name) as num_of_country
    from spotify
    group by song_title
    order by count(distinct country_name) desc
 
 -- Songs whose popularity is greater than or equal to 99.
    select distinct artists,
           song_title,
           popularity
    from spotify
    where popularity >= 99
    order by popularity desc

-- How many times artists had their songs for daily_rank 1 .
    select unnest(string_to_array(artists, ',')) as singer_single,
           count(song_title) as num_of_times
    from spotify
    where daily_rank = '1'
    group by 1
    order by 2 desc
		
-- What is first date for each song to entry in chart .
    with cte as (
         select song_title,
         snapshot_date ,
         daily_rank ,
         row_number() over(partition by song_title order by snapshot_date asc) as num
    from spotify
	)
    select song_title,
           snapshot_date as first_date,
           daily_rank
           from cte
           where num = '1'
