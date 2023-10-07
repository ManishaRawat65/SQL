create database SQL_PROJECT_1;
select * from spotify_final_dataset;

select count(*) as num_raws,
(select count(*) from information_schema.columns
where table_name= 'spotify_final_dataset') as
num_columns from spotify_final_dataset;

alter table spotify_final_dataset rename column `Artist Name` to Artist_Name;
alter table spotify_final_dataset rename column `Song Name` to Song_Name;
alter table spotify_final_dataset rename column `Peak Position` to Peak_Position;
alter table spotify_final_dataset rename column `Total Streams` to Total_Streams;
alter table spotify_final_dataset rename column `Peak Streams` to Peak_Streams;
alter table spotify_final_dataset rename column `Peak Position (xTimes)` to Peak_Position_xtimes;
alter table spotify_final_dataset rename column `Top 10 (xTimes)` to Top_10_xtimes;

select count(distinct Artist_Name) as unique_artist_count
from spotify_final_dataset;

select Artist_Name,count(*) as repitation_count from spotify_final_dataset 
group by artist_name  having count(*) >1 order by repitation_count desc;

select * from spotify_final_dataset;

SELECT ARTIST_NAME, SONG_NAME, TOP_10_XTIMES FROM spotify_final_dataset
order by Top_10_xtimes DESC;

SELECT ARTIST_NAME ,SONG_NAME, PEAK_POSITION_XTIMES FROM spotify_final_dataset
ORDER BY Peak_Position_xtimes DESC;

update spotify_final_dataset set peak_position_xtimes='(x1)' 
where peak_position_xtimes='0';
update spotify_final_dataset 
set peak_position_xtimes = left(peak_position_xtimes, length(peak_position_xtimes)-1);
update spotify_final_dataset
set peak_position_xtimes = substring(peak_position_xtimes,3);

alter table spotify_final_dataset add total_PeakPosition int;
update spotify_final_dataset set total_PeakPosition = peak_position*peak_position_xtimes;

select * from spotify_final_dataset;

select artist_name, song_name,total_peakposition from spotify_final_dataset
order by total_peakposition desc;

select artist_name, song_name, total_streams from spotify_final_dataset
order by Total_Streams desc;

select max(days) as max_days ,min(days) as min_days 
from spotify_final_dataset;

select * from spotify_final_dataset;
alter table spotify_final_dataset
PARTITION BY RANGE (days) (
    PARTITION p0 VALUES LESS THAN (500),
    PARTITION p1 VALUES LESS THAN (1000),
    PARTITION p2 VALUES LESS THAN (1500),
    PARTITION p3 VALUES LESS THAN (2000),
    PARTITION p4 VALUES LESS THAN MAXVALUE
);

select Artist_name, song_name, total_streams,days,
rank() over(partition by artist_name order by days) as rank_releasedays
from spotify_final_dataset;