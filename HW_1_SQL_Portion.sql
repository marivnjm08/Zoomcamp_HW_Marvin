#Question 3. Count records

select count(*)
from green_taxi_trips
where date(lpep_pickup_datetime) = '2019-01-15' and date(lpep_dropoff_datetime) = '2019-01-15'


#Question 4. Largest trip for each day

select date(lpep_pickup_datetime),max(trip_distance) as trip_distance
from green_taxi_trips
group by date(lpep_pickup_datetime)
order by trip_distance desc


#Question 5. The number of passengers

select passenger_count,count(*)
from green_taxi_trips
where date(lpep_pickup_datetime) = '2019-01-01'
group by passenger_count


#Question 6. Largest tip

select c."Zone",max(a."tip_amount") as tip_amount
from green_taxi_trips a
left join taxi_dim b on a."PULocationID" = b."LocationID"  
left join taxi_dim c on a."DOLocationID" = c."LocationID"  
where b."Zone" = 'Astoria'
group by c."Zone"
order by tip_amount desc
