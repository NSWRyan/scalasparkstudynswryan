# Answers explanation.
## Source data
* teleportData
  * The Teleport data contains the teleportId, passengerId, from, to, and date.
  * from, to, and date are unique to the teleportId.
  * One teleportId might have multiple passengerId.
* PassengersData
  * This data contains the passenger information (ID and name).
  * We can map the Teleport data's passengerId with this passengerId.

## Q1. Calculate the monthly Teleport count.
In this question, we are asked the total number of Teleports for each month.
From the homework provided data, we have the teleportData, which is enough to answer this question.

### Approach 1. Assuming date is unique to teleportId (1 to 1 relationship). 2 times groupBy.
1. First we do groupBy on teleportId.
2. Then on the result, we extract (mapValues) the month from each teleportId.
3. Then do another groupBy on the month and count the teleportId.

* This approach is better if the month extraction is expensive.

### Approach 2. Extract the month from date, and do groupBy.
1. First, we convert the date to month.
2. Group by on the month and count the unique teleportId within the month.

* This approach is what I took for this question.
  * On Spark, if the data is large, we can do repartition on the data based on date before doing the group by.
* This approach is simpler and closer to a what we probably use in SQL.
```sql
SELECT 
    EXTRACT(MONTH FROM date) as Month, 
    COUNT(DISTINCT teleportId) 
FROM
    PassengerData
GROUP BY Month;
```

## Q2. Identify the 100 passengers with the highest Teleport frequency.
* In this question, we need to find the passengers that are in 100 most frequent fliers.
* What we can do is extract the passengerId and the teleportId from the teleportData.
* From this information, we can extrapolate the number of Teleport for each passenger.
* Sort descending based on the number of Teleports and take the top 100.
* Do inner/outer join (depending on the requirement) on the passengerId with the PassengerData.

### Approach in SQL.
```sql
SELECT 
    Teleport.passengerId AS "Passenger ID",
    Teleport.teleportCount AS "Number of Teleports",
    passenger.firstName AS "First Name",
    passenger.lastName AS "Last Name"
FROM (
    SELECT passengerId AS passengerId, COUNT(teleportId) AS teleportCount
    FROM teleportData
    GROUP BY passengerId.
) AS Teleport
INNER JOIN
    PassengerData AS passenger
ON
    Teleport.passengerId=passenger.passengerId
ORDER BY
    Teleport.teleportCount DESC
LIMIT 100
```

### Concerns
* In this answer, we are cutting it at exactly 100 rows.
* There might be some passengers with the same Teleport count as the user rank #100 in the answer that did not get in the top 100.
* Because the question does not specifically ask for this case, I did the simplest solution (cutting at 100).
* If a consistent answer is required, we can do:
  * Add a second column to the sort before the limit.
  * Add the remaining passengers with the same Teleport count as the user in rank #100.

## Q3. Given a passenger's travel history, find the longest sequence of countries visited without the ZZ. 
* For example, if the countries a passenger was in were: 
  * ZZ -> FR -> US -> CN -> ZZ -> DE -> ZZ, 
* The correct answer would be 3 countries.
  * The longest non-ZZ sequence is 'FR -> US -> CN.

### Approach
In this question, we can do similarly to the problem statement.
1. Extract the list of countries (from and to) and date for all passengerId based on teleportData.
2. Sort the from and to based on date for each passengerId.
3. Create the countries chain.
    * Preferably with a UDF.
4. Count the length.
    1. By splitting it into subsequences with "zz" as delimiter.
    2. A new UDF that stream the countries and count. The count is reset to 0 for every "zz". We track the count and max.
5. Return the passengerId and the max.

### SQL
* While it is possible to do it in SQL, I am not providing the SQL here because it is going to be complex and might not be efficient.
* This is where Spark excels compared to SQL as UDF creation is more accessible than in some DBs.

### Scala and Spark approaches.
* In my solution, I used the same function to extract the list of countries and count the max without "zz".

## Q4. Identify pairs of passengers who have shared more than three Teleports.
* This question is a little bit tricky because we need to generate combination of passengers for each teleportId.
* Then we group by based on the combination, and do count.
* In SQL we can do self join for teleportData and get the combination.
  * But we also need to filter where passengerId 1 is smaller than passengerId 2, else we will get duplicates.
* The data can be big (m*n*(n+1)/2 where n is the number of passenger for each teleportId and m is the number of teleportId).

### Scala
* As mentioned the data can grow fast.
* If memory is a concern, we can create a hashmap with passengerId 1 and 2 as the key and count as the value.
* In my solution, I simply increased the JVM memory to 2GB to reduce the GC frequency.

### Spark
* The solution is similar to Scala one.
* [Optional] We can do repartition on the teleportId before the groupBy to speed up some operation.
* This results in partitions of data that can be processed in parallel to generate combinations of passengers for each teleportId.
* We repartition the solution based on passengerId1 and do the group by count with both passengerId 1 and 2.
* Write the answer as CSV.

### SQL
```sql
-- A similar approach with self join
SELECT * FROM (
SELECT t1.passengerId AS passangerId1, t2.passengerId AS passangerId2, count(1) as cnt
FROM 
  `teleportData` as t1 
INNER JOIN
  `teleportData` as t2
ON
  t1.passengerId<>t2.passengerId
  AND t1.teleportId=t2.teleportId
WHERE t1.passengerId<t2.passengerId
GROUP BY 1,2
) WHERE cnt>3
ORDER BY cnt DESC
;
```

## Q5. Identify pairs of passengers who have shared more than N Teleports within the date range from [from] to [to].
This is the same as Q4 with extras:
* Parameterize the count and dates.
* Add date filtering.

### Solution.
* The date filtering can be added at the early stage to make the data smaller.
* Date min max extraction can be done at the same stage as the count.
