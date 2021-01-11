# pyDB - Python DB Emulator

A sql lite-esque engine coded from scratch in pure python3 without any data crunching libraries like pandas / scipy. Created as a part of my graduate database project at New York University. Recieved 95/100 points and an A grade.

## Required External Packages :
1. Btrees : https://pypi.org/project/BTrees/ (used for btree indexing in tables)
2. Numpy : https://numpy.org

## Steps to run :
1. Clone the repository.
2. Install and update required python3 packages.
3. `cd repo_directory`
4. `python3 driver.py`
   
## Format of commands :

All the sample commands to be run are found in `commands.txt`. The commands follow the following syntax. More commands can be added to `commands.txt`if required. For a complete list of commands, constraints and requirements please see this [file](readme.pdf).



`R := inputfromfile(sales1)` : Used to import a vertical bar delimited file. Here `R` is the result table. Please ensure you use the `:=`to prevent syntax errors.  The first line contains the column names. The subsequent lines contain one recored each. Please view example files [one](sales1.txt) and [two](sales2.txt) for sample data and formatting. Another example is `S := inputfromfile(sales2)`.

`R1 := select(R, (time > 50) or (qty < 30))`  Equivalent to ***Select * from R where time > 50 or qty < 30***.

`R2 := project(R1, saleid, qty, pricerange)` Equivalent to ***Select saleid, qty, pricerange from R1***.

`R3 := avg(R1, qty)` Eqivalent to ***Select avg(qty) from R1***.

`R4 := sumgroup(R1, time, qty)` Equivalent to ***Select sum(time), qty from R1 group by qty***.

`R5 := sumgroup(R1, qty, time, pricerange)` Equivalent to ***Select sum(qty), time, pricerange from R1 group by time, pricerange***.

`R6 := avggroup(R1, qty, pricerange)` Equivalent to  ***Select avg(qty), pricerange from R1 group by by pricerange***.

`T := join(R, S, R.customerid = S.C)` Equivalent to  ***Select * from R, S where R.customerid = S.C***.

`T1 := join(R1, S, (R1.qty > S.Q) and (R1.saleid = S.saleid))` Equivalent to ***Select * from R1 join S where R1.qty > S.Q and R1.saleid = S.Saleid***

`T2 := sort(T1, S_C)` Equivalent to ***Select * from T1 order by S_C***.

`T2prime := sort(T1, R1_time, S_C)` Equivalent to ***Select * from T1 order by R1_time,S_C (in that order)***

`T3 := movavg(T2prime, R1_qty, 3)` Performs the three item moving average of T2prime on column R_qty. This will be as long as R_qty. Eg : three way moving average of `4 8 9 7` will be `4 6 7 8`

`T4 := movsum(T2prime, R1_qty, 5)`  Performs the five item moving sum of ***T2prime*** on column ***R_qty***

`Btree(R, qty)` or `Hash(R, qty)` Creates an index on R based on column qty. Equality selections and joins on R will use the index if available. Index uses Btree / Hash Table depending on command.

#### For a complete list of commands, constraints and requirements please see this [file](readme.pdf).








