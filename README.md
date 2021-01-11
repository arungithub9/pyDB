# pyDB - Python DB Emulator

A sql lite-esque engine coded from scratch in python without any data crunching libraries like pandas / scipy. Created as a part of my graduate database project at New York University. Recieved 95/100 points and A grade.

## Required External Packages :
1. Btrees : https://pypi.org/project/BTrees/
2. Numpy : https://numpy.org

## Steps to run :
1. Clone the repository.
2. Install and update required python3 packages.
3. `cd repo_directory`
4. `python3 driver.py`
   
## Format of commands :

All the sample commands to be run are found in `commands.txt`. The commands follow the following syntax. More commands can be added to `commands.txt`if required. 

`R := inputfromfile(sales1)` : Used to import a vertical bar delimited file. Here `R` is the result table. Please ensure you use the `:=`to prevent syntax errors.  The first line contains the column names. The subsequent lines contain one recored each. Please view example files [one](sales1.txt) and [two](sales2.txt) for sample data and formatting.

`R1 := select(R, (time > 50) or (qty < 30))` : Equivalent to *select * from R where time > 50 or qty < 30*






