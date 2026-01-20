import duckdb

query = f"""
SELECT
    count(*) as count
    FROM output.parquet
    Where type = 'Generator' and operable_status = 'Operable' and year = '2024'
"""
print('A: currently operable generators (2024 data):')
print(duckdb.sql(query).df())

print()

#B

query2 = f"""
SELECT
    utility_name
    FROM output.parquet
    Where operable_status = 'Operable' and year = '2024'
    group by type
"""
print()
print('B: ran out of time for B')
#print(duckdb.sql(query2).df())

print()
#C
queryC = f"""
SELECT
    state
    , type
    , count(*)
    FROM output.parquet
    Where operable_status = 'Operable' and year = '2024'
    group by type, state
    order by state asc
"""
print("C: counts per type per state")
print(duckdb.sql(queryC).df())
print()
#D
queryD = f"""
CREATE TEMPORARY TABLE temp AS SELECT
    utility_name,
    count(distinct state) as num_states
    FROM output.parquet
    Where operable_status = 'Operable' and year = '2024' and type = 'Solar'
    group by utility_name;
    
    SELECT count(*) as utilities_in_multiple_states FROM temp where num_states > 1
"""
print("D: utilities in multiple states")
print(duckdb.sql(queryD).df())

#E
print("Couldn't answer E. Not sure about batteries")

#F
queryF = f"""
CREATE TEMPORARY TABLE lastyear AS SELECT
    utility_name
    ,type
    ,sum(capacity) as capacity
    FROM output.parquet
    Where operable_status = 'Operable' and year = '2023' and type in ('Solar', 'Wind')
    group by utility_name, type;

CREATE TEMPORARY TABLE thisyear AS SELECT
    utility_name
    ,type
    ,sum(capacity) as capacity
    FROM output.parquet
    Where operable_status = 'Operable' and year = '2024' and type in ('Solar', 'Wind')
    group by utility_name, type;
    
    SELECT thisyear.utility_name
    ,thisyear.type
    ,thisyear.capacity/lastyear.capacity - 1as perc_change
    FROM thisyear
    left join lastyear on lastyear.utility_name = thisyear.utility_name and lastyear.type = thisyear.type
"""
print("F: 2023-2024 % Increase in capacity for wind and solar per utility")
print(duckdb.sql(queryF).df())
#G
#no mention of 'region' in file columns or on website
print()
print('Could not answer G. Unsure about region')
print()
print("H: How many generators are > 40 (as of 2024)")
queryH = f"""
SELECT
    count(generator_id)
    FROM output.parquet
    Where operable_status = 'Operable' and operating_year < '1984' and type = 'Generator'
"""
print(duckdb.sql(queryH).df())

print("H (cont.) and what is their total capacity")
queryHPartTwo = f"""
SELECT
    sum(capacity) as total
    FROM output.parquet
    Where operable_status = 'Operable' and operating_year < '1984' and type = 'Generator'
"""
print(duckdb.sql(queryHPartTwo).df())

#I
print()
print('Ran out of time for I')

#J
queryJ = f"""
SELECT
    sum(capacity) as total
    FROM output.parquet
    Where operable_status = 'Proposed'
"""
print()
print("J: total proposed capacity")
print(duckdb.sql(queryJ).df())
queryJPartTwo = f"""
SELECT
    sum(capacity) as total
    FROM output.parquet
    Where operable_status = 'Retired and Canceled' and retirement_year >= 2019
"""
print("J: capacity retired since 2019 (inclusive):")
print(duckdb.sql(queryJPartTwo).df())

