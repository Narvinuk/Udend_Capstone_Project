class SqlQueries:
    immigration_table_insert = ("""
        SELECT 
        distinct
        CAST (STG.CICID as Integer),
        CAST (STG.I94YR as Integer) ,
        CAST (STG.I94MON  as Integer),
        M.I94MODE ,
        V.I94VISA ,
        C.iso_country as I94CIT ,
        C.iso_country as I94RES ,
        S.statecode as I94STATE, 
        P.code as I94PORT ,
        visatype,
        a.Ident as airport,
        arrival_date ,
        departure_date ,
        AIRLINE ,
        FLTNO ,
        cast(i94bir as int)  as AGE ,
        CASE WHEN TRIM(GENDER) is null THEN 'UNK' ELSE GENDER END AS GENDER 
        ,
        CASE WHEN departure_date is null then  datediff(day, arrival_date ,current_date) 
        ELSE 
        datediff(day,arrival_date,departure_date  ) END As STAY ,
        current_timestamp

        FROM Immigration_Stg STG
        LEFT JOIN Dim_I94mode M ON 
        M.i94mode=cast(STG.i94mode as  integer)
        LEFT JOIN DIM_visa V on
        V.i94visa=cast(STG.i94visa as int)
        LEFT JOIN Dim_Country C ON
        C.iso_country=CAST(STG.i94cit as int)
        LEFT JOIN DIM_i94port P ON
        P.CODE=STG.i94port 
        LEFT JOIN 
        (SELECT Max(ap.ident) as ident ,ap.i94port from Dim_airport ap group by ap.i94port ) a  on
        A.i94port=P.CODE  
        LEFT JOIN DIM_state S ON
        S.statecode=UPPER(Trim(P.state))
            
    """)
    country_table_insert = ("""
        SELECT distinct cast (code as integer), country
        FROM country_stg
       
    """)
    state_table_insert = ("""
        SELECT distinct code, state
        FROM state_stg
       
    """)

    i94mode_table_insert = ("""
        SELECT distinct cast (code as integer), transportation
        FROM i94_mode_stg
    """)
    
    i94visa_table_insert = ("""
        SELECT distinct cast (code as integer) , reason_for_travel 
        FROM visa_stg
    """)
    
    i94port_table_insert = ("""
        SELECT distinct  code, port_of_entry , city,state_or_country 
        FROM i94_port
    """)

    airport_table_insert = ("""
        SELECT distinct  ident  ,p.CODE, type,name,cast (elevation_ft as  integer),iso_country,iso_region,municipality,iata_code,cast (latitude as float),cast (longitude as float) 
        from airport_code_stg a  Join i94_port p 
        on UPPER(TRIM(a.iso_region))=upper(TRIM(p.state_or_country)) 
        and  upper(trim(a.municipality))=upper(trim(p.city))

    """)

    Date_table_insert = ("""
        select distinct arrival_Date as Date ,extract(Day from arrival_Date) as Day ,extract(dayofweek from arrival_Date) DayofWeek,  extract(month from arrival_Date) as Month,
        extract(week from arrival_Date) as weekofyear,     extract(year from arrival_Date)  as year from immigration_stg
        where arrival_Date is not null
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    check_songplay_null = (""" SELECT Count(*) From songplays where start_time is null """)
           
    check_users_null = (""" SELECT Count(*) From users where user_id is null """)
    
    check_songs_null = (""" SELECT Count(*) From songs where title is null """)
    
    check_artists_null = (""" SELECT Count(*) From artists where artist_id is null """)
    
    check_time_null = (""" SELECT Count(*) From time where start_time is null """)


