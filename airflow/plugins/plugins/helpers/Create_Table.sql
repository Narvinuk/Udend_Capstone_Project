Create Table public.Immigration_Stg
    (
        cicid double precision NOT NULL,
        i94yr double precision         ,
        i94mon double precision        ,
        i94cit double precision        ,
        i94res double precision        ,
        i94port varchar(256)           ,
        arrdate double precision       ,
        i94mode double precision       ,
        i94addr varchar(256)           ,
        depdate double precision       ,
        i94bir double precision        ,
        i94visa double precision       ,
        count double precision         ,
        dtadfile varchar(256)          ,
        visapost varchar(256)          ,
        occup    varchar(256)          ,
        entdepa  varchar(256)          ,
        entdepd  varchar(256)          ,
        entdepu  varchar(256)          ,
        matflag  varchar(256)          ,
        biryear double precision       ,
        dtaddto varchar(256)           ,
        gender  varchar(256)           ,
        insnum  varchar(256)           ,
        airline varchar(256)           ,
        admnum double precision        ,
        fltno          varchar(256)             ,
        visatype       varchar(256)             ,
        arrival_date   date                     ,
        departure_date date
    )
;

Create Table Country_Stg
    (
        code    varchar(100),
        country Varchar(256)
    )
;

Create Table I94_Mode_Stg
    (
        Code           varchar(100),
        transportation varchar(256)
		)
;

Create Table Visa_Stg
    (
        code              varchar(100),
        reason_for_travel varchar(100)
    )
;

Create Table State_Stg
    (
        code  varchar(100),
        state varchar(256)
    )
;

Create Table I94_port
    (
        code             varchar(100),
        port_of_entry    varchar(256),
        city             varchar(256),
        state_or_country varchar(256)
    )
;

Create Table Us_Demographics_Stg
    (
        City                   varchar(256),
        State                  varchar(256),
        Median_Age             varchar(100),
        Male_Population        varchar(100),
        Female_Population      varchar(100),
        Total_Population       varchar(100),
        Number_of_Veterans     varchar(100),
        Foreign_born           varchar(100),
        Average_Household_Size varchar(100),
        State_Code             varchar(100),
        Race                   varchar(100),
        count                  varchar(100)
    )
;

Create Table Airport_Code_Stg
    (
        ident        varchar(100),
        type         varchar(100),
        name         varchar(100),
        elevation_ft varchar(100),
        continent    varchar(100),
        iso_country  varchar(100),
        iso_region   varchar(100),
        municipality varchar(100),
        gps_code     varchar(100),
        iata_code    varchar(100),
        local_code   varchar(100),
        coordinates  varchar(100),
        Latitude     varchar(100),
        Longitude    varchar(100)
    )
;



CREATE TABLE Dim_Visa  (
  i94visa integer,
  i94Type varchar(256)
);

CREATE TABLE Dim_State (
statecode varchar(100),
statename varchar(256)  
);

CREATE TABLE Dim_Airport  (
ident varchar(256) not null,
i94port varchar(100),
type varchar(256),
name varchar(256),
elevation_ft integer,
iso_country varchar(256),
iso_region varchar(256),
municipality varchar(256),
iata_code varchar(256),
Latitude float,
Longitude float
);

CREATE TABLE Dim_i94mode (
  i94mode integer,
  MType varchar(256)
);

CREATE TABLE Dim_Country  (
  iso_country integer,
  CName varchar(256)
);

CREATE TABLE Dim_Date (
  Date Date,
  Day integer,
  Moth integer,
  year integer,
  weekofyear integer,
  dayofweek integer
);

CREATE TABLE DIM_i94port (
code varchar(100),
port_of_entry varchar(256),
city varchar(256),
state varchar(100)

);

CREATE TABLE IMMIGRATION_FACT (
    cicid      int  primary key ,
    i94yr      int   ,
    i94mon     int   ,
    i94mode    int   ,
    i94visa    int   ,
    i94cit     int   ,
    i94res     int   ,
    i94state   varchar(100)   ,
    i94port    varchar(100)   ,
    visatype   varchar(100)   ,
    airport    varchar(100)   ,
    arrival_date   date ,
    departure_date date,
    airline      varchar(100) ,
    fltno         varchar(100),
    age           int,
    gender        varchar(100),
    stay  int,
	create_timestamp timestamp
	)
	;