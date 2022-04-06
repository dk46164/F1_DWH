CREATE OR REPLACE WAREHOUSE F1_WH WAREHOUSE_SIZE='X-SMALL' 
INITIALLY_SUSPENDED=FALSE
AUTO_SUSPEND = 300;


CREATE DATABASE F1_DWH;
CREATE SCHEMA F1_DWH.F1_STAGE;


--FILE FORMAT  FOR CSV FILE
CREATE OR REPLACE FILE FORMAT STAGE_FORMAT
  FIELD_DELIMITER = ','
  SKIP_HEADER =1
 ENCODING = 'UTF-8';

----EXT  STAGE
CREATE  OR REPLACE STAGE F1_STAGE
  FILE_FORMAT = STAGE_FORMAT ;


CREATE TABLE circuits (
  circuitId INTEGER NOT NULL AUTOINCREMENT START 1 increment 1,
  circuitRef varchar(255) NOT NULL DEFAULT '',
  name varchar(255) NOT NULL DEFAULT '',
  location varchar(255) DEFAULT NULL,
  country varchar(255) DEFAULT NULL,
  lat float DEFAULT NULL,
  lng float DEFAULT NULL,
  alt INTEGER DEFAULT NULL,
  url varchar(255) NOT NULL DEFAULT '',
  constraint  circuit_Id PRIMARY KEY  (circuitId));



CREATE TABLE constructors (
  constructorId INTEGER NOT NULL AUTOINCREMENT START 1 increment 1,
  constructorRef varchar(255) NOT NULL DEFAULT '',
  name varchar(255) NOT NULL DEFAULT '',
  nationality varchar(255) DEFAULT NULL,
  url varchar(255) NOT NULL DEFAULT '',
 constraint  constructor_Id PRIMARY KEY  (constructorId)
);


CREATE TABLE drivers (
  driverId INTEGER NOT NULL AUTOINCREMENT START 1 increment 1,
  driverRef varchar(255) NOT NULL DEFAULT '',
  number INTEGER DEFAULT NULL,
  code varchar(3) DEFAULT NULL,
  forename varchar(255) NOT NULL DEFAULT '',
  surname varchar(255) NOT NULL DEFAULT '',
  dob date DEFAULT NULL,
  nationality varchar(255) DEFAULT NULL,
  url varchar(255) NOT NULL DEFAULT '',
  constraint  driver_Id PRIMARY KEY  (driverId)
);


CREATE TABLE races (
  raceId INTEGER NOT NULL AUTOINCREMENT START 1 increment 1,
  year INTEGER NOT NULL DEFAULT 0,
  round INTEGER NOT NULL DEFAULT 0,
  circuitId INTEGER NOT NULL DEFAULT 0,
  name varchar(255) NOT NULL DEFAULT '',
  date date NOT NULL,
  time time DEFAULT NULL,
  url varchar(255) DEFAULT NULL,
  constraint  driver_Id PRIMARY KEY  (raceId),
   constraint circuitI_d foreign key (circuitId) references circuits (circuitId) 
);


CREATE TABLE results (
  resultId INTEGER NOT NULL AUTOINCREMENT START 1 increment 1,
  raceId INTEGER NOT NULL DEFAULT 0,
  driverId INTEGER NOT NULL DEFAULT 0,
  constructorId INTEGER NOT NULL DEFAULT 0,
  number INTEGER NOT NULL DEFAULT 0,
  grid INTEGER NOT NULL DEFAULT 0,
  position INTEGER DEFAULT NULL,
  positionText varchar(255) NOT NULL DEFAULT '',
  positionOrder INTEGER NOT NULL DEFAULT 0,
  points float NOT NULL DEFAULT 0,
  laps INTEGER NOT NULL DEFAULT 0,
  time varchar(255) DEFAULT NULL,
  milliseconds INTEGER DEFAULT NULL,
  fastestLap INTEGER DEFAULT NULL,
  rank INTEGER DEFAULT 0,
  fastestLapTime  TIME DEFAULT NULL,
  fastestLapSpeed varchar(255) DEFAULT NULL,
  statusId INTEGER NOT NULL DEFAULT 0,
 constraint  result_Id PRIMARY KEY  (resultId) 
);


CREATE TABLE seasons (
  year INTEGER NOT NULL DEFAULT 0,
  url varchar(255) NOT NULL DEFAULT '',
  constraint  year_id PRIMARY KEY  (year) 
);




CREATE TABLE status (
  statusId INTEGER NOT NULL AUTOINCREMENT START 1 increment 1,
  status varchar(255) NOT NULL DEFAULT '',
  constraint  statusId PRIMARY KEY  (statusId) 
);
