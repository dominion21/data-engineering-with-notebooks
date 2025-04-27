-- This will CREATE OR REPLACE the SQL_CLASS and SQL_VIEWS databases on Snowflake.
-- Select access need to be granted on views and tables on these schemas.
-- At the end of this script is sql which will remove these databases from your system.
-- You may need to modify the script to fit your systems particular requirements
-- but do not change any object names or the join builder demo will not work.
-- If you have any installation issues, please contact helpdesk@coffingdw.com.

CREATE OR REPLACE DATABASE Nom;
USE DATABASE Non;
CREATE OR REPLACE SCHEMA dbo;

CREATE OR REPLACE warehouse Nom_WH;
USE  warehouse Nom_WH;

use SCHEMA dbo;
CREATE OR REPLACE TABLE CUSTOMER_TABLE
 (CUSTOMER_NUMBER  INTEGER, CUSTOMER_NAME VARCHAR(20), PHONE_NUMBER CHAR(8));

CREATE OR REPLACE TABLE ORDER_TABLE
 (ORDER_NUMBER INTEGER, CUSTOMER_NUMBER INTEGER, ORDER_DATE  DATE, ORDER_TOTAL DECIMAL(10,2) );
 
CREATE OR REPLACE TABLE STUDENT_TABLE
 (STUDENT_ID INTEGER, LAST_NAME CHAR(20), FIRST_NAME VARCHAR(12), CLASS_CODE  CHAR(2) , GRADE_PT  DECIMAL(5,2) );

CREATE OR REPLACE TABLE COURSE_TABLE
 (COURSE_ID SMALLINT, COURSE_NAME  VARCHAR(30), CREDITS INTEGER, SEATS  INTEGER);

CREATE OR REPLACE TABLE STUDENT_COURSE_TABLE
 (STUDENT_ID INTEGER, COURSE_ID   SMALLINT);
 
CREATE OR REPLACE TABLE SALES_TABLE
 (PRODUCT_ID INTEGER, SALE_DATE  DATE, DAILY_SALES  DECIMAL(9,2));

CREATE OR REPLACE TABLE EMPLOYEE_TABLE
 (EMPLOYEE_NO INTEGER, DEPT_NO SMALLINT, LAST_NAME CHAR(20), FIRST_NAME VARCHAR(12), SALARY  DECIMAL(8,2) );

CREATE OR REPLACE TABLE DEPARTMENT_TABLE
 (DEPT_NO SMALLINT, DEPARTMENT_NAME CHAR(20), 
 MGR_NO  INTEGER, BUDGET  DECIMAL(10,2));
 
CREATE OR REPLACE TABLE STATS_TABLE
 (COL1 SMALLINT, COL2 SMALLINT, COL3 SMALLINT, COL4 SMALLINT, 
 COL5 SMALLINT, COL6 SMALLINT);

CREATE OR REPLACE TABLE JOB_TABLE 
 (JOB_NO INT NOT NULL UNIQUE, JOB_DESC VARCHAR(20));

CREATE OR REPLACE TABLE EMP_JOB_TABLE
 (JOB_NO INT, EMP_NO INT);

CREATE OR REPLACE TABLE NAMES_TABLE
 (NAME VARCHAR(30) NOT NULL UNIQUE);

CREATE OR REPLACE TABLE HIERARCHY_TABLE 
(EMPLOYEE_NO INTEGER 
,DEPT_NO SMALLINT
,FIRST_NAME VARCHAR(12)
,LAST_NAME CHAR(20) 
,SALARY DECIMAL (10,2) 
,POSITION_NAME CHAR (20) 
,MGR_EMPLOYEE_NO INTEGER 
);

CREATE OR REPLACE TABLE ADDRESSES
 (SUBSCRIBER_NO INTEGER, STREET VARCHAR(30), CITY VARCHAR(20), STATE CHAR(2), ZIP INTEGER, AREACODE SMALLINT, PHONE INTEGER);

CREATE OR REPLACE  TABLE SUBSCRIBERS
 (SUBSCRIBER_NO INTEGER, MEMBER_NO SMALLINT, LAST_NAME CHAR(20), FIRST_NAME VARCHAR(20), GENDER CHAR(1), SSN INTEGER);
  
CREATE OR REPLACE TABLE CLAIMS
 (CLAIM_ID INTEGER, CLAIM_DATE DATE, CLAIM_SERVICE SMALLINT, SUBSCRIBER_NO INTEGER, MEMBER_NO SMALLINT, CLAIM_AMT DECIMAL(12,2), PROVIDER_NO SMALLINT);

CREATE OR REPLACE TABLE PROVIDERS
 (PROVIDER_CODE  SMALLINT, PROVIDER_NAME   VARCHAR(30), P_ADDRESS VARCHAR(30), P_CITY  VARCHAR(20), P_STATE CHAR(2), P_ZIP  INT, P_ERROR_RATE  DEC(4,4));

CREATE OR REPLACE TABLE SERVICES
 (SERVICE_CODE  SMALLINT, SERVICE_DESC  VARCHAR(30), SERVICE_PAY  DECIMAL(7,2));

CREATE OR REPLACE TABLE BANK_SALES 
 (PRODUCT_ID NUMBER(38,0),  PRODUCT_NAME VARCHAR(20), SALE_DATE TIMESTAMP_NTZ(6), DAILY_SALES NUMBER(15,2));

CREATE OR REPLACE TABLE EMPLOYEE_PHONE 
 (FIRST_NAME VARCHAR(20),  LAST_NAME VARCHAR(20),  DEPT_NO NUMBER(38,0),  HOME_PHONE VARCHAR(20), WORK_PHONE VARCHAR(20),  CELL_PHONE VARCHAR(20));

CREATE OR REPLACE TABLE LAT_LONG 
  (STATE VARCHAR(32),   LATITUDE NUMBER(11,0),   LONGITUDE NUMBER(11,0));

CREATE OR REPLACE TABLE PIVOT_TEST_REGION_ALL 
  (REGION VARCHAR(20),   SALES_PERSON VARCHAR(30),   PRODUCT VARCHAR(20),   QUANTITY NUMBER(11,0),   COST NUMBER(10,2),   SALE_DATE DATE,   DAILY_SALES NUMBER(10,2));

CREATE OR REPLACE TABLE SALES_SIMPLE_EXAMPLE 
  (PROD NUMBER(38,0),   TOTAL_SALES NUMBER(10,2),   REGION VARCHAR(10));

CREATE OR REPLACE TABLE SALES_SIMPLE_EXAMPLE_UPDATED 
  (PROD NUMBER(38,0),   TOTAL_SALES NUMBER(10,2),   REGION VARCHAR(10));

CREATE OR REPLACE TABLE STATE_GOVERNORS 
 (STATE VARCHAR(128),  GOV_FIRST_NAME VARCHAR(20),  GOV_MIDDLE_NAME VARCHAR(20),  GOV_LAST_NAME VARCHAR(20),  PARTY VARCHAR(15),  DATE_TOOK_OFFICE DATE,  YEAR_TERM_ENDS NUMBER(11,0));

CREATE OR REPLACE TABLE WEBSITE 
  (CUSTOMER_NUMBER NUMBER(11,0),   WEB_PAGE VARCHAR(30),   CUSTOMER_TIMESTAMP TIMESTAMP_NTZ(6));




INSERT INTO SALES_SIMPLE_EXAMPLE
(PROD, TOTAL_SALES, REGION)
VALUES
(1000, 1.00, 'South'),(2000, 2.00, 'South'),(3000, 3.00, 'South'),(4000, 4.00, 'South'),(5000, 5.00, 'South'),(6000, 6.00, 'North'),(7000, 7.00, 'North'),(8000, 8.00, 'North'),(9000, 999.00, 'North'),(10000, 9999.00, 'North');

INSERT INTO SALES_SIMPLE_EXAMPLE_UPDATED
(PROD, TOTAL_SALES, REGION)
VALUES
(1000, 1.00, 'South'),(2000, 2.00, 'South'),(3000, 3.00, 'South'),(4000, 4.00, 'South'),(5000, 5.00, 'South'),(6000, 5.00, 'North'),(7000, 7.00, 'North'),(8000, 8.00, 'North'),(9000, 999.00, 'North'),(10000, 9999.00, 'North');


INSERT INTO STATE_GOVERNORS
(STATE, GOV_FIRST_NAME, GOV_MIDDLE_NAME, GOV_LAST_NAME, PARTY, DATE_TOOK_OFFICE, YEAR_TERM_ENDS)
VALUES
('AK', 'Bill', NULL, 'Walker', 'Independent', '2014-02-01', 2018),('AL', 'Robert', 'J.', 'Bentley', 'Republican', '2011-01-17', 2019),('AR', 'Asa', NULL, 'Hutchinson', 'Republican', '2015-01-13', 2019),('AZ', 'Doug', NULL, 'Ducey', 'Republican', '2015-01-05', 2019),('CA', 'Jerry', NULL, 'Brown', 'Democrat', '2011-01-03', 2019),('CO', 'John', NULL, 'Hickenlooper', 'Democrat', '2011-01-11', 2019),('CT', 'Dannel', NULL, 'Malloy', 'Democrat', '2011-01-05', 2019),('DE', 'John', NULL, 'Carney', 'Democrat', '2017-01-17', 2021),('FL', 'Rick', NULL, 'Scott', 'Republican', '2011-01-04', 2019),('GA', 'Nathan', NULL, 'Deal', 'Republican', '2011-01-10', 2019),('HI', 'David', NULL, 'Ige', 'Democrat', '2014-12-01', 2018),('IA', 'Terry', NULL, 'Branstad', 'Republican', '2011-01-04', 2019),('ID', 'Butch', NULL, 'Otter', 'Republican', '2007-01-01', 2019),('IL', 'Bruce', NULL, 'Rauner', 'Republican', '2015-01-12', 2019),('IN', 'Eric', NULL, 'Holcomb', 'Republican', '2017-01-09', 2021),('KS', 'Sam', NULL, 'Brownback', 'Republican', '2011-01-10', 2019),('KY', 'Matt', NULL, 'Bevin', 'Republican', '2015-12-08', 2019),('LA', 'John', 'Bel', 'Edwards', 'Democrat', '2016-01-11', 2020),('MA', 'Charlie', NULL, 'Baker', 'Republican', '2015-01-08', 2019),('MD', 'Larry', NULL, 'Hogan', 'Republican', '2015-01-01', 2019),('ME', 'Paul', NULL, 'LePage', 'Republican', '2011-01-11', 2019),('MI', 'Rick', NULL, 'Snyder', 'Republican', '2011-01-01', 2019),('MN', 'Mark', NULL, 'Dayton', 'Democrat', '2011-01-03', 2019),('MO', 'Eric', NULL, 'Greitens', 'Republican', '2017-01-09', 2021),('MS', 'Phil', NULL, 'Bryant', 'Republican', '2012-01-10', 2020),('MT', 'Steve', NULL, 'Bullock', 'Democrat', '2013-01-07', 2021),('NC', 'Roy', NULL, 'Cooper', 'Democrat', '2017-01-01', 2021),('ND', 'Doug', NULL, 'Burgum', 'Republican', '2016-12-15', 2020),('NE', 'Pete', NULL, 'Ricketts', 'Republican', '2015-01-08', 2019),('NH', 'Chris', NULL, 'Sununu', 'Republican', '2017-01-05', 2019),('NJ', 'Chris', NULL, 'Christie', 'Republican', '2010-01-19', 2018),('NM', 'Susana', NULL, 'Martinez', 'Republican', '2011-01-01', 2019),('NV', 'Brian', NULL, 'Sandoval', 'Republican', '2011-01-03', 2019),('NY', 'Andrew', NULL, 'Cuomo', 'Democrat', '2011-01-01', 2018),('OH', 'John', NULL, 'Kasich', 'Republican', '2011-01-10', 2019),('OK', 'Mary', NULL, 'Fallin', 'Republican', '2011-01-10', 2019),('OR', 'Kate', NULL, 'Brown', 'Democrat', '2015-02-15', 2019),('PA', 'Tom', NULL, 'Wolf', 'Democrat', '2015-01-20', 2019),('RI', 'Gina', NULL, 'Raimondo', 'Democrat', '2015-01-06', 2019),('SC', 'Henry', NULL, 'McMaster', 'Republican', '2017-01-24', 2019),('SD', 'Dennis', NULL, 'Daugaard', 'Republican', '2011-01-08', 2019),('TN', 'Bill', NULL, 'Haslam', 'Republican', '2011-01-15', 2019),('TX', 'Greg', NULL, 'Abbott', 'Republican', '2015-01-20', 2019),('UT', 'Gary', NULL, 'Herbert', 'Republican', '2009-08-11', 2021),('VA', 'Terry', NULL, 'McAuliffe', 'Democrat', '2014-01-11', 2018),('VT', 'Phil', NULL, 'Scott', 'Republican', '2017-01-05', 2019),('WA', 'Jay', NULL, 'Inslee', 'Democrat', '2013-01-16', 2021),('WI', 'Scott', NULL, 'Walker', 'Republican', '2011-01-03', 2019),('WV', 'Jim', NULL, 'Justice', 'Democrat', '2017-01-16', 2021),('WY', 'Matt', NULL, 'Mead', 'Republican', '2011-01-03', 2019);

INSERT INTO WEBSITE
(CUSTOMER_NUMBER, WEB_PAGE, CUSTOMER_TIMESTAMP)
VALUES
(1, 'Home', '2022-12-10 07:54:34.000'),(1, 'Electric Cords', '2022-12-10 07:54:55.000'),(1, 'Hammers', '2022-12-10 07:55:36.000'),(1, 'Saws', '2022-12-10 07:58:37.000'),(2, 'Home', '2022-12-10 07:54:34.000'),(2, 'Electric Cords', '2022-12-10 07:54:37.000'),(2, 'Screw Drivers', '2022-12-10 07:54:39.000'),(2, 'Tubing', '2022-12-10 07:54:44.000'),(2, 'Wrenches', '2022-12-10 07:54:35.000'),(3, 'Home', '2022-12-10 07:53:30.000'),(3, 'Hammers', '2022-12-10 07:54:34.000'),(3, 'Screw Drivers', '2022-12-10 07:54:39.000'),(3, 'Tubing', '2022-12-10 07:54:59.000'),(4, 'Home', '2022-12-10 07:54:34.000'),(4, 'Saws', '2022-12-10 07:54:44.000'),(4, 'Wrenches', '2022-12-10 07:55:34.000'),(5, 'Home', '2022-12-10 07:54:34.000'),(5, 'Electric Cords', '2022-12-10 07:54:44.000'),(5, 'Hammers', '2022-12-10 07:55:03.000'),(5, 'Saws', '2022-12-10 07:55:35.000'),(5, 'Tubing', '2022-12-10 07:55:55.000'),(5, 'Wrenches', '2022-12-10 07:56:35.000'),(6, 'Home', '2022-12-10 07:54:35.000'),(6, 'Hammers', '2022-12-10 07:54:37.000'),(6, 'Screw Drivers', '2022-12-10 07:54:39.000'),(6, 'Tubing', '2022-12-10 07:54:41.000'),(6, 'Wrenches', '2022-12-10 07:54:55.000'),(7, 'Home', '2022-12-10 07:53:30.000'),(7, 'Electric Cords', '2022-12-10 07:54:34.000'),(7, 'Hammers', '2022-12-10 07:54:35.000'),(7, 'Nails', '2022-12-10 07:54:37.000'),(7, 'Saws', '2022-12-10 07:54:39.000'),(7, 'Screw Drivers', '2022-12-10 07:54:44.000'),(7, 'Tubing', '2022-12-10 07:54:49.000'),(7, 'Wrenches', '2022-12-10 07:54:55.000'),(8, 'Home', '2022-12-10 07:54:34.000'),(8, 'Hammers', '2022-12-10 07:54:35.000'),(8, 'Nails', '2022-12-10 07:54:39.000'),(9, 'Home', '2022-12-10 07:53:30.000'),(9, 'Saws', '2022-12-10 07:54:34.000'),(9, 'Screw Drivers', '2022-12-10 07:54:39.000'),(9, 'Tubing', '2022-12-10 07:54:45.000'),(9, 'Wrenches', '2022-12-10 07:55:34.000'),(10, 'Home', '2022-12-10 07:53:30.000'),(10, 'Electric Cords', '2022-12-10 07:54:34.000'),(10, 'Hammers', '2022-12-10 07:54:35.000'),(10, 'Tubing', '2022-12-10 07:54:38.000'),(10, 'Wrenches', '2022-12-10 07:54:45.000'),(11, 'Home', '2022-12-10 07:54:34.000'),(11, 'Hammers', '2022-12-10 07:54:38.000'),(12, 'Home', '2022-12-10 07:54:34.000'),(13, 'Home', '2022-12-10 07:54:36.000'),(13, 'Saws', '2022-12-10 07:54:38.000');

