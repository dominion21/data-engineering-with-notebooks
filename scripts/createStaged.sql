-- Number of header lines at the start of the file. 
-- The COPY command skips header lines when loading data
-- All these file formats can be used to create different stage areas

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE OR REPLACE DATABASE LOCAL_FF_DB;
USE DATABASE LOCAL_FF_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE  FILE FORMAT 
		ff_IPEDS_CSVSkipHeaderTabDelimited
			TYPE =   CSV  
			FIELD_DELIMITER = '\t'  COMPRESSION = AUTO
			SKIP_HEADER = 1;
CREATE OR REPLACE  FILE FORMAT 
		ff_IPEDS_CSVSkipHeaderCommaDelimited 
			TYPE =   CSV  
			FIELD_DELIMITER = ','  COMPRESSION = AUTO
			SKIP_HEADER = 1;
-- The COPY command does not skip any lines.  
CREATE OR REPLACE  FILE FORMAT 
		ff_IPEDS_CSVHeaderTabDelimited
			TYPE =   CSV  
			FIELD_DELIMITER = '\t'  COMPRESSION = AUTO
			SKIP_HEADER = 1 ; 
CREATE OR REPLACE  FILE FORMAT 
		ff_IPEDS_CSVHeaderCommaDelimited  
			TYPE =   CSV  
			FIELD_DELIMITER = ','  COMPRESSION = AUTO
			SKIP_HEADER = 0 encoding = 'iso-8859-1' 
			FIELD_OPTIONALLY_ENCLOSED_BY='"'; 
-- create satge area to load csv files for the project
CREATE OR REPLACE STAGE IPEDS_HD FILE_FORMAT 
			= ff_IPEDS_CSVHeaderCommaDelimited;
CREATE OR REPLACE STAGE IPEDS_CM FILE_FORMAT 
			= ff_IPEDS_CSVSkipHeaderTabDelimited; 

-- Create file format to load json file 
CREATE OR REPLACE FILE FORMAT ff_IPEDS_Json
		TYPE =JSON TRIM_SPACE = TRUE; 
-- Create stage area to load json  
CREATE OR REPLACE stage IPEDS_EFFY FILE_FORMAT = ff_IPEDS_Json;

-- Create file format to load ORC file 
CREATE OR REPLACE FILE FORMAT ff_IPEDS_ORC
			TYPE =ORC TRIM_SPACE = TRUE;
-- Create stage area to load ORC file 
CREATE OR REPLACE stage IPEDS_IC FILE_FORMAT = ff_IPEDS_ORC;

-- Create Parquet file format
CREATE OR REPLACE  FILE FORMAT ff_IPEDS_Parquet
			TYPE =PARQUET TRIM_SPACE = TRUE;
-- Create stage area to load Parquet file 
CREATE OR REPLACE stage IPEDS_ADM FILE_FORMAT = ff_IPEDS_Parquet;

put file://D:\snowflake_python_conn\load_sf_py\nexus\hd_csv\HD2017.csv @IPEDS_HD;  
put file://D:\snowflake_python_conn\load_sf_py\nexus\hd_csv\HD2018.csv @IPEDS_HD;
put file://D:\snowflake_python_conn\load_sf_py\nexus\hd_csv\HD2019.csv @IPEDS_HD;

put file://D:\snowflake_python_conn\load_sf_py\nexus\effy_json\effy2017_rv.json @IPEDS_EFFY;
put file://D:\snowflake_python_conn\load_sf_py\nexus\effy_json\effy2018_rv.json @IPEDS_EFFY;
put file://D:\snowflake_python_conn\load_sf_py\nexus\effy_json\effy2019_rv.json @IPEDS_EFFY;

put file://D:\snowflake_python_conn\load_sf_py\nexus\ORC\ic2017_ay_orc.orc @IPEDS_IC/2017/;
put file://D:\snowflake_python_conn\load_sf_py\nexus\ORC\ic2018_ay_orc.orc @IPEDS_IC/2018/;
put file://D:\snowflake_python_conn\load_sf_py\nexus\ORC\ic2019_ay_orc.orc @IPEDS_IC/2019/;

put file://D:\snowflake_python_conn\load_sf_py\nexus\ADM_parquet\adm2017.parquet @IPEDS_ADM/2017/;
put file://D:\snowflake_python_conn\load_sf_py\nexus\ADM_parquet\adm2018.parquet @IPEDS_ADM/2018/;
put file://D:\snowflake_python_conn\load_sf_py\nexus\ADM_parquet\adm2019.parquet @IPEDS_ADM/2019/;

DROP TABLE IF EXISTS od_AcademicInstitution;

CREATE TABLE od_AcademicInstitution (
  InstitutionIdentifier INTEGER,
  InstitutionName STRING,
  InstitutionNameAlias STRING,
  StreetAddress STRING,
  City STRING,
  State VARCHAR(10),
  ZipCode VARCHAR(20),
  StateCode INTEGER,
  EconomicAnalysisRegions INTEGER,
  ChiefAdministrator STRING,
  ChiefAdministratorTitle STRING,
  TelephoneNumber STRING,
  EmployerIdentificationNumber STRING,
  DunBradstreetNumbers STRING,
  PostsecondaryEducationIDNumber STRING,
  TitleIVEligibilityIndicatorCode INTEGER,
  InstitutionsWebAddress STRING,
  AdmissionsOfficeWebAddress STRING,
  FinancialAidOfficeWebAddress STRING,
  OnlineApplicationWebAddress STRING,
  NetPriceCalculatorWebAddress STRING,
  VeteransMilitaryServiceTuitionPoliciesWebAddress STRING,
  StudentRightAthleteGraduationRateWebAddress STRING,
  DisabilityServicesWebAddress STRING,
  SectorOfInstitution INTEGER,
  LevelOfInstitution INTEGER,
  ControlOfInstitution INTEGER,
  HighestLevelOfOffering INTEGER,
  UndergraduateOffering INTEGER,
  GraduateOffering INTEGER,
  HighestDegreeOffered INTEGER,
  DegreeGrantingStatus INTEGER,
  HistoricallyBlackCollegeOrUniversity INTEGER,
  InstitutionHasHospital INTEGER,
  InstitutionGrantsMedicalDegree INTEGER,
  TribalCollege INTEGER,
  DegreeOfUrbanization INTEGER,
  InstitutionOpenToGeneralPublic INTEGER,
  StatusOfInstitution VARCHAR(10),
  UnitidForMergedSchools STRING,
  YearInstitutionWasDeletedFromIPEDS INTEGER,
  DateInstitutionClosed VARCHAR(20),
  InstitutionIsActive INTEGER,
  PrimarilyPostsecondaryIndicator INTEGER,
  PostsecondaryInstitutionIndicator INTEGER,
  PostsecondaryAndTitleIvInstitutionIndicator INTEGER,
  ReportingMethodForStudentCharges INTEGER,
  InstitutionalCategory INTEGER,
  CarnegieClassification2015Basic INTEGER,
  CarnegieClassification2015UndergraduateProgram INTEGER,
  CarnegieClassification2015GraduateProgram INTEGER,
  CarnegieClassification2015UndergraduateProfile INTEGER,
  CarnegieClassification2015EnrollmentProfile INTEGER,
  CarnegieClassification2015SizeSetting INTEGER,
  CarnegieClassification20052010Basic INTEGER,
  CarnegieClassification2000 INTEGER,
  LandGrantInstitution INTEGER,
  InstitutionSizeCategory INTEGER,
  MultiCampusOrganization INTEGER,
  NameOfMultiCampusOrganization STRING,
  IdentificationNumberOfMultiCampusOrganization INTEGER,
  CoreBasedStatisticalArea INTEGER,
  CBSATypeMetropolitanMicropolitan INTEGER,
  CombinedStatisticalArea INTEGER,
  NewEnglandCityAndTownArea INTEGER,
  FIPSCountyCode INTEGER,
  CountyName STRING,
  StateAnd114thCongressionalDistrictID INTEGER,
  LongitudeLocation NUMBER(32, 0),
  LatitudeLocation NUMBER(32, 0),
  NCESGroupCategory INTEGER,
  DataFeedbackReport INTEGER,
  AcademicYear INTEGER,
  IngestedFileName STRING,
  RowNumber INTEGER
);

DROP VIEW IF EXISTS v_od_AcademicInstitution;

CREATE OR REPLACE VIEW v_od_AcademicInstitution  AS 
SELECT
t.$1  AS InstitutionIdentifier,
t.$2  AS InstitutionName,
t.$3  AS InstitutionNameAlias,
t.$4  AS StreetAddress,
t.$5  AS City,
t.$6  AS State,
t.$7  AS ZipCode,
t.$8  AS StateCode,
t.$9  AS EconomicAnalysisRegions,
t.$10 AS ChiefAdministrator,
t.$11 AS ChiefAdministratorTitle,
t.$12 AS TelephoneNumber,
t.$13 AS EmployerIdentificationNumber,
t.$14 AS DunBradstreetNumbers,
t.$15 AS PostsecondaryEducationIDNumber,
t.$16 AS TitleIVEligibilityIndicatorCode,
t.$17 AS InstitutionsWebAddress,
t.$18 AS AdmissionsOfficeWebAddress,
t.$19 AS FinancialAidOfficeWebAddress,
t.$20 AS OnlineApplicationWebAddress,
t.$21 AS NetPriceCalculatorWebAddress,
t.$22 AS VeteransMilitaryServiceTuitionPoliciesWebAddress,
t.$23 AS StudentRightAthleteGraduationRateWebAddress,
t.$24 AS DisabilityServicesWebAddress,
t.$25 AS SectorOfInstitution,
t.$26 AS LevelOfInstitution,
t.$27 AS ControlOfInstitution,
t.$28 AS HighestLevelOfOffering,
t.$29 AS UndergraduateOffering,
t.$30 AS GraduateOffering,
t.$31 AS HighestDegreeOffered,
t.$32 AS DegreeGrantingStatus,
t.$33 AS HistoricallyBlackCollegeOrUniversity,
t.$34 AS InstitutionHasHospital,
t.$35 AS InstitutionGrantsMedicalDegree,
t.$36 AS TribalCollege,
t.$37 AS DegreeOfUrbanization,
t.$38 AS InstitutionOpenToGeneralPublic,
t.$39 AS StatusOfInstitution,
t.$40 AS UnitidForMergedSchools,
t.$41 AS YearInstitutionWasDeletedFromIPEDS,
t.$42 AS DateInstitutionClosed,
t.$43 AS InstitutionIsActive,
t.$44 AS PrimarilyPostsecondaryIndicator,
t.$45 AS PostsecondaryInstitutionIndicator,
t.$46 AS PostsecondaryAndTitleIvInstitutionIndicator,
t.$47 AS ReportingMethodForStudentCharges,
t.$48 AS InstitutionalCategory,
t.$49 AS CarnegieClassification2015Basic,
t.$50 AS CarnegieClassification2015UndergraduateProgram,
t.$51 AS CarnegieClassification2015GraduateProgram,
t.$52 AS CarnegieClassification2015UndergraduateProfile,
t.$53 AS CarnegieClassification2015EnrollmentProfile,
t.$54 AS CarnegieClassification2015SizeSetting,
t.$55 AS CarnegieClassification20052010Basic,
t.$56 AS CarnegieClassification2000,
t.$57 AS LandGrantInstitution,
t.$58 AS InstitutionSizeCategory,
t.$59 AS MultiCampusOrganization,
t.$60 AS NameOfMultiCampusOrganization,
t.$61 AS IdentificationNumberOfMultiCampusOrganization,
t.$62 AS CoreBasedStatisticalArea,
t.$63 AS CBSATypeMetropolitanMicropolitan,
t.$64 AS CombinedStatisticalArea,
t.$65 AS NewEnglandCityAndTownArea,
t.$66 AS FIPSCountyCode,
t.$67 AS CountyName,
t.$68 AS StateAnd114thCongressionalDistrictID,
t.$69 AS LongitudeLocation,
t.$70 AS LatitudeLocation,
t.$71 AS NCESGroupCategory,
t.$72 AS DataFeedbackReport,
CAST(substring(metadata$filename, 3, 4) AS INTEGER) 
			AcademicYear,
metadata$filename IngestedFileName,
metadata$file_row_number-1 RowNumber 
FROM
   @IPEDS_HD t 
WHERE
   metadata$file_row_number > 1;


CREATE OR REPLACE PROCEDURE 
	pr_od_AcademicInstitution_Load (YEAR FLOAT)
RETURNS STRING
LANGUAGE javascript
EXECUTE AS OWNER
AS
$$
var sql_command = `
   INSERT INTO   od_AcademicInstitution 
    (InstitutionIdentifier,
    InstitutionName,
    InstitutionNameAlias,
    StreetAddress,
    City,
    State,
    ZipCode,
    StateCode,
    EconomicAnalysisRegions,
    ChiefAdministrator,
    ChiefAdministratorTitle,
    TelephoneNumber,
    EmployerIdentificationNumber,
    DunBradstreetNumbers,
    PostsecondaryEducationIDNumber,
    TitleIVEligibilityIndicatorCode,
    InstitutionsWebAddress,
    AdmissionsOfficeWebAddress,
    FinancialAidOfficeWebAddress,
    OnlineApplicationWebAddress,
    NetPriceCalculatorWebAddress,
    VeteransMilitaryServiceTuitionPoliciesWebAddress,
    StudentRightAthleteGraduationRateWebAddress,
    DisabilityServicesWebAddress,
    SectorOfInstitution,
    LevelOfInstitution,
    ControlOfInstitution,
    HighestLevelOfOffering,
    UndergraduateOffering,
    GraduateOffering,
    HighestDegreeOffered,
    DegreeGrantingStatus,
    HistoricallyBlackCollegeOrUniversity,
    InstitutionHasHospital,
    InstitutionGrantsMedicalDegree,
    TribalCollege,
    DegreeOfUrbanization,
    InstitutionOpenToGeneralPublic,
    StatusOfInstitution,
    UnitidForMergedSchools,
    YearInstitutionWasDeletedFromIPEDS,
    DateInstitutionClosed,
    InstitutionIsActive,
    PrimarilyPostsecondaryIndicator,
    PostsecondaryInstitutionIndicator,
    PostsecondaryAndTitleIvInstitutionIndicator,
    ReportingMethodForStudentCharges,
    InstitutionalCategory,
    CarnegieClassification2015UndergraduateProgram,
    CarnegieClassification2015GraduateProgram,
    CarnegieClassification2015UndergraduateProfile,
    CarnegieClassification2015EnrollmentProfile,
    CarnegieClassification2015SizeSetting,
    CarnegieClassification2015Basic,
    CarnegieClassification20052010Basic,
    CarnegieClassification2000,
    LandGrantInstitution,
    InstitutionSizeCategory,
    MultiCampusOrganization,
    NameOfMultiCampusOrganization,
    IdentificationNumberOfMultiCampusOrganization,
    CoreBasedStatisticalArea,
    CBSATypeMetropolitanMicropolitan,
    CombinedStatisticalArea,
    NewEnglandCityAndTownArea,
    FIPSCountyCode,
    CountyName,
    StateAnd114thCongressionalDistrictID,
    LongitudeLocation,
    LatitudeLocation,
    NCESGroupCategory,
    DataFeedbackReport,
    AcademicYear,
    IngestedFileName,
    RowNumber  )
	SELECT
	  InstitutionIdentifier,
	  InstitutionName,
	  InstitutionNameAlias,
	  StreetAddress,
	  City,
	  State,
	  ZipCode,
	  StateCode,
	  EconomicAnalysisRegions,
	  ChiefAdministrator,
	  ChiefAdministratorTitle,
	  TelephoneNumber,
	  EmployerIdentificationNumber,
	  DunBradstreetNumbers,
	  PostsecondaryEducationIDNumber,
	  TitleIVEligibilityIndicatorCode,
	  InstitutionsWebAddress,
	  AdmissionsOfficeWebAddress,
	  FinancialAidOfficeWebAddress,
	  OnlineApplicationWebAddress,
	  NetPriceCalculatorWebAddress,
	  VeteransMilitaryServiceTuitionPoliciesWebAddress,
	  StudentRightAthleteGraduationRateWebAddress,
	  DisabilityServicesWebAddress,
	  SectorOfInstitution,
	  LevelOfInstitution,
	  ControlOfInstitution,
	  HighestLevelOfOffering,
	  UndergraduateOffering,
	  GraduateOffering,
	  HighestDegreeOffered,
	  DegreeGrantingStatus,
	  HistoricallyBlackCollegeOrUniversity,
	  InstitutionHasHospital,
	  InstitutionGrantsMedicalDegree,
	  TribalCollege,
	  DegreeOfUrbanization,
	  InstitutionOpenToGeneralPublic,
	  StatusOfInstitution,
	  UnitidForMergedSchools,
	  YearInstitutionWasDeletedFromIPEDS,
	  DateInstitutionClosed,
	  InstitutionIsActive,
	  PrimarilyPostsecondaryIndicator,
	  PostsecondaryInstitutionIndicator,
	  PostsecondaryAndTitleIvInstitutionIndicator,
	  ReportingMethodForStudentCharges,
	  InstitutionalCategory,
	  CarnegieClassification2015UndergraduateProgram,
	  CarnegieClassification2015GraduateProgram,
	  CarnegieClassification2015UndergraduateProfile,
	  CarnegieClassification2015EnrollmentProfile,
	  CarnegieClassification2015SizeSetting,
	  CarnegieClassification2015Basic,
	  CarnegieClassification20052010Basic,
	  CarnegieClassification2000,
	  LandGrantInstitution,
	  InstitutionSizeCategory,
	  MultiCampusOrganization,
	  NameOfMultiCampusOrganization,
	  IdentificationNumberOfMultiCampusOrganization,
	  CoreBasedStatisticalArea,
	  CBSATypeMetropolitanMicropolitan,
	  CombinedStatisticalArea,
	  NewEnglandCityAndTownArea,
	  FIPSCountyCode,
	  CountyName,
	  StateAnd114thCongressionalDistrictID,
	  LongitudeLocation,
	  LatitudeLocation,
	  NCESGroupCategory,
	  DataFeedbackReport,
	  AcademicYear,
	  IngestedFileName,
	  RowNumber
	FROM
	  v_od_AcademicInstitution 
    WHERE AcademicYear = ` + YEAR.toString()+`;`

    try {
        snowflake.execute (
          {sqlText: sql_command}
          );
        return "Succeeded.";  // Return a success/error indicator.
        }
    catch (err) {
        return "Failed: " + err;  // Return a success/error indicator.
        }
$$
;


DROP TABLE IF EXISTS AcademicInstitution;

CREATE TABLE AcademicInstitution (
  AcademicInstitutionUniqueDWSID INTEGER 
		NOT NULL DEFAULT SEQ_IPEDS_HD.NEXTVAL,
  InstitutionIdentifier INTEGER,
  InstitutionName VARCHAR(2000),
  InstitutionNameAlias VARCHAR(2000),
  StreetAddress VARCHAR(500),
  City VARCHAR(100),
  State VARCHAR(20),
  ZipCode VARCHAR(15),
  StateCode INTEGER,
  EconomicAnalysisRegions INTEGER,
  ChiefAdministrator VARCHAR(500),
  ChiefAdministratorTitle VARCHAR(500),
  TelephoneNumber VARCHAR(15),
  EmployerIdentificationNumber VARCHAR(50),
  DunBradstreetNumbers VARCHAR(2000),
  PostsecondaryEducationIDNumber VARCHAR(8),
  TitleIVEligibilityIndicatorCode INTEGER, 
  SectorOfInstitution INTEGER,
  LevelOfInstitution INTEGER,
  ControlOfInstitution INTEGER,
  HighestLevelOfOffering INTEGER,
  UndergraduateOffering INTEGER,
  GraduateOffering INTEGER,
  HighestDegreeOffered INTEGER,
  DegreeGrantingStatus INTEGER,
  HistoricallyBlackCollegeOrUniversity INTEGER,
  InstitutionHasHospital INTEGER,
  InstitutionGrantsMedicalDegree INTEGER,
  TribalCollege INTEGER,
  DegreeOfUrbanization INTEGER,
  InstitutionOpenToGeneralPublic INTEGER,
  StatusOfInstitution VARCHAR(100),
  UnitidForMergedSchools VARCHAR(2000),
  YearInstitutionWasDeletedFromIPEDS INTEGER,
  DateInstitutionClosed VARCHAR(100),
  InstitutionIsActive INTEGER,
  PrimarilyPostsecondaryIndicator INTEGER,
  PostsecondaryInstitutionIndicator INTEGER,
  PostsecondaryAndTitleIvInstitutionIndicator INTEGER,
  ReportingMethodForStudentCharges INTEGER,
  InstitutionalCategory INTEGER,
  LandGrantInstitution INTEGER,
  InstitutionSizeCategory INTEGER,
  MultiCampusOrganization INTEGER,
  NameOfMultiCampusOrganization VARCHAR(800),
  IdentificationNumberOfMultiCampusOrganization VARCHAR(60),
  CoreBasedStatisticalArea INTEGER,
  CBSATypeMetropolitanMicropolitan INTEGER,
  CombinedStatisticalArea INTEGER,
  NewEnglandCityAndTownArea INTEGER,
  FIPSCountyCode INTEGER,
  CountyName VARCHAR(300),
  StateAnd114thCongressionalDistrictID INTEGER,
  LongitudeLocation NUMBER(32, 0),
  LatitudeLocation NUMBER(32, 0),
  NCESGroupCategory INTEGER,
  CarnegieClassification VARIANT,
  WebAddress VARIANT,
  DataFeedbackReport INTEGER,
  AcademicYear  INTEGER,
  RecordCreateDateTime DATETIME,
  RecordUpdateDateTime DATETIME
);

CREATE OR REPLACE PROCEDURE 
	pr_AcademicInstitution_Load(YEAR FLOAT)
  RETURNS STRING
  LANGUAGE javascript
  EXECUTE AS OWNER
  AS
  $$
var sql_command = 
`
MERGE INTO AcademicInstitution T USING (
SELECT  * FROM  od_AcademicInstitution
WHERE  ACADEMICYEAR = ` + YEAR.toString() +`
) S ON T.InstitutionIdentifier = S.InstitutionIdentifier
WHEN MATCHED
AND (
  IFNULL(T.InstitutionName, '') <> IFNULL(S.InstitutionName, '')
  OR IFNULL(T.InstitutionNameAlias, '') 
	<> IFNULL(S.InstitutionNameAlias, '')
  OR IFNULL(T.StreetAddress, '') <> IFNULL(S.StreetAddress, '')
  OR IFNULL(T.City, '') <> IFNULL(S.City, '')
  OR IFNULL(T.State, '') <> IFNULL(S.State, '')
  OR IFNULL(T.ZipCode, '') <> IFNULL(S.ZipCode, '')
  OR IFNULL(T.StateCode, 0) <> IFNULL(S.StateCode, 0)
  OR IFNULL(T.EconomicAnalysisRegions, 0) 
	<> IFNULL(S.EconomicAnalysisRegions, 0)
  OR IFNULL(T.ChiefAdministrator, '') 
	<> IFNULL(S.ChiefAdministrator, '')
  OR IFNULL(T.ChiefAdministratorTitle, '') 
	<> IFNULL(S.ChiefAdministratorTitle, '')
  OR IFNULL(T.TelephoneNumber, '') <> IFNULL(S.TelephoneNumber, '')
  OR IFNULL(T.EmployerIdentificationNumber, '') 
	<> IFNULL(S.EmployerIdentificationNumber, '')
  OR IFNULL(T.DunBradstreetNumbers, '') 
	<> IFNULL(S.DunBradstreetNumbers, '')
  OR IFNULL(T.PostsecondaryEducationIDNumber, '') 
	<> IFNULL(S.PostsecondaryEducationIDNumber, '')
  OR IFNULL(T.TitleIVEligibilityIndicatorCode, 0) 
	<> IFNULL(S.TitleIVEligibilityIndicatorCode, 0)	
  OR IFNULL(T.SectorOfInstitution, 0) 
	<> IFNULL(S.SectorOfInstitution, 0)
  OR IFNULL(T.LevelOfInstitution, 0) 
	<> IFNULL(S.LevelOfInstitution, 0)
  OR IFNULL(T.ControlOfInstitution, 0) 
	<> IFNULL(S.ControlOfInstitution, 0)
  OR IFNULL(T.HighestLevelOfOffering, 0) 
	<> IFNULL(S.HighestLevelOfOffering, 0)
  OR IFNULL(T.UndergraduateOffering, 0) 
	<> IFNULL(S.UndergraduateOffering, 0)
  OR IFNULL(T.GraduateOffering, 0) 
	<> IFNULL(S.GraduateOffering, 0)
  OR IFNULL(T.HighestDegreeOffered, 0) 
	<> IFNULL(S.HighestDegreeOffered, 0)
  OR IFNULL(T.DegreeGrantingStatus, 0) 
	<> IFNULL(S.DegreeGrantingStatus, 0)
  OR IFNULL(T.HistoricallyBlackCollegeOrUniversity, 0) 
	<> IFNULL(S.HistoricallyBlackCollegeOrUniversity, 0)
  OR IFNULL(T.InstitutionHasHospital, 0) 
	<> IFNULL(S.InstitutionHasHospital, 0)
  OR IFNULL(T.InstitutionGrantsMedicalDegree, 0) 
	<> IFNULL(S.InstitutionGrantsMedicalDegree, 0)
  OR IFNULL(T.TribalCollege, 0) <> IFNULL(S.TribalCollege, 0)
  OR IFNULL(T.DegreeOfUrbanization, 0) 
	<> IFNULL(S.DegreeOfUrbanization, 0)
  OR IFNULL(T.InstitutionOpenToGeneralPublic, 0) 
	<> IFNULL(S.InstitutionOpenToGeneralPublic, 0)
  OR IFNULL(T.StatusOfInstitution, '') 
	<> IFNULL(S.StatusOfInstitution, '')
  OR IFNULL(T.UnitidForMergedSchools, 0) 
	<> IFNULL(S.UnitidForMergedSchools, 0)
  OR IFNULL(T.YearInstitutionWasDeletedFromIPEDS, 0) 
	<> IFNULL(S.YearInstitutionWasDeletedFromIPEDS, 0)
  OR IFNULL(T.DateInstitutionClosed, '') 
	<> IFNULL(S.DateInstitutionClosed, '')
  OR IFNULL(T.InstitutionIsActive, 0) 
	<> IFNULL(S.InstitutionIsActive, 0)
  OR IFNULL(T.PrimarilyPostsecondaryIndicator, 0) 
	<> IFNULL(S.PrimarilyPostsecondaryIndicator, 0)
  OR IFNULL(T.PostsecondaryInstitutionIndicator, 0) 
	<> IFNULL(S.PostsecondaryInstitutionIndicator, 0)
  OR IFNULL(T.PostsecondaryAndTitleIvInstitutionIndicator, 0) 
	<> IFNULL(S.PostsecondaryAndTitleIvInstitutionIndicator, 0)
  OR IFNULL(T.ReportingMethodForStudentCharges, 0) 
	<> IFNULL(S.ReportingMethodForStudentCharges, 0)
  OR IFNULL(T.InstitutionalCategory, 0) 
	<> IFNULL(S.InstitutionalCategory, 0)
  OR IFNULL(T.LandGrantInstitution, 0) 
	<> IFNULL(S.LandGrantInstitution, 0)
  OR IFNULL(T.InstitutionSizeCategory, 0) 
	<> IFNULL(S.InstitutionSizeCategory, 0)
  OR IFNULL(T.MultiCampusOrganization, 0) 
	<> IFNULL(S.MultiCampusOrganization, 0)
  OR IFNULL(T.NameOfMultiCampusOrganization, '') 
	<> IFNULL(S.NameOfMultiCampusOrganization, '')
  OR IFNULL(T.IdentificationNumberOfMultiCampusOrganization, '') 
	<> IFNULL(S.IdentificationNumberOfMultiCampusOrganization,'')
  OR IFNULL(T.CoreBasedStatisticalArea, 0) 
	<> IFNULL(S.CoreBasedStatisticalArea, 0)
  OR IFNULL(T.CBSATypeMetropolitanMicropolitan, 0) 
	<> IFNULL(S.CBSATypeMetropolitanMicropolitan, 0)
  OR IFNULL(T.CombinedStatisticalArea, 0) 
	<> IFNULL(S.CombinedStatisticalArea, 0)
  OR IFNULL(T.NewEnglandCityAndTownArea, 0) 
	<> IFNULL(S.NewEnglandCityAndTownArea, 0)
  OR IFNULL(T.FIPSCountyCode, 0) <> IFNULL(S.FIPSCountyCode, 0)
  OR IFNULL(T.CountyName, '') <> IFNULL(S.CountyName, '')
  OR IFNULL(T.StateAnd114thCongressionalDistrictID, 0) 
	<> IFNULL(S.StateAnd114thCongressionalDistrictID, 0)
  OR IFNULL(T.LongitudeLocation, 0.0) 
	<> IFNULL(S.LongitudeLocation, 0.0)
  OR IFNULL(T.LatitudeLocation, 0.0) 
	<> IFNULL(S.LatitudeLocation, 0.0)
  OR IFNULL(T.NCESGroupCategory, 0) 
	<> IFNULL(S.NCESGroupCategory, 0)
  OR IFNULL(T.DataFeedbackReport, 0) 
	<> IFNULL(S.DataFeedbackReport, 0)
  OR CarnegieClassification  <> OBJECT_CONSTRUCT (
	'AcademicInstitutionIdentifier'
		,S.InstitutionIdentifier,
	'CarnegieClassification2000' 
		,S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' 
		,S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' 
		,S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' 
		,S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' 
		,S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' 
		,S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' 
		,S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' 
		,S.CarnegieClassification2015SizeSetting
			   )  
  OR WebAddress <> OBJECT_CONSTRUCT (		
	'AcademicInstitutionIdentifier'
		,S.InstitutionIdentifier,	   
	'InstitutionsWebAddress'
		,S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress'
		,S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress'
		,S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress'
		,S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress'
		,S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress'
		,S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress'
		,S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress'
		,S.DisabilityServicesWebAddress
		   )

) THEN
UPDATE
SET
  InstitutionIdentifier = S.InstitutionIdentifier,
  InstitutionName = S.InstitutionName,
  InstitutionNameAlias = S.InstitutionNameAlias,
  StreetAddress = S.StreetAddress,
  City = S.City,
  State = S.State,
  ZipCode = S.ZipCode,
  StateCode = S.StateCode,
  EconomicAnalysisRegions = S.EconomicAnalysisRegions,
  ChiefAdministrator = S.ChiefAdministrator,
  ChiefAdministratorTitle = S.ChiefAdministratorTitle,
  TelephoneNumber = S.TelephoneNumber,
  EmployerIdentificationNumber = 
		S.EmployerIdentificationNumber,
  DunBradstreetNumbers = S.DunBradstreetNumbers,
  PostsecondaryEducationIDNumber = 
		S.PostsecondaryEducationIDNumber,
  TitleIVEligibilityIndicatorCode = 
		S.TitleIVEligibilityIndicatorCode,
  SectorOfInstitution = S.SectorOfInstitution,
  LevelOfInstitution = S.LevelOfInstitution,
  ControlOfInstitution = S.ControlOfInstitution,
  HighestLevelOfOffering = S.HighestLevelOfOffering,
  UndergraduateOffering = S.UndergraduateOffering,
  GraduateOffering = S.GraduateOffering,
  HighestDegreeOffered = S.HighestDegreeOffered,
  DegreeGrantingStatus = S.DegreeGrantingStatus,
  HistoricallyBlackCollegeOrUniversity 
		= S.HistoricallyBlackCollegeOrUniversity,
  InstitutionHasHospital = 
		S.InstitutionHasHospital,
  InstitutionGrantsMedicalDegree = 
		S.InstitutionGrantsMedicalDegree,
  TribalCollege = S.TribalCollege,
  DegreeOfUrbanization = S.DegreeOfUrbanization,
  InstitutionOpenToGeneralPublic = 
		S.InstitutionOpenToGeneralPublic,
  StatusOfInstitution = S.StatusOfInstitution,
  UnitidForMergedSchools = S.UnitidForMergedSchools,
  YearInstitutionWasDeletedFromIPEDS 
		= S.YearInstitutionWasDeletedFromIPEDS,
  DateInstitutionClosed = S.DateInstitutionClosed,
  InstitutionIsActive = S.InstitutionIsActive,
  PrimarilyPostsecondaryIndicator = 
		S.PrimarilyPostsecondaryIndicator,
  PostsecondaryInstitutionIndicator = 
		S.PostsecondaryInstitutionIndicator,
  PostsecondaryAndTitleIvInstitutionIndicator 
		= S.PostsecondaryAndTitleIvInstitutionIndicator,
  ReportingMethodForStudentCharges 
		= S.ReportingMethodForStudentCharges,
  InstitutionalCategory = S.InstitutionalCategory,
  LandGrantInstitution = S.LandGrantInstitution,
  InstitutionSizeCategory = S.InstitutionSizeCategory,
  MultiCampusOrganization = S.MultiCampusOrganization,
  NameOfMultiCampusOrganization = 
		S.NameOfMultiCampusOrganization,
  IdentificationNumberOfMultiCampusOrganization 
		= S.IdentificationNumberOfMultiCampusOrganization,
  CoreBasedStatisticalArea = 
		S.CoreBasedStatisticalArea,
  CBSATypeMetropolitanMicropolitan = 
		S.CBSATypeMetropolitanMicropolitan,
  CombinedStatisticalArea = S.CombinedStatisticalArea,
  NewEnglandCityAndTownArea = S.NewEnglandCityAndTownArea,
  FIPSCountyCode = S.FIPSCountyCode,
  CountyName = S.CountyName,
  StateAnd114thCongressionalDistrictID 
		= S.StateAnd114thCongressionalDistrictID,
  LongitudeLocation = S.LongitudeLocation,
  LatitudeLocation = S.LatitudeLocation,
  NCESGroupCategory = S.NCESGroupCategory,
  DataFeedbackReport = S.DataFeedbackReport,
  CarnegieClassification =OBJECT_CONSTRUCT (
	'AcademicInstitutionIdentifier'
		,S.InstitutionIdentifier,
	'CarnegieClassification2000' 
		,S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' 
		,S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' 
		,S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' 
		,S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' 
		,S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' 
		,S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' 
		,S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' 
		,S.CarnegieClassification2015SizeSetting
	   ),
  WebAddress=OBJECT_CONSTRUCT (		
	'AcademicInstitutionIdentifier'
		,S.InstitutionIdentifier,	   
	'InstitutionsWebAddress'
		,S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress'
		,S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress'
		,S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress'
		,S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress'
		,S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress'
		,S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress'
		,S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress'
		,S.DisabilityServicesWebAddress
	   ),
  AcademicYear=S.AcademicYear,
  RecordUpdateDateTime = CURRENT_TIMESTAMP
  WHEN NOT MATCHED THEN
INSERT
  (
	InstitutionIdentifier,
	InstitutionName,
	InstitutionNameAlias,
	StreetAddress,
	City,
	State,
	ZipCode,
	StateCode,
	EconomicAnalysisRegions,
	ChiefAdministrator,
	ChiefAdministratorTitle,
	TelephoneNumber,
	EmployerIdentificationNumber,
	DunBradstreetNumbers,
	PostsecondaryEducationIDNumber,
	TitleIVEligibilityIndicatorCode,
	SectorOfInstitution,
	LevelOfInstitution,
	ControlOfInstitution,
	HighestLevelOfOffering,
	UndergraduateOffering,
	GraduateOffering,
	HighestDegreeOffered,
	DegreeGrantingStatus,
	HistoricallyBlackCollegeOrUniversity,
	InstitutionHasHospital,
	InstitutionGrantsMedicalDegree,
	TribalCollege,
	DegreeOfUrbanization,
	InstitutionOpenToGeneralPublic,
	StatusOfInstitution,
	UnitidForMergedSchools,
	YearInstitutionWasDeletedFromIPEDS,
	DateInstitutionClosed,
	InstitutionIsActive,
	PrimarilyPostsecondaryIndicator,
	PostsecondaryInstitutionIndicator,
	PostsecondaryAndTitleIvInstitutionIndicator,
	ReportingMethodForStudentCharges,
	InstitutionalCategory,
	LandGrantInstitution,
	InstitutionSizeCategory,
	MultiCampusOrganization,
	NameOfMultiCampusOrganization,
	IdentificationNumberOfMultiCampusOrganization,
	CoreBasedStatisticalArea,
	CBSATypeMetropolitanMicropolitan,
	CombinedStatisticalArea,
	NewEnglandCityAndTownArea,
	FIPSCountyCode,
	CountyName,
	StateAnd114thCongressionalDistrictID,
	LongitudeLocation,
	LatitudeLocation,
	NCESGroupCategory,
	DataFeedbackReport,
	AcademicYear,
	CarnegieClassification,
	WebAddress, 
	RecordCreateDateTime)
VALUES
  (
	S.InstitutionIdentifier,
	S.InstitutionName,
	S.InstitutionNameAlias,
	S.StreetAddress,
	S.City,
	S.State,
	S.ZipCode,
	S.StateCode,
	S.EconomicAnalysisRegions,
	S.ChiefAdministrator,
	S.ChiefAdministratorTitle,
	S.TelephoneNumber,
	S.EmployerIdentificationNumber,
	S.DunBradstreetNumbers,
	S.PostsecondaryEducationIDNumber,
	S.TitleIVEligibilityIndicatorCode,
	S.SectorOfInstitution,
	S.LevelOfInstitution,
	S.ControlOfInstitution,
	S.HighestLevelOfOffering,
	S.UndergraduateOffering,
	S.GraduateOffering,
	S.HighestDegreeOffered,
	S.DegreeGrantingStatus,
	S.HistoricallyBlackCollegeOrUniversity,
	S.InstitutionHasHospital,
	S.InstitutionGrantsMedicalDegree,
	S.TribalCollege,
	S.DegreeOfUrbanization,
	S.InstitutionOpenToGeneralPublic,
	S.StatusOfInstitution,
	S.UnitidForMergedSchools,
	S.YearInstitutionWasDeletedFromIPEDS,
	S.DateInstitutionClosed,
	S.InstitutionIsActive,
	S.PrimarilyPostsecondaryIndicator,
	S.PostsecondaryInstitutionIndicator,
	S.PostsecondaryAndTitleIvInstitutionIndicator,
	S.ReportingMethodForStudentCharges,
	S.InstitutionalCategory,
	S.LandGrantInstitution,
	S.InstitutionSizeCategory,
	S.MultiCampusOrganization,
	S.NameOfMultiCampusOrganization,
	S.IdentificationNumberOfMultiCampusOrganization,
	S.CoreBasedStatisticalArea,
	S.CBSATypeMetropolitanMicropolitan,
	S.CombinedStatisticalArea,
	S.NewEnglandCityAndTownArea,
	S.FIPSCountyCode,
	S.CountyName,
	S.StateAnd114thCongressionalDistrictID,
	S.LongitudeLocation,
	S.LatitudeLocation,
	S.NCESGroupCategory,
	S.DataFeedbackReport,
	S.AcademicYear	,
	OBJECT_CONSTRUCT (
	'AcademicInstitutionIdentifier'
		,S.InstitutionIdentifier,
	'CarnegieClassification2000' 
		,S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' 
		,S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' 
		,S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' 
		,S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' 
		,S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' 
		,S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' 
		,S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' 
		,S.CarnegieClassification2015SizeSetting
	   ),
	OBJECT_CONSTRUCT (		
	'AcademicInstitutionIdentifier'
		,S.InstitutionIdentifier,	   
	'InstitutionsWebAddress'
		,S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress'
		,S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress'
		,S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress'
		,S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress'
		,S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress'
		,S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress'
		,S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress'
		,S.DisabilityServicesWebAddress
		       ),
			CURRENT_TIMESTAMP
		  );
	`
try {
    snowflake.execute (
      {sqlText: sql_command}
      );
    return "Succeeded.";  // Return a success/error indicator.
    }
  catch (err) {
    return "Failed: " + err;  // Return a success/error indicator.
    }
  $$
  ;

ALTER TABLE  od_AcademicInstitution
ADD ( 
CarnegieClassification2018Basic INTEGER,      
CarnegieClassification2018UndergraduateProgram INTEGER,
CarnegieClassification2018GraduateProgram INTEGER,   
CarnegieClassification2018UndergraduateProfile INTEGER,     
CarnegieClassification2018EnrollmentProfile INTEGER,   
CarnegieClassification2018SizeSetting INTEGER);

DROP VIEW IF EXISTS v_od_AcademicInstitution;

CREATE OR REPLACE VIEW v_od_AcademicInstitution  AS
SELECT
  t.$1 AS InstitutionIdentifier,
  t.$2 AS InstitutionName,
  t.$3 AS InstitutionNameAlias,
  t.$4 AS StreetAddress,
  t.$5 AS City,
  t.$6 AS State,
  t.$7 AS ZipCode,
  t.$8 AS StateCode,
  t.$9 AS EconomicAnalysisRegions,
  t.$10 AS ChiefAdministrator,
  t.$11 AS ChiefAdministratorTitle,
  t.$12 AS TelephoneNumber,
  t.$13 AS EmployerIdentificationNumber,
  t.$14 AS DunBradstreetNumbers,
  t.$15 AS PostsecondaryEducationIDNumber,
  t.$16 AS TitleIVEligibilityIndicatorCode,
  t.$17 AS InstitutionsWebAddress,
  t.$18 AS AdmissionsOfficeWebAddress,
  t.$19 AS FinancialAidOfficeWebAddress,
  t.$20 AS OnlineApplicationWebAddress,
  t.$21 AS NetPriceCalculatorWebAddress,
  t.$22 AS VeteransMilitaryServiceTuitionPoliciesWebAddress,
  t.$23 AS StudentRightAthleteGraduationRateWebAddress,
  t.$24 AS DisabilityServicesWebAddress,
  t.$25 AS SectorOfInstitution,
  t.$26 AS LevelOfInstitution,
  t.$27 AS ControlOfInstitution,
  t.$28 AS HighestLevelOfOffering,
  t.$29 AS UndergraduateOffering,
  t.$30 AS GraduateOffering,
  t.$31 AS HighestDegreeOffered,
  t.$32 AS DegreeGrantingStatus,
  t.$33 AS HistoricallyBlackCollegeOrUniversity,
  t.$34 AS InstitutionHasHospital,
  t.$35 AS InstitutionGrantsMedicalDegree,
  t.$36 AS TribalCollege,
  t.$37 AS DegreeOfUrbanization,
  t.$38 AS InstitutionOpenToGeneralPublic,
  t.$39 AS StatusOfInstitution,
  t.$40 AS UnitidForMergedSchools,
  t.$41 AS YearInstitutionWasDeletedFromIPEDS,
  t.$42 AS DateInstitutionClosed,
  t.$43 AS InstitutionIsActive,
  t.$44 AS PrimarilyPostsecondaryIndicator,
  t.$45 AS PostsecondaryInstitutionIndicator,
  t.$46 AS PostsecondaryAndTitleIvInstitutionIndicator,
  t.$47 AS ReportingMethodForStudentCharges,
  t.$48 AS InstitutionalCategory,
  t.$49 AS CarnegieClassification2015Basic,
  t.$50 AS CarnegieClassification2015UndergraduateProgram,
  t.$51 AS CarnegieClassification2015GraduateProgram,
  t.$52 AS CarnegieClassification2015UndergraduateProfile,
  t.$53 AS CarnegieClassification2015EnrollmentProfile,
  t.$54 AS CarnegieClassification2015SizeSetting,
  NULL AS CarnegieClassification2018Basic,
  NULL AS CarnegieClassification2018UndergraduateProgram,
  NULL AS CarnegieClassification2018GraduateProgram,
  NULL AS CarnegieClassification2018UndergraduateProfile,
  NULL AS CarnegieClassification2018EnrollmentProfile,
  NULL AS CarnegieClassification2018SizeSetting,
  t.$55 AS CarnegieClassification20052010Basic,
  t.$56 AS CarnegieClassification2000,
  t.$57 AS LandGrantInstitution,
  t.$58 AS InstitutionSizeCategory,
  t.$59 AS MultiCampusOrganization,
  t.$60 AS NameOfMultiCampusOrganization,
  t.$61 AS IdentificationNumberOfMultiCampusOrganization,
  t.$62 AS CoreBasedStatisticalArea,
  t.$63 AS CBSATypeMetropolitanMicropolitan,
  t.$64 AS CombinedStatisticalArea,
  t.$65 AS NewEnglandCityAndTownArea,
  t.$66 AS FIPSCountyCode,
  t.$67 AS CountyName,
  t.$68 AS StateAnd114thCongressionalDistrictID,
  t.$69 AS LongitudeLocation,
  t.$70 AS LatitudeLocation,
  t.$71 AS NCESGroupCategory,
  t.$72 AS DataFeedbackReport,
  CAST(substring(metadata$filename, 3, 4) AS INTEGER) AcademicYear,
  metadata$filename IngestedFileName,
  metadata$file_row_number RowNumber
FROM  @IPEDS_HD t
WHERE
  metadata$file_row_number > 1
  AND CAST(substring(metadata$filename, 3, 4) AS INTEGER) < 2018
UNION ALL
SELECT
  t.$1 AS InstitutionIdentifier,
  t.$2 AS InstitutionName,
  t.$3 AS InstitutionNameAlias,
  t.$4 AS StreetAddress,
  t.$5 AS City,
  t.$6 AS State,
  t.$7 AS ZipCode,
  t.$8 AS StateCode,
  t.$9 AS EconomicAnalysisRegions,
  t.$10 AS ChiefAdministrator,
  t.$11 AS ChiefAdministratorTitle,
  t.$12 AS TelephoneNumber,
  t.$13 AS EmployerIdentificationNumber,
  t.$14 AS DunBradstreetNumbers,
  t.$15 AS PostsecondaryEducationIDNumber,
  t.$16 AS TitleIVEligibilityIndicatorCode,
  t.$17 AS InstitutionsWebAddress,
  t.$18 AS AdmissionsOfficeWebAddress,
  t.$19 AS FinancialAidOfficeWebAddress,
  t.$20 AS OnlineApplicationWebAddress,
  t.$21 AS NetPriceCalculatorWebAddress,
  t.$22 AS VeteransMilitaryServiceTuitionPoliciesWebAddress,
  t.$23 AS StudentRightAthleteGraduationRateWebAddress,
  t.$24 AS DisabilityServicesWebAddress,
  t.$25 AS SectorOfInstitution,
  t.$26 AS LevelOfInstitution,
  t.$27 AS ControlOfInstitution,
  t.$28 AS HighestLevelOfOffering,
  t.$29 AS UndergraduateOffering,
  t.$30 AS GraduateOffering,
  t.$31 AS HighestDegreeOffered,
  t.$32 AS DegreeGrantingStatus,
  t.$33 AS HistoricallyBlackCollegeOrUniversity,
  t.$34 AS InstitutionHasHospital,
  t.$35 AS InstitutionGrantsMedicalDegree,
  t.$36 AS TribalCollege,
  t.$37 AS DegreeOfUrbanization,
  t.$38 AS InstitutionOpenToGeneralPublic,
  t.$39 AS StatusOfInstitution,
  t.$40 AS UnitidForMergedSchools,
  t.$41 AS YearInstitutionWasDeletedFromIPEDS,
  t.$42 AS DateInstitutionClosed,
  t.$43 AS InstitutionIsActive,
  t.$44 AS PrimarilyPostsecondaryIndicator,
  t.$45 AS PostsecondaryInstitutionIndicator,
  t.$46 AS PostsecondaryAndTitleIvInstitutionIndicator,
  t.$47 AS ReportingMethodForStudentCharges,
  t.$48 AS InstitutionalCategory,
  t.$55 AS CarnegieClassification2015Basic,
  NULL AS CarnegieClassification2015UndergraduateProgram,
  NULL AS CarnegieClassification2015GraduateProgram,
  NULL AS CarnegieClassification2015UndergraduateProfile,
  NULL CarnegieClassification2015EnrollmentProfile,
  NULL CarnegieClassification2015SizeSetting,
  t.$49 AS CarnegieClassification2018Basic,
  t.$50 AS CarnegieClassification2018UndergraduateProgram,
  t.$51 AS CarnegieClassification2018GraduateProgram,
  t.$52 AS CarnegieClassification2018UndergraduateProfile,
  t.$53 AS CarnegieClassification2018EnrollmentProfile,
  t.$54 AS CarnegieClassification2018SizeSetting,
  t.$56 AS CarnegieClassification20052010Basic,
  t.$57 AS CarnegieClassification2000,
  t.$58 AS LandGrantInstitution,
  t.$59 AS InstitutionSizeCategory,
  t.$60 AS MultiCampusOrganization,
  t.$61 AS NameOfMultiCampusOrganization,
  t.$62 AS IdentificationNumberOfMultiCampusOrganization,
  t.$63 AS CoreBasedStatisticalArea,
  t.$64 AS CBSATypeMetropolitanMicropolitan,
  t.$65 AS CombinedStatisticalArea,
  t.$66 AS NewEnglandCityAndTownArea,
  t.$67 AS FIPSCountyCode,
  t.$68 AS CountyName,
  t.$69 AS StateAnd114thCongressionalDistrictID,
  t.$70 AS LongitudeLocation,
  t.$71 AS LatitudeLocation,
  t.$72 AS NCESGroupCategory,
  t.$73 AS DataFeedbackReport,
  CAST(substring(metadata$filename, 3, 4) AS INTEGER) 
		AcademicYear,
  metadata$filename IngestedFileName,
  metadata$file_row_number RowNumber
FROM
  @IPEDS_HD t
WHERE
  metadata$file_row_number > 1
  AND CAST(substring(metadata$filename, 3, 4) AS INTEGER) > 2017;

CREATE OR REPLACE PROCEDURE pr_od_AcademicInstitution_Load(YEAR FLOAT)
RETURNS STRING
LANGUAGE javascript
EXECUTE AS OWNER
AS
$$
var sql_command = `
    INSERT INTO
	  Od_AcademicInstitution (
		InstitutionIdentifier,
		InstitutionName,
		InstitutionNameAlias,
		StreetAddress,
		City,
		State,
		ZipCode,
		StateCode,
		EconomicAnalysisRegions,
		ChiefAdministrator,
		ChiefAdministratorTitle,
		TelephoneNumber,
		EmployerIdentificationNumber,
		DunBradstreetNumbers,
		PostsecondaryEducationIDNumber,
		TitleIVEligibilityIndicatorCode,
		InstitutionsWebAddress,
		AdmissionsOfficeWebAddress,
		FinancialAidOfficeWebAddress,
		OnlineApplicationWebAddress,
		NetPriceCalculatorWebAddress,
		VeteransMilitaryServiceTuitionPoliciesWebAddress,
		StudentRightAthleteGraduationRateWebAddress,
		DisabilityServicesWebAddress,
		SectorOfInstitution,
		LevelOfInstitution,
		ControlOfInstitution,
		HighestLevelOfOffering,
		UndergraduateOffering,
		GraduateOffering,
		HighestDegreeOffered,
		DegreeGrantingStatus,
		HistoricallyBlackCollegeOrUniversity,
		InstitutionHasHospital,
		InstitutionGrantsMedicalDegree,
		TribalCollege,
		DegreeOfUrbanization,
		InstitutionOpenToGeneralPublic,
		StatusOfInstitution,
		UnitidForMergedSchools,
		YearInstitutionWasDeletedFromIPEDS,
		DateInstitutionClosed,
		InstitutionIsActive,
		PrimarilyPostsecondaryIndicator,
		PostsecondaryInstitutionIndicator,
		PostsecondaryAndTitleIvInstitutionIndicator,
		ReportingMethodForStudentCharges,
		InstitutionalCategory,
		CarnegieClassification2015UndergraduateProgram,
		CarnegieClassification2015GraduateProgram,
		CarnegieClassification2015UndergraduateProfile,
		CarnegieClassification2015EnrollmentProfile,
		CarnegieClassification2015SizeSetting,
		CarnegieClassification2018Basic,
		CarnegieClassification2018UndergraduateProgram,
		CarnegieClassification2018GraduateProgram,
		CarnegieClassification2018UndergraduateProfile,
		CarnegieClassification2018EnrollmentProfile,
		CarnegieClassification2018SizeSetting,
		CarnegieClassification2015Basic,
		CarnegieClassification20052010Basic,
		CarnegieClassification2000,
		LandGrantInstitution,
		InstitutionSizeCategory,
		MultiCampusOrganization,
		NameOfMultiCampusOrganization,
		IdentificationNumberOfMultiCampusOrganization,
		CoreBasedStatisticalArea,
		CBSATypeMetropolitanMicropolitan,
		CombinedStatisticalArea,
		NewEnglandCityAndTownArea,
		FIPSCountyCode,
		CountyName,
		StateAnd114thCongressionalDistrictID,
		LongitudeLocation,
		LatitudeLocation,
		NCESGroupCategory,
		DataFeedbackReport,
		AcademicYear,
		IngestedFileName,
		RowNumber 
	  )
	SELECT
	  InstitutionIdentifier,
	  InstitutionName,
	  InstitutionNameAlias,
	  StreetAddress,
	  City,
	  State,
	  ZipCode,
	  StateCode,
	  EconomicAnalysisRegions,
	  ChiefAdministrator,
	  ChiefAdministratorTitle,
	  TelephoneNumber,
	  EmployerIdentificationNumber,
	  DunBradstreetNumbers,
	  PostsecondaryEducationIDNumber,
	  TitleIVEligibilityIndicatorCode,
	  InstitutionsWebAddress,
	  AdmissionsOfficeWebAddress,
	  FinancialAidOfficeWebAddress,
	  OnlineApplicationWebAddress,
	  NetPriceCalculatorWebAddress,
	  VeteransMilitaryServiceTuitionPoliciesWebAddress,
	  StudentRightAthleteGraduationRateWebAddress,
	  DisabilityServicesWebAddress,
	  SectorOfInstitution,
	  LevelOfInstitution,
	  ControlOfInstitution,
	  HighestLevelOfOffering,
	  UndergraduateOffering,
	  GraduateOffering,
	  HighestDegreeOffered,
	  DegreeGrantingStatus,
	  HistoricallyBlackCollegeOrUniversity,
	  InstitutionHasHospital,
	  InstitutionGrantsMedicalDegree,
	  TribalCollege,
	  DegreeOfUrbanization,
	  InstitutionOpenToGeneralPublic,
	  StatusOfInstitution,
	  UnitidForMergedSchools,
	  YearInstitutionWasDeletedFromIPEDS,
	  DateInstitutionClosed,
	  InstitutionIsActive,
	  PrimarilyPostsecondaryIndicator,
	  PostsecondaryInstitutionIndicator,
	  PostsecondaryAndTitleIvInstitutionIndicator,
	  ReportingMethodForStudentCharges,
	  InstitutionalCategory,
	  CarnegieClassification2015UndergraduateProgram,
	  CarnegieClassification2015GraduateProgram,
	  CarnegieClassification2015UndergraduateProfile,
	  CarnegieClassification2015EnrollmentProfile,
	  CarnegieClassification2015SizeSetting,
	  CarnegieClassification2018Basic,
	  CarnegieClassification2018UndergraduateProgram,
	  CarnegieClassification2018GraduateProgram,
	  CarnegieClassification2018UndergraduateProfile,
	  CarnegieClassification2018EnrollmentProfile,
	  CarnegieClassification2018SizeSetting,
	  CarnegieClassification2015Basic,
	  CarnegieClassification20052010Basic,
	  CarnegieClassification2000,
	  LandGrantInstitution,
	  InstitutionSizeCategory,
	  MultiCampusOrganization,
	  NameOfMultiCampusOrganization,
	  IdentificationNumberOfMultiCampusOrganization,
	  CoreBasedStatisticalArea,
	  CBSATypeMetropolitanMicropolitan,
	  CombinedStatisticalArea,
	  NewEnglandCityAndTownArea,
	  FIPSCountyCode,
	  CountyName,
	  StateAnd114thCongressionalDistrictID,
	  LongitudeLocation,
	  LatitudeLocation,
	  NCESGroupCategory,
	  DataFeedbackReport,
	  AcademicYear,
	  IngestedFileName,
	  RowNumber
	FROM
	  v_od_AcademicInstitution 
    WHERE AcademicYear = ` + YEAR.toString()+`;`

    try {
        snowflake.execute (
          {sqlText: sql_command}
          );
        return "Succeeded.";  // Return a success/error indicator.
        }
    catch (err) {
        return "Failed: " + err;  // Return a success/error indicator.
        }
$$
;


CREATE OR REPLACE PROCEDURE 
		pr_AcademicInstitution_Load(YEAR FLOAT)
  RETURNS STRING
  LANGUAGE javascript
  EXECUTE AS OWNER
  AS
  $$
  var sql_command = `
 MERGE INTO AcademicInstitution T USING (
  SELECT  *  FROM  Od_AcademicInstitution
  WHERE   ACADEMICYEAR = ` + YEAR.toString() +`
) S ON T.InstitutionIdentifier 
		= S.InstitutionIdentifier
WHEN MATCHED
AND (
  IFNULL(T.InstitutionName, '') 
		<> IFNULL(S.InstitutionName, '')	
  OR IFNULL(T.InstitutionNameAlias, '') 
		<> IFNULL(S.InstitutionNameAlias, '')
  OR IFNULL(T.StreetAddress, '') 
		<> IFNULL(S.StreetAddress, '')
  OR IFNULL(T.City, '') <> IFNULL(S.City, '')
  OR IFNULL(T.State, '') <> IFNULL(S.State, '')
  OR IFNULL(T.ZipCode, '') <> IFNULL(S.ZipCode, '')
  OR IFNULL(T.StateCode, 0) <> IFNULL(S.StateCode, 0)
  OR IFNULL(T.EconomicAnalysisRegions, 0) 
		<> IFNULL(S.EconomicAnalysisRegions, 0)
  OR IFNULL(T.ChiefAdministrator, '') 
		<> IFNULL(S.ChiefAdministrator, '')
  OR IFNULL(T.ChiefAdministratorTitle, '') 
		<> IFNULL(S.ChiefAdministratorTitle, '')
  OR IFNULL(T.TelephoneNumber, '') 
		<> IFNULL(S.TelephoneNumber, '')
  OR IFNULL(T.EmployerIdentificationNumber, '') 
		<> IFNULL(S.EmployerIdentificationNumber, '')
  OR IFNULL(T.DunBradstreetNumbers, '') 
		<> IFNULL(S.DunBradstreetNumbers, '')
  OR IFNULL(T.PostsecondaryEducationIDNumber, '') 
		<> IFNULL(S.PostsecondaryEducationIDNumber, '')
  OR IFNULL(T.TitleIVEligibilityIndicatorCode, 0) 
		<> IFNULL(S.TitleIVEligibilityIndicatorCode, 0)
  OR IFNULL(T.SectorOfInstitution, 0) 
		<> IFNULL(S.SectorOfInstitution, 0)
  OR IFNULL(T.LevelOfInstitution, 0) 
		<> IFNULL(S.LevelOfInstitution, 0)
  OR IFNULL(T.ControlOfInstitution, 0) 
		<> IFNULL(S.ControlOfInstitution, 0)
  OR IFNULL(T.HighestLevelOfOffering, 0) 
		<> IFNULL(S.HighestLevelOfOffering, 0)
  OR IFNULL(T.UndergraduateOffering, 0) 
		<> IFNULL(S.UndergraduateOffering, 0)
  OR IFNULL(T.GraduateOffering, 0) 
		<> IFNULL(S.GraduateOffering, 0)
  OR IFNULL(T.HighestDegreeOffered, 0) 
		<> IFNULL(S.HighestDegreeOffered, 0)
  OR IFNULL(T.DegreeGrantingStatus, 0) 
		<> IFNULL(S.DegreeGrantingStatus, 0)
  OR IFNULL(T.HistoricallyBlackCollegeOrUniversity, 0) 
		<> IFNULL(S.HistoricallyBlackCollegeOrUniversity, 0)
  OR IFNULL(T.InstitutionHasHospital, 0) 
		<> IFNULL(S.InstitutionHasHospital, 0)
  OR IFNULL(T.InstitutionGrantsMedicalDegree, 0) 
		<> IFNULL(S.InstitutionGrantsMedicalDegree, 0)
  OR IFNULL(T.TribalCollege, 0) 
		<> IFNULL(S.TribalCollege, 0)
  OR IFNULL(T.DegreeOfUrbanization, 0) 
		<> IFNULL(S.DegreeOfUrbanization, 0)
  OR IFNULL(T.InstitutionOpenToGeneralPublic, 0) 
		<> IFNULL(S.InstitutionOpenToGeneralPublic, 0)
  OR IFNULL(T.StatusOfInstitution, '') 
		<> IFNULL(S.StatusOfInstitution, '')
  OR IFNULL(T.UnitidForMergedSchools, 0) 
		<> IFNULL(S.UnitidForMergedSchools, 0)
  OR IFNULL(T.YearInstitutionWasDeletedFromIPEDS, 0)
		<> IFNULL(S.YearInstitutionWasDeletedFromIPEDS, 0)
  OR IFNULL(T.DateInstitutionClosed, '') 
		<> IFNULL(S.DateInstitutionClosed, '')
  OR IFNULL(T.InstitutionIsActive, 0) 
		<> IFNULL(S.InstitutionIsActive, 0)
  OR IFNULL(T.PrimarilyPostsecondaryIndicator, 0) 
		<> IFNULL(S.PrimarilyPostsecondaryIndicator, 0)
  OR IFNULL(T.PostsecondaryInstitutionIndicator, 0) 
		<> IFNULL(S.PostsecondaryInstitutionIndicator, 0)
  OR IFNULL(T.PostsecondaryAndTitleIvInstitutionIndicator, 0) 
	<> IFNULL(S.PostsecondaryAndTitleIvInstitutionIndicator, 0)
  OR IFNULL(T.ReportingMethodForStudentCharges, 0) 
		<> IFNULL(S.ReportingMethodForStudentCharges, 0)
  OR IFNULL(T.InstitutionalCategory, 0) 
		<> IFNULL(S.InstitutionalCategory, 0)
  OR IFNULL(T.LandGrantInstitution, 0) 
		<> IFNULL(S.LandGrantInstitution, 0)
  OR IFNULL(T.InstitutionSizeCategory, 0) 
		<> IFNULL(S.InstitutionSizeCategory, 0)
  OR IFNULL(T.MultiCampusOrganization, 0) 
		<> IFNULL(S.MultiCampusOrganization, 0)
  OR IFNULL(T.NameOfMultiCampusOrganization, '') 
		<> IFNULL(S.NameOfMultiCampusOrganization, '')
  OR IFNULL(T.IdentificationNumberOfMultiCampusOrganization, ''  ) 
	<> IFNULL(S.IdentificationNumberOfMultiCampusOrganization,'')
  OR IFNULL(T.CoreBasedStatisticalArea, 0) 
		<> IFNULL(S.CoreBasedStatisticalArea, 0)
  OR IFNULL(T.CBSATypeMetropolitanMicropolitan, 0) 
		<> IFNULL(S.CBSATypeMetropolitanMicropolitan, 0)
  OR IFNULL(T.CombinedStatisticalArea, 0) 
		<> IFNULL(S.CombinedStatisticalArea, 0)
  OR IFNULL(T.NewEnglandCityAndTownArea, 0) 
		<> IFNULL(S.NewEnglandCityAndTownArea, 0)
  OR IFNULL(T.FIPSCountyCode, 0) <> IFNULL(S.FIPSCountyCode, 0)
  OR IFNULL(T.CountyName, '') <> IFNULL(S.CountyName, '')
  OR IFNULL(T.StateAnd114thCongressionalDistrictID, 0) 
		<> IFNULL(S.StateAnd114thCongressionalDistrictID, 0)
  OR IFNULL(T.LongitudeLocation, 0.0) 
		<> IFNULL(S.LongitudeLocation, 0.0)
  OR IFNULL(T.LatitudeLocation, 0.0) 
		<> IFNULL(S.LatitudeLocation, 0.0)
  OR IFNULL(T.NCESGroupCategory, 0)
		<> IFNULL(S.NCESGroupCategory, 0)
  OR IFNULL(T.DataFeedbackReport, 0)
		<> IFNULL(S.DataFeedbackReport, 0) 
  OR CarnegieClassification  <> OBJECT_CONSTRUCT (
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,
	'CarnegieClassification2000' ,
		S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' ,
		S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' ,
		S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' ,
		S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' ,
		S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' ,
		S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' ,
		S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' ,
		S.CarnegieClassification2015SizeSetting ,
	'CarnegieClassification2018Basic',
		S.CarnegieClassification2018Basic,
	'CarnegieClassification2018UndergraduateProgram' ,
		S.CarnegieClassification2018UndergraduateProgram,
	'CarnegieClassification2018GraduateProgram',
		S.CarnegieClassification2018GraduateProgram,
	'CarnegieClassification2018UndergraduateProfile' ,
		S.CarnegieClassification2018UndergraduateProfile,
	'CarnegieClassification2018EnrollmentProfile',
		S.CarnegieClassification2018EnrollmentProfile,
	'CarnegieClassification2018SizeSetting' ,
		S.CarnegieClassification2018SizeSetting
		       )  
  OR WebAddress<>OBJECT_CONSTRUCT (		
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,	   
	'InstitutionsWebAddress',
		S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress',
		S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress',
		S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress',
		S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress',
		S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress',
		S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress',
		S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress',
		S.DisabilityServicesWebAddress
		       )

) THEN
UPDATE
SET
  InstitutionIdentifier = S.InstitutionIdentifier,
  InstitutionName = S.InstitutionName,
  InstitutionNameAlias = S.InstitutionNameAlias,
  StreetAddress = S.StreetAddress,
  City = S.City,
  State = S.State,
  ZipCode = S.ZipCode,
  StateCode = S.StateCode,
  EconomicAnalysisRegions = S.EconomicAnalysisRegions,
  ChiefAdministrator = S.ChiefAdministrator,
  ChiefAdministratorTitle = S.ChiefAdministratorTitle,
  TelephoneNumber = S.TelephoneNumber,
  EmployerIdentificationNumber 
		= S.EmployerIdentificationNumber,
  DunBradstreetNumbers = S.DunBradstreetNumbers,
  PostsecondaryEducationIDNumber 
		= S.PostsecondaryEducationIDNumber,
  TitleIVEligibilityIndicatorCode 
		= S.TitleIVEligibilityIndicatorCode,
  SectorOfInstitution = S.SectorOfInstitution,
  LevelOfInstitution = S.LevelOfInstitution,
  ControlOfInstitution = S.ControlOfInstitution,
  HighestLevelOfOffering = S.HighestLevelOfOffering,
  UndergraduateOffering = S.UndergraduateOffering,
  GraduateOffering = S.GraduateOffering,
  HighestDegreeOffered = S.HighestDegreeOffered,
  DegreeGrantingStatus = S.DegreeGrantingStatus,
  HistoricallyBlackCollegeOrUniversity 
		= S.HistoricallyBlackCollegeOrUniversity,
  InstitutionHasHospital = S.InstitutionHasHospital,
  InstitutionGrantsMedicalDegree 
		= S.InstitutionGrantsMedicalDegree,
  TribalCollege = S.TribalCollege,
  DegreeOfUrbanization = S.DegreeOfUrbanization,
  InstitutionOpenToGeneralPublic 
		= S.InstitutionOpenToGeneralPublic,
  StatusOfInstitution = S.StatusOfInstitution,
  UnitidForMergedSchools = S.UnitidForMergedSchools,
  YearInstitutionWasDeletedFromIPEDS 
		= S.YearInstitutionWasDeletedFromIPEDS,
  DateInstitutionClosed = S.DateInstitutionClosed,
  InstitutionIsActive = S.InstitutionIsActive,
  PrimarilyPostsecondaryIndicator 
		= S.PrimarilyPostsecondaryIndicator,
  PostsecondaryInstitutionIndicator 
		= S.PostsecondaryInstitutionIndicator,
  PostsecondaryAndTitleIvInstitutionIndicator 
		= S.PostsecondaryAndTitleIvInstitutionIndicator,
  ReportingMethodForStudentCharges 
		= S.ReportingMethodForStudentCharges,
  InstitutionalCategory = S.InstitutionalCategory,
  LandGrantInstitution = S.LandGrantInstitution,
  InstitutionSizeCategory = S.InstitutionSizeCategory,
  MultiCampusOrganization = S.MultiCampusOrganization,
  NameOfMultiCampusOrganization 
		= S.NameOfMultiCampusOrganization,
  IdentificationNumberOfMultiCampusOrganization 
		= S.IdentificationNumberOfMultiCampusOrganization,
  CoreBasedStatisticalArea 
		= S.CoreBasedStatisticalArea,
  CBSATypeMetropolitanMicropolitan 
		= S.CBSATypeMetropolitanMicropolitan,
  CombinedStatisticalArea = S.CombinedStatisticalArea,
  NewEnglandCityAndTownArea = S.NewEnglandCityAndTownArea,
  FIPSCountyCode = S.FIPSCountyCode,
  CountyName = S.CountyName,
  StateAnd114thCongressionalDistrictID 
		= S.StateAnd114thCongressionalDistrictID,
  LongitudeLocation = S.LongitudeLocation,
  LatitudeLocation = S.LatitudeLocation,
  NCESGroupCategory = S.NCESGroupCategory,
  DataFeedbackReport = S.DataFeedbackReport, 
  AcademicYear=S.AcademicYear,
  CarnegieClassification =OBJECT_CONSTRUCT (
  'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,
	'CarnegieClassification2000' ,
		S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' ,
		S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' ,
		S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' ,
		S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' ,
		S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' ,
		S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' ,
		S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' ,
		S.CarnegieClassification2015SizeSetting ,
	'CarnegieClassification2018Basic',
		S.CarnegieClassification2018Basic,
	'CarnegieClassification2018UndergraduateProgram' ,
		S.CarnegieClassification2018UndergraduateProgram,
	'CarnegieClassification2018GraduateProgram',
		S.CarnegieClassification2018GraduateProgram,
	'CarnegieClassification2018UndergraduateProfile' ,
		S.CarnegieClassification2018UndergraduateProfile,
	'CarnegieClassification2018EnrollmentProfile',
		S.CarnegieClassification2018EnrollmentProfile,
	'CarnegieClassification2018SizeSetting' ,
		S.CarnegieClassification2018SizeSetting
		       ),
	WebAddress=OBJECT_CONSTRUCT (		
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,	   
	'InstitutionsWebAddress',
		S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress',
		S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress',
		S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress',
		S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress',
		S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress',
		S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress',
		S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress',
		S.DisabilityServicesWebAddress
		       ),
  RecordUpdateDateTime = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
	INSERT   (
    InstitutionIdentifier,
    InstitutionName,
    InstitutionNameAlias,
    StreetAddress,
    City,
    State,
    ZipCode,
    StateCode,
    EconomicAnalysisRegions,
    ChiefAdministrator,
    ChiefAdministratorTitle,
    TelephoneNumber,
    EmployerIdentificationNumber,
    DunBradstreetNumbers,
    PostsecondaryEducationIDNumber,
    TitleIVEligibilityIndicatorCode,
    SectorOfInstitution,
    LevelOfInstitution,
    ControlOfInstitution,
    HighestLevelOfOffering,
    UndergraduateOffering,
    GraduateOffering,
    HighestDegreeOffered,
    DegreeGrantingStatus,
    HistoricallyBlackCollegeOrUniversity,
    InstitutionHasHospital,
    InstitutionGrantsMedicalDegree,
    TribalCollege,
    DegreeOfUrbanization,
    InstitutionOpenToGeneralPublic,
    StatusOfInstitution,
    UnitidForMergedSchools,
    YearInstitutionWasDeletedFromIPEDS,
    DateInstitutionClosed,
    InstitutionIsActive,
    PrimarilyPostsecondaryIndicator,
    PostsecondaryInstitutionIndicator,
    PostsecondaryAndTitleIvInstitutionIndicator,
    ReportingMethodForStudentCharges,
    InstitutionalCategory,
    LandGrantInstitution,
    InstitutionSizeCategory,
    MultiCampusOrganization,
    NameOfMultiCampusOrganization,
    IdentificationNumberOfMultiCampusOrganization,
    CoreBasedStatisticalArea,
    CBSATypeMetropolitanMicropolitan,
    CombinedStatisticalArea,
    NewEnglandCityAndTownArea,
    FIPSCountyCode,
    CountyName,
    StateAnd114thCongressionalDistrictID,
    LongitudeLocation,
    LatitudeLocation,
    NCESGroupCategory,
    DataFeedbackReport, 
    AcademicYear, 
	CarnegieClassification,
	WebAddress, 
    RecordCreateDateTime)
VALUES  (
    S.InstitutionIdentifier,
    S.InstitutionName,
    S.InstitutionNameAlias,
    S.StreetAddress,
    S.City,
    S.State,
    S.ZipCode,
    S.StateCode,
    S.EconomicAnalysisRegions,
    S.ChiefAdministrator,
    S.ChiefAdministratorTitle,
    S.TelephoneNumber,
    S.EmployerIdentificationNumber,
    S.DunBradstreetNumbers,
    S.PostsecondaryEducationIDNumber,
    S.TitleIVEligibilityIndicatorCode,
    S.SectorOfInstitution,
    S.LevelOfInstitution,
    S.ControlOfInstitution,
    S.HighestLevelOfOffering,
    S.UndergraduateOffering,
    S.GraduateOffering,
    S.HighestDegreeOffered,
    S.DegreeGrantingStatus,
    S.HistoricallyBlackCollegeOrUniversity,
    S.InstitutionHasHospital,
    S.InstitutionGrantsMedicalDegree,
    S.TribalCollege,
    S.DegreeOfUrbanization,
    S.InstitutionOpenToGeneralPublic,
    S.StatusOfInstitution,
    S.UnitidForMergedSchools,
    S.YearInstitutionWasDeletedFromIPEDS,
    S.DateInstitutionClosed,
    S.InstitutionIsActive,
    S.PrimarilyPostsecondaryIndicator,
    S.PostsecondaryInstitutionIndicator,
    S.PostsecondaryAndTitleIvInstitutionIndicator,
    S.ReportingMethodForStudentCharges,
    S.InstitutionalCategory,
    S.LandGrantInstitution,
    S.InstitutionSizeCategory,
    S.MultiCampusOrganization,
    S.NameOfMultiCampusOrganization,
    S.IdentificationNumberOfMultiCampusOrganization,
    S.CoreBasedStatisticalArea,
    S.CBSATypeMetropolitanMicropolitan,
    S.CombinedStatisticalArea,
    S.NewEnglandCityAndTownArea,
    S.FIPSCountyCode,
    S.CountyName,
    S.StateAnd114thCongressionalDistrictID,
    S.LongitudeLocation,
    S.LatitudeLocation,
    S.NCESGroupCategory,
    S.DataFeedbackReport, 
    S.AcademicYear, 
	OBJECT_CONSTRUCT (
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,
	'CarnegieClassification2000' ,
		S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' ,
		S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' ,
		S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' ,
		S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' ,
		S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' ,
		S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' ,
		S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' ,
		S.CarnegieClassification2015SizeSetting ,
	'CarnegieClassification2018Basic',
		S.CarnegieClassification2018Basic,
	'CarnegieClassification2018UndergraduateProgram' ,
		S.CarnegieClassification2018UndergraduateProgram,
	'CarnegieClassification2018GraduateProgram',
		S.CarnegieClassification2018GraduateProgram,
	'CarnegieClassification2018UndergraduateProfile' ,
		S.CarnegieClassification2018UndergraduateProfile,
	'CarnegieClassification2018EnrollmentProfile',
		S.CarnegieClassification2018EnrollmentProfile,
	'CarnegieClassification2018SizeSetting' ,
		S.CarnegieClassification2018SizeSetting
			  ) ,
	OBJECT_CONSTRUCT (	
		
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier, 
	'InstitutionsWebAddress',
		S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress',
		S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress',
		S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress',
		S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress',
		S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress',
		S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress',
		S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress',
		S.DisabilityServicesWebAddress	 
		       ),
    CURRENT_TIMESTAMP
	);
`
try {
    snowflake.execute (
      {sqlText: sql_command}
      );
    return "Succeeded.";  
    }
  catch (err) {
    return "Failed: " + err;  
    }
  $$
  ;

CREATE OR REPLACE FUNCTION
    ConcatColumns(COLUMNLIST array)
    RETURNS varchar
    LANGUAGE javascript
  AS
  $$
    function return_column_string(ColumnArray) {
      var ColumnString = '';
      var arrayLength = ColumnArray.length;
      for (var i = 0; i < arrayLength; i++) {
        if (i>0) ColumnString += ';' 
        ColumnString += typeof(ColumnArray[i]) 
			== 'undefined' ? '' : ColumnArray[i] ;
      }
      return ColumnString.toUpperCase().trim();
    }
  return return_column_string(COLUMNLIST);
  $$
;

CREATE OR REPLACE FUNCTION
    Hash_Key(SCDKeys array)  
    RETURNS VARCHAR(50)
  as 
  $$
    UPPER(SHA1(ConcatColumns(SCDKeys)))
  $$
;


CREATE OR REPLACE PROCEDURE 
		pr_AcademicInstitutionMergeHashByte_Load(YEAR FLOAT)
  RETURNS STRING
  LANGUAGE javascript
  EXECUTE AS OWNER
  AS
  $$
  var sql_command = `
	MERGE INTO AcademicInstitution T USING (
	SELECT  *  FROM  Od_AcademicInstitution
	WHERE   ACADEMICYEAR = ` + YEAR.toString() +`
	) S ON T.InstitutionIdentifier = S.InstitutionIdentifier
	WHEN MATCHED
		AND ( HashKey(ARRAY_CONSTRUCT(
	T.InstitutionName,
	T.InstitutionNameAlias,
	T.StreetAddress,
	T.City,
	T.State,
	T.ZipCode,
	T.StateCode,
	T.EconomicAnalysisRegions,
	T.ChiefAdministrator,
	T.ChiefAdministratorTitle,
	T.TelephoneNumber,
	T.EmployerIdentificationNumber,
	T.DunBradstreetNumbers,
	T.PostsecondaryEducationIDNumber,
	T.TitleIVEligibilityIndicatorCode,
	T.SectorOfInstitution,
	T.LevelOfInstitution,
	T.ControlOfInstitution,
	T.HighestLevelOfOffering,
	T.UndergraduateOffering,
	T.GraduateOffering,
	T.HighestDegreeOffered,
	T.DegreeGrantingStatus,
	T.HistoricallyBlackCollegeOrUniversity,
	T.InstitutionHasHospital,
	T.InstitutionGrantsMedicalDegree,
	T.TribalCollege,
	T.DegreeOfUrbanization,
	T.InstitutionOpenToGeneralPublic,
	T.StatusOfInstitution,
	T.UnitidForMergedSchools,
	T.YearInstitutionWasDeletedFromIPEDS,
	T.DateInstitutionClosed,
	T.InstitutionIsActive,
	T.PrimarilyPostsecondaryIndicator,
	T.PostsecondaryInstitutionIndicator,
	T.PostsecondaryAndTitleIvInstitutionIndicator,
	T.ReportingMethodForStudentCharges,
	T.InstitutionalCategory,
	T.LandGrantInstitution,
	T.InstitutionSizeCategory,
	T.MultiCampusOrganization,
	T.NameOfMultiCampusOrganization,
	T.IdentificationNumberOfMultiCampusOrganization,
	T.CoreBasedStatisticalArea,
	T.CBSATypeMetropolitanMicropolitan,
	T.CombinedStatisticalArea,
	T.NewEnglandCityAndTownArea,
	T.FIPSCountyCode,
	T.CountyName,
	T.StateAnd114thCongressionalDistrictID,
	T.LongitudeLocation,
	T.LatitudeLocation,
	T.NCESGroupCategory,
	T.DataFeedbackReport,
	T.CarnegieClassification,
	T.WebAddress)) <> HashKey(ARRAY_CONSTRUCT(
		S.InstitutionName,
		S.InstitutionNameAlias,
		S.StreetAddress,
		S.City,
		S.State,
		S.ZipCode,
		S.StateCode,
		S.EconomicAnalysisRegions,
		S.ChiefAdministrator,
		S.ChiefAdministratorTitle,
		S.TelephoneNumber,
		S.EmployerIdentificationNumber,
		S.DunBradstreetNumbers,
		S.PostsecondaryEducationIDNumber,
		S.TitleIVEligibilityIndicatorCode,
		S.SectorOfInstitution,
		S.LevelOfInstitution,
		S.ControlOfInstitution,
		S.HighestLevelOfOffering,
		S.UndergraduateOffering,
		S.GraduateOffering,
		S.HighestDegreeOffered,
		S.DegreeGrantingStatus,
		S.HistoricallyBlackCollegeOrUniversity,
		S.InstitutionHasHospital,
		S.InstitutionGrantsMedicalDegree,
		S.TribalCollege,
		S.DegreeOfUrbanization,
		S.InstitutionOpenToGeneralPublic,
		S.StatusOfInstitution,
		S.UnitidForMergedSchools,
		S.YearInstitutionWasDeletedFromIPEDS,
		S.DateInstitutionClosed,
		S.InstitutionIsActive,
		S.PrimarilyPostsecondaryIndicator,
		S.PostsecondaryInstitutionIndicator,
		S.PostsecondaryAndTitleIvInstitutionIndicator,
		S.ReportingMethodForStudentCharges,
		S.InstitutionalCategory,
		S.LandGrantInstitution,
		S.InstitutionSizeCategory,
		S.MultiCampusOrganization,
		S.NameOfMultiCampusOrganization,
		S.IdentificationNumberOfMultiCampusOrganization,
		S.CoreBasedStatisticalArea,
		S.CBSATypeMetropolitanMicropolitan,
		S.CombinedStatisticalArea,
		S.NewEnglandCityAndTownArea,
		S.FIPSCountyCode,
		S.CountyName,
		S.StateAnd114thCongressionalDistrictID,
		S.LongitudeLocation,
		S.LatitudeLocation,
		S.NCESGroupCategory,
		S.DataFeedbackReport,  
	OBJECT_CONSTRUCT (
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,
	'CarnegieClassification2000' ,
		S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' ,
		S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' ,
		S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' ,
		S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' ,
		S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' ,
		S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' ,
		S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' ,
		S.CarnegieClassification2015SizeSetting ,
	'CarnegieClassification2018Basic',
		S.CarnegieClassification2018Basic,
	'CarnegieClassification2018UndergraduateProgram' ,
		S.CarnegieClassification2018UndergraduateProgram,
	'CarnegieClassification2018GraduateProgram',
		S.CarnegieClassification2018GraduateProgram,
	'CarnegieClassification2018UndergraduateProfile' ,
		S.CarnegieClassification2018UndergraduateProfile,
	'CarnegieClassification2018EnrollmentProfile',
		S.CarnegieClassification2018EnrollmentProfile,
	'CarnegieClassification2018SizeSetting' ,
		S.CarnegieClassification2018SizeSetting
				) ,
	OBJECT_CONSTRUCT (	
	
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,	   
	'InstitutionsWebAddress',
		S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress',
		S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress',
		S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress',
		S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress',
		S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress',
		S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress',
		S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress',
		S.DisabilityServicesWebAddress
	
				)    
	))) THEN	
UPDATE
SET
  InstitutionIdentifier = S.InstitutionIdentifier,
  InstitutionName = S.InstitutionName,
  InstitutionNameAlias = S.InstitutionNameAlias,
  StreetAddress = S.StreetAddress,
  City = S.City,
  State = S.State,
  ZipCode = S.ZipCode,
  StateCode = S.StateCode,
  EconomicAnalysisRegions = S.EconomicAnalysisRegions,
  ChiefAdministrator = S.ChiefAdministrator,
  ChiefAdministratorTitle = S.ChiefAdministratorTitle,
  TelephoneNumber = S.TelephoneNumber,
  EmployerIdentificationNumber = S.EmployerIdentificationNumber,
  DunBradstreetNumbers = S.DunBradstreetNumbers,
  PostsecondaryEducationIDNumber = S.PostsecondaryEducationIDNumber,
  TitleIVEligibilityIndicatorCode = S.TitleIVEligibilityIndicatorCode,
  SectorOfInstitution = S.SectorOfInstitution,
  LevelOfInstitution = S.LevelOfInstitution,
  ControlOfInstitution = S.ControlOfInstitution,
  HighestLevelOfOffering = S.HighestLevelOfOffering,
  UndergraduateOffering = S.UndergraduateOffering,
  GraduateOffering = S.GraduateOffering,
  HighestDegreeOffered = S.HighestDegreeOffered,
  DegreeGrantingStatus = S.DegreeGrantingStatus,
  HistoricallyBlackCollegeOrUniversity
	= S.HistoricallyBlackCollegeOrUniversity,
  InstitutionHasHospital = S.InstitutionHasHospital,
  InstitutionGrantsMedicalDegree = S.InstitutionGrantsMedicalDegree,
  TribalCollege = S.TribalCollege,
  DegreeOfUrbanization = S.DegreeOfUrbanization,
  InstitutionOpenToGeneralPublic 
	= S.InstitutionOpenToGeneralPublic,
  StatusOfInstitution = S.StatusOfInstitution,
  UnitidForMergedSchools = S.UnitidForMergedSchools,
  YearInstitutionWasDeletedFromIPEDS 
	= S.YearInstitutionWasDeletedFromIPEDS,
  DateInstitutionClosed = S.DateInstitutionClosed,
  InstitutionIsActive = S.InstitutionIsActive,
  PrimarilyPostsecondaryIndicator = S.PrimarilyPostsecondaryIndicator,
  PostsecondaryInstitutionIndicator = S.PostsecondaryInstitutionIndicator,
  PostsecondaryAndTitleIvInstitutionIndicator 
	= S.PostsecondaryAndTitleIvInstitutionIndicator,
  ReportingMethodForStudentCharges = S.ReportingMethodForStudentCharges,
  InstitutionalCategory = S.InstitutionalCategory,
  LandGrantInstitution = S.LandGrantInstitution,
  InstitutionSizeCategory = S.InstitutionSizeCategory,
  MultiCampusOrganization = S.MultiCampusOrganization,
  NameOfMultiCampusOrganization = S.NameOfMultiCampusOrganization,
  IdentificationNumberOfMultiCampusOrganization 
	= S.IdentificationNumberOfMultiCampusOrganization,
  CoreBasedStatisticalArea = S.CoreBasedStatisticalArea,
  CBSATypeMetropolitanMicropolitan 
	= S.CBSATypeMetropolitanMicropolitan,
  CombinedStatisticalArea = S.CombinedStatisticalArea,
  NewEnglandCityAndTownArea = S.NewEnglandCityAndTownArea,
  FIPSCountyCode = S.FIPSCountyCode,
  CountyName = S.CountyName,
  StateAnd114thCongressionalDistrictID 
	= S.StateAnd114thCongressionalDistrictID,
  LongitudeLocation = S.LongitudeLocation,
  LatitudeLocation = S.LatitudeLocation,
  NCESGroupCategory = S.NCESGroupCategory,
  DataFeedbackReport = S.DataFeedbackReport, 
  AcademicYear=S.AcademicYear,
  CarnegieClassification =OBJECT_CONSTRUCT (
  'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,
	'CarnegieClassification2000' ,
		S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' ,
		S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' ,
		S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' ,
		S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' ,
		S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' ,
		S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' ,
		S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' ,
		S.CarnegieClassification2015SizeSetting ,
	'CarnegieClassification2018Basic',
		S.CarnegieClassification2018Basic,
	'CarnegieClassification2018UndergraduateProgram' ,
		S.CarnegieClassification2018UndergraduateProgram,
	'CarnegieClassification2018GraduateProgram',
		S.CarnegieClassification2018GraduateProgram,
	'CarnegieClassification2018UndergraduateProfile' ,
		S.CarnegieClassification2018UndergraduateProfile,
	'CarnegieClassification2018EnrollmentProfile',
		S.CarnegieClassification2018EnrollmentProfile,
	'CarnegieClassification2018SizeSetting' ,
		S.CarnegieClassification2018SizeSetting
		
   ),
	WebAddress=OBJECT_CONSTRUCT (	
	
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,	   
	'InstitutionsWebAddress',
		S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress',
		S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress',
		S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress',
		S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress',
		S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress',
		S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress',
		S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress',
		S.DisabilityServicesWebAddress
		 
   ),
  RecordUpdateDateTime = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
	INSERT   (
	InstitutionIdentifier,
	InstitutionName,
	InstitutionNameAlias,
	StreetAddress,
	City,
	State,
	ZipCode,
	StateCode,
	EconomicAnalysisRegions,
	ChiefAdministrator,
	ChiefAdministratorTitle,
	TelephoneNumber,
	EmployerIdentificationNumber,
	DunBradstreetNumbers,
	PostsecondaryEducationIDNumber,
	TitleIVEligibilityIndicatorCode,
	SectorOfInstitution,
	LevelOfInstitution,
	ControlOfInstitution,
	HighestLevelOfOffering,
	UndergraduateOffering,
	GraduateOffering,
	HighestDegreeOffered,
	DegreeGrantingStatus,
	HistoricallyBlackCollegeOrUniversity,
	InstitutionHasHospital,
	InstitutionGrantsMedicalDegree,
	TribalCollege,
	DegreeOfUrbanization,
	InstitutionOpenToGeneralPublic,
	StatusOfInstitution,
	UnitidForMergedSchools,
	YearInstitutionWasDeletedFromIPEDS,
	DateInstitutionClosed,
	InstitutionIsActive,
	PrimarilyPostsecondaryIndicator,
	PostsecondaryInstitutionIndicator,
	PostsecondaryAndTitleIvInstitutionIndicator,
	ReportingMethodForStudentCharges,
	InstitutionalCategory,
	LandGrantInstitution,
	InstitutionSizeCategory,
	MultiCampusOrganization,
	NameOfMultiCampusOrganization,
	IdentificationNumberOfMultiCampusOrganization,
	CoreBasedStatisticalArea,
	CBSATypeMetropolitanMicropolitan,
	CombinedStatisticalArea,
	NewEnglandCityAndTownArea,
	FIPSCountyCode,
	CountyName,
	StateAnd114thCongressionalDistrictID,
	LongitudeLocation,
	LatitudeLocation,
	NCESGroupCategory,
	DataFeedbackReport, 
	AcademicYear, 
	CarnegieClassification,
	WebAddress, 
	RecordCreateDateTime)
VALUES  (
	S.InstitutionIdentifier,
	S.InstitutionName,
	S.InstitutionNameAlias,
	S.StreetAddress,
	S.City,
	S.State,
	S.ZipCode,
	S.StateCode,
	S.EconomicAnalysisRegions,
	S.ChiefAdministrator,
	S.ChiefAdministratorTitle,
	S.TelephoneNumber,
	S.EmployerIdentificationNumber,
	S.DunBradstreetNumbers,
	S.PostsecondaryEducationIDNumber,
	S.TitleIVEligibilityIndicatorCode,
	S.SectorOfInstitution,
	S.LevelOfInstitution,
	S.ControlOfInstitution,
	S.HighestLevelOfOffering,
	S.UndergraduateOffering,
	S.GraduateOffering,
	S.HighestDegreeOffered,
	S.DegreeGrantingStatus,
	S.HistoricallyBlackCollegeOrUniversity,
	S.InstitutionHasHospital,
	S.InstitutionGrantsMedicalDegree,
	S.TribalCollege,
	S.DegreeOfUrbanization,
	S.InstitutionOpenToGeneralPublic,
	S.StatusOfInstitution,
	S.UnitidForMergedSchools,
	S.YearInstitutionWasDeletedFromIPEDS,
	S.DateInstitutionClosed,
	S.InstitutionIsActive,
	S.PrimarilyPostsecondaryIndicator,
	S.PostsecondaryInstitutionIndicator,
	S.PostsecondaryAndTitleIvInstitutionIndicator,
	S.ReportingMethodForStudentCharges,
	S.InstitutionalCategory,
	S.LandGrantInstitution,
	S.InstitutionSizeCategory,
	S.MultiCampusOrganization,
	S.NameOfMultiCampusOrganization,
	S.IdentificationNumberOfMultiCampusOrganization,
	S.CoreBasedStatisticalArea,
	S.CBSATypeMetropolitanMicropolitan,
	S.CombinedStatisticalArea,
	S.NewEnglandCityAndTownArea,
	S.FIPSCountyCode,
	S.CountyName,
	S.StateAnd114thCongressionalDistrictID,
	S.LongitudeLocation,
	S.LatitudeLocation,
	S.NCESGroupCategory,
	S.DataFeedbackReport, 
	S.AcademicYear, 
	OBJECT_CONSTRUCT (
	
    'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,
	'CarnegieClassification2000' ,
		S.CarnegieClassification2000,
	'CarnegieClassification20052010Basic' ,
		S.CarnegieClassification20052010Basic,
	'CarnegieClassification2015Basic' ,
		S.CarnegieClassification2015Basic,
	'CarnegieClassification2015UndergraduateProgram' ,
		S.CarnegieClassification2015UndergraduateProgram,
	'CarnegieClassification2015GraduateProgram' ,
		S.CarnegieClassification2015GraduateProgram,
	'CarnegieClassification2015UndergraduateProfile' ,
		S.CarnegieClassification2015UndergraduateProfile,
	'CarnegieClassification2015EnrollmentProfile' ,
		S.CarnegieClassification2015EnrollmentProfile,
	'CarnegieClassification2015SizeSetting' ,
		S.CarnegieClassification2015SizeSetting ,
	'CarnegieClassification2018Basic',
		S.CarnegieClassification2018Basic,
	'CarnegieClassification2018UndergraduateProgram' ,
		S.CarnegieClassification2018UndergraduateProgram,
	'CarnegieClassification2018GraduateProgram',
		S.CarnegieClassification2018GraduateProgram,
	'CarnegieClassification2018UndergraduateProfile' ,
		S.CarnegieClassification2018UndergraduateProfile,
	'CarnegieClassification2018EnrollmentProfile',
		S.CarnegieClassification2018EnrollmentProfile,
	'CarnegieClassification2018SizeSetting' ,
		S.CarnegieClassification2018SizeSetting 
	  ) ,
	OBJECT_CONSTRUCT (	
	'AcademicInstitutionIdentifier',
		S.InstitutionIdentifier,	   
	'InstitutionsWebAddress',
		S.InstitutionsWebAddress,
	'AdmissionsOfficeWebAddress',
		S.AdmissionsOfficeWebAddress,
	'FinancialAidOfficeWebAddress',
		S.FinancialAidOfficeWebAddress,
	'OnlineApplicationWebAddress',
		S.OnlineApplicationWebAddress,
	'NetPriceCalculatorWebAddress',
		S.NetPriceCalculatorWebAddress,
	'VeteransMilitaryServiceTuitionPoliciesWebAddress',
		S.VeteransMilitaryServiceTuitionPoliciesWebAddress,
	'StudentRightAthleteGraduationRateWebAddress',
		S.StudentRightAthleteGraduationRateWebAddress,
	'DisabilityServicesWebAddress',
		S.DisabilityServicesWebAddress
	   ),			
	CURRENT_TIMESTAMP
		);
`
try {
    snowflake.execute (
      {sqlText: sql_command}
      );
    return "Succeeded.";  // Return a success/error indicator.
    }
  catch (err) {
    return "Failed: " + err;  // Return a success/error indicator.
    }
  $$
  ;

    TRUNCATE TABLE od_AcademicInstitution;
    CALL pr_od_AcademicInstitution_Load(2017::FLOAT); 
    CALL pr_od_AcademicInstitution_Load(2018::FLOAT);
    CALL pr_od_AcademicInstitution_Load(2019::FLOAT);


    