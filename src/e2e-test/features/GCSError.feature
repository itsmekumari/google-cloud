Feature: Verify GSC plugin error scenarios

  @GCS
  Scenario Outline:Verify GCS Source properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | path            |
      | format          |

  @GCS
  Scenario: To verify Error message for invalid bucket name
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "gcsInvalidBucketName" and format "gcsCSVFileFormat"
    Then Verify invalid bucket name error message is displayed for bucket "gcsInvalidBucketName"

  @GCS
  Scenario: To verify Error message for incorrect output path field value
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "gcsOutputFieldTestBucket" and format "gcsCSVFileFormat" with Path Field "gcsInvalidPathField"
    Then Verify Output Path field Error Message for incorrect path field "gcsInvalidPathField"

  @GCS
  Scenario: To verify Error message for incorrect delimiter
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "gcsDelimitedBucket" and format "gcsDelimitedFileFormat" with delimiter field "gcsIncorrectDelimiter"
    Then Verify get schema fails with error

  @GCS
  Scenario: To verify Error message for incorrect override field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "gcsOutputFieldTestBucket" and format "gcsCSVFileFormat" with Override field "gcsInvalidOverrideField" and data type "gcsOverrideDataType"
    Then Verify get schema fails with error

  @GCS
  Scenario: To verify Pipeline preview gets failed for incorrect Regex-Path
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Connect Source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "gcsCsvBucket" and format "gcsCSVFileFormat" with regex path filter "gcsIncorrectRegexPath"
    Then Capture and validate output schema
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "gcsBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"

  @GCS
  Scenario Outline:Verify GCS Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Target is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | path            |
      | format          |
