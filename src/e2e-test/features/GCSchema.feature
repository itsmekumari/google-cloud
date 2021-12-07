Feature: Validate GCS plugin output schema for different formats
  @GCS
  Scenario Outline:GCS Source output schema validation
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "<Bucket>" and format "<FileFormat>"
    Then Capture and validate output schema
    Examples:
      | Bucket              | FileFormat            |
      | gcsCsvBucket        | gcsCSVFileFormat      |
      | gcsTsvBucket        | gcsTSVFileFormat      |

  @GCS
  Scenario Outline:GCS Source output schema validation for delimited files
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "<Bucket>" and format "<FileFormat>" with delimiter field "<Delimiter>"
    Then Capture and validate output schema
    Examples:
      | Bucket              | FileFormat            | Delimiter     |
      | gcsDelimitedBucket  | gcsDelimitedFileFormat| gcsDelimiter  |
      | gcsTextBucket       | gcsTextFileFormat     | gcsDelimiter  |

  @GCS
  Scenario:GCS Source output schema validation for blob format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS Properties
    Then Enter the GCS Properties with GCS bucket "gcsBlobBucket" and format "gcsBlobFileFormat"
    Then Capture and validate output schema for blob format
