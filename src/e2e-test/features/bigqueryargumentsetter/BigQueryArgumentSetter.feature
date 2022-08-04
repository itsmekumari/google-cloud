@BigQueryArgumentSetter
Feature: BigQuery Argument Setter - Validate BigQuery Argument Setter plugin setting the arguments at runtime

  @BQ_SOURCE_DATATYPE_TEST
  Scenario:Verify BigQuery Argument Setter plugin properties setting the arguments at runtime
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Argument Setter" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Argument Setter"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Enter the Arguments Selection Conditions field "bigQueryArgumentValidTableCondition"
    Then Enter the Arguments Columns field as list "bigQueryArgumentValidTableColumn"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Argument Setter" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

