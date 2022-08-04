/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.bigqueryargumentsetter.stepsdesign;

import com.google.cloud.storage.Blob;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cdap.plugin.bigqueryargumentsetter.actions.BQArgumentSetterActions;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.gcsdelete.actions.GCSDeleteActions;
import io.cdap.plugin.gcsdelete.locators.GCSDeleteLocators;
import io.cdap.plugin.utils.E2EHelper;
import io.cdap.plugin.utils.E2ETestConstants;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;

/**
 * BQArgumentSetter Action related stepDesigns.
 */

public class BQArgumentSetter implements E2EHelper {

  @Then("Enter the Arguments Selection Conditions field {string}")
  public void enterTheArgumentsSelectionConditionsField(String jsonArgumentField) {
    BQArgumentSetterActions.enterArgumentSelection(jsonArgumentField);
  }

  @Then("Enter the Arguments Columns field as list {string}")
  public void enterTheArgumentsColumnsFieldAsList(String commaSeparatedFieldList) {
    String[] fields = PluginPropertyUtils.pluginProp(commaSeparatedFieldList).split(",");
    for (int index = 0; index < fields.length; index++) {
      BQArgumentSetterActions.enterListOfFields(index, fields[index]);
      BQArgumentSetterActions.clickFieldsAddRowButton(index);
    }
  }
}
