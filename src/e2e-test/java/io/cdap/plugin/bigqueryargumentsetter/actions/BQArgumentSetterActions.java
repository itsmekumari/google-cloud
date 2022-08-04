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
package io.cdap.plugin.bigqueryargumentsetter.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.bigqueryargumentsetter.locators.BQArgumentSetterLocators;
import io.cdap.plugin.gcsdelete.locators.GCSDeleteLocators;
import io.cucumber.core.logging.Logger;
import io.cucumber.core.logging.LoggerFactory;
import org.junit.Assert;

import java.util.Map;

/**
 * BQArgumentSetter plugin step actions.
 */
public class BQArgumentSetterActions {
  private static final Logger logger = (Logger) LoggerFactory.getLogger(BQArgumentSetterActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(BQArgumentSetterLocators.class);
  }

  public static void enterArgumentSelection(String jsonArgumentField) {
    Map<String, String> fieldsMapping =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonArgumentField));
    int index = 0;
    for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
      ElementHelper.sendKeys(BQArgumentSetterLocators.argumentSelectColumnName(index), entry.getKey().split("#")[0]);
      ElementHelper.sendKeys(BQArgumentSetterLocators.argumentSelectValue(index), entry.getValue());
      ElementHelper.clickOnElement(BQArgumentSetterLocators.fieldArgumentSelectionAddRowButton(index));
      index++;
    }
  }

  public static void enterListOfFields(int row, String argumentColumns) {
    ElementHelper.sendKeys(BQArgumentSetterLocators.listOfArgumentColumns(row), argumentColumns);
  }

  public static void clickFieldsAddRowButton(int row) {
    ElementHelper.clickOnElement(BQArgumentSetterLocators.fieldArgumentColumnAddRowButton(row));
  }
}
