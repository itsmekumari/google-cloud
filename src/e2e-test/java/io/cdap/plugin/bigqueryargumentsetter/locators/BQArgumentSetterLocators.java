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
package io.cdap.plugin.bigqueryargumentsetter.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * BQArgumentSetter Related Locators.
 */
public class BQArgumentSetterLocators {

  public static WebElement argumentSelectColumnName(int row) {
    String xpath = "//div[@data-cy='argumentSelectionConditions']//div[@data-cy= '" + row + "']//input[@placeholder='Column name']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement argumentSelectValue(int row) {
    String xpath = "//div[@data-cy='argumentSelectionConditions']//div[@data-cy= '" + row + "']//input[@placeholder='Value']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement fieldArgumentSelectionAddRowButton(int row) {
    String xpath = "//*[@data-cy='argumentSelectionConditions']//*[@data-cy='" + row + "']//button[@data-cy='add-row']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement listOfArgumentColumns(int row) {
    String xpath = "//*[@data-cy='argumentsColumns']//*[@data-cy= '" + row + "']//*[@data-cy='key']/input";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement fieldArgumentColumnAddRowButton(int row) {
    String xpath = "//*[@data-cy='argumentsColumns']//*[@data-cy='" + row + "']//button[@data-cy='add-row']";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }
}
