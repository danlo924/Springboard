-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema investing
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema investing
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `investing` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ;
USE `investing` ;

-- -----------------------------------------------------
-- Table `investing`.`_stg_div_hist`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`_stg_div_hist` (
  `index` BIGINT NULL DEFAULT NULL,
  `id` TEXT NULL DEFAULT NULL,
  `type` TEXT NULL DEFAULT NULL,
  `attributes.year` BIGINT NULL DEFAULT NULL,
  `attributes.amount` TEXT NULL DEFAULT NULL,
  `attributes.ex_date` TEXT NULL DEFAULT NULL,
  `attributes.freq` TEXT NULL DEFAULT NULL,
  `attributes.declare_date` TEXT NULL DEFAULT NULL,
  `attributes.pay_date` TEXT NULL DEFAULT NULL,
  `attributes.record_date` TEXT NULL DEFAULT NULL,
  `attributes.date` TEXT NULL DEFAULT NULL,
  `attributes.adjusted_amount` DOUBLE NULL DEFAULT NULL,
  `attributes.split_adj_factor` DOUBLE NULL DEFAULT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `ix__stg_div_hist_index` ON `investing`.`_stg_div_hist` (`index` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `investing`.`_stg_has_div`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`_stg_has_div` (
  `Date` DATETIME NULL DEFAULT NULL,
  `Dividends` DOUBLE NULL DEFAULT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `ix__stg_has_div_Date` ON `investing`.`_stg_has_div` (`Date` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `investing`.`_stg_price_hist`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`_stg_price_hist` (
  `Date` DATETIME NULL DEFAULT NULL,
  `Open` DOUBLE NULL DEFAULT NULL,
  `High` DOUBLE NULL DEFAULT NULL,
  `Low` DOUBLE NULL DEFAULT NULL,
  `Close` DOUBLE NULL DEFAULT NULL,
  `Volume` BIGINT NULL DEFAULT NULL,
  `Dividends` BIGINT NULL DEFAULT NULL,
  `Stock Splits` BIGINT NULL DEFAULT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `ix__stg_price_hist_Date` ON `investing`.`_stg_price_hist` (`Date` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `investing`.`_stg_sp_current`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`_stg_sp_current` (
  `index` BIGINT NULL DEFAULT NULL,
  `Symbol` TEXT NULL DEFAULT NULL,
  `Security` TEXT NULL DEFAULT NULL,
  `SEC filings` TEXT NULL DEFAULT NULL,
  `GICS Sector` TEXT NULL DEFAULT NULL,
  `GICS Sub-Industry` TEXT NULL DEFAULT NULL,
  `Headquarters Location` TEXT NULL DEFAULT NULL,
  `Date first added` TEXT NULL DEFAULT NULL,
  `CIK` BIGINT NULL DEFAULT NULL,
  `Founded` TEXT NULL DEFAULT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `ix__stg_sp_current_index` ON `investing`.`_stg_sp_current` (`index` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `investing`.`_stg_sp_history`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`_stg_sp_history` (
  `index` BIGINT NULL DEFAULT NULL,
  `('Date', 'Date')` TEXT NULL DEFAULT NULL,
  `('Added', 'Ticker')` TEXT NULL DEFAULT NULL,
  `('Added', 'Security')` TEXT NULL DEFAULT NULL,
  `('Removed', 'Ticker')` TEXT NULL DEFAULT NULL,
  `('Removed', 'Security')` TEXT NULL DEFAULT NULL,
  `('Reason', 'Reason')` TEXT NULL DEFAULT NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX `ix__stg_sp_history_index` ON `investing`.`_stg_sp_history` (`index` ASC) VISIBLE;


-- -----------------------------------------------------
-- Table `investing`.`companies`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`companies` (
  `CompanyID` INT NOT NULL AUTO_INCREMENT,
  `Name` VARCHAR(100) NOT NULL,
  `Description` TEXT NULL DEFAULT NULL,
  `Ticker` CHAR(15) NOT NULL,
  `Sector` VARCHAR(100) NULL DEFAULT NULL,
  `Industry` VARCHAR(100) NULL DEFAULT NULL,
  `Founded` INT NULL DEFAULT NULL,
  `SPAddDate` DATETIME NULL DEFAULT NULL,
  `SPRemDate` DATETIME NULL DEFAULT NULL,
  `IsIndex` BIT(1) NOT NULL DEFAULT b'0',
  `HasDividend` BIT(1) NULL DEFAULT NULL,
  `UpdatedDate` DATETIME NULL DEFAULT NULL,
  PRIMARY KEY (`CompanyID`))
ENGINE = InnoDB
AUTO_INCREMENT = 1041
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `investing`.`dividends`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`dividends` (
  `CompanyID` INT NOT NULL,
  `FreqType` CHAR(15) NOT NULL,
  `DeclareDate` DATETIME NULL DEFAULT NULL,
  `ExDivDate` DATETIME NOT NULL,
  `RecordDate` DATETIME NULL DEFAULT NULL,
  `PayDate` DATETIME NULL DEFAULT NULL,
  `Amount` FLOAT NULL DEFAULT NULL,
  `AdjAmount` FLOAT NULL DEFAULT NULL,
  `SplitAdjFactor` FLOAT NULL DEFAULT NULL,
  `UpdatedDate` DATETIME NULL DEFAULT NULL,
  PRIMARY KEY (`CompanyID`, `ExDivDate`, `FreqType`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `investing`.`prices`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `investing`.`prices` (
  `CompanyID` INT NOT NULL,
  `Date` DATETIME NOT NULL,
  `Open` FLOAT NULL DEFAULT NULL,
  `High` FLOAT NULL DEFAULT NULL,
  `Low` FLOAT NULL DEFAULT NULL,
  `Close` FLOAT NULL DEFAULT NULL,
  `Volume` BIGINT NULL DEFAULT NULL,
  `UpdatedDate` DATETIME NULL DEFAULT NULL,
  PRIMARY KEY (`CompanyID`, `Date`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

USE `investing` ;

-- -----------------------------------------------------
-- procedure sp_get_dividends_and_prices_by_ticker
-- -----------------------------------------------------

DELIMITER $$
USE `investing`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_get_dividends_and_prices_by_ticker`(
	IN StockTicker CHAR(15)
    )
BEGIN
	
    DECLARE CompanyID_filter INT;
    DECLARE SandPCompanyID INT;
    SET CompanyID_filter = (SELECT CompanyID FROM Companies WHERE Ticker = StockTicker);
    SET SandPCompanyID = (SELECT CompanyID FROM Companies WHERE Name = 'S&P 500 Index');
    
    SELECT
		c.Ticker, 
        d.FreqType,
        d.ExDivDate, 
        d.AdjAmount, 
        p.Date AS PriceDate, 
        DATEDIFF(p.Date, d.ExDivDate) as ExDivDays,
        (p.Open + p.High + p.Low + p.Close) / 4.0 AS AvgPrice,
        (sandp.Open + sandp.High + sandp.Low + sandp.Close) / 4.0 AS SandPAvgPrice
    FROM companies c
		LEFT OUTER JOIN dividends d ON c.CompanyID = d.CompanyId
        LEFT OUTER JOIN prices p ON p.CompanyID = c.CompanyID
			AND p.Date BETWEEN d.ExDivDate + interval -42 day AND d.ExDivDate + interval 42 day 
		LEFT OUTER JOIN prices sandp ON sandp.CompanyID = SandPCompanyID
			AND p.Date = sandp.Date
	WHERE c.Ticker = StockTicker
	ORDER BY d.ExDivDate, p.Date;
    
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure sp_refresh_companies
-- -----------------------------------------------------

DELIMITER $$
USE `investing`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_refresh_companies`()
BEGIN

	INSERT INTO companies(Name, Ticker, Sector, Industry, Founded, SPAddDate, IsIndex, UpdatedDate)
    SELECT spc.`Security`
		,spc.Symbol
        ,spc.`GICS Sector`
        ,spc.`GICS Sub-Industry`
        ,substring_index(substring_INDEX(spc.Founded, ' ', 1), '/', 1)
        ,substring_INDEX(spc.`Date first added`, ' (', 1)
        ,0
        ,sysdate(3)
    FROM _stg_sp_current spc
		LEFT OUTER JOIN companies c ON spc.Symbol = c.Ticker
	WHERE c.Ticker IS NULL;
    
    UPDATE companies c
    INNER JOIN _stg_sp_current spc ON spc.Symbol = c.Ticker
    SET c.Name = spc.`Security`
		,c.Sector = spc.`GICS Sector`
        ,c.Industry = spc.`GICS Sub-Industry`
        ,c.Founded = substring_index(substring_INDEX(spc.Founded, ' ', 1), '/', 1)
        ,c.SPAddDate = substring_INDEX(spc.`Date first added`, ' (', 1)
        ,c.IsIndex = 0
        ,c.UpdatedDate = sysdate(3);
        
	INSERT INTO companies(Name, Ticker, UpdatedDate)
	SELECT h.`('Removed', 'Security')` AS Name
		,h.`('Removed', 'Ticker')` AS Ticker
        ,sysdate(3) AS UpdatedDate
	FROM _stg_sp_history h
	LEFT JOIN companies c ON h.`('Removed', 'Ticker')` = c.Ticker
	WHERE h.`('Removed', 'Security')` IS NOT NULL
		AND c.Ticker IS NULL;
        
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure sp_refresh_dividends
-- -----------------------------------------------------

DELIMITER $$
USE `investing`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_refresh_dividends`(
	IN StockTicker CHAR(15)
    )
BEGIN
	
    DECLARE CompanyID_filter INT;
    SET CompanyID_filter = (SELECT CompanyID FROM Companies WHERE Ticker = StockTicker);

	INSERT INTO dividends
    (
		CompanyID
        ,FreqType
        ,DeclareDate
        ,ExDivDate
        ,RecordDate
        ,PayDate
        ,Amount
        ,AdjAmount
        ,SplitAdjFactor
        ,UpdatedDate
	)
    SELECT 
		c.CompanyID
        ,dh.`attributes.freq`
        ,dh.`attributes.declare_date`
        ,dh.`attributes.ex_date`
        ,dh.`attributes.record_date`
        ,dh.`attributes.pay_date`
        ,dh.`attributes.amount`
        ,dh.`attributes.adjusted_amount`
        ,dh.`attributes.split_adj_factor`
        ,sysdate(3)
	FROM _stg_div_hist dh
		INNER JOIN companies c ON 1=1
		LEFT OUTER JOIN dividends d ON c.CompanyID = d.CompanyID
			AND d.ExDivDate = dh.`attributes.ex_date`
            AND d.FreqType = dh.`attributes.freq`
	WHERE 
		c.CompanyID = CompanyID_filter
        AND dh.`attributes.ex_date` IS NOT NULL
        AND d.ExDivDate IS NULL;
        
    UPDATE dividends d
    INNER JOIN _stg_div_hist dh ON d.ExDivDate = dh.`attributes.ex_date`
		AND d.FreqType = dh.`attributes.freq`
		AND d.CompanyID = CompanyID_filter
    SET d.DeclareDate = dh.`attributes.declare_date`
        ,d.RecordDate = dh.`attributes.record_date`
        ,d.PayDate = dh.`attributes.pay_date`
        ,d.Amount = dh.`attributes.amount`
        ,d.AdjAmount = dh.`attributes.adjusted_amount`
        ,d.SplitAdjFactor = dh.`attributes.split_adj_factor`
        ,d.UpdatedDate = sysdate(3)
	WHERE d.CompanyID = CompanyID_filter;
    
END$$

DELIMITER ;

-- -----------------------------------------------------
-- procedure sp_refresh_prices_company_data
-- -----------------------------------------------------

DELIMITER $$
USE `investing`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_refresh_prices_company_data`(
	IN StockTicker CHAR(15)
    )
BEGIN
	
    DECLARE CompanyID_filter INT;
    SET CompanyID_filter = (SELECT CompanyID FROM Companies WHERE Ticker = StockTicker);
    
    IF (SELECT COUNT(*) FROM _stg_has_div) > 0 THEN 
		UPDATE companies
        SET HasDividend = 1
			,UpdatedDate = sysdate(3)
        WHERE CompanyID = CompanyID_filter;
	END IF;

	INSERT INTO Prices
    (
		CompanyID
        ,Date
        ,Open
        ,High
        ,Low
        ,Close
        ,Volume
        ,UpdatedDate
	)
    SELECT 
		c.CompanyID
        ,ph.Date
        ,ph.Open
        ,ph.High
        ,ph.Low
        ,ph.Close
        ,ph.Volume
        ,sysdate(3)
	FROM _stg_price_hist ph
		INNER JOIN companies c ON 1=1
		LEFT OUTER JOIN Prices p ON c.CompanyID = p.CompanyID
			AND p.Date = ph.Date
	WHERE 
		c.CompanyID = CompanyID_filter
        AND ph.Date < CURRENT_DATE()
        AND ph.Date >= '1980-01-01 00:00:00'
        AND p.Date IS NULL;
        
    UPDATE prices p
    INNER JOIN _stg_price_hist ph ON p.Date = ph.Date
		AND p.CompanyID = CompanyID_filter
    SET p.Open = ph.Open
		,p.High = ph.High
        ,p.Low = ph.Low
        ,p.Close = ph.Close
        ,p.Volume = ph.Volume
        ,p.UpdatedDate = sysdate(3)
	WHERE p.CompanyID = CompanyID_filter;
    
    TRUNCATE TABLE _stg_has_div; 
    
END$$

DELIMITER ;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
