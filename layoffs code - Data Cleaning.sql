-- To always have a copy of the raw data, create a staging table
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- Confirm the staging table is good and begin the cleaning process
SELECT *
FROM layoffs_staging;
 
 -- 1. REMOVING DUPLICATES
 -- Create a unique identifier to match across every column, sth like a row number
 -- date is done with the backticks as its a keyword in MySQL
 SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, industry, total_laid_off, percentage_laid_off,'date') AS row_num
 FROM layoffs_staging; 
 
 -- Creating a CTE
 WITH duplicate_cte AS
 (
  SELECT *,
 ROW_NUMBER() OVER(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 )
 SELECT *
 FROM duplicate_cte
 WHERE row_num > 1; 
 
 -- Test running one of the duplicate companies to confirm
SELECT *
FROM layoffs_staging
WHERE company = 'WHOOP';

-- When removing duplicates, ensure to always retain one row of the duplicates
-- We cant use the DELETE function within the CTE as it registers as an update feature in MySQL


-- Creating a different table
-- RC on staging table, copy to clipboard, create statement, on query space RC then paste to reveal the code




CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- the new table created, then insert values into it
SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- first do a select, then delete statement to identify what youre deleting
SELECT *
FROM layoffs_staging2
WHERE row_num>1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num>1; 

-- this is now the table without any duplicate data
SELECT *
FROM layoffs_staging2;


-- 2. STANDARDIZING DATA; looking for any issues within the data and solving those
-- off the bat, there seems to be white spaces before the company name, remove those
-- for this, go column by column fixing the issues

SELECT DISTINCT company, (TRIM(company))
FROM layoffs_staging2;

-- update the table so we have only one column with company names
UPDATE layoffs_staging2
SET company = (TRIM(company));

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1; 

-- merge allindustrys that sound like the same thing ie crypto and cyptocurrency
-- using the % wildcard with the LIKE fxn to select all industry names that start with Crypto
SELECT  industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; 

-- the merge them so they appear as constant
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;
-- USA appears twice, to fix that

SELECT * 
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- to fix this, update the table 
UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';


-- Because we'll do time sries, and visualization on this date, the date needs some work as well
-- right now the date reads as text data type
-- we want to format the date as month, day, year

SELECT `date`
FROM layoffs_staging2;

-- convert this date data from text to date, and formating it to our want
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- do the update to have the date column well formated
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- if we refresh the schema, the date data type still reads as text, to change that
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. ADDRESSING NULL AND BLANK VALUES
-- run you table to see columns with NULL and Blanks, address column by colm
-- just by looking at the table, we can further and to the query to include 2 related colmns ie

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- recall that you had earlier perused the data and established columns with nulls and blanks{actually, it would be useful to note this done
-- for the blank, no space btwn the quotes
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- pick one of the blanks and check if some entries for the same company have the industry type captured
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- here we establish the industry that Airbnb belongs, then we update
-- while all of these entries belong to the same location, i added the `AND` for location to ensure Airbnb in say, Chigago does not get interupted with if it belong to a different industry
UPDATE layoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb'
AND location = 'SF Bay Area';

-- while the above works perfectly, it calls for running all the null/blank industries 
-- to do this once, we introduce a JOIN statement
-- for this JOIN we are joining the table on itself ie
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;    

-- to have a better visual, select just the industry column from the 2 tables
 SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;    

-- the goal is to have t2.indutry all popolated, therefore peform this update
UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

-- Worked! Now we make the updates onto t1
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- running the below again, the company Bally's comes up, but only because it had to industry column populated
-- no way to address this, unless we scrape the web and find it, for its particular country, locaton etc
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- for the 2 laid_off columns, we cannot address the null cells as we do not have any data to help us populate that
-- columns like stage and funds_raised can get filled by a web scrape

-- 4. REMOVING COLUMNS AND ROWS WE DONT NEED / USELESS FOR PROJECT
-- retaining them slows down query runtime etc
-- establish what exactly the project looks to achieve,this will help decide which columns/rows are relevant
-- total & percentage_laid_off columns that are both null are useless EDA
-- to see how many of such rows we have
SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- now delete them
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
 
-- remember the row_num column, we dont need that either
-- in this case this is an entire column we are getting rid of
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- run the entire table and see the difference
-- this now is our clean data against which we will run to find, trends, patterns etc
SELECT * 
FROM layoffs_staging2;