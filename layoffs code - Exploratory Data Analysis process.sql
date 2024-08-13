-- EXPLORATORY DATA ANALYSIS
-- this process, you might have an agenda with the data. or sometimes not
 
 SELECT *
 FROM layoffs_staging2;
 
 -- here, kinda just have fun with the data, querying it to have better understanding
 -- look at the maximum layffs 
 SELECT MAX(total_laid_off), MAX(percentage_laid_off)
 FROM layoffs_staging2;
 
 -- look at companies whose percentage is 1, meaning all employees were laid off etc
 -- funds raised implies the money the company got from investors etc
SELECT *
 FROM layoffs_staging2
 WHERE percentage_laid_off = 1
 AND (country = 'United States') AND (location = 'New York City')
  ORDER BY funds_raised_millions DESC;
  
  -- IF you aggregate a query as in the SELECT below, you have to include GROUP BY
   -- for the ORDER BY query you can use column number as opposed to writing the column name
   -- check companies with the largest layoffs
 SELECT company, SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company
 ORDER BY 2 DESC; 
 
-- check the timeline for these layoffs
 SELECT MIN(`date`), MAX(`date`)
 FROM layoffs_staging2;
 
 -- check the baddest hit industries
 SELECT industry, SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY industry 
 ORDER BY 2 DESC; 

-- check the baddest hit countries
SELECT country, SUM(total_laid_off) 
 FROM layoffs_staging2
 GROUP BY country 
 ORDER BY 2 DESC; 
 
 -- further breakdown to see layoffs/year 
 SELECT YEAR(`date`), SUM(total_laid_off) 
 FROM layoffs_staging2
 GROUP BY YEAR(`date`)
 ORDER BY 2 DESC;
 
 -- check the stage of the companies vs layoffs
  SELECT stage, SUM(total_laid_off) 
 FROM layoffs_staging2
 GROUP BY stage 
 ORDER BY 2 DESC;
 
 -- we can look at the progression of layoffs {rolling sum}
 SELECT *
 FROM layoffs_staging2;
 -- we now introduce a substring
  SELECT SUBSTRING(`date`, 6, 2) AS `MONTH`, SUM(total_laid_off)
  FROM layoffs_staging2;
  
  -- recall that if you aggregate on the SELECt statement, you have to introduce a group by fxn
  -- this shows he sum total for a particular month, across the years represented
   SELECT SUBSTRING(`date`, 6, 2) AS `MONTH`, SUM(total_laid_off)
  FROM layoffs_staging2
  GROUP BY MONTH;
 
 -- to be more clear, include the year
 -- introduve the WHERE clause to remove any data from unknown timelines
 SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) 
  FROM layoffs_staging2
  WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
  GROUP BY `MONTH`
  ORDER BY 1;
  
 -- introduce a rolling sum column
 -- essentially, rolling totals are great for a progressive analyis overtime, also great for vizualizations
 -- to do this, use a CTE
 -- LOOK INTO CTEs
 WITH Rolling_Total AS
 (
 SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS month_total
  FROM layoffs_staging2
  WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
  GROUP BY `MONTH`
  ORDER BY 1
  )
 SELECT `MONTH`, month_total,
SUM(month_total) OVER (ORDER BY `MONTH`) AS rolling_total
 FROM Rolling_Total;
 
 -- Now lets look at company breakdowns and how many people they laid off progressively
  SELECT company, SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company
 ORDER BY 2 DESC; 
 -- layoffs companywise yearly
  SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ORDER BY 3 DESC;
 
 -- we can then rank these such that the company with highest laidoffs ranks 1
 -- we actually want to see top 5 for very year
 -- again, we introduce a CTE
  SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ORDER BY 3 DESC;
  
 WITH Company_Year (company, years, total_laid_off)  AS
 (
  SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company, YEAR(`date`)
 )
 SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
 FROM Company_Year
 WHERE years IS NOT NULL
 ORDER BY Ranking ASC;   
 
 -- adding another CTE and querying off of that to see only top five
 -- NB: the DENSE_RANK allows for the results to show entries with tied values, we can have six entires/year if 2 companies share a value
 WITH Company_Year (company, years, total_laid_off)  AS
 (
  SELECT company, YEAR(`date`), SUM(total_laid_off)
 FROM layoffs_staging2
 GROUP BY company, YEAR(`date`)
 ), Company_Year_Rank AS
( SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
 FROM Company_Year
 WHERE years IS NOT NULL
  )
  SELECT*
  FROM Company_Year_rank
  WHERE Ranking<=5;
 